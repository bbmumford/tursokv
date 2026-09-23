package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
)

// scriptConn is a driver connection whose ExecContext answers from a script
// keyed by statement, consuming one answer per call, and records every
// statement it was given.
type scriptConn struct {
	answers map[string][]error
	log     []string
}

func (c *scriptConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.log = append(c.log, query)
	queue := c.answers[query]
	if len(queue) == 0 {
		return driver.RowsAffected(0), nil
	}
	c.answers[query] = queue[1:]
	if queue[0] != nil {
		return nil, queue[0]
	}
	return driver.RowsAffected(0), nil
}

func (c *scriptConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, errors.New("not scripted")
}

func (c *scriptConn) PrepareContext(context.Context, string) (driver.Stmt, error) {
	return nil, errors.New("not scripted")
}
func (c *scriptConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not scripted") }
func (c *scriptConn) Close() error                        { return nil }
func (c *scriptConn) Begin() (driver.Tx, error)           { return nil, errors.New("not scripted") }

type scriptConnector struct{ conn *scriptConn }

func (c scriptConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c scriptConnector) Driver() driver.Driver                        { return nil }

var (
	errConflict = errors.New("write-write conflict")
	errNoTx     = errors.New("no transaction is active")
	errIO       = errors.New("i/o error")
)

func beginOn(t *testing.T, sc *scriptConn) (*txConn, driver.Tx) {
	t.Helper()
	conn, err := (&txConnector{base: scriptConnector{sc}, begin: "BEGIN CONCURRENT"}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tc := conn.(*txConn)
	tx, err := tc.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return tc, tx
}

func TestTxConnEndsTransactions(t *testing.T) {
	cases := []struct {
		name       string
		answers    map[string][]error
		end        func(driver.Tx) error
		wantErr    error
		wantBroken bool
		wantLog    []string
	}{
		{
			name:    "commit succeeds",
			answers: map[string][]error{},
			end:     driver.Tx.Commit,
			wantLog: []string{"BEGIN CONCURRENT", "COMMIT"},
		},
		{
			name:    "rollback succeeds",
			answers: map[string][]error{},
			end:     driver.Tx.Rollback,
			wantLog: []string{"BEGIN CONCURRENT", "ROLLBACK"},
		},
		{
			// The engine refused the commit and kept the transaction open:
			// the follow-up ROLLBACK closes it.
			name:    "commit refused, transaction still open",
			answers: map[string][]error{"COMMIT": {errIO}},
			end:     driver.Tx.Commit,
			wantErr: errIO,
			wantLog: []string{"BEGIN CONCURRENT", "COMMIT", "ROLLBACK"},
		},
		{
			// The engine ended the transaction itself on a conflict: every
			// ROLLBACK fails, and a BEGIN that succeeds proves the
			// connection is in autocommit.
			name:    "commit after engine ended the transaction",
			answers: map[string][]error{"COMMIT": {errConflict}, "ROLLBACK": {errNoTx}},
			end:     driver.Tx.Commit,
			wantErr: errConflict,
			wantLog: []string{"BEGIN CONCURRENT", "COMMIT", "ROLLBACK", "BEGIN CONCURRENT", "ROLLBACK"},
		},
		{
			name:    "rollback after engine ended the transaction",
			answers: map[string][]error{"ROLLBACK": {errNoTx, errNoTx}},
			end:     driver.Tx.Rollback,
			wantLog: []string{"BEGIN CONCURRENT", "ROLLBACK", "ROLLBACK", "BEGIN CONCURRENT", "ROLLBACK"},
		},
		{
			// Neither ROLLBACK nor a fresh BEGIN succeeds, so the connection
			// may still hold the transaction: it must not be reused.
			name:       "rollback and probe both fail",
			answers:    map[string][]error{"ROLLBACK": {errIO, errIO}, "BEGIN CONCURRENT": {nil, errIO}},
			end:        driver.Tx.Rollback,
			wantErr:    errIO,
			wantBroken: true,
			wantLog:    []string{"BEGIN CONCURRENT", "ROLLBACK", "ROLLBACK", "BEGIN CONCURRENT"},
		},
		{
			name:       "probe begins but cannot roll back",
			answers:    map[string][]error{"COMMIT": {errIO}, "ROLLBACK": {errIO, errIO}},
			end:        driver.Tx.Commit,
			wantErr:    errIO,
			wantBroken: true,
			wantLog:    []string{"BEGIN CONCURRENT", "COMMIT", "ROLLBACK", "BEGIN CONCURRENT", "ROLLBACK"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := &scriptConn{answers: tc.answers}
			conn, tx := beginOn(t, sc)
			if err := tc.end(tx); !errors.Is(err, tc.wantErr) {
				t.Fatalf("end err = %v, want %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(sc.log, tc.wantLog) {
				t.Fatalf("statements = %q, want %q", sc.log, tc.wantLog)
			}
			if got := !conn.IsValid(); got != tc.wantBroken {
				t.Fatalf("broken = %v, want %v", got, tc.wantBroken)
			}
			resetErr := conn.ResetSession(context.Background())
			if tc.wantBroken != errors.Is(resetErr, driver.ErrBadConn) {
				t.Fatalf("ResetSession = %v with broken = %v", resetErr, tc.wantBroken)
			}
			if err := tc.end(tx); !errors.Is(err, errTxDone) {
				t.Fatalf("second end err = %v, want %v", err, errTxDone)
			}
		})
	}
}

func TestTxConnectorRefusesConnectionWithoutContextMethods(t *testing.T) {
	_, err := (&txConnector{base: bareConnector{}, begin: "BEGIN"}).Connect(context.Background())
	if !errors.Is(err, errConnShape) {
		t.Fatalf("err = %v, want %v", err, errConnShape)
	}
}

type bareConn struct{}

func (bareConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (bareConn) Close() error                        { return nil }
func (bareConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }

type bareConnector struct{}

func (bareConnector) Connect(context.Context) (driver.Conn, error) { return bareConn{}, nil }
func (bareConnector) Driver() driver.Driver                        { return nil }

// TestTxConnGuardsTransactionAfterFailedStatement scripts a statement failing
// inside a transaction and checks what the guard sends next: a probing BEGIN,
// then either the caller's statements (the transaction is still open) or
// nothing more (the engine had ended it).
func TestTxConnGuardsTransactionAfterFailedStatement(t *testing.T) {
	errInTx := errors.New("cannot start a transaction within a transaction")
	cases := []struct {
		name       string
		answers    map[string][]error
		run        func(*txConn, driver.Tx) (stmtErr, endErr error)
		wantStmt   error
		wantEnd    error
		wantBroken bool
		wantLog    []string
	}{
		{
			name:    "engine ended it: next statement refused, commit refused",
			answers: map[string][]error{"UPDATE hot": {errConflict}},
			run: func(c *txConn, tx driver.Tx) (error, error) {
				_, _ = c.ExecContext(context.Background(), "UPDATE hot", nil)
				_, err := c.ExecContext(context.Background(), "INSERT after", nil)
				return err, tx.Commit()
			},
			wantStmt: errTxEnded,
			wantEnd:  errTxEnded,
			wantLog:  []string{"BEGIN CONCURRENT", "UPDATE hot", "BEGIN CONCURRENT", "ROLLBACK"},
		},
		{
			name:    "engine ended it: commit probes and is refused",
			answers: map[string][]error{"UPDATE hot": {errConflict}},
			run: func(c *txConn, tx driver.Tx) (error, error) {
				_, err := c.ExecContext(context.Background(), "UPDATE hot", nil)
				return err, tx.Commit()
			},
			wantStmt: errConflict,
			wantEnd:  errTxEnded,
			wantLog:  []string{"BEGIN CONCURRENT", "UPDATE hot", "BEGIN CONCURRENT", "ROLLBACK"},
		},
		{
			name:    "engine ended it: rollback has nothing left to end",
			answers: map[string][]error{"UPDATE hot": {errConflict}},
			run: func(c *txConn, tx driver.Tx) (error, error) {
				_, _ = c.ExecContext(context.Background(), "UPDATE hot", nil)
				_, err := c.ExecContext(context.Background(), "INSERT after", nil)
				return err, tx.Rollback()
			},
			wantStmt: errTxEnded,
			wantLog:  []string{"BEGIN CONCURRENT", "UPDATE hot", "BEGIN CONCURRENT", "ROLLBACK"},
		},
		{
			name:    "transaction still open: statements and commit go through",
			answers: map[string][]error{"INSERT dup": {errIO}, "BEGIN CONCURRENT": {nil, errInTx}},
			run: func(c *txConn, tx driver.Tx) (error, error) {
				_, _ = c.ExecContext(context.Background(), "INSERT dup", nil)
				_, err := c.ExecContext(context.Background(), "INSERT next", nil)
				return err, tx.Commit()
			},
			wantLog: []string{"BEGIN CONCURRENT", "INSERT dup", "BEGIN CONCURRENT", "INSERT next", "COMMIT"},
		},
		{
			name:    "probe begins but cannot roll back",
			answers: map[string][]error{"UPDATE hot": {errConflict}, "ROLLBACK": {errIO}},
			run: func(c *txConn, tx driver.Tx) (error, error) {
				_, _ = c.ExecContext(context.Background(), "UPDATE hot", nil)
				_, err := c.ExecContext(context.Background(), "INSERT after", nil)
				return err, tx.Rollback()
			},
			wantStmt:   errTxEnded,
			wantBroken: true,
			wantLog:    []string{"BEGIN CONCURRENT", "UPDATE hot", "BEGIN CONCURRENT", "ROLLBACK"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := &scriptConn{answers: tc.answers}
			conn, tx := beginOn(t, sc)
			stmtErr, endErr := tc.run(conn, tx)
			if !errors.Is(stmtErr, tc.wantStmt) {
				t.Fatalf("statement err = %v, want %v", stmtErr, tc.wantStmt)
			}
			if !errors.Is(endErr, tc.wantEnd) {
				t.Fatalf("end err = %v, want %v", endErr, tc.wantEnd)
			}
			if !reflect.DeepEqual(sc.log, tc.wantLog) {
				t.Fatalf("statements = %q, want %q", sc.log, tc.wantLog)
			}
			if got := !conn.IsValid(); got != tc.wantBroken {
				t.Fatalf("broken = %v, want %v", got, tc.wantBroken)
			}
			if tc.wantBroken {
				return
			}
			// Back in autocommit, statements are not guarded.
			if _, err := conn.ExecContext(context.Background(), "INSERT later", nil); err != nil {
				t.Fatalf("autocommit statement after the transaction: %v", err)
			}
		})
	}
}

// holdHotRow opens an Update that writes the row 'hot' of kv and stays open
// until the returned function is called, which ends it and returns its error.
func holdHotRow(t *testing.T, db *DB) func() error {
	t.Helper()
	wrote := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *sql.Tx) error {
			if _, err := tx.Exec("UPDATE kv SET v = 'first' WHERE k = 'hot'"); err != nil {
				return err
			}
			close(wrote)
			<-release
			return nil
		})
	}()
	awaitFirstWrite(t, wrote, done)
	return func() error {
		close(release)
		return <-done
	}
}

// TestUpdateKeepsNothingAfterEngineEndsTransaction runs Updates whose fn goes
// on after a statement that made the engine end the transaction. Every later
// statement must be refused rather than committed on its own, and Update must
// fail; a failed statement that leaves the transaction open must not stop it.
func TestUpdateKeepsNothingAfterEngineEndsTransaction(t *testing.T) {
	setup := "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT); INSERT INTO kv VALUES ('hot', '0')"
	count := func(t *testing.T, db *DB, k string) int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM kv WHERE k = ?", k).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	openKV := func(t *testing.T, mode WriteMode) (*DB, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "guard.db")
		db, err := Open(path, &Options{WriteMode: mode})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(setup); err != nil {
			t.Fatal(err)
		}
		return db, path
	}

	t.Run("conflict through Exec", func(t *testing.T) {
		db, path := openKV(t, WriteModeConcurrent)
		release := holdHotRow(t, db)
		var conflict, after error
		err := db.Update(func(tx *sql.Tx) error {
			_, conflict = tx.Exec("UPDATE kv SET v = 'second' WHERE k = 'hot'")
			_, after = tx.Exec("INSERT INTO kv VALUES ('after', 'x')")
			return nil
		})
		if firstErr := release(); firstErr != nil {
			t.Fatalf("first Update: %v", firstErr)
		}
		if conflict == nil {
			t.Fatal("no write-write conflict; the test did not reach the engine-ended case")
		}
		if !errors.Is(after, errTxEnded) || !errors.Is(err, errTxEnded) {
			t.Fatalf("statement after the conflict = %v, Update = %v; want %v for both", after, err, errTxEnded)
		}
		if n := count(t, db, "after"); n != 0 {
			t.Fatalf("the statement after the conflict was committed (%d rows)", n)
		}
		db.Close()
		db, err = Open(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if n := count(t, db, "after"); n != 0 {
			t.Fatalf("after reopen, the statement after the conflict was kept (%d rows)", n)
		}
	})

	t.Run("conflict through Query rows, then a prepared statement", func(t *testing.T) {
		db, _ := openKV(t, WriteModeConcurrent)
		defer db.Close()
		release := holdHotRow(t, db)
		var rowsErr, stmtErr error
		err := db.Update(func(tx *sql.Tx) error {
			stmt, err := tx.Prepare("INSERT INTO kv VALUES (?, 'x')")
			if err != nil {
				return err
			}
			defer stmt.Close()
			rows, err := tx.Query("UPDATE kv SET v = 'second' WHERE k = 'hot' RETURNING v")
			if err != nil {
				return err
			}
			for rows.Next() {
				var v string
				_ = rows.Scan(&v)
			}
			rowsErr = rows.Err()
			rows.Close()
			_, stmtErr = stmt.Exec("via-stmt")
			return nil
		})
		if firstErr := release(); firstErr != nil {
			t.Fatalf("first Update: %v", firstErr)
		}
		if rowsErr == nil {
			t.Fatal("no write-write conflict from the rows; the test did not reach the engine-ended case")
		}
		if !errors.Is(stmtErr, errTxEnded) || !errors.Is(err, errTxEnded) {
			t.Fatalf("prepared statement after the conflict = %v, Update = %v; want %v for both", stmtErr, err, errTxEnded)
		}
		if n := count(t, db, "via-stmt"); n != 0 {
			t.Fatalf("the prepared statement after the conflict was committed (%d rows)", n)
		}
	})

	// The guard sits under every kind of pool; a replica's statements reach
	// the engine through sqlConn.
	kinds := []struct {
		name string
		open func(t *testing.T) *DB
	}{
		{"concurrent", func(t *testing.T) *DB {
			db, _ := openKV(t, WriteModeConcurrent)
			t.Cleanup(func() { db.Close() })
			return db
		}},
		{"serial", func(t *testing.T) *DB {
			db, _ := openKV(t, WriteModeSerial)
			t.Cleanup(func() { db.Close() })
			return db
		}},
		{"synced replica", func(t *testing.T) *DB {
			remote := httptest.NewServer(&pathRecorder{})
			t.Cleanup(remote.Close)
			db := openOfflineReplica(t, &Options{SyncURL: remote.URL})
			if _, err := db.Exec(setup); err != nil {
				t.Fatal(err)
			}
			return db
		}},
	}
	for _, kind := range kinds {
		t.Run("INSERT OR ROLLBACK, "+kind.name, func(t *testing.T) {
			db := kind.open(t)
			var rolled, after error
			err := db.Update(func(tx *sql.Tx) error {
				if _, err := tx.Exec("INSERT INTO kv VALUES ('before', 'x')"); err != nil {
					return err
				}
				_, rolled = tx.Exec("INSERT OR ROLLBACK INTO kv VALUES ('hot', 'dup')")
				_, after = tx.Exec("INSERT INTO kv VALUES ('after', 'x')")
				return nil
			})
			if rolled == nil {
				t.Fatal("INSERT OR ROLLBACK of a duplicate key succeeded")
			}
			if !errors.Is(after, errTxEnded) || !errors.Is(err, errTxEnded) {
				t.Fatalf("statement after the rollback = %v, Update = %v; want %v for both", after, err, errTxEnded)
			}
			if b, a := count(t, db, "before"), count(t, db, "after"); b != 0 || a != 0 {
				t.Fatalf("rows kept: before=%d after=%d, want none", b, a)
			}
		})

		t.Run("failed statement leaves transaction open, "+kind.name, func(t *testing.T) {
			db := kind.open(t)
			var dup, after error
			err := db.Update(func(tx *sql.Tx) error {
				_, dup = tx.Exec("INSERT INTO kv VALUES ('hot', 'dup')")
				_, after = tx.Exec("INSERT INTO kv VALUES ('after', 'x')")
				return nil
			})
			if dup == nil {
				t.Fatal("duplicate key accepted")
			}
			if after != nil || err != nil {
				t.Fatalf("statement after a constraint failure = %v, Update = %v; want both nil", after, err)
			}
			if n := count(t, db, "after"); n != 1 {
				t.Fatalf("rows for the later statement = %d, want 1", n)
			}
		})
	}
}
