package sqlx

import (
	"context"
	"database/sql/driver"
	"errors"
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
