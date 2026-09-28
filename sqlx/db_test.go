package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "turso.tech/database/tursogo"
)

const fkSchema = `
CREATE TABLE IF NOT EXISTS parent (id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS child (pid INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE)`

// TestEngineStartsConnectionsWithForeignKeysOff is the control for every
// foreign-key test here: a connection the driver opens directly accepts an
// orphan, so a test that sees an orphan refused is measuring the pool.
func TestEngineStartsConnectionsWithForeignKeysOff(t *testing.T) {
	raw, err := sql.Open("turso", filepath.Join(t.TempDir(), "raw.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.Exec(fkSchema); err != nil {
		t.Fatal(err)
	}
	var fk int
	if err := raw.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatal(err)
	}
	if fk != 0 {
		t.Fatalf("driver connection starts with foreign_keys=%d; the control expects 0", fk)
	}
	if _, err := raw.Exec("INSERT INTO child VALUES (42)"); err != nil {
		t.Fatalf("driver connection refused an orphan with foreign keys off: %v", err)
	}
}

// TestForeignKeysOnEveryConnection holds every connection of a concurrent
// pool at once, so each is a distinct physical connection, and requires each
// to report foreign keys on, refuse an orphan and refuse turning them off.
func TestForeignKeysOnEveryConnection(t *testing.T) {
	for _, mode := range []WriteMode{WriteModeConcurrent, WriteModeSerial} {
		t.Run(mode.String(), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "fk.db"), &Options{WriteMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(fkSchema); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			n := db.DB().Stats().MaxOpenConnections
			conns := make([]*sql.Conn, 0, n)
			for range n {
				c, err := db.DB().Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				conns = append(conns, c)
			}
			for i, c := range conns {
				requireForeignKeysEnforced(t, ctx, c, fmt.Sprintf("connection %d", i))
				c.Close()
			}
			if got := db.DB().Stats().OpenConnections; got != n {
				t.Fatalf("OpenConnections = %d, want %d distinct connections checked", got, n)
			}
		})
	}
}

func requireForeignKeysEnforced(t *testing.T, ctx context.Context, c *sql.Conn, label string) {
	t.Helper()
	var fk int
	if err := c.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if fk != 1 {
		t.Fatalf("%s: foreign_keys = %d, want 1", label, fk)
	}
	if _, err := c.ExecContext(ctx, "INSERT INTO child VALUES (4242)"); err == nil {
		t.Fatalf("%s: orphan insert accepted", label)
	}
	if _, err := c.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err == nil {
		t.Fatalf("%s: PRAGMA foreign_keys=OFF accepted", label)
	}
}

// TestConcurrentTransactionsOverlap keeps one Update open while a second
// Update on another row commits. Under BEGIN the second would queue behind
// the first; under BEGIN CONCURRENT it finishes first.
func TestConcurrentTransactionsOverlap(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "overlap.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}

	firstWrote := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.Update(func(tx *sql.Tx) error {
			if _, err := tx.Exec("INSERT INTO kv VALUES ('a', '1')"); err != nil {
				return err
			}
			close(firstWrote)
			<-release
			return nil
		})
	}()
	awaitFirstWrite(t, firstWrote, firstDone)

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- db.Update(func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO kv VALUES ('b', '2')")
			return err
		})
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second Update: %v", err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("second Update waited for the open first Update; transactions are not concurrent")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Update: %v", err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM kv").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
}

// TestUpdateNeverFallsBackToPlainTransaction turns MVCC off under a
// concurrent pool. BEGIN CONCURRENT is then refused by the engine, and Update
// must report that instead of running the transaction some other way.
func TestUpdateNeverFallsBackToPlainTransaction(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "nofallback.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode=wal").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("switch to wal: mode=%q err=%v", mode, err)
	}
	err = db.Update(func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO rkey (key, type) VALUES ('k', 1)")
		return err
	})
	if err == nil {
		t.Fatal("Update succeeded without MVCC; BEGIN CONCURRENT fell back")
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM rkey").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("rkey rows = %d after refused Update, want 0", n)
	}
}

// TestWritesAfterUpdateSurviveReopen mixes Updates with autocommit writes on
// a concurrent pool, then reopens the file. A connection returned to the pool
// inside a transaction would hold the autocommit writes uncommitted, and they
// would be gone after the reopen.
func TestWritesAfterUpdateSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	const total = 21
	for i := range total {
		if i%5 == 0 {
			err = db.Update(func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO rkey (key, type) VALUES (?, 1)", fmt.Sprint("k", i))
				return err
			})
		} else {
			_, err = db.Exec("INSERT INTO rkey (key, type) VALUES (?, 1)", fmt.Sprint("k", i))
		}
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM rkey").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != total {
		t.Fatalf("rows after reopen = %d, want %d", n, total)
	}
}

// TestOpenRefusesOptionsItCannotApply gives Open each ReadOnly or Cipher
// setting it cannot apply: Open returns that setting's error and never creates
// the database's directory. The Cipher it applies opens: a file written under
// Cipher "aes256gcm" and a key reads back with the key and does not open
// without it.
func TestOpenRefusesOptionsItCannotApply(t *testing.T) {
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cases := []struct {
		name string
		opts Options
		want error
	}{
		{"read-only", Options{ReadOnly: true}, ErrReadOnlyUnsupported},
		{"read-only with the local cipher and a key", Options{ReadOnly: true, EncryptionKey: key, Cipher: localCipher}, ErrReadOnlyUnsupported},
		{"another cipher with a key", Options{EncryptionKey: key, Cipher: "aegis256"}, ErrUnsupportedCipher},
		{"another cipher without a key", Options{Cipher: "aegis256"}, ErrUnsupportedCipher},
		{"the local cipher without a key", Options{Cipher: localCipher}, ErrCipherWithoutKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "never-created")
			db, err := Open(filepath.Join(dir, "kv.db"), &tc.opts)
			if err == nil {
				db.Close()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open: err = %v, want %v", err, tc.want)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("the refused Open created %s (stat err %v)", dir, err)
			}
		})
	}

	path := filepath.Join(t.TempDir(), "cipher.db")
	opts := &Options{EncryptionKey: key, Cipher: localCipher}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open with the local cipher and a key: %v", err)
	}
	if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('kept', 1)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if plain, err := Open(path, nil); err == nil {
		plain.Close()
		t.Fatal("the file written under Cipher and a key opened without the key")
	}
	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("reopen with the key: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM rkey WHERE key = 'kept'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows after reopen = %d, %v; want 1", n, err)
	}
}

// TestConflictKeepsNothingAndFreesConnection makes two concurrent Updates
// write the same row. The loser fails and keeps none of its writes, and every
// pool connection is still usable and outside a transaction afterwards.
func TestConflictKeepsNothingAndFreesConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conflict.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT); INSERT INTO kv VALUES ('hot', '0')"); err != nil {
		t.Fatal(err)
	}

	firstWrote := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.Update(func(tx *sql.Tx) error {
			if _, err := tx.Exec("UPDATE kv SET v = 'first' WHERE k = 'hot'"); err != nil {
				return err
			}
			close(firstWrote)
			<-release
			return nil
		})
	}()
	awaitFirstWrite(t, firstWrote, firstDone)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- db.Update(func(tx *sql.Tx) error {
			if _, err := tx.Exec("INSERT INTO kv VALUES ('loser-side', 'x')"); err != nil {
				return err
			}
			_, err := tx.Exec("UPDATE kv SET v = 'second' WHERE k = 'hot'")
			return err
		})
	}()
	var errSecond error
	select {
	case errSecond = <-secondDone:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("second Update waited for the open first Update instead of conflicting")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Update: %v", err)
	}
	if errSecond == nil {
		t.Fatal("second Update wrote the row the open first Update had written, without a conflict")
	}

	// Every connection takes an autocommit write; one left inside the
	// failed transaction would hold its write uncommitted.
	ctx := context.Background()
	n := db.DB().Stats().MaxOpenConnections
	conns := make([]*sql.Conn, 0, n)
	for i := range n {
		c, err := db.DB().Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ExecContext(ctx, "INSERT INTO kv VALUES (?, 'after')", fmt.Sprint("after", i)); err != nil {
			t.Fatalf("connection %d after conflict: %v", i, err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Close()
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v string
	if err := db.QueryRow("SELECT v FROM kv WHERE k = 'hot'").Scan(&v); err != nil || v != "first" {
		t.Fatalf("hot = %q (err %v), want the first Update's value", v, err)
	}
	err = db.QueryRow("SELECT v FROM kv WHERE k = 'loser-side'").Scan(&v)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("the failed Update's other write survived: err=%v", err)
	}
	var after int
	if err := db.QueryRow("SELECT COUNT(*) FROM kv WHERE v = 'after'").Scan(&after); err != nil || after != n {
		t.Fatalf("autocommit writes after the conflict = %d (err %v), want %d", after, err, n)
	}
}

// TestMemoryDatabaseOutlivesConnectionLifetime sets a lifetime far shorter
// than the test. An in-memory database whose connection was recycled would
// come back empty, without even its schema.
func TestMemoryDatabaseOutlivesConnectionLifetime(t *testing.T) {
	db, err := Open(":memory:", &Options{Timeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('k', 1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM rkey").Scan(&n); err != nil {
		t.Fatalf("after the lifetime: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
}

// TestParallelAutocommitWritesOnConcurrentPool drives every connection of a
// concurrent pool with autocommit writes to one row; the engine serialises
// them, so none fails and none is lost.
func TestParallelAutocommitWritesOnConcurrentPool(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "parallel.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE counter (k TEXT PRIMARY KEY, n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	const workers, each = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				if _, err := db.Exec("INSERT INTO counter VALUES ('c', 1) ON CONFLICT(k) DO UPDATE SET n = n + 1"); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("autocommit write: %v", err)
	}
	var n int
	if err := db.QueryRow("SELECT n FROM counter WHERE k = 'c'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != workers*each {
		t.Fatalf("counter = %d, want %d", n, workers*each)
	}
}

// awaitFirstWrite waits until the first Update has written, failing the test
// when that Update ends first, so a refused Update fails instead of leaving
// the test blocked on a signal that never comes.
func awaitFirstWrite(t *testing.T, wrote <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-wrote:
	case err := <-done:
		t.Fatalf("first Update ended before writing: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("first Update neither wrote nor ended")
	}
}
