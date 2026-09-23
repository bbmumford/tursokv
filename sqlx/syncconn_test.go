package sqlx

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	turso "turso.tech/database/tursogo"
)

// countingConnector counts the physical connections the engine opens.
type countingConnector struct {
	base  driver.Connector
	opens atomic.Int64
}

func (c *countingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.opens.Add(1)
	return c.base.Connect(ctx)
}

func (c *countingConnector) Driver() driver.Driver { return c.base.Driver() }

// wrappedLocalPool stands a local tursogo pool in for the synced pool
// sqlConnector wraps in production: both are finished *sql.DB values whose
// connections start with foreign keys off.
func wrappedLocalPool(t *testing.T) (*sql.DB, *sql.DB, *countingConnector) {
	t.Helper()
	base, err := turso.NewConnector(filepath.Join(t.TempDir(), "inner.db"))
	if err != nil {
		t.Fatal(err)
	}
	counter := &countingConnector{base: base}
	inner := sql.OpenDB(counter)
	inner.SetMaxOpenConns(1)
	inner.SetMaxIdleConns(1)
	if _, err := inner.Exec(fkSchema); err != nil {
		t.Fatal(err)
	}
	outer, err := openPool(context.Background(), &sqlConnector{db: inner}, WriteModeSerial, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { outer.Close() })
	return outer, inner, counter
}

func TestSQLConnectorProvesForeignKeys(t *testing.T) {
	outer, _, _ := wrappedLocalPool(t)
	ctx := context.Background()
	c, err := outer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	requireForeignKeysEnforced(t, ctx, c, "wrapped connection")
}

// TestSQLConnectorReprovesAfterDiscard discards the outer connection. The
// wrapped physical connection must be discarded with it, and the replacement,
// which the engine opens with foreign keys off, must be proved before use.
func TestSQLConnectorReprovesAfterDiscard(t *testing.T) {
	outer, inner, counter := wrappedLocalPool(t)
	ctx := context.Background()
	before := counter.opens.Load()

	c, err := outer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("discard: %v", err)
	}

	c, err = outer.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := counter.opens.Load(); got != before+1 {
		t.Fatalf("physical opens = %d after discard, want %d: the wrapped connection was reused", got, before+1)
	}
	if got := inner.Stats().OpenConnections; got != 1 {
		t.Fatalf("wrapped pool holds %d connections, want 1", got)
	}
	requireForeignKeysEnforced(t, ctx, c, "replacement connection")
}

// TestSQLConnectorRoundTrip carries each engine value type, named and
// positional arguments, prepared statements and both transaction outcomes
// through the wrapper.
func TestSQLConnectorRoundTrip(t *testing.T) {
	outer, _, _ := wrappedLocalPool(t)
	if _, err := outer.Exec("CREATE TABLE vals (i INTEGER, f REAL, s TEXT, b BLOB, n TEXT)"); err != nil {
		t.Fatal(err)
	}
	blob := []byte{0, 1, 2, 255}
	if _, err := outer.Exec("INSERT INTO vals VALUES (:i, :f, :s, :b, :n)",
		sql.Named("i", int64(7)), sql.Named("f", 2.5), sql.Named("s", "text"), sql.Named("b", blob), sql.Named("n", nil)); err != nil {
		t.Fatal(err)
	}

	var (
		i int64
		f float64
		s string
		b []byte
		n sql.NullString
	)
	if err := outer.QueryRow("SELECT i, f, s, b, n FROM vals WHERE i = ?", 7).Scan(&i, &f, &s, &b, &n); err != nil {
		t.Fatal(err)
	}
	if i != 7 || f != 2.5 || s != "text" || !bytes.Equal(b, blob) || n.Valid {
		t.Fatalf("round trip = %v %v %q %v %v", i, f, s, b, n)
	}

	stmt, err := outer.Prepare("INSERT INTO vals (i) VALUES (?)")
	if err != nil {
		t.Fatal(err)
	}
	for k := int64(10); k < 13; k++ {
		if _, err := stmt.Exec(k); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()

	tx, err := outer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO vals (i) VALUES (100)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = outer.Begin()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query("SELECT i FROM vals WHERE i >= 10 ORDER BY i")
	if err != nil {
		t.Fatal(err)
	}
	var seen []int64
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, k)
		// A statement while rows are open runs on the same connection.
		if _, err := tx.Exec("INSERT INTO vals (i) VALUES (?)", k+1000); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("rows seen = %v, want 10..12", seen)
	}
	var count int
	if err := outer.QueryRow("SELECT COUNT(*) FROM vals WHERE i = 100").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled-back row count = %d (err %v), want 0", count, err)
	}
	if err := outer.QueryRow("SELECT COUNT(*) FROM vals WHERE i >= 1000").Scan(&count); err != nil || count != 3 {
		t.Fatalf("committed rows = %d (err %v), want 3", count, err)
	}
}
