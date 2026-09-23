package sqlx

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
)

var (
	errConnShape = errors.New("sqlx: driver connection lacks context exec, query or prepare")
	errTxDone    = errors.New("sqlx: transaction already finished")
)

// txConnector opens connections whose transactions begin with begin and that
// never go back to the pool while a transaction is still open on them.
//
// WHAT IT CONSUMES: the engine connector (a local tursogo connector, or
// sqlConnector over a synced pool). WHAT CONSUMES IT: policypool.OpenConnector
// in Open, which proves foreign keys on every connection this returns.
//
// WHY THE BEGIN STATEMENT LIVES HERE. database/sql starts every transaction
// through driver BeginTx, and the tursogo connection's BeginTx always issues
// BEGIN. Issuing BEGIN CONCURRENT from here keeps each transaction a *sql.Tx,
// so database/sql pins the connection for it and releases it only after COMMIT
// or ROLLBACK; a BEGIN issued as a plain statement on a borrowed connection
// escapes that bookkeeping, and a connection returned to the pool inside a
// transaction carries every later caller's writes into a transaction nobody
// commits.
type txConnector struct {
	base  driver.Connector
	begin string
}

func (c *txConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	exec, okExec := conn.(driver.ExecerContext)
	query, okQuery := conn.(driver.QueryerContext)
	prep, okPrep := conn.(driver.ConnPrepareContext)
	if !okExec || !okQuery || !okPrep {
		_ = conn.Close()
		return nil, errConnShape
	}
	return &txConn{Conn: conn, exec: exec, query: query, prep: prep, begin: c.begin}, nil
}

func (c *txConnector) Driver() driver.Driver { return c.base.Driver() }

// Close releases the base connector when it owns resources, which
// sqlConnector does: it owns the synced pool it draws from.
func (c *txConnector) Close() error {
	if closer, ok := c.base.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// txConn forwards statements to the engine connection and owns transaction
// boundaries. policypool's connection wrapper type-asserts the optional
// driver interfaces on this value, so each one the engine offers is forwarded
// explicitly rather than hidden behind the embedded driver.Conn.
type txConn struct {
	driver.Conn
	exec  driver.ExecerContext
	query driver.QueryerContext
	prep  driver.ConnPrepareContext
	begin string

	// broken is set when the connection could not be shown to be outside a
	// transaction. database/sql reads it through IsValid and ResetSession and
	// discards the connection instead of reusing it.
	broken bool
}

func (c *txConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prep.PrepareContext(ctx, query)
}

func (c *txConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.exec.ExecContext(ctx, query, args)
}

func (c *txConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.query.QueryContext(ctx, query, args)
}

func (c *txConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *txConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx opens a transaction with the write mode's begin statement. Options
// are not applied, matching the tursogo connection's own BeginTx: the engine
// has one isolation level and no read-only transaction.
func (c *txConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if _, err := c.exec.ExecContext(ctx, c.begin, nil); err != nil {
		return nil, err
	}
	return &txn{conn: c}, nil
}

func (c *txConn) IsValid() bool { return !c.broken }

func (c *txConn) ResetSession(context.Context) error {
	if c.broken {
		return driver.ErrBadConn
	}
	return nil
}

// settle brings the connection back to autocommit after COMMIT or ROLLBACK
// failed, and reports whether it got there.
//
// WHY A PROBING BEGIN. The engine ends a transaction by itself when a
// statement hits a write-write conflict, after which ROLLBACK fails with
// nothing to roll back; a COMMIT refused for contention can instead leave the
// transaction open. ROLLBACK closes the second case. When ROLLBACK fails, a
// BEGIN that succeeds proves no transaction remains, and its own ROLLBACK
// leaves the connection as it found it. Any other outcome marks the connection
// broken so database/sql discards it.
func (c *txConn) settle() bool {
	ctx := context.Background()
	if _, err := c.exec.ExecContext(ctx, "ROLLBACK", nil); err == nil {
		return true
	}
	if _, err := c.exec.ExecContext(ctx, c.begin, nil); err != nil {
		c.broken = true
		return false
	}
	if _, err := c.exec.ExecContext(ctx, "ROLLBACK", nil); err != nil {
		c.broken = true
		return false
	}
	return true
}

// txn ends a transaction txConn began. database/sql calls exactly one of
// Commit or Rollback, from Tx.Commit, Tx.Rollback or its context watcher, so
// both run on a background context: a cancelled caller context must not
// stop the statement that ends the transaction.
type txn struct {
	conn *txConn
	done bool
}

// Commit returns the engine's COMMIT error unchanged; after a failed COMMIT
// the connection is settled so none of the transaction survives on it.
func (t *txn) Commit() error {
	if t.done {
		return errTxDone
	}
	t.done = true
	_, err := t.conn.exec.ExecContext(context.Background(), "COMMIT", nil)
	if err != nil {
		t.conn.settle()
	}
	return err
}

// Rollback succeeds when the connection ends outside a transaction, including
// when the engine had already ended it: the engine ends a transaction only by
// COMMIT, which Commit alone issues, or by discarding its writes.
func (t *txn) Rollback() error {
	if t.done {
		return errTxDone
	}
	t.done = true
	_, err := t.conn.exec.ExecContext(context.Background(), "ROLLBACK", nil)
	if err == nil || t.conn.settle() {
		return nil
	}
	return err
}
