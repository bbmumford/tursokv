package sqlx

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
)

var (
	errConnShape = errors.New("sqlx: driver connection lacks context exec, query or prepare")
	errStmtShape = errors.New("sqlx: driver statement lacks context exec or query")
	errTxDone    = errors.New("sqlx: transaction already finished")

	// errTxEnded is what every statement, and Commit, returns once the engine
	// has ended the transaction on a failed statement.
	errTxEnded = errors.New("sqlx: the engine ended this transaction when a statement failed; none of its writes are kept and it takes no further statements")
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

	// tx is the transaction open on this connection, nil in autocommit.
	// database/sql calls a connection's methods, its statements' and its rows'
	// one at a time, so tx and the flags it carries need no lock.
	tx *txn

	// broken is set when the connection could not be shown to be outside a
	// transaction. database/sql reads it through IsValid and ResetSession and
	// discards the connection instead of reusing it.
	broken bool
}

// PrepareContext returns statements that check the transaction open on this
// connection when they run, not when they are prepared: database/sql reuses
// a statement prepared on a connection for a transaction later begun on it.
func (c *txConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := c.guard(); err != nil {
		return nil, err
	}
	stmt, err := c.prep.PrepareContext(ctx, query)
	c.noteFailure(err)
	if err != nil {
		return nil, err
	}
	stmtExec, okExec := stmt.(driver.StmtExecContext)
	stmtQuery, okQuery := stmt.(driver.StmtQueryContext)
	if !okExec || !okQuery {
		_ = stmt.Close()
		return nil, errStmtShape
	}
	return &txStmt{Stmt: stmt, exec: stmtExec, query: stmtQuery, conn: c}, nil
}

func (c *txConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.guard(); err != nil {
		return nil, err
	}
	res, err := c.exec.ExecContext(ctx, query, args)
	c.noteFailure(err)
	return res, err
}

func (c *txConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.guard(); err != nil {
		return nil, err
	}
	rows, err := c.query.QueryContext(ctx, query, args)
	c.noteFailure(err)
	return c.watchRows(rows), err
}

// guard runs before every statement on the connection and refuses it once
// the engine has ended the open transaction.
//
// WHY. The engine ends a transaction by itself when a statement hits a
// write-write conflict, or runs INSERT OR ROLLBACK against a constraint, and
// the connection is then in autocommit. A caller that goes on after the
// failed statement would have every later statement committed at once, one
// by one, while Commit reports a failure; Update's promise that a failed
// transaction keeps nothing would not hold. So after any failed statement
// inside a transaction the next statement, or Commit, first finds out
// whether the transaction is still open.
func (c *txConn) guard() error {
	t := c.tx
	if t == nil {
		return nil
	}
	if t.suspect {
		c.probe(t)
	}
	if t.ended {
		return errTxEnded
	}
	return nil
}

// noteFailure marks the open transaction for a probe when a statement in it
// failed.
func (c *txConn) noteFailure(err error) {
	if err != nil && c.tx != nil {
		c.tx.suspect = true
	}
}

// probe decides whether t is still open with the begin statement, which the
// engine refuses inside an open transaction ("cannot start a transaction
// within a transaction"). A refusal leaves t open and untouched. A BEGIN that
// succeeds proves the engine had ended t: it is rolled back at once, t is
// marked ended, and a connection whose ROLLBACK fails is discarded.
func (c *txConn) probe(t *txn) {
	t.suspect = false
	ctx := context.Background()
	if _, err := c.exec.ExecContext(ctx, c.begin, nil); err != nil {
		return
	}
	t.ended = true
	if _, err := c.exec.ExecContext(ctx, "ROLLBACK", nil); err != nil {
		c.broken = true
	}
}

// watchRows reports a row that fails to step inside a transaction to the
// guard: the engine reports a write-write conflict in a statement run through
// Query, such as UPDATE ... RETURNING, from Next. Rows outside a transaction
// are returned as the engine made them.
func (c *txConn) watchRows(rows driver.Rows) driver.Rows {
	if rows == nil || c.tx == nil {
		return rows
	}
	return &txRows{Rows: rows, tx: c.tx}
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
	c.tx = &txn{conn: c}
	return c.tx, nil
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

	// suspect is set when a statement in the transaction failed and nothing
	// has since shown the transaction to be open; guard and Commit probe it.
	suspect bool
	// ended is set once a probe showed the engine had ended the transaction.
	ended bool
}

// finish marks the transaction done and returns the connection to autocommit
// bookkeeping.
func (t *txn) finish() {
	t.done = true
	if t.conn.tx == t {
		t.conn.tx = nil
	}
}

// Commit returns the engine's COMMIT error unchanged; after a failed COMMIT
// the connection is settled so none of the transaction survives on it. A
// transaction the engine ended returns errTxEnded without issuing COMMIT:
// none of it is left to commit, and the guard kept every later statement from
// running in autocommit.
func (t *txn) Commit() error {
	if t.done {
		return errTxDone
	}
	if t.suspect {
		t.conn.probe(t)
	}
	t.finish()
	if t.ended {
		return errTxEnded
	}
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
	t.finish()
	if t.ended {
		return nil
	}
	_, err := t.conn.exec.ExecContext(context.Background(), "ROLLBACK", nil)
	if err == nil || t.conn.settle() {
		return nil
	}
	return err
}

// txStmt runs an engine statement under the guard of the connection that
// prepared it, whichever transaction is open on that connection when it runs.
// database/sql calls ExecContext and QueryContext; Exec and Query serve a
// caller holding the driver statement directly.
type txStmt struct {
	driver.Stmt
	exec  driver.StmtExecContext
	query driver.StmtQueryContext
	conn  *txConn
}

func (s *txStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.conn.guard(); err != nil {
		return nil, err
	}
	res, err := s.exec.ExecContext(ctx, args)
	s.conn.noteFailure(err)
	return res, err
}

func (s *txStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.conn.guard(); err != nil {
		return nil, err
	}
	rows, err := s.query.QueryContext(ctx, args)
	s.conn.noteFailure(err)
	return s.conn.watchRows(rows), err
}

func (s *txStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), ordinalArgs(args))
}

func (s *txStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), ordinalArgs(args))
}

// ordinalArgs numbers positional values from 1, as database/sql does.
func ordinalArgs(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}

// txRows marks the transaction that opened it for a probe when a row fails to
// step. io.EOF is the end of the rows, not a failure.
type txRows struct {
	driver.Rows
	tx *txn
}

func (r *txRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err != nil && !errors.Is(err, io.EOF) && !r.tx.done {
		r.tx.suspect = true
	}
	return err
}
