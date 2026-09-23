package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
)

var errBeginOnTxConn = errors.New("sqlx: transactions on a synced connection begin through txConn")

// sqlConnector presents a finished *sql.DB as a driver.Connector: every
// connection it opens holds one *sql.Conn from that pool for its whole life
// and runs each statement on it.
//
// WHAT IT CONSUMES: the pool tursogo's TursoSyncDb.Connect returns. WHAT
// CONSUMES IT: txConnector, and through it policypool.OpenConnector in
// Open.
//
// WHY IT EXISTS. TursoSyncDb.Connect builds its *sql.DB on an unexported
// connector, so no caller can hand that connector to policypool, and every
// connection it opens starts with foreign keys off. Wrapping the finished pool
// one *sql.Conn at a time gives policypool a connector to prove foreign keys
// on: each physical connection is proved when it opens and again on every
// reuse, and a statement that would turn enforcement off is refused.
//
// CONDITIONS. The wrapped pool is held to one connection by Open, so the
// outer pool and the wrapped one move together. The *sql.Conn is only ever
// used through its own methods, never through Conn.Raw's driver connection
// outside the callback. Ownership of the wrapped pool passes to this
// connector: Close closes it, which the outer pool triggers when it closes.
type sqlConnector struct {
	db *sql.DB
}

func (c *sqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlConn{conn: conn}, nil
}

func (c *sqlConnector) Driver() driver.Driver { return c.db.Driver() }

func (c *sqlConnector) Close() error { return c.db.Close() }

// sqlConn is one borrowed *sql.Conn exposed through the driver interfaces
// txConn and policypool type-assert. Transactions are opened by txConn with a
// BEGIN statement on this same connection, so Begin is never reached.
type sqlConn struct {
	conn *sql.Conn
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *sqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{stmt: stmt}, nil
}

// Close discards the borrowed physical connection instead of returning it:
// the outer pool closes a connection only when it will not reuse it, and a
// connection that failed its proof must not be handed out again by the
// wrapped pool. Conn.Raw closes the *sql.Conn and its physical connection
// when the callback reports driver.ErrBadConn.
func (c *sqlConn) Close() error {
	err := c.conn.Raw(func(any) error { return driver.ErrBadConn })
	if err == nil || errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return nil
	}
	return err
}

func (c *sqlConn) Begin() (driver.Tx, error) { return nil, errBeginOnTxConn }

func (c *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.conn.ExecContext(ctx, query, namedArgs(args)...)
}

func (c *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.conn.QueryContext(ctx, query, namedArgs(args)...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows}, nil
}

func (c *sqlConn) Ping(ctx context.Context) error { return c.conn.PingContext(ctx) }

// sqlStmt serves prepared statements for callers of DB.DB that prepare
// explicitly; database/sql's own Exec and Query paths use sqlConn directly.
type sqlStmt struct {
	stmt *sql.Stmt
}

func (s *sqlStmt) Close() error { return s.stmt.Close() }

// NumInput reports -1 so database/sql leaves argument counting to the
// wrapped statement, which checks against the engine's own count.
func (s *sqlStmt) NumInput() int { return -1 }

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.stmt.Exec(valueArgs(args)...)
}

func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.stmt.Query(valueArgs(args)...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows}, nil
}

func (s *sqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.stmt.ExecContext(ctx, namedArgs(args)...)
}

func (s *sqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.stmt.QueryContext(ctx, namedArgs(args)...)
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows}, nil
}

// sqlRows reads a *sql.Rows as driver rows. Scanning into *any keeps the
// engine's value types (int64, float64, string, []byte, time.Time, nil), and
// database/sql copies []byte values, so a row stays valid after Next moves on.
type sqlRows struct {
	rows *sql.Rows
	cols []string
}

func (r *sqlRows) Columns() []string {
	if r.cols == nil {
		cols, err := r.rows.Columns()
		if err != nil {
			return nil
		}
		r.cols = cols
	}
	return r.cols
}

func (r *sqlRows) Close() error { return r.rows.Close() }

func (r *sqlRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	values := make([]any, len(dest))
	targets := make([]any, len(dest))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := r.rows.Scan(targets...); err != nil {
		return err
	}
	for i := range dest {
		dest[i] = values[i]
	}
	return nil
}

// namedArgs rebuilds database/sql arguments from driver arguments, keeping
// names so the wrapped driver binds :name parameters as the caller wrote them.
func namedArgs(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, arg := range args {
		if arg.Name != "" {
			out[i] = sql.Named(arg.Name, arg.Value)
		} else {
			out[i] = arg.Value
		}
	}
	return out
}

func valueArgs(args []driver.Value) []any {
	out := make([]any, len(args))
	for i, arg := range args {
		out[i] = arg
	}
	return out
}
