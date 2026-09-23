// Package sqlx is the database connection layer for TursoKV, built on
// turso.tech/database/tursogo.
//
// tursogo is pure Go at build time (CGO_ENABLED=0 works) and loads the Turso
// engine at run time through purego's Dlopen, so a binary that uses this
// package needs a runtime image with libc and the dynamic loader.
//
// Every pool Open returns proves foreign-key enforcement on each physical
// connection through policypool, because the engine opens every connection
// with foreign keys off and the schema's ON DELETE CASCADE removes a key's
// values only while they are on.
//
// Encryption at rest applies to local files only, through the DSN options
// encryption_cipher=aes256gcm and encryption_hexkey. tursogo's sync API takes
// no encryption settings, so a synced database stores data unencrypted.
package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bbmumford/policypool"
	turso "turso.tech/database/tursogo"
)

// Options for database connection.
type Options struct {
	// Timeout sets each file-backed connection's maximum lifetime to ten times
	// its value. It is not applied to in-memory databases, whose data lives
	// only as long as their one connection.
	Timeout time.Duration

	// ReadOnly is accepted for configuration compatibility.
	// TODO: apply it; the driver has no read-only open mode, so enforcing it
	// needs PRAGMA query_only proved on every connection.
	ReadOnly bool

	// SyncURL is the remote for an embedded replica, for example
	// "libsql://mydb-myorg.turso.io". Empty opens a local-only database.
	SyncURL string

	// AuthToken authenticates to SyncURL.
	AuthToken string

	// EncryptionKey is a hex-encoded AES-256 key for encryption at rest. It
	// applies to local files only; with SyncURL set, Open logs a warning and
	// the replica is stored unencrypted.
	EncryptionKey string

	// WriteMode selects how writes are taken; see WriteMode. The zero value
	// resolves by database kind.
	WriteMode WriteMode

	// SyncInterval is how often a synced database runs Sync in the
	// background. Zero or negative runs no background loop.
	SyncInterval time.Duration

	// SyncErrors receives each failed background Sync. Nil drops them.
	SyncErrors func(error)
}

// DB is a TursoKV database: a policy-proved connection pool plus, for an
// embedded replica, the sync engine it pushes and pulls through.
type DB struct {
	db   *sql.DB
	sync syncer
	mode WriteMode

	// syncMu runs Sync, Push and Pull one at a time.
	syncMu sync.Mutex
	// loopCancel stops the background sync loop and loopDone closes once it
	// has returned; both are nil when no loop runs.
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

// Open opens a local, in-memory or synced database, establishes the write
// mode, proves foreign keys on the first connection and creates the schema.
//
// Examples:
//
//	db, _ := Open("/path/to/local.db", nil)            // concurrent writes
//	db, _ := Open(":memory:", nil)                     // serial, one connection
//	db, _ := Open("/path/to/replica.db", &Options{     // serial, synced
//	    SyncURL:   "libsql://mydb-myorg.turso.io",
//	    AuthToken: "your-token",
//	})
func Open(path string, opts *Options) (*DB, error) {
	return open(path, opts, true)
}

// open is Open with the replica's first pull made optional. With bootstrap
// set, as Open always sets it, an empty replica pulls the remote's state
// before open returns. Tests pass false to build a replica through this same
// wiring against a remote that does not serve the sync protocol.
func open(path string, opts *Options, bootstrap bool) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	memory := isMemoryPath(path)
	synced := opts.SyncURL != ""
	mode, err := resolveWriteMode(opts.WriteMode, memory, synced)
	if err != nil {
		return nil, err
	}
	if !memory {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create directory: %w", err)
		}
	}

	ctx := context.Background()
	var base driver.Connector
	var engine syncer
	if synced {
		syncDb, inner, err := openSyncEngine(ctx, path, opts, bootstrap)
		if err != nil {
			return nil, err
		}
		engine = syncDb
		base = &sqlConnector{db: inner}
	} else {
		local, err := turso.NewConnector(buildLocalDSN(path, opts.EncryptionKey))
		if err != nil {
			return nil, fmt.Errorf("open local db: %w", err)
		}
		base = local
	}
	sdb, err := openOn(ctx, base, engine, mode, wantJournal(mode, memory, synced), memory, opts.Timeout)
	if err != nil {
		return nil, err
	}
	if engine != nil && opts.SyncInterval > 0 {
		sdb.startSync(opts.SyncInterval, opts.SyncErrors)
	}
	return sdb, nil
}

// openOn builds a DB on an engine connector: the policy-proved pool, the
// journal mode the write mode needs, and the schema. engine is nil for a
// local database.
func openOn(ctx context.Context, base driver.Connector, engine syncer, mode WriteMode, journal string, memory bool, timeout time.Duration) (*DB, error) {
	pool, err := openPool(ctx, base, mode, memory, timeout)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := establishJournal(ctx, pool, journal); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.ExecContext(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &DB{db: pool, sync: engine, mode: mode}, nil
}

// openSyncEngine creates the embedded replica and its tursogo pool, held to
// one connection so the policy pool wrapped around it maps one to one. With
// bootstrap set, an empty replica pulls the remote's state before returning.
func openSyncEngine(ctx context.Context, path string, opts *Options, bootstrap bool) (*turso.TursoSyncDb, *sql.DB, error) {
	if opts.EncryptionKey != "" {
		log.Printf("[tursokv] WARNING: EncryptionKey set with SyncURL; tursogo's sync API takes no encryption settings, so the replica is stored unencrypted")
	}
	syncDb, err := turso.NewTursoSyncDb(ctx, turso.TursoSyncDbConfig{
		Path:             path,
		RemoteUrl:        opts.SyncURL,
		AuthToken:        opts.AuthToken,
		BootstrapIfEmpty: &bootstrap,
		// The engine refuses to open a database that holds SQL triggers
		// unless this feature is on, and a remote may hold them.
		ExperimentalFeatures: "triggers",
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create sync db: %w", err)
	}
	inner, err := syncDb.Connect(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("connect sync db: %w", err)
	}
	inner.SetMaxOpenConns(1)
	inner.SetMaxIdleConns(1)
	return syncDb, inner, nil
}

// openPool wraps base so every transaction begins with the write mode's
// statement and every physical connection proves foreign keys, then sizes the
// pool for the mode.
//
// An in-memory pool keeps its one connection for good: the driver opens a new,
// empty database for each connection, so recycling it would discard the data
// and the schema.
func openPool(ctx context.Context, base driver.Connector, mode WriteMode, memory bool, timeout time.Duration) (*sql.DB, error) {
	pool, err := policypool.OpenConnector(ctx, &txConnector{base: base, begin: beginStatement(mode)}, policypool.ForeignKeysRequired)
	if err != nil {
		return nil, err
	}
	conns := 1
	if mode == WriteModeConcurrent {
		conns = concurrentConns
	}
	pool.SetMaxOpenConns(conns)
	pool.SetMaxIdleConns(conns)
	if timeout > 0 && !memory {
		pool.SetConnMaxLifetime(timeout * 10)
	}
	return pool, nil
}

// buildLocalDSN composes the path with optional encryption query params in
// the format tursogo expects. An empty key disables encryption.
//
// The "triggers" experimental feature is always on so the engine can open a
// database that contains SQL triggers.
func buildLocalDSN(path, encryptionHexKey string) string {
	features := []string{"triggers"}
	q := url.Values{}
	if encryptionHexKey != "" {
		features = append(features, "encryption")
		q.Set("encryption_cipher", "aes256gcm")
		q.Set("encryption_hexkey", encryptionHexKey)
	}
	q.Set("experimental", strings.Join(features, ","))
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + q.Encode()
}

// Close stops the background sync loop, waiting for a round in progress,
// then closes the pool. For a synced database this also closes the tursogo
// pool it wraps; tursogo's TursoSyncDb has no Close of its own.
func (db *DB) Close() error {
	db.stopSync()
	return db.db.Close()
}

// WriteMode reports the mode Open resolved and established.
func (db *DB) WriteMode() WriteMode { return db.mode }

// View runs fn in a transaction and commits it when fn returns nil.
// The engine has no read-only transaction, so fn is trusted not to write.
func (db *DB) View(fn func(tx *sql.Tx) error) error {
	return db.inTx(fn, &sql.TxOptions{ReadOnly: true})
}

// Update runs fn in a read-write transaction and commits it when fn returns
// nil; any error rolls it back. The transaction begins with BEGIN CONCURRENT
// in WriteModeConcurrent and with BEGIN in WriteModeSerial. In
// WriteModeConcurrent, Update fails when fn writes a row another open
// transaction has written; nothing fn wrote is kept and fn may be retried.
//
// fn must use tx for every statement. A statement run through the DB while fn
// runs takes another pool connection, and in WriteModeSerial the pool's only
// connection is the one tx holds, so it waits forever.
func (db *DB) Update(fn func(tx *sql.Tx) error) error {
	return db.inTx(fn, nil)
}

func (db *DB) inTx(fn func(tx *sql.Tx) error, opts *sql.TxOptions) error {
	tx, err := db.db.BeginTx(context.Background(), opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Exec executes a query without returning rows.
func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

// Query executes a query that returns rows.
func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.db.Query(query, args...)
}

// QueryRow executes a query that returns at most one row.
func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.db.QueryRow(query, args...)
}

// DB returns the policy-proved pool for statements beyond the TursoKV API.
// Every connection it hands out has foreign keys on and refuses statements
// that would turn them off. Transactions must be opened with BeginTx: a BEGIN
// run as a plain statement is not ended when the connection returns to the
// pool.
func (db *DB) DB() *sql.DB {
	return db.db
}
