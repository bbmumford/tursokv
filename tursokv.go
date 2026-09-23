// Package tursokv implements a Redis-compatible key-value store backed by the
// Turso Database engine through turso.tech/database/tursogo.
//
// TursoKV provides Redis-like data structures (strings, lists, sets, hashes,
// sorted sets) with the durability and SQL queryability of SQLite, plus
// Turso's engine features:
//   - Concurrent writes via BEGIN CONCURRENT on an MVCC local file
//   - Encryption at rest for local files
//   - Embedded replicas that push to and pull from Turso Cloud
//
// Unlike Redis, TursoKV:
//   - Persists data to disk by default
//   - Supports ACID transactions
//   - Allows SQL queries against stored data
//   - Can sync across devices/regions via Turso
//
// Basic usage:
//
//	db, err := tursokv.Open("data.db", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	// String operations
//	db.Str().Set("name", "alice")
//	name, _ := db.Str().Get("name")
//
//	// With transactions
//	db.Update(func(tx *tursokv.Tx) error {
//	    tx.Str().Set("key1", "value1")
//	    tx.Str().Set("key2", "value2")
//	    return nil
//	})
//
// With Turso Cloud sync:
//
//	db, err := tursokv.Open("local.db", &tursokv.Options{
//	    SyncURL:   "libsql://mydb-myorg.turso.io",
//	    AuthToken: "your-token",
//	})
package tursokv

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"time"

	"github.com/bbmumford/tursokv/internal/rhash"
	"github.com/bbmumford/tursokv/internal/rkey"
	"github.com/bbmumford/tursokv/internal/rlist"
	"github.com/bbmumford/tursokv/internal/rset"
	"github.com/bbmumford/tursokv/internal/rstring"
	"github.com/bbmumford/tursokv/internal/rzset"
	"github.com/bbmumford/tursokv/sqlx"
)

// Options configures the database connection.
type Options struct {
	// Timeout sets each file-backed connection's maximum lifetime to ten
	// times its value. Default: 5 seconds.
	Timeout time.Duration

	// Logger receives background sync failures. Default: silent logger.
	Logger *slog.Logger

	// ReadOnly is accepted for configuration compatibility.
	// TODO: apply it; the driver has no read-only open mode, so enforcing it
	// needs PRAGMA query_only proved on every connection.
	ReadOnly bool

	// --- Turso-specific options ---

	// SyncURL is the Turso Cloud URL for sync (e.g., "libsql://mydb-org.turso.io").
	// If set, the database is an embedded replica that syncs with it.
	SyncURL string

	// AuthToken for Turso Cloud authentication.
	AuthToken string

	// SyncInterval is how often the background loop runs Sync when SyncURL
	// is set. Zero means the default of one minute; a negative value turns
	// the loop off, leaving Sync to the caller.
	SyncInterval time.Duration

	// EncryptionKey is a hex-encoded 32-byte key for encryption at rest with
	// the aes256gcm cipher. It applies to local files only: a synced replica
	// is stored unencrypted, and Open logs a warning when both are set.
	EncryptionKey string

	// Cipher is accepted for configuration compatibility.
	// TODO: apply it; local encryption uses aes256gcm whatever this holds.
	Cipher string

	// WriteMode selects how writes are taken. The zero value, WriteModeAuto,
	// resolves to WriteModeConcurrent for a local file and WriteModeSerial
	// for ":memory:" or a synced database; DB.WriteMode reports the result.
	// Open refuses WriteModeConcurrent for ":memory:" or a synced database.
	WriteMode WriteMode
}

// WriteMode selects how a database takes writes; see the constants.
type WriteMode = sqlx.WriteMode

const (
	// WriteModeAuto resolves by database kind at Open.
	WriteModeAuto = sqlx.WriteModeAuto

	// WriteModeConcurrent runs a local file in the MVCC journal mode and
	// begins every Update with BEGIN CONCURRENT on a multi-connection pool.
	// An Update that writes a row another open transaction has written fails
	// and keeps none of its writes; the caller decides whether to retry. A
	// call made outside Update that writes, such as db.Str().Set or
	// db.Key().Expire, runs as one transaction and is run again after a
	// short wait when it meets such a conflict, a bounded number of times;
	// when the conflict outlasts those runs the call returns it and keeps
	// none of its writes.
	WriteModeConcurrent = sqlx.WriteModeConcurrent

	// WriteModeSerial begins every Update with BEGIN on a single connection,
	// so writers queue.
	WriteModeSerial = sqlx.WriteModeSerial
)

// defaultSyncInterval is the background sync period when SyncInterval is zero.
const defaultSyncInterval = time.Minute

// Default options.
var defaultOptions = Options{
	Timeout:      5 * time.Second,
	Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	SyncInterval: defaultSyncInterval,
}

// DB is a Redis-like key-value store backed by the Turso Database engine.
// Thread-safe for concurrent use.
type DB struct {
	sdb    *sqlx.DB
	opts   Options
	logger *slog.Logger

	// Repositories
	keyRepo  *rkey.DB
	strRepo  *rstring.DB
	hashRepo *rhash.DB
	listRepo *rlist.DB
	setRepo  *rset.DB
	zsetRepo *rzset.DB
}

// Open opens or creates a TursoKV database at the given path.
//
// Path formats:
//   - "path/to/db.db" - Local file database
//   - ":memory:" - In-memory database, held on one connection for its life
//
// With encryption at rest (local files only):
//
//	db, _ := tursokv.Open("data.db", &tursokv.Options{
//	    EncryptionKey: "<64 hex characters>",
//	})
//
// With Turso sync:
//
//	db, _ := tursokv.Open("local.db", &tursokv.Options{
//	    SyncURL:   "libsql://mydb-org.turso.io",
//	    AuthToken: "token",
//	})
//
// opts is read, never modified.
func Open(path string, opts *Options) (*DB, error) {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	mergeOptions(&o, &defaultOptions)

	sdb, err := sqlx.Open(path, connectionOptions(o))
	if err != nil {
		return nil, err
	}

	db := &DB{
		sdb:    sdb,
		opts:   o,
		logger: o.Logger,
	}

	// Initialize repositories
	db.keyRepo = rkey.New(sdb)
	db.strRepo = rstring.New(sdb, db.keyRepo)
	db.hashRepo = rhash.New(sdb, db.keyRepo)
	db.listRepo = rlist.New(sdb, db.keyRepo)
	db.setRepo = rset.New(sdb, db.keyRepo)
	db.zsetRepo = rzset.New(sdb, db.keyRepo)

	return db, nil
}

// Close stops the background sync loop, waiting for a round in progress to
// return, then closes the database.
func (db *DB) Close() error {
	return db.sdb.Close()
}

// WriteMode reports the write mode Open resolved and established.
func (db *DB) WriteMode() WriteMode {
	return db.sdb.WriteMode()
}

// Key returns the key repository for key management operations.
func (db *DB) Key() *rkey.DB {
	return db.keyRepo
}

// Str returns the string repository for string operations.
func (db *DB) Str() *rstring.DB {
	return db.strRepo
}

// Hash returns the hash repository for hash operations.
func (db *DB) Hash() *rhash.DB {
	return db.hashRepo
}

// List returns the list repository for list operations.
func (db *DB) List() *rlist.DB {
	return db.listRepo
}

// Set returns the set repository for set operations.
func (db *DB) Set() *rset.DB {
	return db.setRepo
}

// ZSet returns the sorted set repository for sorted set operations.
func (db *DB) ZSet() *rzset.DB {
	return db.zsetRepo
}

// Log returns the database logger.
func (db *DB) Log() *slog.Logger {
	return db.logger
}

// SQL returns the pool for SQL beyond the Redis-like API. Every connection it
// hands out has foreign keys on and refuses statements that turn them off.
// Open transactions with BeginTx, never with a BEGIN statement: a connection
// returned to the pool inside a transaction carries later callers' writes into
// it.
func (db *DB) SQL() *sql.DB {
	return db.sdb.DB()
}

// View executes a transaction for reads. The engine has no read-only
// transaction, so fn is trusted not to write.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.sdb.View(func(stx *sql.Tx) error {
		tx := newTx(stx, db.keyRepo, true)
		return fn(tx)
	})
}

// Update executes a read-write transaction, committing when fn returns nil.
// It begins with BEGIN CONCURRENT in WriteModeConcurrent and with BEGIN in
// WriteModeSerial. In WriteModeConcurrent, Update fails when fn writes a row
// another open transaction has written; nothing fn wrote is kept and fn may be
// retried, even when fn ignored the failed call and went on: once the engine
// has ended the transaction, every later call through tx fails. fn must use tx
// for every statement: in WriteModeSerial the pool's only connection is the
// one tx holds.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.sdb.Update(func(stx *sql.Tx) error {
		tx := newTx(stx, db.keyRepo, false)
		return fn(tx)
	})
}

// --- Sync operations ---

// Sync pushes local writes to the remote, then pulls remote changes. It
// returns nil at once for a database opened without SyncURL, and runs one at a
// time with the background loop. A failed push returns its error without
// pulling.
//
// CONDITIONS, as measured with two replicas against a tursodb 0.7.2 sync
// server (TestSyncConditionsLive); Turso Cloud has not been measured.
//   - A row both replicas wrote holds the later push's value everywhere:
//     when one replica counts a counter to 10 and the other to 3, and the
//     second pushes last, both read 3.
//   - Each replica assigns rkey ids itself, so new keys created on two
//     replicas between syncs can take the same id. The later push then
//     replaces the earlier key's row: that key and its value are gone on
//     every replica, and no Sync reports an error.
//   - A change the server refuses, such as a new key the other replica
//     pushed first (rkey's UNIQUE key), fails that Sync. The next Sync
//     returns nil and the refused change is discarded: this replica reads
//     the other replica's value for the key, and the refused value row lands
//     on whichever key holds its rkey id on the server.
//
// So two replicas that both create keys can lose one another's writes
// without an error; only a database with one writing replica avoids it.
func (db *DB) Sync(ctx context.Context) error {
	return db.sdb.Sync(ctx)
}

// Checkpoint optimizes the WAL while preserving sync metadata.
func (db *DB) Checkpoint(ctx context.Context) error {
	return db.sdb.Checkpoint(ctx)
}

// connectionOptions maps merged Options onto the connection layer: a negative
// SyncInterval becomes zero, which runs no background loop, and background
// sync failures go to the Logger.
func connectionOptions(o Options) *sqlx.Options {
	logger := o.Logger
	return &sqlx.Options{
		Timeout:       o.Timeout,
		ReadOnly:      o.ReadOnly,
		SyncURL:       o.SyncURL,
		AuthToken:     o.AuthToken,
		EncryptionKey: o.EncryptionKey,
		WriteMode:     o.WriteMode,
		SyncInterval:  max(o.SyncInterval, 0),
		SyncErrors: func(err error) {
			logger.Warn("background sync failed", "error", err)
		},
	}
}

// mergeOptions fills opts fields left at their zero value from defaults.
func mergeOptions(opts, defaults *Options) {
	if opts.Timeout == 0 {
		opts.Timeout = defaults.Timeout
	}
	if opts.Logger == nil {
		opts.Logger = defaults.Logger
	}
	if opts.SyncInterval == 0 {
		opts.SyncInterval = defaults.SyncInterval
	}
}
