// Package tursokv implements a Redis-compatible key-value store backed by Turso/libraryQL.
//
// TursoKV provides Redis-like data structures (strings, lists, sets, hashes, sorted sets)
// with the durability and SQL queryability of libraryQL/SQLite, plus Turso's advanced features:
//   - Concurrent writes via BEGIN CONCURRENT (MVCC)
//   - Native encryption for data-at-rest
//   - Embedded replica sync with Turso Cloud
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
	"sync"
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
	// Timeout for database operations. Default: 5 seconds.
	Timeout time.Duration

	// Logger for database operations. Default: silent logger.
	Logger *slog.Logger

	// ReadOnly opens the database in read-only mode.
	ReadOnly bool

	// --- Turso-specific options ---

	// SyncURL is the Turso Cloud URL for sync (e.g., "libsql://mydb-org.turso.io").
	// If set, enables embedded replica mode with automatic sync.
	SyncURL string

	// AuthToken for Turso Cloud authentication.
	AuthToken string

	// SyncInterval for automatic background sync. Default: 1 minute.
	// Set to 0 to disable automatic sync (manual Push/Pull only).
	SyncInterval time.Duration

	// EncryptionKey for data-at-rest encryption (32 bytes hex-encoded).
	// Uses AEGIS-256 cipher by default.
	EncryptionKey string

	// Cipher algorithm: "aegis256" (default) or "aes256".
	Cipher string

	// UseConcurrent enables BEGIN CONCURRENT for MVCC writes.
	// Allows multiple writers with row-level conflict detection.
	UseConcurrent bool
}

// Default options.
var defaultOptions = Options{
	Timeout:       5 * time.Second,
	Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	SyncInterval:  time.Minute,
	Cipher:        "aegis256",
	UseConcurrent: true,
}

// DB is a Redis-like key-value store backed by Turso/libraryQL.
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

	// Sync control
	syncMu     sync.Mutex
	syncCancel context.CancelFunc
}

// Open opens or creates a TursoKV database at the given path.
//
// Path formats:
//   - "path/to/db.db" - Local file database
//   - ":memory:" - In-memory database
//   - "file:db.db?mode=ro" - SQLite URI format
//
// With encryption:
//
//	db, _ := tursokv.Open("file:data.db?cipher=aegis256&hexkey=...", nil)
//
// With Turso sync:
//
//	db, _ := tursokv.Open("local.db", &tursokv.Options{
//	    SyncURL:   "libsql://mydb-org.turso.io",
//	    AuthToken: "token",
//	})
func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	mergeOptions(opts, &defaultOptions)

	// Build connection options
	sqlOpts := &sqlx.Options{
		Timeout:       opts.Timeout,
		ReadOnly:      opts.ReadOnly,
		SyncURL:       opts.SyncURL,
		AuthToken:     opts.AuthToken,
		EncryptionKey: opts.EncryptionKey,
		SyncInterval:  opts.SyncInterval,
		UseConcurrent: opts.UseConcurrent,
	}

	// Open database
	sdb, err := sqlx.Open(path, sqlOpts)
	if err != nil {
		return nil, err
	}

	db := &DB{
		sdb:    sdb,
		opts:   *opts,
		logger: opts.Logger,
	}

	// Initialize repositories
	db.keyRepo = rkey.New(sdb)
	db.strRepo = rstring.New(sdb, db.keyRepo)
	db.hashRepo = rhash.New(sdb, db.keyRepo)
	db.listRepo = rlist.New(sdb, db.keyRepo)
	db.setRepo = rset.New(sdb, db.keyRepo)
	db.zsetRepo = rzset.New(sdb, db.keyRepo)

	// Start background sync if configured
	if opts.SyncURL != "" && opts.SyncInterval > 0 {
		db.startSync()
	}

	return db, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	db.stopSync()
	return db.sdb.Close()
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

// SQL returns the underlying *sql.DB for direct SQL operations.
// Use this when you need raw SQL queries beyond the Redis-like API.
func (db *DB) SQL() *sql.DB {
	return db.sdb.DB()
}

// View executes a read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.sdb.View(func(stx *sql.Tx) error {
		tx := newTx(stx, db.keyRepo, true)
		return fn(tx)
	})
}

// Update executes a read-write transaction.
// Uses BEGIN CONCURRENT if UseConcurrent is enabled.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.sdb.Update(func(stx *sql.Tx) error {
		tx := newTx(stx, db.keyRepo, false)
		return fn(tx)
	})
}

// --- Sync operations ---

// Sync synchronizes with Turso Cloud.
// In embedded replica mode, this pulls remote changes and commits local ones.
func (db *DB) Sync(ctx context.Context) error {
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	return db.sdb.Sync(ctx)
}

// Checkpoint optimizes the WAL while preserving sync metadata.
func (db *DB) Checkpoint(ctx context.Context) error {
	return db.sdb.Checkpoint(ctx)
}

// startSync starts the background sync goroutine.
func (db *DB) startSync() {
	ctx, cancel := context.WithCancel(context.Background())
	db.syncCancel = cancel

	go func() {
		ticker := time.NewTicker(db.opts.SyncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := db.Sync(ctx); err != nil {
					db.logger.Warn("background sync failed", "error", err)
				}
			}
		}
	}()
}

// stopSync stops the background sync goroutine.
func (db *DB) stopSync() {
	if db.syncCancel != nil {
		db.syncCancel()
	}
}

// mergeOptions merges user options with defaults.
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
	if opts.Cipher == "" {
		opts.Cipher = defaults.Cipher
	}
}
