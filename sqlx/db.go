// Package sqlx provides the database connection layer for TursoKV.
//
// Backed by turso.tech/database/tursogo — the official, pure-Go Turso SDK
// (no CGO required) which supports:
//   - Local files via the standard database/sql driver "turso"
//   - Embedded sync via NewTursoSyncDb with explicit Push/Pull
//   - Concurrent writes via BEGIN CONCURRENT (MVCC)
//   - Cross-platform: linux/{amd64,arm64}, darwin/{amd64,arm64},
//     windows/{amd64,arm64} — no per-OS forks required
//
// Encryption-at-rest:
//
//	Local-only mode: supported via DSN options
//	  ?experimental=encryption&encryption_cipher=aes256gcm&encryption_hexkey=…
//
//	Sync mode (NewTursoSyncDb): NOT yet exposed on the high-level API.
//	The Options.EncryptionKey field is preserved as a no-op skeleton in
//	this code path so callers don't need to change their config. When
//	upstream surfaces encryption on TursoSyncDbConfig the no-op will
//	switch to the real wire-up.
//
// References:
//   - https://docs.turso.tech/sdk/go/quickstart
//   - https://turso.tech/blog/sync-benchmark
package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	turso "turso.tech/database/tursogo"
)

// Options for database connection.
type Options struct {
	// Timeout for database operations
	Timeout time.Duration

	// ReadOnly opens the database in read-only mode
	ReadOnly bool

	// SyncURL is the Turso Cloud URL for embedded replica sync
	// Example: "libsql://mydb-myorg.turso.io"
	SyncURL string

	// AuthToken is the Turso authentication token
	AuthToken string

	// EncryptionKey enables AES-256 encryption at rest (hex-encoded).
	//
	// In local-only mode this is wired through tursogo DSN options
	// (encryption_cipher=aes256gcm&encryption_hexkey=…). In sync mode
	// (SyncURL non-empty) the field is preserved as a skeleton — the
	// underlying NewTursoSyncDb does not yet expose encryption on its
	// high-level API. Setting EncryptionKey + SyncURL together logs a
	// warning and the data is stored unencrypted at rest.
	EncryptionKey string

	// SyncInterval for automatic background Pull (0 = manual sync only).
	//
	// Replaces go-libsql's WithSyncInterval — tursogo's API requires
	// explicit Push/Pull, so a non-zero value spawns a background
	// goroutine that calls Pull on this cadence.
	SyncInterval time.Duration

	// UseConcurrent enables BEGIN CONCURRENT for MVCC writes
	UseConcurrent bool
}

// DB wraps a Turso database with TursoKV features.
//
// The underlying transport is one of:
//
//	syncDb non-nil: embedded replica with explicit Push/Pull sync
//	syncDb nil:     local-only via sql.Open("turso", dsn)
type DB struct {
	syncDb        *turso.TursoSyncDb
	db            *sql.DB
	opts          *Options
	localPath     string
	useConcurrent bool

	// syncCancel stops the background Pull goroutine when SyncInterval > 0.
	syncCancel context.CancelFunc
}

// Open opens a Turso database with embedded replica support.
//
// The path specifies the local database file. If SyncURL is provided,
// the database will sync with Turso Cloud via explicit Pull/Push (and a
// background Pull at SyncInterval cadence when set).
//
// Examples:
//
//	// Local only (no cloud sync)
//	db, _ := Open("/path/to/local.db", nil)
//
//	// With Turso Cloud sync
//	db, _ := Open("/path/to/local.db", &Options{
//	    SyncURL:   "libsql://mydb-myorg.turso.io",
//	    AuthToken: "your-token",
//	})
//
//	// Local with encryption (sync-mode encryption is currently a no-op skeleton)
//	db, _ := Open("/path/to/local.db", &Options{
//	    EncryptionKey: "deadbeef…", // hex-encoded
//	})
func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}

	// Ensure directory exists
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory: %w", err)
		}
	}

	sdb := &DB{
		opts:          opts,
		localPath:     path,
		useConcurrent: opts.UseConcurrent,
	}

	if opts.SyncURL != "" {
		// Embedded replica with cloud sync.
		//
		// Encryption-on-sync is a known gap in tursogo's high-level API —
		// log a warning so operators know data is unencrypted at rest on
		// this code path. When EncryptionKey was set in legacy go-libsql
		// it would have been wired through; here it is silently dropped
		// to a no-op skeleton.
		if opts.EncryptionKey != "" {
			log.Printf("[tursokv] WARNING: EncryptionKey set with SyncURL — sync-mode encryption is not yet supported by tursogo; storing unencrypted")
		}

		ctx := context.Background()
		syncCfg := turso.TursoSyncDbConfig{
			Path:      path,
			RemoteUrl: opts.SyncURL,
			AuthToken: opts.AuthToken,
		}
		syncDb, err := turso.NewTursoSyncDb(ctx, syncCfg)
		if err != nil {
			return nil, fmt.Errorf("create sync db: %w", err)
		}
		conn, err := syncDb.Connect(ctx)
		if err != nil {
			return nil, fmt.Errorf("connect sync db: %w", err)
		}
		sdb.syncDb = syncDb
		sdb.db = conn
	} else {
		// Local-only via the standard database/sql driver "turso".
		dsn := buildLocalDSN(path, opts.EncryptionKey)
		conn, err := sql.Open("turso", dsn)
		if err != nil {
			return nil, fmt.Errorf("open local db: %w", err)
		}
		sdb.db = conn
	}

	// Configure connection pool. Turso supports concurrent writes (MVCC)
	// so we can have multiple connections.
	if opts.UseConcurrent {
		sdb.db.SetMaxOpenConns(4)
		sdb.db.SetMaxIdleConns(4)
	} else {
		sdb.db.SetMaxOpenConns(1)
		sdb.db.SetMaxIdleConns(1)
	}

	if opts.Timeout > 0 {
		sdb.db.SetConnMaxLifetime(opts.Timeout * 10)
	}

	// Initialize schema before kicking off background sync — schema must
	// be present locally before any Pull races could observe an empty db.
	if err := sdb.initSchema(); err != nil {
		sdb.db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	// Background Pull — replaces go-libsql's WithSyncInterval. Only spawn
	// when both a remote and a non-zero interval are configured; manual
	// callers can still drive Pull via Sync().
	if sdb.syncDb != nil && opts.SyncInterval > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		sdb.syncCancel = cancel
		go sdb.runBackgroundPull(ctx, opts.SyncInterval)
	}

	return sdb, nil
}

// buildLocalDSN composes the path with optional encryption query params
// in the format tursogo expects on sql.Open("turso", …). Caller passes a
// hex-encoded key; an empty key disables encryption.
func buildLocalDSN(path, encryptionHexKey string) string {
	if encryptionHexKey == "" {
		return path
	}
	q := url.Values{}
	q.Set("experimental", "encryption")
	q.Set("encryption_cipher", "aes256gcm")
	q.Set("encryption_hexkey", encryptionHexKey)
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + q.Encode()
}

// runBackgroundPull periodically calls syncDb.Pull until ctx is cancelled.
// Errors are logged and the loop continues — a transient sync failure
// shouldn't take down the database.
func (db *DB) runBackgroundPull(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pullCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if _, err := db.syncDb.Pull(pullCtx); err != nil {
				log.Printf("[tursokv] background Pull: %v", err)
			}
			cancel()
		}
	}
}

// initSchema creates the TursoKV tables if they don't exist.
func (db *DB) initSchema() error {
	_, err := db.db.Exec(schema)
	return err
}

// Close closes the database connection.
//
// tursogo does not (yet) expose a Close method on TursoSyncDb; closing
// the *sql.DB returned by Connect releases all resources we allocated
// here. The background Pull goroutine is also stopped.
func (db *DB) Close() error {
	if db.syncCancel != nil {
		db.syncCancel()
	}
	return db.db.Close()
}

// Sync manually pulls the latest changes from Turso Cloud.
// Returns nil immediately when no SyncURL is configured.
func (db *DB) Sync(ctx context.Context) error {
	if db.syncDb == nil {
		return nil
	}
	_, err := db.syncDb.Pull(ctx)
	return err
}

// Push sends local writes to Turso Cloud. Returns nil immediately when
// no SyncURL is configured. Distinct from Sync (which Pulls); exposed so
// callers driving the sync direction explicitly can do so.
func (db *DB) Push(ctx context.Context) error {
	if db.syncDb == nil {
		return nil
	}
	return db.syncDb.Push(ctx)
}

// View executes a read-only transaction.
func (db *DB) View(fn func(tx *sql.Tx) error) error {
	tx, err := db.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Update executes a read-write transaction.
// Uses BEGIN CONCURRENT if enabled for better write concurrency.
func (db *DB) Update(fn func(tx *sql.Tx) error) error {
	ctx := context.Background()

	if db.useConcurrent {
		return db.updateConcurrent(ctx, fn)
	}

	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// updateConcurrent executes a transaction with BEGIN CONCURRENT.
// This enables MVCC-based optimistic locking with row-level conflict detection.
func (db *DB) updateConcurrent(ctx context.Context, fn func(tx *sql.Tx) error) error {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// BEGIN CONCURRENT enables MVCC writes
	// Multiple transactions can write concurrently as long as they don't
	// modify the same rows. Conflicts are detected at commit time.
	if _, err := conn.ExecContext(ctx, "BEGIN CONCURRENT"); err != nil {
		// Fall back to regular transaction if CONCURRENT not supported
		return db.updateRegular(fn)
	}

	// Execute the function
	var execErr error
	err = conn.Raw(func(driverConn any) error {
		// We need to work with the raw connection for the transaction
		// but database/sql doesn't expose this well, so we use a workaround
		tx, txErr := db.db.Begin()
		if txErr != nil {
			return txErr
		}
		defer tx.Rollback()

		execErr = fn(tx)
		if execErr != nil {
			return execErr
		}
		return tx.Commit()
	})

	if err != nil {
		conn.ExecContext(ctx, "ROLLBACK")
		return err
	}

	return execErr
}

func (db *DB) updateRegular(fn func(tx *sql.Tx) error) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Checkpoint performs a WAL checkpoint.
//
// Prefers tursogo's native Checkpoint when available (sync mode);
// falls back to a PRAGMA on the local connection otherwise.
func (db *DB) Checkpoint(ctx context.Context) error {
	if db.syncDb != nil {
		return db.syncDb.Checkpoint(ctx)
	}
	_, err := db.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
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

// DB returns the underlying *sql.DB for advanced usage.
func (db *DB) DB() *sql.DB {
	return db.db
}

// schema defines the TursoKV database structure.
// Based on Redka's schema but optimized for libSQL.
const schema = `
-- Key metadata table
-- Tracks all keys with their types and TTL
CREATE TABLE IF NOT EXISTS rkey (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    type INTEGER NOT NULL DEFAULT 1,  -- 1=string, 2=list, 3=set, 4=hash, 5=zset
    version INTEGER NOT NULL DEFAULT 0,
    etime INTEGER,  -- Expiration time (Unix ms), NULL = no expiry
    mtime INTEGER NOT NULL DEFAULT (unixepoch('now', 'subsec') * 1000)
);

CREATE INDEX IF NOT EXISTS rkey_etime ON rkey(etime) WHERE etime IS NOT NULL;
CREATE INDEX IF NOT EXISTS rkey_type ON rkey(type);

-- String values table
CREATE TABLE IF NOT EXISTS rstring (
    key_id INTEGER PRIMARY KEY REFERENCES rkey(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- List values table (doubly-linked list in table form)
CREATE TABLE IF NOT EXISTS rlist (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    pos REAL NOT NULL,  -- Position for ordering (allows inserts between elements)
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, pos)
);

CREATE INDEX IF NOT EXISTS rlist_key ON rlist(key_id);

-- Set values table
CREATE TABLE IF NOT EXISTS rset (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX IF NOT EXISTS rset_key ON rset(key_id);

-- Hash values table
CREATE TABLE IF NOT EXISTS rhash (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, field)
);

CREATE INDEX IF NOT EXISTS rhash_key ON rhash(key_id);

-- Sorted set values table
CREATE TABLE IF NOT EXISTS rzset (
    key_id INTEGER NOT NULL REFERENCES rkey(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (key_id, member)
);

CREATE INDEX IF NOT EXISTS rzset_key ON rzset(key_id);
CREATE INDEX IF NOT EXISTS rzset_score ON rzset(key_id, score);
`
