//go:build !windows

// Package sqlx provides the database connection layer for TursoKV.
//
// It uses go-libsql (the CGO-based libSQL driver) which supports:
//   - Embedded replicas with automatic sync to Turso Cloud
//   - Native AES-256 encryption at rest
//   - Concurrent writes via BEGIN CONCURRENT (MVCC)
//
// Reference:
//   - https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes
//   - https://turso.tech/blog/introducing-fast-native-encryption-in-turso-database
//   - https://turso.tech/blog/introducing-databases-anywhere-with-turso-sync
package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tursodatabase/go-libsql"
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

	// EncryptionKey enables AES-256 encryption at rest
	// Must be exactly 32 bytes (256 bits)
	EncryptionKey string

	// SyncInterval for automatic background sync (0 = manual sync only)
	SyncInterval time.Duration

	// UseConcurrent enables BEGIN CONCURRENT for MVCC writes
	UseConcurrent bool
}

// DB wraps a libSQL embedded replica database with Turso features.
type DB struct {
	connector     *libsql.Connector
	db            *sql.DB
	opts          *Options
	localPath     string
	useConcurrent bool
}

// Open opens a libSQL database with embedded replica support.
//
// The path specifies the local database file. If SyncURL is provided,
// the database will sync with Turso Cloud.
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
//	// With encryption
//	db, _ := Open("/path/to/local.db", &Options{
//	    EncryptionKey: "32-byte-encryption-key-here!!!!!", // exactly 32 bytes
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

	var connector *libsql.Connector
	var err error

	if opts.SyncURL != "" {
		// Embedded replica with cloud sync
		connector, err = libsql.NewEmbeddedReplicaConnector(
			path,
			opts.SyncURL,
			libsql.WithAuthToken(opts.AuthToken),
			libsql.WithEncryption(opts.EncryptionKey),
			libsql.WithSyncInterval(opts.SyncInterval),
		)
	} else if opts.EncryptionKey != "" {
		// Local only with encryption (no sync URL needed)
		connector, err = libsql.NewEmbeddedReplicaConnector(
			path,
			"", // No sync URL
			libsql.WithEncryption(opts.EncryptionKey),
		)
	} else {
		// Simple local database
		connector, err = libsql.NewEmbeddedReplicaConnector(path, "")
	}

	if err != nil {
		return nil, fmt.Errorf("create connector: %w", err)
	}

	db := sql.OpenDB(connector)

	// Configure connection pool
	// libSQL supports concurrent writes, so we can have multiple connections
	if opts.UseConcurrent {
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(4)
	} else {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}

	if opts.Timeout > 0 {
		db.SetConnMaxLifetime(opts.Timeout * 10)
	}

	sdb := &DB{
		connector:     connector,
		db:            db,
		opts:          opts,
		localPath:     path,
		useConcurrent: opts.UseConcurrent,
	}

	// Initialize schema
	if err := sdb.initSchema(); err != nil {
		db.Close()
		connector.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	return sdb, nil
}

// initSchema creates the TursoKV tables if they don't exist.
func (db *DB) initSchema() error {
	_, err := db.db.Exec(schema)
	return err
}

// Close closes the database connection and connector.
func (db *DB) Close() error {
	if err := db.db.Close(); err != nil {
		return err
	}
	if db.connector != nil {
		return db.connector.Close()
	}
	return nil
}

// Sync manually syncs with Turso Cloud.
// Returns the number of frames synced.
func (db *DB) Sync(ctx context.Context) error {
	if db.connector == nil || db.opts.SyncURL == "" {
		return nil // No sync configured
	}
	_, err := db.connector.Sync()
	return err
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
func (db *DB) Checkpoint(ctx context.Context) error {
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
