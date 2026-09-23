package sqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// WriteMode selects how a database takes writes: the statement every
// transaction begins with, the journal mode the file runs in, and how many
// connections the pool holds. Open resolves it once; DB.WriteMode reports the
// result.
type WriteMode int

const (
	// WriteModeAuto resolves to WriteModeConcurrent for a local file and to
	// WriteModeSerial for an in-memory or synced database.
	WriteModeAuto WriteMode = iota

	// WriteModeConcurrent runs the file in the MVCC journal mode, begins
	// every transaction with BEGIN CONCURRENT and holds concurrentConns
	// connections. Transactions that write different rows proceed in
	// parallel. A statement that writes a row another open transaction has
	// already written fails with a write-write conflict instead of waiting,
	// and the engine ends that statement's transaction, keeping none of its
	// writes; the caller decides whether to retry.
	WriteModeConcurrent

	// WriteModeSerial begins every transaction with BEGIN and holds one
	// connection, so writers queue behind each other. A local file runs in
	// the WAL journal mode; a synced database keeps the journal mode its
	// sync engine chose.
	WriteModeSerial
)

// concurrentConns is the pool size in WriteModeConcurrent. Serial pools hold
// one connection.
const concurrentConns = 4

var (
	errConcurrentSynced = errors.New("sqlx: WriteModeConcurrent needs a local file: the sync engine refuses the MVCC journal mode while it captures changes")
	errConcurrentMemory = errors.New("sqlx: WriteModeConcurrent needs a file: each connection to :memory: opens its own database")
)

// String names the mode for logs and errors.
func (m WriteMode) String() string {
	switch m {
	case WriteModeAuto:
		return "auto"
	case WriteModeConcurrent:
		return "concurrent"
	case WriteModeSerial:
		return "serial"
	default:
		return fmt.Sprintf("WriteMode(%d)", int(m))
	}
}

// resolveWriteMode turns the requested mode into the one Open establishes,
// refusing a request the database cannot honour rather than downgrading it.
//
// A synced database cannot run MVCC: the sync engine records local writes by
// change-data capture and the engine refuses to change the journal mode while
// capture is active. An in-memory database cannot hold more than one
// connection, because the driver opens a separate database per connection.
func resolveWriteMode(requested WriteMode, memory, synced bool) (WriteMode, error) {
	switch requested {
	case WriteModeAuto:
		if memory || synced {
			return WriteModeSerial, nil
		}
		return WriteModeConcurrent, nil
	case WriteModeConcurrent:
		if synced {
			return 0, errConcurrentSynced
		}
		if memory {
			return 0, errConcurrentMemory
		}
		return WriteModeConcurrent, nil
	case WriteModeSerial:
		return WriteModeSerial, nil
	default:
		return 0, fmt.Errorf("sqlx: unknown write mode %d", int(requested))
	}
}

// beginStatement is what txConn executes to open a transaction in mode.
func beginStatement(mode WriteMode) string {
	if mode == WriteModeConcurrent {
		return "BEGIN CONCURRENT"
	}
	return "BEGIN"
}

// wantJournal is the journal mode Open establishes on a local file, or "" when
// Open leaves the journal as the engine opened it: an in-memory database has
// no file to journal, and a synced database's journal belongs to its sync
// engine.
func wantJournal(mode WriteMode, memory, synced bool) string {
	switch {
	case memory || synced:
		return ""
	case mode == WriteModeConcurrent:
		return "mvcc"
	default:
		return "wal"
	}
}

// establishJournal sets the journal mode the write mode needs and reads the
// engine's answer back, so a file that stays in another mode refuses to open
// instead of serving transactions under a mode nobody chose. BEGIN
// CONCURRENT outside MVCC is refused by the engine, so running concurrent
// transactions on a WAL file would fail every Update.
func establishJournal(ctx context.Context, db *sql.DB, want string) error {
	if want == "" {
		return nil
	}
	var got string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode="+want).Scan(&got); err != nil {
		return fmt.Errorf("set journal_mode=%s: %w", want, err)
	}
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("sqlx: journal_mode=%s requested, engine reports %q", want, got)
	}
	return nil
}

// isMemoryPath reports whether path names the in-memory database, with or
// without DSN options after '?', matching how the driver splits the path.
func isMemoryPath(path string) bool {
	base, _, _ := strings.Cut(path, "?")
	return base == ":memory:"
}
