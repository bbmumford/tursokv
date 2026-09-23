package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestResolveWriteMode(t *testing.T) {
	cases := []struct {
		name      string
		requested WriteMode
		memory    bool
		synced    bool
		want      WriteMode
		wantErr   error
	}{
		{"auto local file", WriteModeAuto, false, false, WriteModeConcurrent, nil},
		{"auto memory", WriteModeAuto, true, false, WriteModeSerial, nil},
		{"auto synced", WriteModeAuto, false, true, WriteModeSerial, nil},
		{"concurrent local file", WriteModeConcurrent, false, false, WriteModeConcurrent, nil},
		{"concurrent memory refused", WriteModeConcurrent, true, false, 0, errConcurrentMemory},
		{"concurrent synced refused", WriteModeConcurrent, false, true, 0, errConcurrentSynced},
		{"serial local file", WriteModeSerial, false, false, WriteModeSerial, nil},
		{"serial memory", WriteModeSerial, true, false, WriteModeSerial, nil},
		{"serial synced", WriteModeSerial, false, true, WriteModeSerial, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveWriteMode(tc.requested, tc.memory, tc.synced)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Fatalf("mode = %v, want %v", got, tc.want)
			}
		})
	}
	if _, err := resolveWriteMode(WriteMode(99), false, false); err == nil {
		t.Fatal("unknown write mode resolved without error")
	}
}

func TestWantJournal(t *testing.T) {
	cases := []struct {
		mode           WriteMode
		memory, synced bool
		want           string
	}{
		{WriteModeConcurrent, false, false, "mvcc"},
		{WriteModeSerial, false, false, "wal"},
		{WriteModeSerial, true, false, ""},
		{WriteModeSerial, false, true, ""},
	}
	for _, tc := range cases {
		if got := wantJournal(tc.mode, tc.memory, tc.synced); got != tc.want {
			t.Errorf("wantJournal(%v, memory=%v, synced=%v) = %q, want %q", tc.mode, tc.memory, tc.synced, got, tc.want)
		}
	}
}

func TestIsMemoryPath(t *testing.T) {
	for path, want := range map[string]bool{
		":memory:":                  true,
		":memory:?_busy_timeout=10": true,
		"data.db":                   false,
		"/tmp/:memory:":             false,
		"file.db?x=:memory:":        false,
	} {
		if got := isMemoryPath(path); got != want {
			t.Errorf("isMemoryPath(%q) = %v, want %v", path, got, want)
		}
	}
}

// TestOpenEstablishesWriteMode opens each kind of local database and reads
// back what Open established: the resolved mode, the journal mode on disk and
// the pool size.
func TestOpenEstablishesWriteMode(t *testing.T) {
	cases := []struct {
		name      string
		path      string
		requested WriteMode
		mode      WriteMode
		journal   string
		conns     int
	}{
		{"default local file", "auto.db", WriteModeAuto, WriteModeConcurrent, "mvcc", concurrentConns},
		{"serial local file", "serial.db", WriteModeSerial, WriteModeSerial, "wal", 1},
		{"default memory", ":memory:", WriteModeAuto, WriteModeSerial, "", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.path
			if path != ":memory:" {
				path = filepath.Join(t.TempDir(), path)
			}
			db, err := Open(path, &Options{WriteMode: tc.requested})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if got := db.WriteMode(); got != tc.mode {
				t.Fatalf("WriteMode = %v, want %v", got, tc.mode)
			}
			if got := db.DB().Stats().MaxOpenConnections; got != tc.conns {
				t.Fatalf("MaxOpenConnections = %d, want %d", got, tc.conns)
			}
			if tc.journal != "" {
				if got := journalMode(t, db); got != tc.journal {
					t.Fatalf("journal_mode = %q, want %q", got, tc.journal)
				}
			}
		})
	}
}

// TestOpenSwitchesJournalToChosenMode reopens one file under each mode: the
// journal on disk follows the mode requested at each open, and every row
// written under an earlier mode is still there.
func TestOpenSwitchesJournalToChosenMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "switch.db")
	for i, step := range []struct {
		mode    WriteMode
		journal string
	}{
		{WriteModeConcurrent, "mvcc"},
		{WriteModeSerial, "wal"},
		{WriteModeConcurrent, "mvcc"},
		{WriteModeSerial, "wal"},
	} {
		db, err := Open(path, &Options{WriteMode: step.mode})
		if err != nil {
			t.Fatalf("open %v: %v", step.mode, err)
		}
		if got := journalMode(t, db); got != step.journal {
			t.Fatalf("after open %v: journal_mode = %q, want %q", step.mode, got, step.journal)
		}
		if err := db.Update(func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO rkey (key, type) VALUES (?, 1)", fmt.Sprint("k", i))
			return err
		}); err != nil {
			t.Fatalf("write under %v: %v", step.mode, err)
		}
		if got := rowCount(t, db); got != i+1 {
			t.Fatalf("after open %v: rows = %d, want %d", step.mode, got, i+1)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestEncryptedFileRunsConcurrent opens an encrypted local file, which
// resolves to WriteModeConcurrent, and reopens it: the MVCC journal and the
// encryption hold together, and the file refuses to open without the key.
func TestEncryptedFileRunsConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "encrypted.db")
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	db, err := Open(path, &Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if db.WriteMode() != WriteModeConcurrent || journalMode(t, db) != "mvcc" {
		t.Fatalf("mode %v journal %q, want concurrent on mvcc", db.WriteMode(), journalMode(t, db))
	}
	if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('secret', 1)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if db, err := Open(path, nil); err == nil {
		db.Close()
		t.Fatal("encrypted file opened without its key")
	}
	db, err = Open(path, &Options{EncryptionKey: key})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := rowCount(t, db); got != 1 {
		t.Fatalf("rows after reopen = %d, want 1", got)
	}
}

func rowCount(t *testing.T, db *DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM rkey").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestOpenRefusesConcurrentMemory(t *testing.T) {
	if _, err := Open(":memory:", &Options{WriteMode: WriteModeConcurrent}); !errors.Is(err, errConcurrentMemory) {
		t.Fatalf("err = %v, want %v", err, errConcurrentMemory)
	}
}

func TestOpenRefusesConcurrentSyncedBeforeContactingRemote(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "r.db"), &Options{
		SyncURL:   "http://127.0.0.1:1",
		WriteMode: WriteModeConcurrent,
	})
	if !errors.Is(err, errConcurrentSynced) {
		t.Fatalf("err = %v, want %v", err, errConcurrentSynced)
	}
}

func journalMode(t *testing.T, db *DB) string {
	t.Helper()
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}
