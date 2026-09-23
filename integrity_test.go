package tursokv_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
	"github.com/bbmumford/tursokv/internal/rzset"
)

var valueTables = []string{"rstring", "rhash", "rlist", "rset", "rzset"}

func countRows(t *testing.T, db *tursokv.DB, table string) int {
	t.Helper()
	var n int
	if err := db.SQL().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func writeOneOfEachType(t *testing.T, db *tursokv.DB) {
	t.Helper()
	if err := db.Str().Set("s", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Hash().Set("h", "f", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.List().RPush("l", "a", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Set().Add("st", "m"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ZSet().Add("z", rzset.ScoreMember{Member: "m", Score: 1}); err != nil {
		t.Fatal(err)
	}
}

// TestKeyDeleteRemovesValues deletes one key of every type, through the DB
// and through a transaction, in each write mode. The value tables reference
// rkey ON DELETE CASCADE, so no value row may outlive its key.
func TestKeyDeleteRemovesValues(t *testing.T) {
	for _, mode := range []tursokv.WriteMode{tursokv.WriteModeConcurrent, tursokv.WriteModeSerial} {
		for _, viaTx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/tx=%v", mode, viaTx), func(t *testing.T) {
				db, err := tursokv.Open(filepath.Join(t.TempDir(), "cascade.db"), &tursokv.Options{WriteMode: mode})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				writeOneOfEachType(t, db)
				for _, table := range valueTables {
					if countRows(t, db, table) == 0 {
						t.Fatalf("%s is empty before the delete; the test would pass vacuously", table)
					}
				}

				keys := []string{"s", "h", "l", "st", "z"}
				var deleted int
				if viaTx {
					err = db.Update(func(tx *tursokv.Tx) error {
						deleted, err = tx.Key().Delete(keys...)
						return err
					})
				} else {
					deleted, err = db.Key().Delete(keys...)
				}
				if err != nil || deleted != len(keys) {
					t.Fatalf("Delete = %d, %v; want %d", deleted, err, len(keys))
				}
				for _, table := range valueTables {
					if n := countRows(t, db, table); n != 0 {
						t.Errorf("%s holds %d rows after their keys were deleted", table, n)
					}
				}
			})
		}
	}
}

func TestDefaultWriteModeIsConcurrentForFiles(t *testing.T) {
	db, err := tursokv.Open(filepath.Join(t.TempDir(), "default.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.WriteMode(); got != tursokv.WriteModeConcurrent {
		t.Fatalf("WriteMode = %v, want concurrent", got)
	}
	if got := db.SQL().Stats().MaxOpenConnections; got <= 1 {
		t.Fatalf("MaxOpenConnections = %d, want a multi-connection pool", got)
	}
	var journal string
	if err := db.SQL().QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "mvcc" {
		t.Fatalf("journal_mode = %q (err %v), want mvcc", journal, err)
	}

	mem, err := tursokv.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()
	if got := mem.WriteMode(); got != tursokv.WriteModeSerial {
		t.Fatalf(":memory: WriteMode = %v, want serial", got)
	}
	if _, err := tursokv.Open(":memory:", &tursokv.Options{WriteMode: tursokv.WriteModeConcurrent}); err == nil {
		t.Fatal("Open accepted WriteModeConcurrent for :memory:")
	}
}

// TestUpdateThenWritesSurviveReopen interleaves Updates with plain writes and
// reopens the file: every write must be durable.
func TestUpdateThenWritesSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")
	db, err := tursokv.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	const total = 21
	if err := db.Update(func(tx *tursokv.Tx) error { return tx.Str().Set("k0", "v") }); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < total; i++ {
		if i%7 == 0 {
			err = db.Update(func(tx *tursokv.Tx) error { return tx.Str().Set(fmt.Sprint("k", i), "v") })
		} else {
			err = db.Str().Set(fmt.Sprint("k", i), "v")
		}
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = tursokv.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if n := countRows(t, db, "rstring"); n != total {
		t.Fatalf("string values after reopen = %d, want %d", n, total)
	}
}

func TestMemoryDatabaseKeepsDataPastConnectionLifetime(t *testing.T) {
	db, err := tursokv.Open(":memory:", &tursokv.Options{Timeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Str().Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if v, err := db.Str().Get("k"); err != nil || v != "v" {
		t.Fatalf("Get = %q, %v; want \"v\"", v, err)
	}
}

func TestOpenDoesNotModifyOptions(t *testing.T) {
	opts := &tursokv.Options{}
	db, err := tursokv.Open(filepath.Join(t.TempDir(), "opts.db"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if *opts != (tursokv.Options{}) {
		t.Fatalf("Open modified the caller's options: %+v", *opts)
	}
}

// TestUpdateIgnoringConflictKeepsNothing writes the way the package example
// does, ignoring each Set's error, while another Update holds the same key.
// The Set that conflicts ends the transaction in the engine; the Set after it
// must not be committed on its own, and Update must report the failure.
func TestUpdateIgnoringConflictKeepsNothing(t *testing.T) {
	db, err := tursokv.Open(filepath.Join(t.TempDir(), "conflict.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Str().Set("hot", "0"); err != nil {
		t.Fatal(err)
	}

	wrote := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- db.Update(func(tx *tursokv.Tx) error {
			if err := tx.Str().Set("hot", "first"); err != nil {
				return err
			}
			close(wrote)
			<-release
			return nil
		})
	}()
	select {
	case <-wrote:
	case err := <-firstDone:
		t.Fatalf("first Update ended before writing: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("first Update neither wrote nor ended")
	}

	var conflict error
	second := db.Update(func(tx *tursokv.Tx) error {
		conflict = tx.Str().Set("hot", "second")
		_ = tx.Str().Set("other", "x")
		return nil
	})
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Update: %v", err)
	}
	if conflict == nil {
		t.Fatal("second Set of the held key did not conflict; the test did not reach the engine-ended case")
	}
	if second == nil {
		t.Fatal("Update reported success after its transaction was ended by a conflict")
	}
	if n, err := db.Key().Exists("other"); err != nil || n != 0 {
		t.Fatalf("Exists(other) = %d, %v; the write after the conflict was committed", n, err)
	}
	if v, err := db.Str().Get("hot"); err != nil || v != "first" {
		t.Fatalf("hot = %q, %v; want the first Update's value", v, err)
	}
}
