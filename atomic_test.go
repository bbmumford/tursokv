package tursokv_test

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
)

// boomTrigger refuses any rstring row whose value is "boom", so a call fails
// at its second statement, after the rkey row has been written.
const boomTrigger = `CREATE TRIGGER boom BEFORE INSERT ON rstring
	WHEN NEW.value = CAST('boom' AS BLOB) BEGIN SELECT RAISE(ABORT, 'boom refused'); END`

func openCounted(t *testing.T, mode tursokv.WriteMode) (*tursokv.DB, *atomic.Int64) {
	t.Helper()
	db, err := tursokv.Open(filepath.Join(t.TempDir(), "atomic.db"), &tursokv.Options{WriteMode: mode})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, tursokv.CountAtomicRuns(db)
}

// holdUpdate opens an Update that runs write and stays open until the
// returned function is called, which commits it and returns its error.
func holdUpdate(t *testing.T, db *tursokv.DB, write func(tx *tursokv.Tx) error) func() error {
	t.Helper()
	wrote := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Update(func(tx *tursokv.Tx) error {
			if err := write(tx); err != nil {
				return err
			}
			close(wrote)
			<-release
			return nil
		})
	}()
	select {
	case <-wrote:
	case err := <-done:
		t.Fatalf("holding Update ended before writing: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("holding Update neither wrote nor ended")
	}
	var once atomic.Bool
	return func() error {
		if !once.CompareAndSwap(false, true) {
			return nil
		}
		close(release)
		return <-done
	}
}

func isConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Write-write conflict")
}

// TestFailedCallKeepsNothing makes the second statement of a call fail and
// requires none of the call's writes to remain, in both write modes. The
// failure is not a conflict, so the call runs once.
func TestFailedCallKeepsNothing(t *testing.T) {
	for _, mode := range []tursokv.WriteMode{tursokv.WriteModeConcurrent, tursokv.WriteModeSerial} {
		t.Run(mode.String(), func(t *testing.T) {
			db, runs := openCounted(t, mode)
			if _, err := db.SQL().Exec(boomTrigger); err != nil {
				t.Fatal(err)
			}

			if err := db.Str().Set("b", "boom"); err == nil {
				t.Fatal("Set of a refused value succeeded; the trigger did not fire")
			}
			if n := runs.Load(); n != 1 {
				t.Errorf("Set ran %d times; a refused value is not a conflict and must run once", n)
			}
			if k, v := countRows(t, db, "rkey"), countRows(t, db, "rstring"); k != 0 || v != 0 {
				t.Fatalf("after the failed Set: %d rkey and %d rstring rows, want 0 and 0", k, v)
			}

			pairs := map[string]string{"a": "1", "b": "boom", "c": "3"}
			if err := db.Str().MSet(pairs); err == nil {
				t.Fatal("MSet with a refused value succeeded")
			}
			if k, v := countRows(t, db, "rkey"), countRows(t, db, "rstring"); k != 0 || v != 0 {
				t.Fatalf("after the failed MSet: %d rkey and %d rstring rows, want 0 and 0", k, v)
			}

			if err := db.Str().Set("n", "not a number"); err != nil {
				t.Fatal(err)
			}
			runs.Store(0)
			if _, err := db.Str().Incr("n"); err == nil {
				t.Fatal("Incr of a non-numeric value succeeded")
			}
			if n := runs.Load(); n != 1 {
				t.Errorf("Incr ran %d times; a parse failure is not a conflict and must run once", n)
			}
			if v, err := db.Str().Get("n"); err != nil || v != "not a number" {
				t.Fatalf("n = %q, %v after the failed Incr", v, err)
			}
		})
	}
}

// TestConflictPastBudgetKeepsNothing holds a row the call must write in an
// open Update for as long as the call runs. The call keeps conflicting,
// returns the conflict once its retries are spent, and keeps none of the
// writes it made before the conflicting statement.
func TestConflictPastBudgetKeepsNothing(t *testing.T) {
	t.Run("SetEx", func(t *testing.T) {
		db, runs := openCounted(t, tursokv.WriteModeConcurrent)
		if err := db.Str().Set("k", "v0"); err != nil {
			t.Fatal(err)
		}
		before, err := db.Key().Get("k")
		if err != nil {
			t.Fatal(err)
		}
		// The holder writes only the rstring row; SetEx writes the rkey row
		// first, which does not conflict, then the rstring row, which does.
		release := holdUpdate(t, db, func(tx *tursokv.Tx) error { return tx.Str().Set("k", "held") })
		defer release()
		runs.Store(0)
		err = db.Str().SetEx("k", "new", time.Hour)
		if !isConflict(err) {
			t.Fatalf("SetEx against a held row = %v, want a write-write conflict", err)
		}
		if n := runs.Load(); n < 2 {
			t.Errorf("SetEx ran %d times; a conflict must be retried", n)
		}
		if err := release(); err != nil {
			t.Fatalf("holding Update: %v", err)
		}
		after, err := db.Key().Get("k")
		if err != nil {
			t.Fatal(err)
		}
		if after.Version != before.Version || after.ETime != nil {
			t.Fatalf("rkey after the failed SetEx: version %d etime %v, want version %d and no expiry",
				after.Version, after.ETime, before.Version)
		}
		if v, err := db.Str().Get("k"); err != nil || v != "held" {
			t.Fatalf("k = %q, %v; want the holding Update's value", v, err)
		}
	})

	t.Run("Move", func(t *testing.T) {
		db, _ := openCounted(t, tursokv.WriteModeConcurrent)
		if _, err := db.Set().Add("src", "m"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Set().Add("dst", "x"); err != nil {
			t.Fatal(err)
		}
		// Move deletes from src, then writes dst's rkey row, which the
		// holder has written.
		release := holdUpdate(t, db, func(tx *tursokv.Tx) error {
			_, err := tx.Key().Expire("dst", time.Hour)
			return err
		})
		defer release()
		moved, err := db.Set().Move("src", "dst", "m")
		if !isConflict(err) || moved {
			t.Fatalf("Move against a held key = %v, %v; want false and a write-write conflict", moved, err)
		}
		if err := release(); err != nil {
			t.Fatalf("holding Update: %v", err)
		}
		inSrc, err := db.Set().IsMember("src", "m")
		if err != nil {
			t.Fatal(err)
		}
		inDst, err := db.Set().IsMember("dst", "m")
		if err != nil {
			t.Fatal(err)
		}
		if !inSrc || inDst {
			t.Fatalf("after the failed Move: in src %v, in dst %v; want it still in src only", inSrc, inDst)
		}
	})
}

// TestConflictWithinBudgetSucceeds releases the holding Update once the call
// has conflicted and started again; a later run commits the whole call.
func TestConflictWithinBudgetSucceeds(t *testing.T) {
	db, runs := openCounted(t, tursokv.WriteModeConcurrent)
	if err := db.Str().Set("k", "v0"); err != nil {
		t.Fatal(err)
	}
	release := holdUpdate(t, db, func(tx *tursokv.Tx) error { return tx.Str().Set("k", "held") })
	defer release()
	runs.Store(0)
	released := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for runs.Load() < 2 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Microsecond)
		}
		released <- release()
	}()

	if err := db.Str().SetEx("k", "new", time.Hour); err != nil {
		t.Fatalf("SetEx once the holder released: %v", err)
	}
	if err := <-released; err != nil {
		t.Fatalf("holding Update: %v", err)
	}
	if n := runs.Load(); n < 2 {
		t.Fatalf("SetEx ran %d times; the held row did not make it retry", n)
	}
	k, err := db.Key().Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if k.Version != 1 || k.ETime == nil {
		t.Fatalf("rkey after SetEx: version %d etime %v; want one committed update with an expiry", k.Version, k.ETime)
	}
	if v, err := db.Str().Get("k"); err != nil || v != "new" {
		t.Fatalf("k = %q, %v; want \"new\"", v, err)
	}
}
