package tursokv_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
	"github.com/bbmumford/tursokv/internal/rkey"
)

// holdKeyRow opens an Update that writes k's rkey row and holds it open.
func holdKeyRow(t *testing.T, db *tursokv.DB) func() error {
	t.Helper()
	return holdUpdate(t, db, func(tx *tursokv.Tx) error {
		_, err := tx.Key().Expire("k", time.Hour)
		return err
	})
}

// TestKeyWritesRetryHeldRow calls each Key() write while an open Update holds
// the key's rkey row, and releases the holder once the call has run twice. A
// write-write conflict must make the call run again, so it succeeds once the
// holder commits.
func TestKeyWritesRetryHeldRow(t *testing.T) {
	later := time.Now().Add(3 * time.Hour).Truncate(time.Millisecond)
	cases := []struct {
		name  string
		call  func(db *tursokv.DB) (bool, error)
		check func(k rkey.Key, found bool) error
	}{
		{"Expire", func(db *tursokv.DB) (bool, error) { return db.Key().Expire("k", 2*time.Hour) },
			func(k rkey.Key, found bool) error {
				if !found || k.ETime == nil || *k.ETime < time.Now().Add(90*time.Minute).UnixMilli() {
					return fmt.Errorf("found %v, etime %v; want the two-hour expiry", found, k.ETime)
				}
				return nil
			}},
		{"ExpireAt", func(db *tursokv.DB) (bool, error) { return db.Key().ExpireAt("k", later) },
			func(k rkey.Key, found bool) error {
				if !found || k.ETime == nil || *k.ETime != later.UnixMilli() {
					return fmt.Errorf("found %v, etime %v; want %d", found, k.ETime, later.UnixMilli())
				}
				return nil
			}},
		{"Persist", func(db *tursokv.DB) (bool, error) { return db.Key().Persist("k") },
			func(k rkey.Key, found bool) error {
				if !found || k.ETime != nil {
					return fmt.Errorf("found %v, etime %v; want no expiry", found, k.ETime)
				}
				return nil
			}},
		{"Delete", func(db *tursokv.DB) (bool, error) {
			n, err := db.Key().Delete("k")
			return n == 1, err
		}, func(_ rkey.Key, found bool) error {
			if found {
				return errors.New("the key is still there")
			}
			return nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, runs := openCounted(t, tursokv.WriteModeConcurrent)
			if err := db.Str().SetEx("k", "v", time.Hour); err != nil {
				t.Fatal(err)
			}
			// The key already expires, so each call's own snapshot finds the
			// row it updates or deletes (Persist matches only a row that
			// expires) and writes it while the holder has it written.
			release := holdKeyRow(t, db)
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

			ok, err := tc.call(db)
			if err != nil || !ok {
				t.Fatalf("%s once the holder released = %v, %v; want true and no error", tc.name, ok, err)
			}
			if err := <-released; err != nil {
				t.Fatalf("holding Update: %v", err)
			}
			if n := runs.Load(); n < 2 {
				t.Fatalf("%s ran %d times; the held row did not make it run again", tc.name, n)
			}
			k, err := db.Key().Get("k")
			found := err == nil
			if err != nil && !errors.Is(err, rkey.ErrNotFound) {
				t.Fatal(err)
			}
			if err := tc.check(k, found); err != nil {
				t.Fatalf("after %s: %v", tc.name, err)
			}
		})
	}
}

// TestKeyWriteConflictPastBudgetKeepsKey holds the key's rkey row for as long
// as Delete runs: Delete returns the conflict once its runs are spent and the
// key keeps its value.
func TestKeyWriteConflictPastBudgetKeepsKey(t *testing.T) {
	db, runs := openCounted(t, tursokv.WriteModeConcurrent)
	if err := db.Str().Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	release := holdKeyRow(t, db)
	defer release()
	runs.Store(0)
	n, err := db.Key().Delete("k")
	if !isConflict(err) || n != 0 {
		t.Fatalf("Delete against a held key = %d, %v; want 0 and a write-write conflict", n, err)
	}
	if runs.Load() < 2 {
		t.Errorf("Delete ran %d times; a conflict must be retried", runs.Load())
	}
	if err := release(); err != nil {
		t.Fatalf("holding Update: %v", err)
	}
	if v, err := db.Str().Get("k"); err != nil || v != "v" {
		t.Fatalf("k = %q, %v after the failed Delete; want v", v, err)
	}
}

// TestParallelKeyWritesBesideCalls runs Expire and Persist on a key while
// other goroutines Incr it. Each Incr is a transaction that writes the key's
// rkey row, so a Key() write that meets one must wait and run again rather
// than fail.
func TestParallelKeyWritesBesideCalls(t *testing.T) {
	db, _ := openCounted(t, tursokv.WriteModeConcurrent)
	const each = 25
	errs := runParallel(12, each, func(w int) error {
		var err error
		switch w % 3 {
		case 0:
			_, err = db.Str().Incr("k")
		case 1:
			_, err = db.Key().Expire("k", time.Hour)
		default:
			_, err = db.Key().Persist("k")
		}
		return err
	})
	requireNoErrors(t, "Incr, Expire and Persist", errs)
	if v, err := db.Str().Get("k"); err != nil || v != fmt.Sprint(4*each) {
		t.Fatalf("k = %q, %v; want %d", v, err, 4*each)
	}
}
