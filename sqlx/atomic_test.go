package sqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	turso "turso.tech/database/tursogo"
)

// withRetry runs the test under p and restores the package budget after it.
func withRetry(t *testing.T, p retryPolicy) {
	t.Helper()
	saved := atomicRetry
	atomicRetry = p
	t.Cleanup(func() { atomicRetry = saved })
}

func openConcurrentKV(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "atomic.db"), &Options{WriteMode: WriteModeConcurrent})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT); INSERT INTO kv VALUES ('hot', '0')"); err != nil {
		t.Fatal(err)
	}
	return db
}

// engineConflicts returns the conflicts the engine reports at a statement
// and at COMMIT, taken from real transactions on a concurrent pool.
func engineConflicts(t *testing.T) (statement, commit error) {
	t.Helper()
	db := openConcurrentKV(t)

	release := holdHotRow(t, db)
	err := db.Update(func(tx *sql.Tx) error {
		_, err := tx.Exec("UPDATE kv SET v = 'second' WHERE k = 'hot'")
		statement = err
		return err
	})
	if err := release(); err != nil {
		t.Fatalf("holding Update: %v", err)
	}
	if statement == nil || err == nil {
		t.Fatal("a write to a held row did not fail; no statement-level conflict to classify")
	}

	a, err := db.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, tx := range []*sql.Tx{a, b} {
		if _, err := tx.Exec("INSERT INTO kv VALUES ('new', 'x')"); err != nil {
			t.Fatalf("insert of a new key in a concurrent transaction: %v", err)
		}
	}
	if err := a.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	commit = b.Commit()
	if commit == nil {
		t.Fatal("both transactions that inserted the same key committed; no COMMIT-level conflict to classify")
	}
	return statement, commit
}

func TestIsWriteConflict(t *testing.T) {
	statement, commit := engineConflicts(t)

	db := openConcurrentKV(t)
	_, constraint := db.Exec("INSERT INTO kv VALUES ('hot', 'dup')")
	_, generic := db.Exec("SELECT * FROM no_such_table")
	if constraint == nil || generic == nil {
		t.Fatalf("control errors missing: constraint %v, generic %v", constraint, generic)
	}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"statement-level conflict", statement, true},
		{"COMMIT-level conflict", commit, true},
		{"wrapped conflict", fmt.Errorf("zadd: %w", statement), true},
		{"constraint failure", constraint, false},
		{"generic engine error", generic, false},
		{"busy", fmt.Errorf("%w: database is busy", turso.ErrTursoBusy), false},
		{"conflict text without the engine error", errors.New("Write-write conflict"), false},
		{"transaction ended by the engine", errTxEnded, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isWriteConflict(tc.err); got != tc.want {
			t.Errorf("%s (%v): isWriteConflict = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestAtomicRetriesOnlyConflicts counts the runs Atomic makes of fn under a
// small budget for each kind of failure.
func TestAtomicRetriesOnlyConflicts(t *testing.T) {
	withRetry(t, retryPolicy{attempts: 5, base: time.Microsecond, ceiling: 10 * time.Microsecond})
	conflict := fmt.Errorf("%w: Write-write conflict", turso.ErrTursoGeneric)
	db := openConcurrentKV(t)

	cases := []struct {
		name     string
		fail     func(run int) error
		wantRuns int
		wantErr  error
	}{
		{"conflict every run", func(int) error { return conflict }, 5, conflict},
		{"conflict twice, then success", func(run int) error {
			if run <= 2 {
				return conflict
			}
			return nil
		}, 3, nil},
		{"constraint failure", func(int) error { return turso.ErrTursoConstraint }, 1, turso.ErrTursoConstraint},
		{"busy", func(int) error { return turso.ErrTursoBusy }, 1, turso.ErrTursoBusy},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := fmt.Sprint("k", i)
			runs := 0
			err := db.Atomic(func(tx *sql.Tx) error {
				runs++
				if _, err := tx.Exec("INSERT INTO kv VALUES (?, ?)", key, fmt.Sprint(runs)); err != nil {
					return err
				}
				return tc.fail(runs)
			})
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil) != (err == nil) {
				t.Fatalf("Atomic = %v, want %v", err, tc.wantErr)
			}
			if runs != tc.wantRuns {
				t.Fatalf("fn ran %d times, want %d", runs, tc.wantRuns)
			}
			var n int
			if err := db.QueryRow("SELECT COUNT(*) FROM kv WHERE k = ?", key).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if want := map[bool]int{true: 1, false: 0}[tc.wantErr == nil]; n != want {
				t.Fatalf("%d rows for %s after Atomic, want %d: a failed run's insert was kept", n, key, want)
			}
		})
	}
}

// TestAtomicRetriesCommitConflict makes fn's first COMMIT fail: while the
// first run is open, another transaction inserts the same new key and
// commits. The engine accepts both inserts and refuses the second COMMIT, so
// Atomic must run fn again, and the second run updates the committed row.
func TestAtomicRetriesCommitConflict(t *testing.T) {
	withRetry(t, retryPolicy{attempts: 5, base: time.Microsecond, ceiling: 10 * time.Microsecond})
	db := openConcurrentKV(t)
	runs := 0
	err := db.Atomic(func(tx *sql.Tx) error {
		runs++
		if _, err := tx.Exec("INSERT INTO kv VALUES ('late', 'atomic') ON CONFLICT(k) DO UPDATE SET v = excluded.v"); err != nil {
			return err
		}
		if runs == 1 {
			other, err := db.DB().Begin()
			if err != nil {
				return err
			}
			if _, err := other.Exec("INSERT INTO kv VALUES ('late', 'other')"); err != nil {
				other.Rollback()
				return err
			}
			if err := other.Commit(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Atomic: %v", err)
	}
	if runs != 2 {
		t.Fatalf("fn ran %d times, want 2: the COMMIT conflict was not retried once", runs)
	}
	var v string
	if err := db.QueryRow("SELECT v FROM kv WHERE k = 'late'").Scan(&v); err != nil || v != "atomic" {
		t.Fatalf("late = %q, %v; want the retried run's value", v, err)
	}
}

func TestRetryWaitBounds(t *testing.T) {
	p := retryPolicy{attempts: 64, base: 100 * time.Microsecond, ceiling: 6400 * time.Microsecond}
	for run := 1; run <= 200; run++ {
		bound := p.ceiling
		if run <= 7 {
			bound = p.base << (run - 1)
		}
		for range 20 {
			if w := p.wait(run); w < bound/2 || w > bound {
				t.Fatalf("wait(%d) = %v, want within [%v, %v]", run, w, bound/2, bound)
			}
		}
	}
	if w := (retryPolicy{}).wait(1); w != 0 {
		t.Fatalf("a zero policy waits %v, want 0", w)
	}
}
