package tursokv_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/bbmumford/tursokv"
	"github.com/bbmumford/tursokv/internal/rzset"
)

// runParallel starts workers goroutines together, each running op each times
// with its worker index, and returns every error they reported.
func runParallel(workers, each int, op func(worker int) error) []error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	start := make(chan struct{})
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range each {
				if err := op(w); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	return errs
}

func requireNoErrors(t *testing.T, what string, errs []error) {
	t.Helper()
	for i, err := range errs {
		if i == 3 {
			t.Errorf("%s: %d errors in all", what, len(errs))
			break
		}
		t.Errorf("%s: %v", what, err)
	}
}

// TestParallelIncrLosesNothing increments one key from eight goroutines on
// the four-connection pool. Each Incr reads the value and writes it back; a
// call whose row changed underneath it conflicts and runs again.
func TestParallelIncrLosesNothing(t *testing.T) {
	db, runs := openCounted(t, tursokv.WriteModeConcurrent)
	const workers, each = 8, 25
	errs := runParallel(workers, each, func(int) error {
		_, err := db.Str().Incr("counter")
		return err
	})
	requireNoErrors(t, "Incr", errs)
	if v, err := db.Str().Get("counter"); err != nil || v != fmt.Sprint(workers*each) {
		t.Fatalf("counter = %q, %v; want %d", v, err, workers*each)
	}
	t.Logf("%d Incr calls took %d runs", workers*each, runs.Load())
}

// TestParallelSetNXHasOneWinner races eight SetNX calls on each of several
// new keys; exactly one may report that it set the key.
func TestParallelSetNXHasOneWinner(t *testing.T) {
	db, _ := openCounted(t, tursokv.WriteModeConcurrent)
	for round := range 5 {
		key := fmt.Sprint("lock", round)
		var mu sync.Mutex
		winners := 0
		errs := runParallel(8, 1, func(w int) error {
			won, err := db.Str().SetNX(key, fmt.Sprint("holder", w))
			if won {
				mu.Lock()
				winners++
				mu.Unlock()
			}
			return err
		})
		requireNoErrors(t, "SetNX", errs)
		if winners != 1 {
			t.Fatalf("round %d: %d SetNX calls reported setting %s, want 1", round, winners, key)
		}
	}
}

// TestParallelPopsNeverRepeat pops a set, a list and a sorted set of forty
// members from eight goroutines. Each pop reads a member and deletes it; two
// pops of the same member conflict, so every member comes out exactly once.
func TestParallelPopsNeverRepeat(t *testing.T) {
	db, _ := openCounted(t, tursokv.WriteModeConcurrent)
	members := make([]string, 40)
	for i := range members {
		members[i] = fmt.Sprint("m", i)
	}
	if _, err := db.Set().Add("set", members...); err != nil {
		t.Fatal(err)
	}
	if _, err := db.List().RPush("list", members...); err != nil {
		t.Fatal(err)
	}
	for i, m := range members {
		if _, err := db.ZSet().Add("zset", rzset.ScoreMember{Member: m, Score: float64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	pops := map[string]func() (string, error){
		"Set.Pop":   func() (string, error) { return db.Set().Pop("set") },
		"List.LPop": func() (string, error) { return db.List().LPop("list") },
		"ZSet.PopMin": func() (string, error) {
			m, _, err := db.ZSet().PopMin("zset")
			return m.Member, err
		},
	}
	for name, pop := range pops {
		var mu sync.Mutex
		seen := map[string]int{}
		errs := runParallel(8, 5, func(int) error {
			m, err := pop()
			if err == nil && m != "" {
				mu.Lock()
				seen[m]++
				mu.Unlock()
			}
			return err
		})
		requireNoErrors(t, name, errs)
		for m, n := range seen {
			if n > 1 {
				t.Errorf("%s returned %s %d times", name, m, n)
			}
		}
		if len(seen) != len(members) {
			t.Errorf("%s returned %d distinct members, want %d", name, len(seen), len(members))
		}
	}
}

// TestParallelSetOfNewKeyCommitsOnce sets one new key from eight goroutines.
// Transactions that begin before the key exists each insert its rkey row; the
// engine accepts every such insert and refuses the COMMIT of all but the
// first, so those calls must run again and update the row the first created.
func TestParallelSetOfNewKeyCommitsOnce(t *testing.T) {
	db, _ := openCounted(t, tursokv.WriteModeConcurrent)
	for round := range 5 {
		key := fmt.Sprint("fresh", round)
		errs := runParallel(8, 1, func(w int) error {
			return db.Str().Set(key, fmt.Sprint("v", w))
		})
		requireNoErrors(t, "Set", errs)
		var keys, values int
		if err := db.SQL().QueryRow("SELECT COUNT(*) FROM rkey WHERE key = ?", key).Scan(&keys); err != nil {
			t.Fatal(err)
		}
		if err := db.SQL().QueryRow("SELECT COUNT(*) FROM rstring s JOIN rkey k ON k.id = s.key_id WHERE k.key = ?", key).Scan(&values); err != nil {
			t.Fatal(err)
		}
		if keys != 1 || values != 1 {
			t.Fatalf("round %d: %d rkey and %d rstring rows for %s, want 1 and 1", round, keys, values, key)
		}
	}
}
