package sqlx

import (
	"database/sql"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	turso "turso.tech/database/tursogo"
)

// Atomic runs fn in one transaction and commits it, so a call made of several
// statements keeps all of its writes or none of them. When the transaction
// fails with a write-write conflict or a commit-dependency abort, Atomic runs
// fn again in a fresh transaction after a short wait, and a call that keeps
// conflicting runs alone.
//
// WHAT CONSUMES IT: the DB-path methods of the repositories in
// internal/rstring, rhash, rlist, rset and rzset that write with more than
// one statement, and the writes of internal/rkey, directly or through
// internal/store.Do; *DB is the store.Store tursokv.Open hands each
// repository.
//
// CONDITIONS. fn must be safe to run more than once: every statement goes
// through tx, and results are assigned inside fn, so the run that commits
// supplies them. A write-write conflict, at a statement or at COMMIT, and a
// commit-dependency abort (the engine aborting a transaction whose commit
// depended on one that rolled back) are retried until atomicRetry.attempts
// runs have been made, waiting a jittered, capped, doubling interval between
// runs; after the last run Atomic returns that error and none of fn's writes
// are kept. Every other error returns at once. From run
// atomicRetry.exclusiveAfter on, a run holds the DB's atomic gate
// exclusively, so no other Atomic transaction is open while it runs: only a
// transaction outside Atomic (an Update or View a caller holds) can still
// conflict with it. fn must not call Update, View or Atomic, nor run
// statements through the DB, for the same reason Update gives.
//
// WHY THIS SHAPE. rstring's setBytes upserts the rkey row and then the
// rstring row, and incrBy reads a value and writes it back. Only one
// transaction around the whole call makes a conflict on the second write
// discard the first, and makes the engine refuse incrBy's write when another
// transaction changed the row it read. The retry lives here and not in
// Update because Update's fn is the caller's, and only the caller knows
// whether it is safe to run twice; the repository functions are, since each
// run reads what it needs through tx. Retries alone do not bound a call's
// runs: with eight or more goroutines writing one row on a loaded host, a
// call can lose every race until its budget is spent, so a call that has
// conflicted atomicRetry.exclusiveAfter-1 times runs alone.
func (db *DB) Atomic(fn func(tx *sql.Tx) error) error {
	for run := 1; ; run++ {
		err := db.atomicRun(fn, atomicRetry.exclusiveAfter > 0 && run >= atomicRetry.exclusiveAfter)
		if err == nil || run >= atomicRetry.attempts || !isWriteConflict(err) {
			return err
		}
		time.Sleep(atomicRetry.wait(run))
	}
}

// atomicGate is held shared by every Atomic run and exclusively by a run
// that has to run alone (Atomic's exclusiveAfter). It is per DB: two DBs on
// one file do not share it, and each holds its own pool.
type atomicGate struct{ sync.RWMutex }

// atomicRun runs one transaction of an Atomic call under the DB's atomic
// gate, held exclusively when alone is set.
func (db *DB) atomicRun(fn func(tx *sql.Tx) error, alone bool) error {
	if alone {
		db.gate.Lock()
		defer db.gate.Unlock()
	} else {
		db.gate.RLock()
		defer db.gate.RUnlock()
	}
	return db.inTx(fn, nil)
}

// retryPolicy bounds how many times Atomic runs fn and how long it waits
// between runs.
type retryPolicy struct {
	// attempts is the most runs of fn, the first included.
	attempts int
	// base bounds the wait after the first run; each later bound doubles, up
	// to ceiling.
	base, ceiling time.Duration
	// exclusiveAfter is the first run made holding the atomic gate
	// exclusively; zero never runs alone.
	exclusiveAfter int
}

// atomicRetry is the budget of every Atomic call. Eight or sixteen goroutines
// each calling rstring's Incr on one key over the four-connection pool needed
// at most 31 runs for a single call under this backoff; 64 runs leaves twice
// that while bounding the waits of a call that keeps conflicting, such as one
// against a row an open Update holds, to between about 0.19 and 0.37
// seconds. A call's fourth run and every later one run alone, so a call
// conflicting only with other Atomic calls finishes by its fourth run. Tests
// replace it.
var atomicRetry = retryPolicy{attempts: 64, base: 100 * time.Microsecond, ceiling: 6400 * time.Microsecond, exclusiveAfter: 4}

// wait is the pause after run (counted from 1): base doubled once per earlier
// run and capped at ceiling, drawn from the upper half of that bound so calls
// that conflicted with each other do not run again in step.
func (p retryPolicy) wait(run int) time.Duration {
	bound := p.ceiling
	if shift := run - 1; shift < 32 {
		if d := p.base << shift; d > 0 && d < bound {
			bound = d
		}
	}
	half := bound / 2
	return half + rand.N(bound-half+1)
}

// writeConflictText is the engine's message for a write-write conflict, and
// commitDependencyText the start of its message when it aborts a transaction
// whose commit depended on one that rolled back ("Commit dependency aborted,
// rollback and retry the whole transaction"); both ask for the whole
// transaction to run again.
const (
	writeConflictText    = "Write-write conflict"
	commitDependencyText = "Commit dependency aborted"
)

// isWriteConflict reports whether err is the engine's write-write conflict or
// its commit-dependency abort. tursogo has no error of its own for either:
// statusToError wraps turso.ErrTursoGeneric with the engine's message, the
// same whether the conflict is found at a statement, in a row a query steps
// to, or at COMMIT, so the message is matched as well as the sentinel.
func isWriteConflict(err error) bool {
	if err == nil || !errors.Is(err, turso.ErrTursoGeneric) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, writeConflictText) || strings.Contains(msg, commitDependencyText)
}
