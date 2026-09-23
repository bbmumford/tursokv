package sqlx

import (
	"database/sql"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	turso "turso.tech/database/tursogo"
)

// Atomic runs fn in one transaction and commits it, so a call made of several
// statements keeps all of its writes or none of them. When the transaction
// fails with a write-write conflict, Atomic runs fn again in a fresh
// transaction after a short wait.
//
// WHAT CONSUMES IT: the DB-path methods of the repositories in
// internal/rstring, rhash, rlist, rset and rzset that write with more than
// one statement, directly or through internal/store.Do; *DB is the
// store.Store tursokv.Open hands each repository.
//
// CONDITIONS. fn must be safe to run more than once: every statement goes
// through tx, and results are assigned inside fn, so the run that commits
// supplies them. A write-write conflict, at a statement or at COMMIT, is
// retried until atomicRetry.attempts runs have been made, waiting a jittered,
// capped, doubling interval between runs; after the last run Atomic returns
// that conflict and none of fn's writes are kept. Every other error returns
// at once. fn must not call Update, View or Atomic, nor run statements
// through the DB, for the same reason Update gives.
//
// WHY THIS SHAPE. rstring's setBytes upserts the rkey row and then the
// rstring row, and incrBy reads a value and writes it back. Only one
// transaction around the whole call makes a conflict on the second write
// discard the first, and makes the engine refuse incrBy's write when another
// transaction changed the row it read. The retry lives here and not in
// Update because Update's fn is the caller's, and only the caller knows
// whether it is safe to run twice; the repository functions are, since each
// run reads what it needs through tx.
func (db *DB) Atomic(fn func(tx *sql.Tx) error) error {
	for run := 1; ; run++ {
		err := db.inTx(fn, nil)
		if err == nil || run >= atomicRetry.attempts || !isWriteConflict(err) {
			return err
		}
		time.Sleep(atomicRetry.wait(run))
	}
}

// retryPolicy bounds how many times Atomic runs fn and how long it waits
// between runs.
type retryPolicy struct {
	// attempts is the most runs of fn, the first included.
	attempts int
	// base bounds the wait after the first run; each later bound doubles, up
	// to ceiling.
	base, ceiling time.Duration
}

// atomicRetry is the budget of every Atomic call. Eight or sixteen goroutines
// each calling rstring's Incr on one key over the four-connection pool needed
// at most 31 runs for a single call under this backoff; 64 runs leaves twice
// that while bounding the waits of a call that keeps conflicting, such as one
// against a row an open Update holds, to between about 0.19 and 0.37
// seconds. Tests replace it.
var atomicRetry = retryPolicy{attempts: 64, base: 100 * time.Microsecond, ceiling: 6400 * time.Microsecond}

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

// writeConflictText is the engine's message for a write-write conflict.
const writeConflictText = "Write-write conflict"

// isWriteConflict reports whether err is the engine's write-write conflict.
// tursogo has no error of its own for it: statusToError wraps
// turso.ErrTursoGeneric with the engine's message, the same whether the
// conflict is found at a statement, in a row a query steps to, or at COMMIT,
// so the message is matched as well as the sentinel.
func isWriteConflict(err error) bool {
	return err != nil && errors.Is(err, turso.ErrTursoGeneric) && strings.Contains(err.Error(), writeConflictText)
}
