package tursokv

import (
	"database/sql"
	"sync/atomic"

	"github.com/bbmumford/tursokv/internal/rhash"
	"github.com/bbmumford/tursokv/internal/rkey"
	"github.com/bbmumford/tursokv/internal/rlist"
	"github.com/bbmumford/tursokv/internal/rset"
	"github.com/bbmumford/tursokv/internal/rstring"
	"github.com/bbmumford/tursokv/internal/rzset"
	"github.com/bbmumford/tursokv/sqlx"
)

// countingStore is the pool Open built, with every run of a function passed
// to Atomic counted: Atomic runs its function once per attempt.
type countingStore struct {
	*sqlx.DB
	runs atomic.Int64
}

func (c *countingStore) Atomic(fn func(tx *sql.Tx) error) error {
	return c.DB.Atomic(func(tx *sql.Tx) error {
		c.runs.Add(1)
		return fn(tx)
	})
}

// CountAtomicRuns rebuilds db's key and value repositories on the same pool
// with each Atomic run counted, and returns the counter. Tests read it to tell
// a call that was retried from one that ran once.
func CountAtomicRuns(db *DB) *atomic.Int64 {
	c := &countingStore{DB: db.sdb}
	db.keyRepo = rkey.New(c)
	db.strRepo = rstring.New(c, db.keyRepo)
	db.hashRepo = rhash.New(c, db.keyRepo)
	db.listRepo = rlist.New(c, db.keyRepo)
	db.setRepo = rset.New(c, db.keyRepo)
	db.zsetRepo = rzset.New(c, db.keyRepo)
	return &c.runs
}
