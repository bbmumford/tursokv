// Package rlist provides the list repository for TursoKV.
// Implements Redis list commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, etc.
package rlist

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rkey"
	"github.com/bbmumford/tursokv/internal/store"
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// Position constants for list operations.
const posGap = 1.0

// DB is the list repository for calls made outside Update. A call that
// writes with more than one statement runs through db.Atomic, so it keeps
// all of its writes or none; every other call runs each statement in
// autocommit.
type DB struct {
	db    store.Store
	keyDB *rkey.DB
}

// New creates a new list repository.
func New(db store.Store, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// LPush prepends values to a list. Returns the new length.
func (d *DB) LPush(key string, values ...string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return lpush(tx, key, values...) })
}

// RPush appends values to a list. Returns the new length.
func (d *DB) RPush(key string, values ...string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return rpush(tx, key, values...) })
}

// LPop removes and returns the first element of a list.
func (d *DB) LPop(key string) (string, error) {
	return store.Do(d.db, func(tx *sql.Tx) (string, error) { return lpop(tx, key) })
}

// RPop removes and returns the last element of a list.
func (d *DB) RPop(key string) (string, error) {
	return store.Do(d.db, func(tx *sql.Tx) (string, error) { return rpop(tx, key) })
}

// LIndex returns the element at index in the list.
func (d *DB) LIndex(key string, index int) (string, error) {
	return lindex(d.db, key, index)
}

// LRange returns elements from start to stop (inclusive).
func (d *DB) LRange(key string, start, stop int) ([]string, error) {
	return lrange(d.db, key, start, stop)
}

// LLen returns the length of a list.
func (d *DB) LLen(key string) (int, error) {
	return llen(d.db, key)
}

// LSet sets the element at index.
func (d *DB) LSet(key string, index int, value string) error {
	return d.db.Atomic(func(tx *sql.Tx) error { return lset(tx, key, index, value) })
}

// LTrim trims a list to the specified range.
func (d *DB) LTrim(key string, start, stop int) error {
	return d.db.Atomic(func(tx *sql.Tx) error { return ltrim(tx, key, start, stop) })
}

// LInsertBefore inserts value before the pivot.
func (d *DB) LInsertBefore(key, pivot, value string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return linsert(tx, key, pivot, value, true) })
}

// LInsertAfter inserts value after the pivot.
func (d *DB) LInsertAfter(key, pivot, value string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return linsert(tx, key, pivot, value, false) })
}

// LRem removes count occurrences of value from the list.
// count > 0: Remove from head to tail.
// count < 0: Remove from tail to head.
// count = 0: Remove all occurrences.
func (d *DB) LRem(key string, count int, value string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return lrem(tx, key, count, value) })
}
