// Package rzset provides the sorted set repository for TursoKV.
// Implements Redis sorted set commands: ZADD, ZREM, ZRANGE, ZSCORE, ZRANK, etc.
package rzset

import (
	"database/sql"
	"math"

	"github.com/bbmumford/tursokv/internal/rkey"
	"github.com/bbmumford/tursokv/internal/store"
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// ScoreMember represents a member with its score.
type ScoreMember struct {
	Member string
	Score  float64
}

// ZAddOptions for ZADD command variants.
type ZAddOptions struct {
	// NX: Only add new elements (don't update existing scores)
	NX bool
	// XX: Only update existing elements (don't add new members)
	XX bool
	// GT: Only update when new score > current score
	GT bool
	// LT: Only update when new score < current score
	LT bool
	// CH: Return number of changed elements (added + updated)
	CH bool
}

// DB is the sorted set repository for calls made outside Update. A call that
// writes with more than one statement runs through db.Atomic, so it keeps
// all of its writes or none; every other call runs each statement in
// autocommit.
type DB struct {
	db    store.Store
	keyDB *rkey.DB
}

// New creates a new sorted set repository.
func New(db store.Store, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// Add adds members with scores to a sorted set.
// Returns the number of new members added.
func (d *DB) Add(key string, members ...ScoreMember) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return zadd(tx, key, nil, members...) })
}

// AddWithOptions adds members with options.
func (d *DB) AddWithOptions(key string, opts *ZAddOptions, members ...ScoreMember) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return zadd(tx, key, opts, members...) })
}

// Remove removes members from a sorted set.
func (d *DB) Remove(key string, members ...string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return zrem(tx, key, members...) })
}

// Score returns the score of a member.
func (d *DB) Score(key, member string) (float64, bool, error) {
	return zscore(d.db, key, member)
}

// Rank returns the rank of a member (0-based, lowest score first).
func (d *DB) Rank(key, member string) (int, bool, error) {
	return zrank(d.db, key, member)
}

// RevRank returns the reverse rank (0-based, highest score first).
func (d *DB) RevRank(key, member string) (int, bool, error) {
	return zrevrank(d.db, key, member)
}

// Card returns the number of members.
func (d *DB) Card(key string) (int, error) {
	return zcard(d.db, key)
}

// Count returns count of members with scores between min and max.
func (d *DB) Count(key string, min, max float64) (int, error) {
	return zcount(d.db, key, min, max)
}

// Range returns members by rank range (inclusive).
func (d *DB) Range(key string, start, stop int) ([]string, error) {
	return zrange(d.db, key, start, stop)
}

// RangeWithScores returns members with scores by rank range.
func (d *DB) RangeWithScores(key string, start, stop int) ([]ScoreMember, error) {
	return zrangeWithScores(d.db, key, start, stop)
}

// RevRange returns members by rank range (highest first).
func (d *DB) RevRange(key string, start, stop int) ([]string, error) {
	return zrevrange(d.db, key, start, stop)
}

// RevRangeWithScores returns members with scores by rank range (highest first).
func (d *DB) RevRangeWithScores(key string, start, stop int) ([]ScoreMember, error) {
	return zrevrangeWithScores(d.db, key, start, stop)
}

// RangeByScore returns members with scores between min and max.
func (d *DB) RangeByScore(key string, min, max float64) ([]string, error) {
	return zrangebyscore(d.db, key, min, max, -1, 0)
}

// RangeByScoreWithLimit returns members with scores in range with offset and count.
func (d *DB) RangeByScoreWithLimit(key string, min, max float64, offset, count int) ([]string, error) {
	return zrangebyscore(d.db, key, min, max, count, offset)
}

// IncrBy increments the score of a member.
func (d *DB) IncrBy(key, member string, delta float64) (float64, error) {
	return store.Do(d.db, func(tx *sql.Tx) (float64, error) { return zincrby(tx, key, member, delta) })
}

// PopMin removes and returns the member with lowest score.
func (d *DB) PopMin(key string) (ScoreMember, bool, error) {
	return d.pop(key, zpopmin)
}

// PopMax removes and returns the member with highest score.
func (d *DB) PopMax(key string) (ScoreMember, bool, error) {
	return d.pop(key, zpopmax)
}

// popped carries a pop's two results out of store.Do.
type popped struct {
	member ScoreMember
	found  bool
}

// pop runs zpopmin or zpopmax in one transaction: the read that picks the
// member and the delete that removes it commit together, so two callers
// cannot both pop the same member.
func (d *DB) pop(key string, fn func(Querier, string) (ScoreMember, bool, error)) (ScoreMember, bool, error) {
	r, err := store.Do(d.db, func(tx *sql.Tx) (popped, error) {
		m, found, err := fn(tx, key)
		return popped{m, found}, err
	})
	return r.member, r.found, err
}

// RemRangeByRank removes members by rank range.
func (d *DB) RemRangeByRank(key string, start, stop int) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return zremrangebyrank(tx, key, start, stop) })
}

// RemRangeByScore removes members by score range.
func (d *DB) RemRangeByScore(key string, min, max float64) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return zremrangebyscore(tx, key, min, max) })
}

// NegInf represents negative infinity for score ranges.
var NegInf = math.Inf(-1)

// PosInf represents positive infinity for score ranges.
var PosInf = math.Inf(1)
