// Package rset provides the set repository for TursoKV.
// Implements Redis set commands: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, etc.
package rset

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rkey"
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// DB is the set repository for non-transactional operations.
type DB struct {
	db    Querier
	keyDB *rkey.DB
}

// New creates a new set repository.
func New(db Querier, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// Add adds members to a set. Returns the number of new members added.
func (d *DB) Add(key string, members ...string) (int, error) {
	return sadd(d.db, key, members...)
}

// Remove removes members from a set. Returns the number removed.
func (d *DB) Remove(key string, members ...string) (int, error) {
	return srem(d.db, key, members...)
}

// IsMember checks if a member exists in a set.
func (d *DB) IsMember(key, member string) (bool, error) {
	return sismember(d.db, key, member)
}

// Members returns all members of a set.
func (d *DB) Members(key string) ([]string, error) {
	return smembers(d.db, key)
}

// Card returns the number of members in a set.
func (d *DB) Card(key string) (int, error) {
	return scard(d.db, key)
}

// Pop removes and returns a random member from a set.
func (d *DB) Pop(key string) (string, error) {
	return spop(d.db, key)
}

// RandMember returns a random member without removing it.
func (d *DB) RandMember(key string) (string, error) {
	return srandmember(d.db, key, 1)
}

// RandMembers returns count random members without removing them.
func (d *DB) RandMembers(key string, count int) ([]string, error) {
	return srandmembers(d.db, key, count)
}

// Union returns the union of all given sets.
func (d *DB) Union(keys ...string) ([]string, error) {
	return sunion(d.db, keys...)
}

// Inter returns the intersection of all given sets.
func (d *DB) Inter(keys ...string) ([]string, error) {
	return sinter(d.db, keys...)
}

// Diff returns the difference between the first set and all successive sets.
func (d *DB) Diff(keys ...string) ([]string, error) {
	return sdiff(d.db, keys...)
}

// Move moves a member from src to dst set.
func (d *DB) Move(src, dst, member string) (bool, error) {
	return smove(d.db, src, dst, member)
}
