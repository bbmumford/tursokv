// Package rhash provides the hash repository for TursoKV.
// Implements Redis hash commands: HGET, HSET, HDEL, HGETALL, HMGET, HMSET, etc.
package rhash

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

// DB is the hash repository for non-transactional operations.
type DB struct {
	db    Querier
	keyDB *rkey.DB
}

// New creates a new hash repository.
func New(db Querier, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// Get retrieves the value of a field in a hash.
// Returns empty string and nil error if field doesn't exist.
func (d *DB) Get(key, field string) (string, error) {
	return hget(d.db, key, field)
}

// GetBytes retrieves the value of a field as bytes.
func (d *DB) GetBytes(key, field string) ([]byte, error) {
	return hgetBytes(d.db, key, field)
}

// Set sets the value of a field in a hash.
// Returns true if field is new, false if updated.
func (d *DB) Set(key, field, value string) (bool, error) {
	return hset(d.db, key, field, value)
}

// SetBytes sets the value of a field as bytes.
func (d *DB) SetBytes(key, field string, value []byte) (bool, error) {
	return hsetBytes(d.db, key, field, value)
}

// SetNX sets the value only if the field doesn't exist.
func (d *DB) SetNX(key, field, value string) (bool, error) {
	return hsetnx(d.db, key, field, value)
}

// MGet retrieves multiple fields from a hash.
func (d *DB) MGet(key string, fields ...string) ([]string, error) {
	return hmget(d.db, key, fields...)
}

// MSet sets multiple fields in a hash.
func (d *DB) MSet(key string, pairs map[string]string) error {
	return hmset(d.db, key, pairs)
}

// Del deletes fields from a hash.
// Returns the number of fields deleted.
func (d *DB) Del(key string, fields ...string) (int, error) {
	return hdel(d.db, key, fields...)
}

// Exists checks if a field exists in a hash.
func (d *DB) Exists(key, field string) (bool, error) {
	return hexists(d.db, key, field)
}

// GetAll retrieves all fields and values from a hash.
func (d *DB) GetAll(key string) (map[string]string, error) {
	return hgetall(d.db, key)
}

// Keys retrieves all field names from a hash.
func (d *DB) Keys(key string) ([]string, error) {
	return hkeys(d.db, key)
}

// Vals retrieves all values from a hash.
func (d *DB) Vals(key string) ([]string, error) {
	return hvals(d.db, key)
}

// Len returns the number of fields in a hash.
func (d *DB) Len(key string) (int, error) {
	return hlen(d.db, key)
}

// IncrBy increments the integer value of a field.
func (d *DB) IncrBy(key, field string, delta int64) (int64, error) {
	return hincrby(d.db, key, field, delta)
}

// IncrByFloat increments the float value of a field.
func (d *DB) IncrByFloat(key, field string, delta float64) (float64, error) {
	return hincrbyfloat(d.db, key, field, delta)
}
