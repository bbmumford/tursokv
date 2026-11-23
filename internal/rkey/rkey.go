// Package rkey provides the key repository for managing key metadata.
// Keys are the foundation of all data types in TursoKV.
package rkey

import (
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/bbmumford/tursokv/internal/store"
)

// Type represents the data type stored for a key.
type Type int

const (
	TypeString Type = 1
	TypeList   Type = 2
	TypeSet    Type = 3
	TypeHash   Type = 4
	TypeZSet   Type = 5
)

// String returns the string representation of the type.
func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeHash:
		return "hash"
	case TypeZSet:
		return "zset"
	default:
		return "unknown"
	}
}

// Key represents metadata about a stored key.
type Key struct {
	ID      int64
	Key     string
	Type    Type
	Version int64
	ETime   *int64 // Expiration time in Unix milliseconds
	MTime   int64  // Modification time in Unix milliseconds
}

// Expired returns true if the key has expired.
func (k Key) Expired() bool {
	if k.ETime == nil {
		return false
	}
	return *k.ETime <= time.Now().UnixMilli()
}

// TTL returns the remaining time-to-live in milliseconds.
// Returns -1 if no expiration, -2 if expired or not found.
func (k Key) TTL() int64 {
	if k.ETime == nil {
		return -1
	}
	ttl := *k.ETime - time.Now().UnixMilli()
	if ttl <= 0 {
		return -2
	}
	return ttl
}

// Errors
var (
	ErrNotFound  = errors.New("key not found")
	ErrWrongType = errors.New("wrong type for key")
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// DB is the key repository for calls made outside Update. Each call that
// writes (Delete, Expire, ExpireAt, Persist) is one statement run through
// db.Atomic; reads run in autocommit.
//
// WHY WRITES GO THROUGH ATOMIC. The DB-path writes of rstring, rhash, rlist,
// rset and rzset run as transactions through Atomic, and those that add
// values upsert the key's rkey row: rstring's setBytes, rhash's hsetBytes and
// the getOrCreateKey of rlist, rset and rzset. In WriteModeConcurrent an
// autocommit statement that writes a row such an open transaction has
// written fails with a write-write conflict and is not run again: Expire and
// Persist update the rkey row, and Delete removes it and, through ON DELETE
// CASCADE, the key's value rows. Through Atomic the call is run again, as
// those writes are.
type DB struct {
	db store.Store
}

// New creates a new key repository.
func New(db store.Store) *DB {
	return &DB{db: db}
}

// Tx creates a transactional key repository.
func (d *DB) Tx(tx *sql.Tx) *Tx {
	return &Tx{db: tx}
}

// Get retrieves key metadata.
func (d *DB) Get(key string) (Key, error) {
	return get(d.db, key)
}

// Exists checks if a key exists (and is not expired).
func (d *DB) Exists(keys ...string) (int, error) {
	return exists(d.db, keys...)
}

// Delete removes keys.
func (d *DB) Delete(keys ...string) (int, error) {
	return store.Do(d.db, func(tx *sql.Tx) (int, error) { return del(tx, keys...) })
}

// Expire sets a TTL on a key.
func (d *DB) Expire(key string, ttl time.Duration) (bool, error) {
	etime := time.Now().Add(ttl).UnixMilli()
	return store.Do(d.db, func(tx *sql.Tx) (bool, error) { return expire(tx, key, etime) })
}

// ExpireAt sets an absolute expiration time.
func (d *DB) ExpireAt(key string, at time.Time) (bool, error) {
	return store.Do(d.db, func(tx *sql.Tx) (bool, error) { return expire(tx, key, at.UnixMilli()) })
}

// Persist removes the expiration from a key.
func (d *DB) Persist(key string) (bool, error) {
	return store.Do(d.db, func(tx *sql.Tx) (bool, error) { return persist(tx, key) })
}

// Keys returns all keys matching a pattern.
func (d *DB) Keys(pattern string) ([]string, error) {
	return keys(d.db, pattern)
}

// Tx is the key repository for transactional operations.
type Tx struct {
	db Querier
}

// Get retrieves key metadata.
func (tx *Tx) Get(key string) (Key, error) {
	return get(tx.db, key)
}

// GetOrCreate gets existing key or creates new with specified type.
func (tx *Tx) GetOrCreate(key string, typ Type) (Key, error) {
	return getOrCreate(tx.db, key, typ)
}

// Exists checks if a key exists (and is not expired).
func (tx *Tx) Exists(keys ...string) (int, error) {
	return exists(tx.db, keys...)
}

// Delete removes keys.
func (tx *Tx) Delete(keys ...string) (int, error) {
	return del(tx.db, keys...)
}

// Expire sets a TTL on a key.
func (tx *Tx) Expire(key string, ttl time.Duration) (bool, error) {
	etime := time.Now().Add(ttl).UnixMilli()
	return expire(tx.db, key, etime)
}

// ExpireAt sets an absolute expiration time.
func (tx *Tx) ExpireAt(key string, at time.Time) (bool, error) {
	return expire(tx.db, key, at.UnixMilli())
}

// Persist removes the expiration from a key.
func (tx *Tx) Persist(key string) (bool, error) {
	return persist(tx.db, key)
}

// Keys returns all keys matching a pattern.
func (tx *Tx) Keys(pattern string) ([]string, error) {
	return keys(tx.db, pattern)
}

// --- Implementation functions ---

func get(db Querier, key string) (Key, error) {
	const query = `
		SELECT id, key, type, version, etime, mtime
		FROM rkey
		WHERE key = ? AND (etime IS NULL OR etime > ?)
	`
	now := time.Now().UnixMilli()
	var k Key
	err := db.QueryRow(query, key, now).Scan(
		&k.ID, &k.Key, &k.Type, &k.Version, &k.ETime, &k.MTime,
	)
	if err == sql.ErrNoRows {
		return Key{}, ErrNotFound
	}
	return k, err
}

// getOrCreate answers key's live row of type typ without writing it, refuses a
// live row of another type with ErrWrongType, and otherwise claims the key
// (Claim): a key past its expiry starts afresh, so the plain insert of a new
// row never meets the expired row's unique key.
func getOrCreate(db Querier, key string, typ Type) (Key, error) {
	k, err := get(db, key)
	if err == nil {
		if k.Type != typ {
			return Key{}, ErrWrongType
		}
		return k, nil
	}
	if err != ErrNotFound {
		return Key{}, err
	}
	if _, err := Claim(db, key, typ, false); err != nil {
		return Key{}, err
	}
	return get(db, key)
}

// valueTables are the tables that hold a key's values, each by keyId.
var valueTables = []string{"rstring", "rlist", "rset", "rhash", "rzset"}

// Claim answers the id of key's row for a write of type typ, so that the
// values the caller then writes under the id are the ones reads find. Every
// value type's write path calls it inside the write's transaction:
//   - no row: a new row of typ;
//   - a row whose expiry has passed: that key no longer exists, so its old
//     values are deleted and the row is reused as a new key of typ with no
//     expiry (reads filter expired rows, and a write that kept the old expiry
//     would be invisible);
//   - a live row of typ: that row, its version and mtime advanced and its
//     expiry kept;
//   - a live row of another type: ErrWrongType, unless replace is set (SET
//     replaces a key of any type), in which case the old values are deleted
//     and the row becomes typ.
func Claim(db Querier, key string, typ Type, replace bool) (int64, error) {
	now := time.Now().UnixMilli()
	var (
		id    int64
		have  Type
		etime sql.NullInt64
	)
	err := db.QueryRow(`SELECT id, type, etime FROM rkey WHERE key = ?`, key).Scan(&id, &have, &etime)
	if err == sql.ErrNoRows {
		err = db.QueryRow(`INSERT INTO rkey (key, type, version, mtime) VALUES (?, ?, 0, ?) RETURNING id`, key, typ, now).Scan(&id)
		return id, err
	}
	if err != nil {
		return 0, err
	}
	expired := etime.Valid && etime.Int64 <= now
	if !expired && have != typ && !replace {
		return 0, ErrWrongType
	}
	if expired || have != typ {
		for _, table := range valueTables {
			if _, err := db.Exec(`DELETE FROM `+table+` WHERE keyId = ?`, id); err != nil {
				return 0, err
			}
		}
		_, err = db.Exec(`UPDATE rkey SET type = ?, version = version + 1, etime = NULL, mtime = ? WHERE id = ?`, typ, now, id)
		return id, err
	}
	_, err = db.Exec(`UPDATE rkey SET version = version + 1, mtime = ? WHERE id = ?`, now, id)
	return id, err
}

func exists(db Querier, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	const query = `
		SELECT COUNT(*) FROM rkey
		WHERE key IN (%s) AND (etime IS NULL OR etime > ?)
	`
	placeholders := makePlaceholders(len(keys))
	now := time.Now().UnixMilli()

	args := make([]any, len(keys)+1)
	for i, k := range keys {
		args[i] = k
	}
	args[len(keys)] = now

	var count int
	err := db.QueryRow(sprintf(query, placeholders), args...).Scan(&count)
	return count, err
}

func del(db Querier, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	const query = `DELETE FROM rkey WHERE key IN (%s)`
	placeholders := makePlaceholders(len(keys))

	args := make([]any, len(keys))
	for i, k := range keys {
		args[i] = k
	}

	result, err := db.Exec(sprintf(query, placeholders), args...)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

func expire(db Querier, key string, etime int64) (bool, error) {
	const query = `
		UPDATE rkey SET etime = ?, mtime = ?
		WHERE key = ? AND (etime IS NULL OR etime > ?)
	`
	now := time.Now().UnixMilli()
	result, err := db.Exec(query, etime, now, key, now)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func persist(db Querier, key string) (bool, error) {
	const query = `
		UPDATE rkey SET etime = NULL, mtime = ?
		WHERE key = ? AND etime IS NOT NULL AND etime > ?
	`
	now := time.Now().UnixMilli()
	result, err := db.Exec(query, now, key, now)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func keys(db Querier, pattern string) ([]string, error) {
	// Convert Redis glob pattern to SQL LIKE pattern
	likePattern := globToLike(pattern)

	const query = `
		SELECT key FROM rkey
		WHERE key LIKE ? ESCAPE '\'
		AND (etime IS NULL OR etime > ?)
		ORDER BY key
	`
	now := time.Now().UnixMilli()
	rows, err := db.Query(query, likePattern, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		result = append(result, k)
	}
	return result, rows.Err()
}

// --- Helper functions ---

func makePlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	if n == 1 {
		return "?"
	}
	s := make([]byte, n*2-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			s[i*2-1] = ','
		}
		s[i*2] = '?'
	}
	return string(s)
}

// sprintf substitutes arg for the first "%s" in format and leaves every other
// byte as it is. exists and del use it to put makePlaceholders' "?,?,..." into
// their IN (%s) queries, whose "%s" is not at the end of the text; the engine's
// parser refuses a query that still holds a '%', so the whole two-byte verb
// must be replaced wherever it sits.
func sprintf(format, arg string) string {
	return strings.Replace(format, "%s", arg, 1)
}

// globToLike converts Redis glob pattern to SQL LIKE pattern.
// * -> %
// ? -> _
// [abc] -> [abc] (already LIKE compatible)
// Escapes % and _ in the original pattern.
func globToLike(pattern string) string {
	result := make([]byte, 0, len(pattern)*2)
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			result = append(result, '%')
		case '?':
			result = append(result, '_')
		case '%':
			result = append(result, '\\', '%')
		case '_':
			result = append(result, '\\', '_')
		case '\\':
			result = append(result, '\\', '\\')
		default:
			result = append(result, pattern[i])
		}
	}
	return string(result)
}
