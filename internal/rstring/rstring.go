// Package rstring provides the string repository for TursoKV.
// Implements Redis string commands: GET, SET, MGET, MSET, INCR, etc.
package rstring

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// SetOptions for SET command variants.
type SetOptions struct {
	// TTL sets expiration time.
	TTL time.Duration

	// IfNotExists only sets if key doesn't exist (NX).
	IfNotExists bool

	// IfExists only sets if key already exists (XX).
	IfExists bool

	// GetOld returns the old value before setting.
	GetOld bool
}

// DB is the string repository for non-transactional operations.
type DB struct {
	db    Querier
	keyDB *rkey.DB
}

// New creates a new string repository.
func New(db Querier, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// Get retrieves the value of a key.
// Returns empty string and nil error if key doesn't exist.
func (d *DB) Get(key string) (string, error) {
	return getString(d.db, key)
}

// GetBytes retrieves the value of a key as bytes.
func (d *DB) GetBytes(key string) ([]byte, error) {
	return getBytes(d.db, key)
}

// Set sets the value of a key.
func (d *DB) Set(key, value string) error {
	return setString(d.db, key, value, nil)
}

// SetBytes sets the value of a key as bytes.
func (d *DB) SetBytes(key string, value []byte) error {
	return setBytes(d.db, key, value, nil)
}

// SetEx sets the value with a TTL.
func (d *DB) SetEx(key, value string, ttl time.Duration) error {
	return setString(d.db, key, value, &SetOptions{TTL: ttl})
}

// SetNX sets the value only if the key doesn't exist.
func (d *DB) SetNX(key, value string) (bool, error) {
	return setNX(d.db, key, value)
}

// MGet retrieves multiple keys.
func (d *DB) MGet(keys ...string) ([]string, error) {
	return mget(d.db, keys...)
}

// MSet sets multiple key-value pairs.
func (d *DB) MSet(pairs map[string]string) error {
	return mset(d.db, pairs)
}

// Incr increments the integer value of a key by 1.
func (d *DB) Incr(key string) (int64, error) {
	return incrBy(d.db, key, 1)
}

// IncrBy increments the integer value of a key by delta.
func (d *DB) IncrBy(key string, delta int64) (int64, error) {
	return incrBy(d.db, key, delta)
}

// Decr decrements the integer value of a key by 1.
func (d *DB) Decr(key string) (int64, error) {
	return incrBy(d.db, key, -1)
}

// Append appends a value to the key.
func (d *DB) Append(key, value string) (int, error) {
	return appendStr(d.db, key, value)
}

// Strlen returns the length of the value stored at key.
func (d *DB) Strlen(key string) (int, error) {
	return strlen(d.db, key)
}

// Tx is the string repository for transactional operations.
type Tx struct {
	db    *sql.Tx
	keyTx *rkey.Tx
}

// NewTx creates a new transactional string repository.
func NewTx(tx *sql.Tx, keyTx *rkey.Tx) *Tx {
	return &Tx{db: tx, keyTx: keyTx}
}

// Get retrieves the value of a key.
func (tx *Tx) Get(key string) (string, error) {
	return getString(tx.db, key)
}

// GetBytes retrieves the value of a key as bytes.
func (tx *Tx) GetBytes(key string) ([]byte, error) {
	return getBytes(tx.db, key)
}

// Set sets the value of a key.
func (tx *Tx) Set(key, value string) error {
	return setStringTx(tx.db, tx.keyTx, key, value, nil)
}

// SetBytes sets the value of a key as bytes.
func (tx *Tx) SetBytes(key string, value []byte) error {
	return setBytesTx(tx.db, tx.keyTx, key, value, nil)
}

// SetEx sets the value with a TTL.
func (tx *Tx) SetEx(key, value string, ttl time.Duration) error {
	return setStringTx(tx.db, tx.keyTx, key, value, &SetOptions{TTL: ttl})
}

// SetNX sets the value only if the key doesn't exist.
func (tx *Tx) SetNX(key, value string) (bool, error) {
	return setNXTx(tx.db, tx.keyTx, key, value)
}

// MGet retrieves multiple keys.
func (tx *Tx) MGet(keys ...string) ([]string, error) {
	return mget(tx.db, keys...)
}

// MSet sets multiple key-value pairs.
func (tx *Tx) MSet(pairs map[string]string) error {
	return msetTx(tx.db, tx.keyTx, pairs)
}

// Incr increments the integer value of a key by 1.
func (tx *Tx) Incr(key string) (int64, error) {
	return incrByTx(tx.db, tx.keyTx, key, 1)
}

// IncrBy increments the integer value of a key by delta.
func (tx *Tx) IncrBy(key string, delta int64) (int64, error) {
	return incrByTx(tx.db, tx.keyTx, key, delta)
}

// Decr decrements the integer value of a key by 1.
func (tx *Tx) Decr(key string) (int64, error) {
	return incrByTx(tx.db, tx.keyTx, key, -1)
}

// Append appends a value to the key.
func (tx *Tx) Append(key, value string) (int, error) {
	return appendStrTx(tx.db, tx.keyTx, key, value)
}

// Strlen returns the length of the value stored at key.
func (tx *Tx) Strlen(key string) (int, error) {
	return strlen(tx.db, key)
}

// --- Implementation functions ---

func getString(db Querier, key string) (string, error) {
	b, err := getBytes(db, key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func getBytes(db Querier, key string) ([]byte, error) {
	const query = `
		SELECT s.value FROM rstring s
		JOIN rkey k ON k.id = s.key_id
		WHERE k.key = ? AND k.type = ? AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var value []byte
	err := db.QueryRow(query, key, rkey.TypeString, now).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, nil // Key not found is not an error, just empty
	}
	return value, err
}

func setString(db Querier, key, value string, opts *SetOptions) error {
	return setBytes(db, key, []byte(value), opts)
}

func setBytes(db Querier, key string, value []byte, opts *SetOptions) error {
	// Use upsert (INSERT OR REPLACE) for simplicity
	now := time.Now().UnixMilli()
	var etime *int64
	if opts != nil && opts.TTL > 0 {
		e := now + opts.TTL.Milliseconds()
		etime = &e
	}

	const upsertKey = `
		INSERT INTO rkey (key, type, version, etime, mtime)
		VALUES (?, ?, 0, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			version = version + 1,
			etime = excluded.etime,
			mtime = excluded.mtime
		RETURNING id
	`
	var keyID int64
	err := db.QueryRow(upsertKey, key, rkey.TypeString, etime, now).Scan(&keyID)
	if err != nil {
		return err
	}

	const upsertValue = `
		INSERT INTO rstring (key_id, value)
		VALUES (?, ?)
		ON CONFLICT(key_id) DO UPDATE SET value = excluded.value
	`
	_, err = db.Exec(upsertValue, keyID, value)
	return err
}

func setStringTx(db Querier, keyTx *rkey.Tx, key, value string, opts *SetOptions) error {
	return setBytesTx(db, keyTx, key, []byte(value), opts)
}

func setBytesTx(db Querier, keyTx *rkey.Tx, key string, value []byte, opts *SetOptions) error {
	k, err := keyTx.GetOrCreate(key, rkey.TypeString)
	if err != nil {
		return err
	}

	if opts != nil && opts.TTL > 0 {
		etime := time.Now().Add(opts.TTL)
		if _, err := keyTx.ExpireAt(key, etime); err != nil {
			return err
		}
	}

	const upsert = `
		INSERT INTO rstring (key_id, value)
		VALUES (?, ?)
		ON CONFLICT(key_id) DO UPDATE SET value = excluded.value
	`
	_, err = db.Exec(upsert, k.ID, value)
	return err
}

func setNX(db Querier, key, value string) (bool, error) {
	// Check if key exists
	const check = `
		SELECT 1 FROM rkey
		WHERE key = ? AND (etime IS NULL OR etime > ?)
		LIMIT 1
	`
	now := time.Now().UnixMilli()
	var exists int
	err := db.QueryRow(check, key, now).Scan(&exists)
	if err == nil {
		return false, nil // Key exists
	}
	if err != sql.ErrNoRows {
		return false, err
	}

	// Key doesn't exist, set it
	return true, setString(db, key, value, nil)
}

func setNXTx(db Querier, keyTx *rkey.Tx, key, value string) (bool, error) {
	_, err := keyTx.Get(key)
	if err == nil {
		return false, nil // Key exists
	}
	if err != rkey.ErrNotFound {
		return false, err
	}
	return true, setStringTx(db, keyTx, key, value, nil)
}

func mget(db Querier, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	result := make([]string, len(keys))
	for i, key := range keys {
		val, err := getString(db, key)
		if err != nil {
			return nil, err
		}
		result[i] = val
	}
	return result, nil
}

func mset(db Querier, pairs map[string]string) error {
	for key, value := range pairs {
		if err := setString(db, key, value, nil); err != nil {
			return err
		}
	}
	return nil
}

func msetTx(db Querier, keyTx *rkey.Tx, pairs map[string]string) error {
	for key, value := range pairs {
		if err := setStringTx(db, keyTx, key, value, nil); err != nil {
			return err
		}
	}
	return nil
}

func incrBy(db Querier, key string, delta int64) (int64, error) {
	// Get current value
	val, err := getString(db, key)
	if err != nil {
		return 0, err
	}

	var current int64
	if val != "" {
		current, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	newVal := current + delta
	if err := setString(db, key, strconv.FormatInt(newVal, 10), nil); err != nil {
		return 0, err
	}
	return newVal, nil
}

func incrByTx(db Querier, keyTx *rkey.Tx, key string, delta int64) (int64, error) {
	val, err := getString(db, key)
	if err != nil {
		return 0, err
	}

	var current int64
	if val != "" {
		current, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	newVal := current + delta
	if err := setStringTx(db, keyTx, key, strconv.FormatInt(newVal, 10), nil); err != nil {
		return 0, err
	}
	return newVal, nil
}

func appendStr(db Querier, key, value string) (int, error) {
	current, err := getString(db, key)
	if err != nil {
		return 0, err
	}
	newVal := current + value
	if err := setString(db, key, newVal, nil); err != nil {
		return 0, err
	}
	return len(newVal), nil
}

func appendStrTx(db Querier, keyTx *rkey.Tx, key, value string) (int, error) {
	current, err := getString(db, key)
	if err != nil {
		return 0, err
	}
	newVal := current + value
	if err := setStringTx(db, keyTx, key, newVal, nil); err != nil {
		return 0, err
	}
	return len(newVal), nil
}

func strlen(db Querier, key string) (int, error) {
	val, err := getBytes(db, key)
	if err != nil {
		return 0, err
	}
	return len(val), nil
}
