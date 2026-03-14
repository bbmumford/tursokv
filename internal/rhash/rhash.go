// Package rhash provides the hash repository for TursoKV.
// Implements Redis hash commands: HGET, HSET, HDEL, HGETALL, HMGET, HMSET, etc.
package rhash

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

// Tx is the hash repository for transactional operations.
type Tx struct {
	db    *sql.Tx
	keyTx *rkey.Tx
}

// NewTx creates a new transactional hash repository.
func NewTx(tx *sql.Tx, keyTx *rkey.Tx) *Tx {
	return &Tx{db: tx, keyTx: keyTx}
}

// Get retrieves the value of a field in a hash.
func (tx *Tx) Get(key, field string) (string, error) {
	return hget(tx.db, key, field)
}

// GetBytes retrieves the value of a field as bytes.
func (tx *Tx) GetBytes(key, field string) ([]byte, error) {
	return hgetBytes(tx.db, key, field)
}

// Set sets the value of a field in a hash.
func (tx *Tx) Set(key, field, value string) (bool, error) {
	return hsetTx(tx.db, tx.keyTx, key, field, value)
}

// SetBytes sets the value of a field as bytes.
func (tx *Tx) SetBytes(key, field string, value []byte) (bool, error) {
	return hsetBytesTx(tx.db, tx.keyTx, key, field, value)
}

// SetNX sets the value only if the field doesn't exist.
func (tx *Tx) SetNX(key, field, value string) (bool, error) {
	return hsetnxTx(tx.db, tx.keyTx, key, field, value)
}

// MGet retrieves multiple fields from a hash.
func (tx *Tx) MGet(key string, fields ...string) ([]string, error) {
	return hmget(tx.db, key, fields...)
}

// MSet sets multiple fields in a hash.
func (tx *Tx) MSet(key string, pairs map[string]string) error {
	return hmsetTx(tx.db, tx.keyTx, key, pairs)
}

// Del deletes fields from a hash.
func (tx *Tx) Del(key string, fields ...string) (int, error) {
	return hdelTx(tx.db, tx.keyTx, key, fields...)
}

// Exists checks if a field exists in a hash.
func (tx *Tx) Exists(key, field string) (bool, error) {
	return hexists(tx.db, key, field)
}

// GetAll retrieves all fields and values from a hash.
func (tx *Tx) GetAll(key string) (map[string]string, error) {
	return hgetall(tx.db, key)
}

// Keys retrieves all field names from a hash.
func (tx *Tx) Keys(key string) ([]string, error) {
	return hkeys(tx.db, key)
}

// Vals retrieves all values from a hash.
func (tx *Tx) Vals(key string) ([]string, error) {
	return hvals(tx.db, key)
}

// Len returns the number of fields in a hash.
func (tx *Tx) Len(key string) (int, error) {
	return hlen(tx.db, key)
}

// IncrBy increments the integer value of a field.
func (tx *Tx) IncrBy(key, field string, delta int64) (int64, error) {
	return hincrbyTx(tx.db, tx.keyTx, key, field, delta)
}

// IncrByFloat increments the float value of a field.
func (tx *Tx) IncrByFloat(key, field string, delta float64) (float64, error) {
	return hincrbyfloatTx(tx.db, tx.keyTx, key, field, delta)
}

// --- Implementation functions ---

func hget(db Querier, key, field string) (string, error) {
	b, err := hgetBytes(db, key, field)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func hgetBytes(db Querier, key, field string) ([]byte, error) {
	const query = `
		SELECT h.value FROM rhash h
		JOIN rkey k ON k.id = h.key_id
		WHERE k.key = ? AND k.type = ? AND h.field = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var value []byte
	err := db.QueryRow(query, key, rkey.TypeHash, field, now).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return value, err
}

func hset(db Querier, key, field, value string) (bool, error) {
	return hsetBytes(db, key, field, []byte(value))
}

func hsetBytes(db Querier, key, field string, value []byte) (bool, error) {
	now := time.Now().UnixMilli()

	// Upsert key
	const upsertKey = `
		INSERT INTO rkey (key, type, version, mtime)
		VALUES (?, ?, 0, ?)
		ON CONFLICT(key) DO UPDATE SET
			version = version + 1,
			mtime = excluded.mtime
		RETURNING id
	`
	var keyID int64
	if err := db.QueryRow(upsertKey, key, rkey.TypeHash, now).Scan(&keyID); err != nil {
		return false, err
	}

	// Check if field exists
	const checkField = `SELECT 1 FROM rhash WHERE key_id = ? AND field = ?`
	var exists int
	isNew := db.QueryRow(checkField, keyID, field).Scan(&exists) == sql.ErrNoRows

	// Upsert field
	const upsertField = `
		INSERT INTO rhash (key_id, field, value)
		VALUES (?, ?, ?)
		ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value
	`
	_, err := db.Exec(upsertField, keyID, field, value)
	return isNew, err
}

func hsetTx(db Querier, keyTx *rkey.Tx, key, field, value string) (bool, error) {
	return hsetBytesTx(db, keyTx, key, field, []byte(value))
}

func hsetBytesTx(db Querier, keyTx *rkey.Tx, key, field string, value []byte) (bool, error) {
	k, err := keyTx.GetOrCreate(key, rkey.TypeHash)
	if err != nil {
		return false, err
	}

	// Check if field exists
	const checkField = `SELECT 1 FROM rhash WHERE key_id = ? AND field = ?`
	var exists int
	isNew := db.QueryRow(checkField, k.ID, field).Scan(&exists) == sql.ErrNoRows

	// Upsert field
	const upsertField = `
		INSERT INTO rhash (key_id, field, value)
		VALUES (?, ?, ?)
		ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value
	`
	_, err = db.Exec(upsertField, k.ID, field, value)
	return isNew, err
}

func hsetnx(db Querier, key, field, value string) (bool, error) {
	// Check if field exists
	exists, err := hexists(db, key, field)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	_, err = hset(db, key, field, value)
	return err == nil, err
}

func hsetnxTx(db Querier, keyTx *rkey.Tx, key, field, value string) (bool, error) {
	exists, err := hexists(db, key, field)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	_, err = hsetBytesTx(db, keyTx, key, field, []byte(value))
	return err == nil, err
}

func hmget(db Querier, key string, fields ...string) ([]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	results := make([]string, len(fields))
	for i, field := range fields {
		val, err := hget(db, key, field)
		if err != nil {
			return nil, err
		}
		results[i] = val
	}
	return results, nil
}

func hmset(db Querier, key string, pairs map[string]string) error {
	for field, value := range pairs {
		if _, err := hset(db, key, field, value); err != nil {
			return err
		}
	}
	return nil
}

func hmsetTx(db Querier, keyTx *rkey.Tx, key string, pairs map[string]string) error {
	for field, value := range pairs {
		if _, err := hsetBytesTx(db, keyTx, key, field, []byte(value)); err != nil {
			return err
		}
	}
	return nil
}

func hdel(db Querier, key string, fields ...string) (int, error) {
	if len(fields) == 0 {
		return 0, nil
	}

	const getKeyID = `
		SELECT id FROM rkey
		WHERE key = ? AND type = ? AND (etime IS NULL OR etime > ?)
	`
	now := time.Now().UnixMilli()
	var keyID int64
	if err := db.QueryRow(getKeyID, key, rkey.TypeHash, now).Scan(&keyID); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	deleted := 0
	for _, field := range fields {
		const delField = `DELETE FROM rhash WHERE key_id = ? AND field = ?`
		res, err := db.Exec(delField, keyID, field)
		if err != nil {
			return deleted, err
		}
		n, _ := res.RowsAffected()
		deleted += int(n)
	}

	return deleted, nil
}

func hdelTx(db Querier, keyTx *rkey.Tx, key string, fields ...string) (int, error) {
	if len(fields) == 0 {
		return 0, nil
	}

	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	deleted := 0
	for _, field := range fields {
		const delField = `DELETE FROM rhash WHERE key_id = ? AND field = ?`
		res, err := db.Exec(delField, k.ID, field)
		if err != nil {
			return deleted, err
		}
		n, _ := res.RowsAffected()
		deleted += int(n)
	}

	return deleted, nil
}

func hexists(db Querier, key, field string) (bool, error) {
	const query = `
		SELECT 1 FROM rhash h
		JOIN rkey k ON k.id = h.key_id
		WHERE k.key = ? AND k.type = ? AND h.field = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var exists int
	err := db.QueryRow(query, key, rkey.TypeHash, field, now).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func hgetall(db Querier, key string) (map[string]string, error) {
	const query = `
		SELECT h.field, h.value FROM rhash h
		JOIN rkey k ON k.id = h.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	rows, err := db.Query(query, key, rkey.TypeHash, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var field string
		var value []byte
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		result[field] = string(value)
	}
	return result, rows.Err()
}

func hkeys(db Querier, key string) ([]string, error) {
	const query = `
		SELECT h.field FROM rhash h
		JOIN rkey k ON k.id = h.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	rows, err := db.Query(query, key, rkey.TypeHash, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []string
	for rows.Next() {
		var field string
		if err := rows.Scan(&field); err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, rows.Err()
}

func hvals(db Querier, key string) ([]string, error) {
	const query = `
		SELECT h.value FROM rhash h
		JOIN rkey k ON k.id = h.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	rows, err := db.Query(query, key, rkey.TypeHash, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, string(value))
	}
	return values, rows.Err()
}

func hlen(db Querier, key string) (int, error) {
	const query = `
		SELECT COUNT(*) FROM rhash h
		JOIN rkey k ON k.id = h.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var count int
	err := db.QueryRow(query, key, rkey.TypeHash, now).Scan(&count)
	return count, err
}

func hincrby(db Querier, key, field string, delta int64) (int64, error) {
	// Get current value
	val, err := hget(db, key, field)
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
	_, err = hset(db, key, field, strconv.FormatInt(newVal, 10))
	return newVal, err
}

func hincrbyTx(db Querier, keyTx *rkey.Tx, key, field string, delta int64) (int64, error) {
	val, err := hget(db, key, field)
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
	_, err = hsetBytesTx(db, keyTx, key, field, []byte(strconv.FormatInt(newVal, 10)))
	return newVal, err
}

func hincrbyfloat(db Querier, key, field string, delta float64) (float64, error) {
	val, err := hget(db, key, field)
	if err != nil {
		return 0, err
	}

	var current float64
	if val != "" {
		current, err = strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, err
		}
	}

	newVal := current + delta
	_, err = hset(db, key, field, strconv.FormatFloat(newVal, 'f', -1, 64))
	return newVal, err
}

func hincrbyfloatTx(db Querier, keyTx *rkey.Tx, key, field string, delta float64) (float64, error) {
	val, err := hget(db, key, field)
	if err != nil {
		return 0, err
	}

	var current float64
	if val != "" {
		current, err = strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, err
		}
	}

	newVal := current + delta
	_, err = hsetBytesTx(db, keyTx, key, field, []byte(strconv.FormatFloat(newVal, 'f', -1, 64)))
	return newVal, err
}
