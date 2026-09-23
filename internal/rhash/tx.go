package rhash

import (
	"database/sql"
	"strconv"

	"github.com/bbmumford/tursokv/internal/rkey"
)

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

func hmsetTx(db Querier, keyTx *rkey.Tx, key string, pairs map[string]string) error {
	for field, value := range pairs {
		if _, err := hsetBytesTx(db, keyTx, key, field, []byte(value)); err != nil {
			return err
		}
	}
	return nil
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
