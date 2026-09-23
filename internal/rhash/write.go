package rhash

import (
	"database/sql"
	"strconv"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

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

func hmset(db Querier, key string, pairs map[string]string) error {
	for field, value := range pairs {
		if _, err := hset(db, key, field, value); err != nil {
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
