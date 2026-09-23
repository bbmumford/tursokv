package rhash

import (
	"database/sql"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

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
