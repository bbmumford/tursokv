package rlist

import (
	"database/sql"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

func getKeyID(db Querier, key string) (int64, error) {
	const query = `
		SELECT id FROM rkey
		WHERE key = ? AND type = ? AND (etime IS NULL OR etime > ?)
	`
	now := time.Now().UnixMilli()
	var keyID int64
	err := db.QueryRow(query, key, rkey.TypeList, now).Scan(&keyID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return keyID, err
}

func lindex(db Querier, key string, index int) (string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return "", err
	}

	var query string
	if index >= 0 {
		query = `
			SELECT value FROM rlist
			WHERE key_id = ?
			ORDER BY pos ASC
			LIMIT 1 OFFSET ?
		`
	} else {
		query = `
			SELECT value FROM rlist
			WHERE key_id = ?
			ORDER BY pos DESC
			LIMIT 1 OFFSET ?
		`
		index = -index - 1
	}

	var value []byte
	if err := db.QueryRow(query, keyID, index).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	return string(value), nil
}

func lrange(db Querier, key string, start, stop int) ([]string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return nil, err
	}

	// Get total length for negative index handling
	length, err := llen(db, key)
	if err != nil || length == 0 {
		return nil, err
	}

	// Convert negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp to valid range
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return nil, nil
	}

	limit := stop - start + 1
	const query = `
		SELECT value FROM rlist
		WHERE key_id = ?
		ORDER BY pos ASC
		LIMIT ? OFFSET ?
	`
	rows, err := db.Query(query, keyID, limit, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		result = append(result, string(value))
	}

	return result, rows.Err()
}

func llen(db Querier, key string) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	const query = `SELECT COUNT(*) FROM rlist WHERE key_id = ?`
	var count int
	err = db.QueryRow(query, keyID).Scan(&count)
	return count, err
}
