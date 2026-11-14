package rlist

import (
	"database/sql"
	"math"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

func getOrCreateKey(db Querier, key string) (int64, error) {
	now := time.Now().UnixMilli()
	const upsert = `
		INSERT INTO rkey (key, type, version, mtime)
		VALUES (?, ?, 0, ?)
		ON CONFLICT(key) DO UPDATE SET
			version = version + 1,
			mtime = excluded.mtime
		RETURNING id
	`
	var keyID int64
	err := db.QueryRow(upsert, key, rkey.TypeList, now).Scan(&keyID)
	return keyID, err
}

func getMinPos(db Querier, keyID int64) (float64, error) {
	const query = `SELECT MIN(pos) FROM rlist WHERE keyId = ?`
	var pos sql.NullFloat64
	if err := db.QueryRow(query, keyID).Scan(&pos); err != nil {
		return 0, err
	}
	if !pos.Valid {
		return 0, nil
	}
	return pos.Float64, nil
}

func getMaxPos(db Querier, keyID int64) (float64, error) {
	const query = `SELECT MAX(pos) FROM rlist WHERE keyId = ?`
	var pos sql.NullFloat64
	if err := db.QueryRow(query, keyID).Scan(&pos); err != nil {
		return 0, err
	}
	if !pos.Valid {
		return 0, nil
	}
	return pos.Float64, nil
}

func lpush(db Querier, key string, values ...string) (int, error) {
	if len(values) == 0 {
		return llen(db, key)
	}

	keyID, err := getOrCreateKey(db, key)
	if err != nil {
		return 0, err
	}

	minPos, err := getMinPos(db, keyID)
	if err != nil {
		return 0, err
	}

	// Insert values in reverse order so first value ends up first
	const insert = `INSERT INTO rlist (keyId, pos, value) VALUES (?, ?, ?)`
	for i := len(values) - 1; i >= 0; i-- {
		minPos -= posGap
		if _, err := db.Exec(insert, keyID, minPos, []byte(values[i])); err != nil {
			return 0, err
		}
	}

	return llen(db, key)
}

func rpush(db Querier, key string, values ...string) (int, error) {
	if len(values) == 0 {
		return llen(db, key)
	}

	keyID, err := getOrCreateKey(db, key)
	if err != nil {
		return 0, err
	}

	maxPos, err := getMaxPos(db, keyID)
	if err != nil {
		return 0, err
	}

	const insert = `INSERT INTO rlist (keyId, pos, value) VALUES (?, ?, ?)`
	for _, value := range values {
		maxPos += posGap
		if _, err := db.Exec(insert, keyID, maxPos, []byte(value)); err != nil {
			return 0, err
		}
	}

	return llen(db, key)
}

func lpop(db Querier, key string) (string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return "", err
	}

	const query = `
		SELECT pos, value FROM rlist
		WHERE keyId = ?
		ORDER BY pos ASC
		LIMIT 1
	`
	var pos float64
	var value []byte
	if err := db.QueryRow(query, keyID).Scan(&pos, &value); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const del = `DELETE FROM rlist WHERE keyId = ? AND pos = ?`
	if _, err := db.Exec(del, keyID, pos); err != nil {
		return "", err
	}

	return string(value), nil
}

func rpop(db Querier, key string) (string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return "", err
	}

	const query = `
		SELECT pos, value FROM rlist
		WHERE keyId = ?
		ORDER BY pos DESC
		LIMIT 1
	`
	var pos float64
	var value []byte
	if err := db.QueryRow(query, keyID).Scan(&pos, &value); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const del = `DELETE FROM rlist WHERE keyId = ? AND pos = ?`
	if _, err := db.Exec(del, keyID, pos); err != nil {
		return "", err
	}

	return string(value), nil
}

func lset(db Querier, key string, index int, value string) error {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return err
	}

	// Get position at index
	var query string
	if index >= 0 {
		query = `
			SELECT pos FROM rlist
			WHERE keyId = ?
			ORDER BY pos ASC
			LIMIT 1 OFFSET ?
		`
	} else {
		query = `
			SELECT pos FROM rlist
			WHERE keyId = ?
			ORDER BY pos DESC
			LIMIT 1 OFFSET ?
		`
		index = -index - 1
	}

	var pos float64
	if err := db.QueryRow(query, keyID, index).Scan(&pos); err != nil {
		return err
	}

	const update = `UPDATE rlist SET value = ? WHERE keyId = ? AND pos = ?`
	_, err = db.Exec(update, []byte(value), keyID, pos)
	return err
}

func ltrim(db Querier, key string, start, stop int) error {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return err
	}

	length, err := llen(db, key)
	if err != nil || length == 0 {
		return err
	}

	// Convert negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Get positions to keep
	const getPositions = `
		SELECT pos FROM rlist
		WHERE keyId = ?
		ORDER BY pos ASC
		LIMIT ? OFFSET ?
	`
	limit := stop - start + 1
	if limit <= 0 {
		// Delete all
		const delAll = `DELETE FROM rlist WHERE keyId = ?`
		_, err = db.Exec(delAll, keyID)
		return err
	}

	rows, err := db.Query(getPositions, keyID, limit, start)
	if err != nil {
		return err
	}
	defer rows.Close()

	var keepPositions []float64
	for rows.Next() {
		var pos float64
		if err := rows.Scan(&pos); err != nil {
			return err
		}
		keepPositions = append(keepPositions, pos)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(keepPositions) == 0 {
		const delAll = `DELETE FROM rlist WHERE keyId = ?`
		_, err = db.Exec(delAll, keyID)
		return err
	}

	// Delete positions outside the range
	minKeep := keepPositions[0]
	maxKeep := keepPositions[len(keepPositions)-1]

	const delOutside = `DELETE FROM rlist WHERE keyId = ? AND (pos < ? OR pos > ?)`
	_, err = db.Exec(delOutside, keyID, minKeep, maxKeep)
	return err
}

func linsert(db Querier, key, pivot, value string, before bool) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return -1, err
	}

	// Find pivot position
	const findPivot = `
		SELECT pos FROM rlist
		WHERE keyId = ? AND value = ?
		ORDER BY pos ASC
		LIMIT 1
	`
	var pivotPos float64
	if err := db.QueryRow(findPivot, keyID, []byte(pivot)).Scan(&pivotPos); err != nil {
		if err == sql.ErrNoRows {
			return -1, nil // Pivot not found
		}
		return 0, err
	}

	var newPos float64
	if before {
		// Get previous position
		const getPrev = `
			SELECT MAX(pos) FROM rlist
			WHERE keyId = ? AND pos < ?
		`
		var prevPos sql.NullFloat64
		if err := db.QueryRow(getPrev, keyID, pivotPos).Scan(&prevPos); err != nil {
			return 0, err
		}
		if prevPos.Valid {
			newPos = (prevPos.Float64 + pivotPos) / 2
		} else {
			newPos = pivotPos - posGap
		}
	} else {
		// Get next position
		const getNext = `
			SELECT MIN(pos) FROM rlist
			WHERE keyId = ? AND pos > ?
		`
		var nextPos sql.NullFloat64
		if err := db.QueryRow(getNext, keyID, pivotPos).Scan(&nextPos); err != nil {
			return 0, err
		}
		if nextPos.Valid {
			newPos = (pivotPos + nextPos.Float64) / 2
		} else {
			newPos = pivotPos + posGap
		}
	}

	const insert = `INSERT INTO rlist (keyId, pos, value) VALUES (?, ?, ?)`
	if _, err := db.Exec(insert, keyID, newPos, []byte(value)); err != nil {
		return 0, err
	}

	return llen(db, key)
}

// lremSelect builds the query that picks which occurrences of value LREM
// removes: every one for count 0, the first count from the head for count > 0,
// and the last -count from the tail for count < 0. The limit is a bound
// parameter, so a count of any size stays one integer argument.
func lremSelect(keyID int64, count int, value string) (string, []any) {
	args := []any{keyID, []byte(value)}
	const match = `SELECT pos FROM rlist WHERE keyId = ? AND value = ?`
	switch {
	case count == 0:
		return match, args
	case count > 0:
		return match + ` ORDER BY pos ASC LIMIT ?`, append(args, int64(count))
	default:
		limit := -int64(count)
		if limit < 0 {
			// -count overflows only for the most negative int, which asks
			// for more occurrences than a list can hold.
			limit = math.MaxInt64
		}
		return match + ` ORDER BY pos DESC LIMIT ?`, append(args, limit)
	}
}

func lrem(db Querier, key string, count int, value string) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	query, args := lremSelect(keyID, count, value)
	rows, err := db.Query(query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var positions []float64
	for rows.Next() {
		var pos float64
		if err := rows.Scan(&pos); err != nil {
			return 0, err
		}
		positions = append(positions, pos)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	removed := 0
	for _, pos := range positions {
		const del = `DELETE FROM rlist WHERE keyId = ? AND pos = ?`
		if _, err := db.Exec(del, keyID, pos); err != nil {
			return removed, err
		}
		removed++
	}

	return removed, nil
}
