package rset

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rkey"
)

// getOrCreateKey claims key for a write of this type (rkey.Claim): a key past
// its expiry starts afresh, and a live key of another type is refused with
// rkey.ErrWrongType.
func getOrCreateKey(db Querier, key string) (int64, error) {
	return rkey.Claim(db, key, rkey.TypeSet, false)
}

func sadd(db Querier, key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	keyID, err := getOrCreateKey(db, key)
	if err != nil {
		return 0, err
	}

	added := 0
	const insert = `
		INSERT INTO rset (keyId, member)
		VALUES (?, ?)
		ON CONFLICT(keyId, member) DO NOTHING
	`
	for _, member := range members {
		res, err := db.Exec(insert, keyID, []byte(member))
		if err != nil {
			return added, err
		}
		n, _ := res.RowsAffected()
		added += int(n)
	}

	return added, nil
}

func srem(db Querier, key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	removed := 0
	const del = `DELETE FROM rset WHERE keyId = ? AND member = ?`
	for _, member := range members {
		res, err := db.Exec(del, keyID, []byte(member))
		if err != nil {
			return removed, err
		}
		n, _ := res.RowsAffected()
		removed += int(n)
	}

	return removed, nil
}

func spop(db Querier, key string) (string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return "", err
	}

	// Get a random member (SQLite's RANDOM() function)
	const query = `
		SELECT member FROM rset
		WHERE keyId = ?
		ORDER BY RANDOM()
		LIMIT 1
	`
	var member []byte
	if err := db.QueryRow(query, keyID).Scan(&member); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	// Delete the member
	const del = `DELETE FROM rset WHERE keyId = ? AND member = ?`
	if _, err := db.Exec(del, keyID, member); err != nil {
		return "", err
	}

	return string(member), nil
}

func smove(db Querier, src, dst, member string) (bool, error) {
	srcID, err := getKeyID(db, src)
	if err != nil || srcID == 0 {
		return false, err
	}

	// Check if member exists in source
	exists, err := sismember(db, src, member)
	if err != nil || !exists {
		return false, err
	}

	// Remove from source
	const del = `DELETE FROM rset WHERE keyId = ? AND member = ?`
	if _, err := db.Exec(del, srcID, []byte(member)); err != nil {
		return false, err
	}

	// Add to destination
	_, err = sadd(db, dst, member)
	return err == nil, err
}
