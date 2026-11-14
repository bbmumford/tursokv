package rset

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
	err := db.QueryRow(query, key, rkey.TypeSet, now).Scan(&keyID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return keyID, err
}

func sismember(db Querier, key, member string) (bool, error) {
	const query = `
		SELECT 1 FROM rset s
		JOIN rkey k ON k.id = s.keyId
		WHERE k.key = ? AND k.type = ? AND s.member = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var exists int
	err := db.QueryRow(query, key, rkey.TypeSet, []byte(member), now).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func smembers(db Querier, key string) ([]string, error) {
	const query = `
		SELECT s.member FROM rset s
		JOIN rkey k ON k.id = s.keyId
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	rows, err := db.Query(query, key, rkey.TypeSet, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}

	return members, rows.Err()
}

func scard(db Querier, key string) (int, error) {
	const query = `
		SELECT COUNT(*) FROM rset s
		JOIN rkey k ON k.id = s.keyId
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var count int
	err := db.QueryRow(query, key, rkey.TypeSet, now).Scan(&count)
	return count, err
}

func srandmember(db Querier, key string, count int) (string, error) {
	members, err := srandmembers(db, key, count)
	if err != nil || len(members) == 0 {
		return "", err
	}
	return members[0], nil
}

func srandmembers(db Querier, key string, count int) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}

	const query = `
		SELECT s.member FROM rset s
		JOIN rkey k ON k.id = s.keyId
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
		ORDER BY RANDOM()
		LIMIT ?
	`
	now := time.Now().UnixMilli()
	rows, err := db.Query(query, key, rkey.TypeSet, now, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}

	return members, rows.Err()
}

func sunion(db Querier, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	memberSet := make(map[string]struct{})
	for _, key := range keys {
		members, err := smembers(db, key)
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			memberSet[m] = struct{}{}
		}
	}

	result := make([]string, 0, len(memberSet))
	for m := range memberSet {
		result = append(result, m)
	}
	return result, nil
}

func sinter(db Querier, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Get members of first set
	first, err := smembers(db, keys[0])
	if err != nil {
		return nil, err
	}

	// Check each member against all other sets
	var result []string
	for _, member := range first {
		inAll := true
		for _, key := range keys[1:] {
			exists, err := sismember(db, key, member)
			if err != nil {
				return nil, err
			}
			if !exists {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, member)
		}
	}

	return result, nil
}

func sdiff(db Querier, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Get members of first set
	first, err := smembers(db, keys[0])
	if err != nil {
		return nil, err
	}

	// Check each member is not in any other set
	var result []string
	for _, member := range first {
		inOther := false
		for _, key := range keys[1:] {
			exists, err := sismember(db, key, member)
			if err != nil {
				return nil, err
			}
			if exists {
				inOther = true
				break
			}
		}
		if !inOther {
			result = append(result, member)
		}
	}

	return result, nil
}
