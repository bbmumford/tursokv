package rzset

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
	err := db.QueryRow(query, key, rkey.TypeZSet, now).Scan(&keyID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return keyID, err
}

func zscore(db Querier, key, member string) (float64, bool, error) {
	const query = `
		SELECT z.score FROM rzset z
		JOIN rkey k ON k.id = z.key_id
		WHERE k.key = ? AND k.type = ? AND z.member = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var score float64
	err := db.QueryRow(query, key, rkey.TypeZSet, []byte(member), now).Scan(&score)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}

func zrank(db Querier, key, member string) (int, bool, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, false, err
	}

	// Get rank by counting members with lower score or same score and lower member
	const query = `
		SELECT COUNT(*) FROM rzset
		WHERE key_id = ? AND (
			score < (SELECT score FROM rzset WHERE key_id = ? AND member = ?) OR
			(score = (SELECT score FROM rzset WHERE key_id = ? AND member = ?) AND member < ?)
		)
	`
	var rank int
	memberBytes := []byte(member)
	err = db.QueryRow(query, keyID, keyID, memberBytes, keyID, memberBytes, memberBytes).Scan(&rank)
	if err != nil {
		// Check if member exists
		const check = `SELECT 1 FROM rzset WHERE key_id = ? AND member = ?`
		var exists int
		if db.QueryRow(check, keyID, memberBytes).Scan(&exists) == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}

	return rank, true, nil
}

func zrevrank(db Querier, key, member string) (int, bool, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, false, err
	}

	// Get reverse rank
	const query = `
		SELECT COUNT(*) FROM rzset
		WHERE key_id = ? AND (
			score > (SELECT score FROM rzset WHERE key_id = ? AND member = ?) OR
			(score = (SELECT score FROM rzset WHERE key_id = ? AND member = ?) AND member > ?)
		)
	`
	var rank int
	memberBytes := []byte(member)
	err = db.QueryRow(query, keyID, keyID, memberBytes, keyID, memberBytes, memberBytes).Scan(&rank)
	if err != nil {
		const check = `SELECT 1 FROM rzset WHERE key_id = ? AND member = ?`
		var exists int
		if db.QueryRow(check, keyID, memberBytes).Scan(&exists) == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}

	return rank, true, nil
}

func zcard(db Querier, key string) (int, error) {
	const query = `
		SELECT COUNT(*) FROM rzset z
		JOIN rkey k ON k.id = z.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var count int
	err := db.QueryRow(query, key, rkey.TypeZSet, now).Scan(&count)
	return count, err
}

func zcount(db Querier, key string, min, max float64) (int, error) {
	const query = `
		SELECT COUNT(*) FROM rzset z
		JOIN rkey k ON k.id = z.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
		AND z.score >= ? AND z.score <= ?
	`
	now := time.Now().UnixMilli()
	var count int
	err := db.QueryRow(query, key, rkey.TypeZSet, now, min, max).Scan(&count)
	return count, err
}

func zrange(db Querier, key string, start, stop int) ([]string, error) {
	members, err := zrangeWithScores(db, key, start, stop)
	if err != nil {
		return nil, err
	}

	result := make([]string, len(members))
	for i, m := range members {
		result[i] = m.Member
	}
	return result, nil
}

func zrangeWithScores(db Querier, key string, start, stop int) ([]ScoreMember, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return nil, err
	}

	// Handle negative indices
	count, err := zcard(db, key)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	if start < 0 {
		start = count + start
	}
	if stop < 0 {
		stop = count + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= count {
		stop = count - 1
	}
	if start > stop {
		return nil, nil
	}

	limit := stop - start + 1
	const query = `
		SELECT member, score FROM rzset
		WHERE key_id = ?
		ORDER BY score ASC, member ASC
		LIMIT ? OFFSET ?
	`
	rows, err := db.Query(query, keyID, limit, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ScoreMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		result = append(result, ScoreMember{Member: string(member), Score: score})
	}

	return result, rows.Err()
}

func zrevrange(db Querier, key string, start, stop int) ([]string, error) {
	members, err := zrevrangeWithScores(db, key, start, stop)
	if err != nil {
		return nil, err
	}

	result := make([]string, len(members))
	for i, m := range members {
		result[i] = m.Member
	}
	return result, nil
}

func zrevrangeWithScores(db Querier, key string, start, stop int) ([]ScoreMember, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return nil, err
	}

	count, err := zcard(db, key)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	if start < 0 {
		start = count + start
	}
	if stop < 0 {
		stop = count + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= count {
		stop = count - 1
	}
	if start > stop {
		return nil, nil
	}

	limit := stop - start + 1
	const query = `
		SELECT member, score FROM rzset
		WHERE key_id = ?
		ORDER BY score DESC, member DESC
		LIMIT ? OFFSET ?
	`
	rows, err := db.Query(query, keyID, limit, start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ScoreMember
	for rows.Next() {
		var member []byte
		var score float64
		if err := rows.Scan(&member, &score); err != nil {
			return nil, err
		}
		result = append(result, ScoreMember{Member: string(member), Score: score})
	}

	return result, rows.Err()
}

func zrangebyscore(db Querier, key string, min, max float64, count, offset int) ([]string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return nil, err
	}

	var query string
	if count < 0 {
		query = `
			SELECT member FROM rzset
			WHERE key_id = ? AND score >= ? AND score <= ?
			ORDER BY score ASC, member ASC
		`
	} else {
		query = `
			SELECT member FROM rzset
			WHERE key_id = ? AND score >= ? AND score <= ?
			ORDER BY score ASC, member ASC
			LIMIT ? OFFSET ?
		`
	}

	var rows *sql.Rows
	if count < 0 {
		rows, err = db.Query(query, keyID, min, max)
	} else {
		rows, err = db.Query(query, keyID, min, max, count, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		result = append(result, string(member))
	}

	return result, rows.Err()
}
