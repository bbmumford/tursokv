package rzset

import (
	"database/sql"
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
	err := db.QueryRow(upsert, key, rkey.TypeZSet, now).Scan(&keyID)
	return keyID, err
}

func zadd(db Querier, key string, opts *ZAddOptions, members ...ScoreMember) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	keyID, err := getOrCreateKey(db, key)
	if err != nil {
		return 0, err
	}

	return zaddInternal(db, keyID, opts, members...)
}

func zaddInternal(db Querier, keyID int64, opts *ZAddOptions, members ...ScoreMember) (int, error) {
	added := 0
	changed := 0

	for _, sm := range members {
		// Check if member exists
		const getScore = `SELECT score FROM rzset WHERE key_id = ? AND member = ?`
		var currentScore float64
		err := db.QueryRow(getScore, keyID, []byte(sm.Member)).Scan(&currentScore)
		exists := err == nil

		if exists {
			// Member exists - check options
			if opts != nil && opts.NX {
				continue // Don't update existing
			}
			if opts != nil && opts.GT && sm.Score <= currentScore {
				continue // Only update if greater
			}
			if opts != nil && opts.LT && sm.Score >= currentScore {
				continue // Only update if less
			}

			// Update score
			const update = `UPDATE rzset SET score = ? WHERE key_id = ? AND member = ?`
			if _, err := db.Exec(update, sm.Score, keyID, []byte(sm.Member)); err != nil {
				return added, err
			}
			if sm.Score != currentScore {
				changed++
			}
		} else {
			// New member
			if opts != nil && opts.XX {
				continue // Only update existing
			}

			const insert = `INSERT INTO rzset (key_id, member, score) VALUES (?, ?, ?)`
			if _, err := db.Exec(insert, keyID, []byte(sm.Member), sm.Score); err != nil {
				return added, err
			}
			added++
			changed++
		}
	}

	if opts != nil && opts.CH {
		return changed, nil
	}
	return added, nil
}

func zrem(db Querier, key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	removed := 0
	const del = `DELETE FROM rzset WHERE key_id = ? AND member = ?`
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

func zincrby(db Querier, key, member string, delta float64) (float64, error) {
	keyID, err := getOrCreateKey(db, key)
	if err != nil {
		return 0, err
	}

	return zincrbyInternal(db, keyID, member, delta)
}

func zincrbyInternal(db Querier, keyID int64, member string, delta float64) (float64, error) {
	memberBytes := []byte(member)

	// Get current score
	const getScore = `SELECT score FROM rzset WHERE key_id = ? AND member = ?`
	var currentScore float64
	err := db.QueryRow(getScore, keyID, memberBytes).Scan(&currentScore)
	if err == sql.ErrNoRows {
		currentScore = 0
	} else if err != nil {
		return 0, err
	}

	newScore := currentScore + delta

	const upsert = `
		INSERT INTO rzset (key_id, member, score)
		VALUES (?, ?, ?)
		ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score
	`
	if _, err := db.Exec(upsert, keyID, memberBytes, newScore); err != nil {
		return 0, err
	}

	return newScore, nil
}

func zpopmin(db Querier, key string) (ScoreMember, bool, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return ScoreMember{}, false, err
	}

	return zpopminInternal(db, keyID)
}

func zpopminInternal(db Querier, keyID int64) (ScoreMember, bool, error) {
	const query = `
		SELECT member, score FROM rzset
		WHERE key_id = ?
		ORDER BY score ASC, member ASC
		LIMIT 1
	`
	var member []byte
	var score float64
	if err := db.QueryRow(query, keyID).Scan(&member, &score); err != nil {
		if err == sql.ErrNoRows {
			return ScoreMember{}, false, nil
		}
		return ScoreMember{}, false, err
	}

	const del = `DELETE FROM rzset WHERE key_id = ? AND member = ?`
	if _, err := db.Exec(del, keyID, member); err != nil {
		return ScoreMember{}, false, err
	}

	return ScoreMember{Member: string(member), Score: score}, true, nil
}

func zpopmax(db Querier, key string) (ScoreMember, bool, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return ScoreMember{}, false, err
	}

	return zpopmaxInternal(db, keyID)
}

func zpopmaxInternal(db Querier, keyID int64) (ScoreMember, bool, error) {
	const query = `
		SELECT member, score FROM rzset
		WHERE key_id = ?
		ORDER BY score DESC, member DESC
		LIMIT 1
	`
	var member []byte
	var score float64
	if err := db.QueryRow(query, keyID).Scan(&member, &score); err != nil {
		if err == sql.ErrNoRows {
			return ScoreMember{}, false, nil
		}
		return ScoreMember{}, false, err
	}

	const del = `DELETE FROM rzset WHERE key_id = ? AND member = ?`
	if _, err := db.Exec(del, keyID, member); err != nil {
		return ScoreMember{}, false, err
	}

	return ScoreMember{Member: string(member), Score: score}, true, nil
}

func zremrangebyrank(db Querier, key string, start, stop int) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	// Get members to remove
	members, err := zrange(db, key, start, stop)
	if err != nil || len(members) == 0 {
		return 0, err
	}

	removed := 0
	const del = `DELETE FROM rzset WHERE key_id = ? AND member = ?`
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

func zremrangebyscore(db Querier, key string, min, max float64) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	const del = `DELETE FROM rzset WHERE key_id = ? AND score >= ? AND score <= ?`
	res, err := db.Exec(del, keyID, min, max)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
