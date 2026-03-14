// Package rzset provides the sorted set repository for TursoKV.
// Implements Redis sorted set commands: ZADD, ZREM, ZRANGE, ZSCORE, ZRANK, etc.
package rzset

import (
	"database/sql"
	"math"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// ScoreMember represents a member with its score.
type ScoreMember struct {
	Member string
	Score  float64
}

// ZAddOptions for ZADD command variants.
type ZAddOptions struct {
	// NX: Only add new elements (don't update existing scores)
	NX bool
	// XX: Only update existing elements (don't add new members)
	XX bool
	// GT: Only update when new score > current score
	GT bool
	// LT: Only update when new score < current score
	LT bool
	// CH: Return number of changed elements (added + updated)
	CH bool
}

// DB is the sorted set repository for non-transactional operations.
type DB struct {
	db    Querier
	keyDB *rkey.DB
}

// New creates a new sorted set repository.
func New(db Querier, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// Add adds members with scores to a sorted set.
// Returns the number of new members added.
func (d *DB) Add(key string, members ...ScoreMember) (int, error) {
	return zadd(d.db, key, nil, members...)
}

// AddWithOptions adds members with options.
func (d *DB) AddWithOptions(key string, opts *ZAddOptions, members ...ScoreMember) (int, error) {
	return zadd(d.db, key, opts, members...)
}

// Remove removes members from a sorted set.
func (d *DB) Remove(key string, members ...string) (int, error) {
	return zrem(d.db, key, members...)
}

// Score returns the score of a member.
func (d *DB) Score(key, member string) (float64, bool, error) {
	return zscore(d.db, key, member)
}

// Rank returns the rank of a member (0-based, lowest score first).
func (d *DB) Rank(key, member string) (int, bool, error) {
	return zrank(d.db, key, member)
}

// RevRank returns the reverse rank (0-based, highest score first).
func (d *DB) RevRank(key, member string) (int, bool, error) {
	return zrevrank(d.db, key, member)
}

// Card returns the number of members.
func (d *DB) Card(key string) (int, error) {
	return zcard(d.db, key)
}

// Count returns count of members with scores between min and max.
func (d *DB) Count(key string, min, max float64) (int, error) {
	return zcount(d.db, key, min, max)
}

// Range returns members by rank range (inclusive).
func (d *DB) Range(key string, start, stop int) ([]string, error) {
	return zrange(d.db, key, start, stop)
}

// RangeWithScores returns members with scores by rank range.
func (d *DB) RangeWithScores(key string, start, stop int) ([]ScoreMember, error) {
	return zrangeWithScores(d.db, key, start, stop)
}

// RevRange returns members by rank range (highest first).
func (d *DB) RevRange(key string, start, stop int) ([]string, error) {
	return zrevrange(d.db, key, start, stop)
}

// RevRangeWithScores returns members with scores by rank range (highest first).
func (d *DB) RevRangeWithScores(key string, start, stop int) ([]ScoreMember, error) {
	return zrevrangeWithScores(d.db, key, start, stop)
}

// RangeByScore returns members with scores between min and max.
func (d *DB) RangeByScore(key string, min, max float64) ([]string, error) {
	return zrangebyscore(d.db, key, min, max, -1, 0)
}

// RangeByScoreWithLimit returns members with scores in range with offset and count.
func (d *DB) RangeByScoreWithLimit(key string, min, max float64, offset, count int) ([]string, error) {
	return zrangebyscore(d.db, key, min, max, count, offset)
}

// IncrBy increments the score of a member.
func (d *DB) IncrBy(key, member string, delta float64) (float64, error) {
	return zincrby(d.db, key, member, delta)
}

// PopMin removes and returns the member with lowest score.
func (d *DB) PopMin(key string) (ScoreMember, bool, error) {
	return zpopmin(d.db, key)
}

// PopMax removes and returns the member with highest score.
func (d *DB) PopMax(key string) (ScoreMember, bool, error) {
	return zpopmax(d.db, key)
}

// RemRangeByRank removes members by rank range.
func (d *DB) RemRangeByRank(key string, start, stop int) (int, error) {
	return zremrangebyrank(d.db, key, start, stop)
}

// RemRangeByScore removes members by score range.
func (d *DB) RemRangeByScore(key string, min, max float64) (int, error) {
	return zremrangebyscore(d.db, key, min, max)
}

// Tx is the sorted set repository for transactional operations.
type Tx struct {
	db    *sql.Tx
	keyTx *rkey.Tx
}

// NewTx creates a new transactional sorted set repository.
func NewTx(tx *sql.Tx, keyTx *rkey.Tx) *Tx {
	return &Tx{db: tx, keyTx: keyTx}
}

// Add adds members with scores.
func (tx *Tx) Add(key string, members ...ScoreMember) (int, error) {
	return zaddTx(tx.db, tx.keyTx, key, nil, members...)
}

// AddWithOptions adds members with options.
func (tx *Tx) AddWithOptions(key string, opts *ZAddOptions, members ...ScoreMember) (int, error) {
	return zaddTx(tx.db, tx.keyTx, key, opts, members...)
}

// Remove removes members.
func (tx *Tx) Remove(key string, members ...string) (int, error) {
	return zremTx(tx.db, tx.keyTx, key, members...)
}

// Score returns the score of a member.
func (tx *Tx) Score(key, member string) (float64, bool, error) {
	return zscore(tx.db, key, member)
}

// Rank returns the rank of a member.
func (tx *Tx) Rank(key, member string) (int, bool, error) {
	return zrank(tx.db, key, member)
}

// RevRank returns the reverse rank.
func (tx *Tx) RevRank(key, member string) (int, bool, error) {
	return zrevrank(tx.db, key, member)
}

// Card returns the number of members.
func (tx *Tx) Card(key string) (int, error) {
	return zcard(tx.db, key)
}

// Count returns count of members with scores between min and max.
func (tx *Tx) Count(key string, min, max float64) (int, error) {
	return zcount(tx.db, key, min, max)
}

// Range returns members by rank range.
func (tx *Tx) Range(key string, start, stop int) ([]string, error) {
	return zrange(tx.db, key, start, stop)
}

// RangeWithScores returns members with scores by rank range.
func (tx *Tx) RangeWithScores(key string, start, stop int) ([]ScoreMember, error) {
	return zrangeWithScores(tx.db, key, start, stop)
}

// RevRange returns members by rank range (highest first).
func (tx *Tx) RevRange(key string, start, stop int) ([]string, error) {
	return zrevrange(tx.db, key, start, stop)
}

// IncrBy increments the score of a member.
func (tx *Tx) IncrBy(key, member string, delta float64) (float64, error) {
	return zincrbyTx(tx.db, tx.keyTx, key, member, delta)
}

// PopMin removes and returns the member with lowest score.
func (tx *Tx) PopMin(key string) (ScoreMember, bool, error) {
	return zpopminTx(tx.db, tx.keyTx, key)
}

// PopMax removes and returns the member with highest score.
func (tx *Tx) PopMax(key string) (ScoreMember, bool, error) {
	return zpopmaxTx(tx.db, tx.keyTx, key)
}

// --- Implementation functions ---

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

func zaddTx(db Querier, keyTx *rkey.Tx, key string, opts *ZAddOptions, members ...ScoreMember) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	k, err := keyTx.GetOrCreate(key, rkey.TypeZSet)
	if err != nil {
		return 0, err
	}

	return zaddInternal(db, k.ID, opts, members...)
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

func zremTx(db Querier, keyTx *rkey.Tx, key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	removed := 0
	const del = `DELETE FROM rzset WHERE key_id = ? AND member = ?`
	for _, member := range members {
		res, err := db.Exec(del, k.ID, []byte(member))
		if err != nil {
			return removed, err
		}
		n, _ := res.RowsAffected()
		removed += int(n)
	}

	return removed, nil
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

func zincrby(db Querier, key, member string, delta float64) (float64, error) {
	keyID, err := getOrCreateKey(db, key)
	if err != nil {
		return 0, err
	}

	return zincrbyInternal(db, keyID, member, delta)
}

func zincrbyTx(db Querier, keyTx *rkey.Tx, key, member string, delta float64) (float64, error) {
	k, err := keyTx.GetOrCreate(key, rkey.TypeZSet)
	if err != nil {
		return 0, err
	}

	return zincrbyInternal(db, k.ID, member, delta)
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

func zpopminTx(db Querier, keyTx *rkey.Tx, key string) (ScoreMember, bool, error) {
	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return ScoreMember{}, false, nil
		}
		return ScoreMember{}, false, err
	}

	return zpopminInternal(db, k.ID)
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

func zpopmaxTx(db Querier, keyTx *rkey.Tx, key string) (ScoreMember, bool, error) {
	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return ScoreMember{}, false, nil
		}
		return ScoreMember{}, false, err
	}

	return zpopmaxInternal(db, k.ID)
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

// NegInf represents negative infinity for score ranges.
var NegInf = math.Inf(-1)

// PosInf represents positive infinity for score ranges.
var PosInf = math.Inf(1)
