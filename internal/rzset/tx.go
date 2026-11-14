package rzset

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rkey"
)

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
	const del = `DELETE FROM rzset WHERE keyId = ? AND member = ?`
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

func zincrbyTx(db Querier, keyTx *rkey.Tx, key, member string, delta float64) (float64, error) {
	k, err := keyTx.GetOrCreate(key, rkey.TypeZSet)
	if err != nil {
		return 0, err
	}

	return zincrbyInternal(db, k.ID, member, delta)
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
