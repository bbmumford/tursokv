package rset

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rkey"
)

// Tx is the set repository for transactional operations.
type Tx struct {
	db    *sql.Tx
	keyTx *rkey.Tx
}

// NewTx creates a new transactional set repository.
func NewTx(tx *sql.Tx, keyTx *rkey.Tx) *Tx {
	return &Tx{db: tx, keyTx: keyTx}
}

// Add adds members to a set.
func (tx *Tx) Add(key string, members ...string) (int, error) {
	return saddTx(tx.db, tx.keyTx, key, members...)
}

// Remove removes members from a set.
func (tx *Tx) Remove(key string, members ...string) (int, error) {
	return sremTx(tx.db, tx.keyTx, key, members...)
}

// IsMember checks if a member exists.
func (tx *Tx) IsMember(key, member string) (bool, error) {
	return sismember(tx.db, key, member)
}

// Members returns all members.
func (tx *Tx) Members(key string) ([]string, error) {
	return smembers(tx.db, key)
}

// Card returns the number of members.
func (tx *Tx) Card(key string) (int, error) {
	return scard(tx.db, key)
}

// Pop removes and returns a random member.
func (tx *Tx) Pop(key string) (string, error) {
	return spopTx(tx.db, tx.keyTx, key)
}

// Union returns the union of sets.
func (tx *Tx) Union(keys ...string) ([]string, error) {
	return sunion(tx.db, keys...)
}

// Inter returns the intersection of sets.
func (tx *Tx) Inter(keys ...string) ([]string, error) {
	return sinter(tx.db, keys...)
}

// Diff returns the difference of sets.
func (tx *Tx) Diff(keys ...string) ([]string, error) {
	return sdiff(tx.db, keys...)
}

// Move moves a member between sets.
func (tx *Tx) Move(src, dst, member string) (bool, error) {
	return smoveTx(tx.db, tx.keyTx, src, dst, member)
}

func saddTx(db Querier, keyTx *rkey.Tx, key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	k, err := keyTx.GetOrCreate(key, rkey.TypeSet)
	if err != nil {
		return 0, err
	}

	added := 0
	const insert = `
		INSERT INTO rset (key_id, member)
		VALUES (?, ?)
		ON CONFLICT(key_id, member) DO NOTHING
	`
	for _, member := range members {
		res, err := db.Exec(insert, k.ID, []byte(member))
		if err != nil {
			return added, err
		}
		n, _ := res.RowsAffected()
		added += int(n)
	}

	return added, nil
}

func sremTx(db Querier, keyTx *rkey.Tx, key string, members ...string) (int, error) {
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
	const del = `DELETE FROM rset WHERE key_id = ? AND member = ?`
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

func spopTx(db Querier, keyTx *rkey.Tx, key string) (string, error) {
	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const query = `
		SELECT member FROM rset
		WHERE key_id = ?
		ORDER BY RANDOM()
		LIMIT 1
	`
	var member []byte
	if err := db.QueryRow(query, k.ID).Scan(&member); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const del = `DELETE FROM rset WHERE key_id = ? AND member = ?`
	if _, err := db.Exec(del, k.ID, member); err != nil {
		return "", err
	}

	return string(member), nil
}

func smoveTx(db Querier, keyTx *rkey.Tx, src, dst, member string) (bool, error) {
	srcKey, err := keyTx.Get(src)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	// Check if member exists in source
	exists, err := sismember(db, src, member)
	if err != nil || !exists {
		return false, err
	}

	// Remove from source
	const del = `DELETE FROM rset WHERE key_id = ? AND member = ?`
	if _, err := db.Exec(del, srcKey.ID, []byte(member)); err != nil {
		return false, err
	}

	// Add to destination
	_, err = saddTx(db, keyTx, dst, member)
	return err == nil, err
}
