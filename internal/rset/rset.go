// Package rset provides the set repository for TursoKV.
// Implements Redis set commands: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, etc.
package rset

import (
	"database/sql"
	"time"

	"github.com/bbmumford/tursokv/internal/rkey"
)

// Querier interface for database operations.
type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// DB is the set repository for non-transactional operations.
type DB struct {
	db    Querier
	keyDB *rkey.DB
}

// New creates a new set repository.
func New(db Querier, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// Add adds members to a set. Returns the number of new members added.
func (d *DB) Add(key string, members ...string) (int, error) {
	return sadd(d.db, key, members...)
}

// Remove removes members from a set. Returns the number removed.
func (d *DB) Remove(key string, members ...string) (int, error) {
	return srem(d.db, key, members...)
}

// IsMember checks if a member exists in a set.
func (d *DB) IsMember(key, member string) (bool, error) {
	return sismember(d.db, key, member)
}

// Members returns all members of a set.
func (d *DB) Members(key string) ([]string, error) {
	return smembers(d.db, key)
}

// Card returns the number of members in a set.
func (d *DB) Card(key string) (int, error) {
	return scard(d.db, key)
}

// Pop removes and returns a random member from a set.
func (d *DB) Pop(key string) (string, error) {
	return spop(d.db, key)
}

// RandMember returns a random member without removing it.
func (d *DB) RandMember(key string) (string, error) {
	return srandmember(d.db, key, 1)
}

// RandMembers returns count random members without removing them.
func (d *DB) RandMembers(key string, count int) ([]string, error) {
	return srandmembers(d.db, key, count)
}

// Union returns the union of all given sets.
func (d *DB) Union(keys ...string) ([]string, error) {
	return sunion(d.db, keys...)
}

// Inter returns the intersection of all given sets.
func (d *DB) Inter(keys ...string) ([]string, error) {
	return sinter(d.db, keys...)
}

// Diff returns the difference between the first set and all successive sets.
func (d *DB) Diff(keys ...string) ([]string, error) {
	return sdiff(d.db, keys...)
}

// Move moves a member from src to dst set.
func (d *DB) Move(src, dst, member string) (bool, error) {
	return smove(d.db, src, dst, member)
}

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

// --- Implementation functions ---

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
	err := db.QueryRow(upsert, key, rkey.TypeSet, now).Scan(&keyID)
	return keyID, err
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
		INSERT INTO rset (key_id, member)
		VALUES (?, ?)
		ON CONFLICT(key_id, member) DO NOTHING
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

func srem(db Querier, key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, nil
	}

	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	removed := 0
	const del = `DELETE FROM rset WHERE key_id = ? AND member = ?`
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

func sismember(db Querier, key, member string) (bool, error) {
	const query = `
		SELECT 1 FROM rset s
		JOIN rkey k ON k.id = s.key_id
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
		JOIN rkey k ON k.id = s.key_id
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
		JOIN rkey k ON k.id = s.key_id
		WHERE k.key = ? AND k.type = ?
		AND (k.etime IS NULL OR k.etime > ?)
	`
	now := time.Now().UnixMilli()
	var count int
	err := db.QueryRow(query, key, rkey.TypeSet, now).Scan(&count)
	return count, err
}

func spop(db Querier, key string) (string, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return "", err
	}

	// Get a random member (SQLite's RANDOM() function)
	const query = `
		SELECT member FROM rset
		WHERE key_id = ?
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
	const del = `DELETE FROM rset WHERE key_id = ? AND member = ?`
	if _, err := db.Exec(del, keyID, member); err != nil {
		return "", err
	}

	return string(member), nil
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
		JOIN rkey k ON k.id = s.key_id
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
	const del = `DELETE FROM rset WHERE key_id = ? AND member = ?`
	if _, err := db.Exec(del, srcID, []byte(member)); err != nil {
		return false, err
	}

	// Add to destination
	_, err = sadd(db, dst, member)
	return err == nil, err
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
