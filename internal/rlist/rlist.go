// Package rlist provides the list repository for TursoKV.
// Implements Redis list commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, etc.
package rlist

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

// Position constants for list operations.
const posGap = 1.0

// DB is the list repository for non-transactional operations.
type DB struct {
	db    Querier
	keyDB *rkey.DB
}

// New creates a new list repository.
func New(db Querier, keyDB *rkey.DB) *DB {
	return &DB{db: db, keyDB: keyDB}
}

// LPush prepends values to a list. Returns the new length.
func (d *DB) LPush(key string, values ...string) (int, error) {
	return lpush(d.db, key, values...)
}

// RPush appends values to a list. Returns the new length.
func (d *DB) RPush(key string, values ...string) (int, error) {
	return rpush(d.db, key, values...)
}

// LPop removes and returns the first element of a list.
func (d *DB) LPop(key string) (string, error) {
	return lpop(d.db, key)
}

// RPop removes and returns the last element of a list.
func (d *DB) RPop(key string) (string, error) {
	return rpop(d.db, key)
}

// LIndex returns the element at index in the list.
func (d *DB) LIndex(key string, index int) (string, error) {
	return lindex(d.db, key, index)
}

// LRange returns elements from start to stop (inclusive).
func (d *DB) LRange(key string, start, stop int) ([]string, error) {
	return lrange(d.db, key, start, stop)
}

// LLen returns the length of a list.
func (d *DB) LLen(key string) (int, error) {
	return llen(d.db, key)
}

// LSet sets the element at index.
func (d *DB) LSet(key string, index int, value string) error {
	return lset(d.db, key, index, value)
}

// LTrim trims a list to the specified range.
func (d *DB) LTrim(key string, start, stop int) error {
	return ltrim(d.db, key, start, stop)
}

// LInsertBefore inserts value before the pivot.
func (d *DB) LInsertBefore(key, pivot, value string) (int, error) {
	return linsert(d.db, key, pivot, value, true)
}

// LInsertAfter inserts value after the pivot.
func (d *DB) LInsertAfter(key, pivot, value string) (int, error) {
	return linsert(d.db, key, pivot, value, false)
}

// LRem removes count occurrences of value from the list.
// count > 0: Remove from head to tail.
// count < 0: Remove from tail to head.
// count = 0: Remove all occurrences.
func (d *DB) LRem(key string, count int, value string) (int, error) {
	return lrem(d.db, key, count, value)
}

// Tx is the list repository for transactional operations.
type Tx struct {
	db    *sql.Tx
	keyTx *rkey.Tx
}

// NewTx creates a new transactional list repository.
func NewTx(tx *sql.Tx, keyTx *rkey.Tx) *Tx {
	return &Tx{db: tx, keyTx: keyTx}
}

// LPush prepends values to a list.
func (tx *Tx) LPush(key string, values ...string) (int, error) {
	return lpushTx(tx.db, tx.keyTx, key, values...)
}

// RPush appends values to a list.
func (tx *Tx) RPush(key string, values ...string) (int, error) {
	return rpushTx(tx.db, tx.keyTx, key, values...)
}

// LPop removes and returns the first element.
func (tx *Tx) LPop(key string) (string, error) {
	return lpopTx(tx.db, tx.keyTx, key)
}

// RPop removes and returns the last element.
func (tx *Tx) RPop(key string) (string, error) {
	return rpopTx(tx.db, tx.keyTx, key)
}

// LIndex returns the element at index.
func (tx *Tx) LIndex(key string, index int) (string, error) {
	return lindex(tx.db, key, index)
}

// LRange returns elements from start to stop.
func (tx *Tx) LRange(key string, start, stop int) ([]string, error) {
	return lrange(tx.db, key, start, stop)
}

// LLen returns the length of a list.
func (tx *Tx) LLen(key string) (int, error) {
	return llen(tx.db, key)
}

// LSet sets the element at index.
func (tx *Tx) LSet(key string, index int, value string) error {
	return lsetTx(tx.db, tx.keyTx, key, index, value)
}

// LTrim trims a list to the specified range.
func (tx *Tx) LTrim(key string, start, stop int) error {
	return ltrimTx(tx.db, tx.keyTx, key, start, stop)
}

// --- Implementation functions ---

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
	const query = `SELECT MIN(pos) FROM rlist WHERE key_id = ?`
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
	const query = `SELECT MAX(pos) FROM rlist WHERE key_id = ?`
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
	const insert = `INSERT INTO rlist (key_id, pos, value) VALUES (?, ?, ?)`
	for i := len(values) - 1; i >= 0; i-- {
		minPos -= posGap
		if _, err := db.Exec(insert, keyID, minPos, []byte(values[i])); err != nil {
			return 0, err
		}
	}

	return llen(db, key)
}

func lpushTx(db Querier, keyTx *rkey.Tx, key string, values ...string) (int, error) {
	if len(values) == 0 {
		return llen(db, key)
	}

	k, err := keyTx.GetOrCreate(key, rkey.TypeList)
	if err != nil {
		return 0, err
	}

	minPos, err := getMinPos(db, k.ID)
	if err != nil {
		return 0, err
	}

	const insert = `INSERT INTO rlist (key_id, pos, value) VALUES (?, ?, ?)`
	for i := len(values) - 1; i >= 0; i-- {
		minPos -= posGap
		if _, err := db.Exec(insert, k.ID, minPos, []byte(values[i])); err != nil {
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

	const insert = `INSERT INTO rlist (key_id, pos, value) VALUES (?, ?, ?)`
	for _, value := range values {
		maxPos += posGap
		if _, err := db.Exec(insert, keyID, maxPos, []byte(value)); err != nil {
			return 0, err
		}
	}

	return llen(db, key)
}

func rpushTx(db Querier, keyTx *rkey.Tx, key string, values ...string) (int, error) {
	if len(values) == 0 {
		return llen(db, key)
	}

	k, err := keyTx.GetOrCreate(key, rkey.TypeList)
	if err != nil {
		return 0, err
	}

	maxPos, err := getMaxPos(db, k.ID)
	if err != nil {
		return 0, err
	}

	const insert = `INSERT INTO rlist (key_id, pos, value) VALUES (?, ?, ?)`
	for _, value := range values {
		maxPos += posGap
		if _, err := db.Exec(insert, k.ID, maxPos, []byte(value)); err != nil {
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
		WHERE key_id = ?
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

	const del = `DELETE FROM rlist WHERE key_id = ? AND pos = ?`
	if _, err := db.Exec(del, keyID, pos); err != nil {
		return "", err
	}

	return string(value), nil
}

func lpopTx(db Querier, keyTx *rkey.Tx, key string) (string, error) {
	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const query = `
		SELECT pos, value FROM rlist
		WHERE key_id = ?
		ORDER BY pos ASC
		LIMIT 1
	`
	var pos float64
	var value []byte
	if err := db.QueryRow(query, k.ID).Scan(&pos, &value); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const del = `DELETE FROM rlist WHERE key_id = ? AND pos = ?`
	if _, err := db.Exec(del, k.ID, pos); err != nil {
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
		WHERE key_id = ?
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

	const del = `DELETE FROM rlist WHERE key_id = ? AND pos = ?`
	if _, err := db.Exec(del, keyID, pos); err != nil {
		return "", err
	}

	return string(value), nil
}

func rpopTx(db Querier, keyTx *rkey.Tx, key string) (string, error) {
	k, err := keyTx.Get(key)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const query = `
		SELECT pos, value FROM rlist
		WHERE key_id = ?
		ORDER BY pos DESC
		LIMIT 1
	`
	var pos float64
	var value []byte
	if err := db.QueryRow(query, k.ID).Scan(&pos, &value); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	const del = `DELETE FROM rlist WHERE key_id = ? AND pos = ?`
	if _, err := db.Exec(del, k.ID, pos); err != nil {
		return "", err
	}

	return string(value), nil
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
			WHERE key_id = ?
			ORDER BY pos ASC
			LIMIT 1 OFFSET ?
		`
	} else {
		query = `
			SELECT pos FROM rlist
			WHERE key_id = ?
			ORDER BY pos DESC
			LIMIT 1 OFFSET ?
		`
		index = -index - 1
	}

	var pos float64
	if err := db.QueryRow(query, keyID, index).Scan(&pos); err != nil {
		return err
	}

	const update = `UPDATE rlist SET value = ? WHERE key_id = ? AND pos = ?`
	_, err = db.Exec(update, []byte(value), keyID, pos)
	return err
}

func lsetTx(db Querier, keyTx *rkey.Tx, key string, index int, value string) error {
	k, err := keyTx.Get(key)
	if err != nil {
		return err
	}

	var query string
	if index >= 0 {
		query = `
			SELECT pos FROM rlist
			WHERE key_id = ?
			ORDER BY pos ASC
			LIMIT 1 OFFSET ?
		`
	} else {
		query = `
			SELECT pos FROM rlist
			WHERE key_id = ?
			ORDER BY pos DESC
			LIMIT 1 OFFSET ?
		`
		index = -index - 1
	}

	var pos float64
	if err := db.QueryRow(query, k.ID, index).Scan(&pos); err != nil {
		return err
	}

	const update = `UPDATE rlist SET value = ? WHERE key_id = ? AND pos = ?`
	_, err = db.Exec(update, []byte(value), k.ID, pos)
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
		WHERE key_id = ?
		ORDER BY pos ASC
		LIMIT ? OFFSET ?
	`
	limit := stop - start + 1
	if limit <= 0 {
		// Delete all
		const delAll = `DELETE FROM rlist WHERE key_id = ?`
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
		const delAll = `DELETE FROM rlist WHERE key_id = ?`
		_, err = db.Exec(delAll, keyID)
		return err
	}

	// Delete positions outside the range
	minKeep := keepPositions[0]
	maxKeep := keepPositions[len(keepPositions)-1]

	const delOutside = `DELETE FROM rlist WHERE key_id = ? AND (pos < ? OR pos > ?)`
	_, err = db.Exec(delOutside, keyID, minKeep, maxKeep)
	return err
}

func ltrimTx(db Querier, _ *rkey.Tx, key string, start, stop int) error {
	return ltrim(db, key, start, stop)
}

func linsert(db Querier, key, pivot, value string, before bool) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return -1, err
	}

	// Find pivot position
	const findPivot = `
		SELECT pos FROM rlist
		WHERE key_id = ? AND value = ?
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
			WHERE key_id = ? AND pos < ?
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
			WHERE key_id = ? AND pos > ?
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

	const insert = `INSERT INTO rlist (key_id, pos, value) VALUES (?, ?, ?)`
	if _, err := db.Exec(insert, keyID, newPos, []byte(value)); err != nil {
		return 0, err
	}

	return llen(db, key)
}

func lrem(db Querier, key string, count int, value string) (int, error) {
	keyID, err := getKeyID(db, key)
	if err != nil || keyID == 0 {
		return 0, err
	}

	var query string
	var limit string

	if count == 0 {
		// Remove all
		query = `
			SELECT pos FROM rlist
			WHERE key_id = ? AND value = ?
		`
	} else if count > 0 {
		// Remove from head
		query = `
			SELECT pos FROM rlist
			WHERE key_id = ? AND value = ?
			ORDER BY pos ASC
		`
		limit = " LIMIT " + string(rune(count+'0'))
	} else {
		// Remove from tail
		query = `
			SELECT pos FROM rlist
			WHERE key_id = ? AND value = ?
			ORDER BY pos DESC
		`
		limit = " LIMIT " + string(rune(-count+'0'))
	}

	rows, err := db.Query(query+limit, keyID, []byte(value))
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
		const del = `DELETE FROM rlist WHERE key_id = ? AND pos = ?`
		if _, err := db.Exec(del, keyID, pos); err != nil {
			return removed, err
		}
		removed++
	}

	return removed, nil
}
