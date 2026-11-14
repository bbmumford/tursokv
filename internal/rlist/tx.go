package rlist

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rkey"
)

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

	const insert = `INSERT INTO rlist (keyId, pos, value) VALUES (?, ?, ?)`
	for i := len(values) - 1; i >= 0; i-- {
		minPos -= posGap
		if _, err := db.Exec(insert, k.ID, minPos, []byte(values[i])); err != nil {
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

	const insert = `INSERT INTO rlist (keyId, pos, value) VALUES (?, ?, ?)`
	for _, value := range values {
		maxPos += posGap
		if _, err := db.Exec(insert, k.ID, maxPos, []byte(value)); err != nil {
			return 0, err
		}
	}

	return llen(db, key)
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
		WHERE keyId = ?
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

	const del = `DELETE FROM rlist WHERE keyId = ? AND pos = ?`
	if _, err := db.Exec(del, k.ID, pos); err != nil {
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
		WHERE keyId = ?
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

	const del = `DELETE FROM rlist WHERE keyId = ? AND pos = ?`
	if _, err := db.Exec(del, k.ID, pos); err != nil {
		return "", err
	}

	return string(value), nil
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
	if err := db.QueryRow(query, k.ID, index).Scan(&pos); err != nil {
		return err
	}

	const update = `UPDATE rlist SET value = ? WHERE keyId = ? AND pos = ?`
	_, err = db.Exec(update, []byte(value), k.ID, pos)
	return err
}

func ltrimTx(db Querier, _ *rkey.Tx, key string, start, stop int) error {
	return ltrim(db, key, start, stop)
}
