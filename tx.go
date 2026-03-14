package tursokv

import (
	"database/sql"

	"github.com/bbmumford/tursokv/internal/rhash"
	"github.com/bbmumford/tursokv/internal/rkey"
	"github.com/bbmumford/tursokv/internal/rlist"
	"github.com/bbmumford/tursokv/internal/rset"
	"github.com/bbmumford/tursokv/internal/rstring"
	"github.com/bbmumford/tursokv/internal/rzset"
)

// Tx is a transaction wrapper providing access to all data type repositories.
// A Tx is either read-only (from View) or read-write (from Update).
type Tx struct {
	tx       *sql.Tx
	readOnly bool

	// Repositories scoped to this transaction
	keyRepo  *rkey.Tx
	strRepo  *rstring.Tx
	hashRepo *rhash.Tx
	listRepo *rlist.Tx
	setRepo  *rset.Tx
	zsetRepo *rzset.Tx
}

// newTx creates a new transaction wrapper.
func newTx(tx *sql.Tx, keyDB *rkey.DB, readOnly bool) *Tx {
	keyTx := keyDB.Tx(tx)
	return &Tx{
		tx:       tx,
		readOnly: readOnly,
		keyRepo:  keyTx,
		strRepo:  rstring.NewTx(tx, keyTx),
		hashRepo: rhash.NewTx(tx, keyTx),
		listRepo: rlist.NewTx(tx, keyTx),
		setRepo:  rset.NewTx(tx, keyTx),
		zsetRepo: rzset.NewTx(tx, keyTx),
	}
}

// Key returns the key repository for this transaction.
func (tx *Tx) Key() *rkey.Tx {
	return tx.keyRepo
}

// Str returns the string repository for this transaction.
func (tx *Tx) Str() *rstring.Tx {
	return tx.strRepo
}

// Hash returns the hash repository for this transaction.
func (tx *Tx) Hash() *rhash.Tx {
	return tx.hashRepo
}

// List returns the list repository for this transaction.
func (tx *Tx) List() *rlist.Tx {
	return tx.listRepo
}

// Set returns the set repository for this transaction.
func (tx *Tx) Set() *rset.Tx {
	return tx.setRepo
}

// ZSet returns the sorted set repository for this transaction.
func (tx *Tx) ZSet() *rzset.Tx {
	return tx.zsetRepo
}

// ReadOnly returns true if this is a read-only transaction.
func (tx *Tx) ReadOnly() bool {
	return tx.readOnly
}
