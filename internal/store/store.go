// Package store is what the tursokv repositories' DB-path methods run on.
//
// WHAT CONSUMES IT: New in internal/rstring, rhash, rlist, rset and rzset,
// which take a Store, and their DB methods, which run every call of more than
// one statement through Store.Atomic, directly or through Do. WHAT IT
// CONSUMES: *sqlx.DB, the Store tursokv.Open passes to every repository.
package store

import "database/sql"

// Store runs a single statement in autocommit through Exec, Query and
// QueryRow, and a call made of several statements through Atomic, which
// commits them together and runs the call again when the engine reports a
// write-write conflict.
type Store interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Atomic(fn func(tx *sql.Tx) error) error
}

// Do runs op in one transaction of s through Atomic and returns the result of
// the run that committed. When no run commits it returns the zero value with
// the error: the transaction kept none of op's writes, so a count or value op
// computed would describe writes that did not happen.
//
// op may run more than once, so it must run every statement through tx.
func Do[T any](s Store, op func(tx *sql.Tx) (T, error)) (T, error) {
	var out T
	err := s.Atomic(func(tx *sql.Tx) error {
		var err error
		out, err = op(tx)
		return err
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}
