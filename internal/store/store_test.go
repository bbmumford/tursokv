package store

import (
	"database/sql"
	"errors"
	"testing"
)

// commitFails is a Store whose Atomic runs fn with no transaction and then
// reports err, as Atomic does when fn succeeded but its COMMIT did not.
type commitFails struct {
	Store
	err error
}

func (c commitFails) Atomic(fn func(tx *sql.Tx) error) error {
	if err := fn(nil); err != nil {
		return err
	}
	return c.err
}

func TestDoReturnsZeroWhenNothingCommits(t *testing.T) {
	refused := errors.New("commit refused")
	got, err := Do(commitFails{err: refused}, func(*sql.Tx) (int, error) { return 5, nil })
	if !errors.Is(err, refused) || got != 0 {
		t.Fatalf("Do = %d, %v; want 0 and the commit error, since the 5 describes writes that were not kept", got, err)
	}

	opFailed := errors.New("op failed")
	got, err = Do(commitFails{}, func(*sql.Tx) (int, error) { return 3, opFailed })
	if !errors.Is(err, opFailed) || got != 0 {
		t.Fatalf("Do = %d, %v; want 0 and the op's error", got, err)
	}

	got, err = Do(commitFails{}, func(*sql.Tx) (int, error) { return 7, nil })
	if err != nil || got != 7 {
		t.Fatalf("Do = %d, %v; want 7 from the run that committed", got, err)
	}
}
