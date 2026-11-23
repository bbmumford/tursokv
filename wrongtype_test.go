package tursokv_test

import (
	"errors"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
	"github.com/bbmumford/tursokv/internal/rzset"
)

// A value-type write over a live key of another type answers ErrWrongType.
func TestAWriteOverAnotherTypeAnswersErrWrongType(t *testing.T) {
	db := openKV(t)
	if err := db.Str().Set("t", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Set().Add("t", "m"); !errors.Is(err, tursokv.ErrWrongType) {
		t.Fatalf("SADD: got %v, want ErrWrongType", err)
	}
	if _, err := db.Hash().Set("t", "f", "x"); !errors.Is(err, tursokv.ErrWrongType) {
		t.Fatalf("HSET: got %v, want ErrWrongType", err)
	}
	if _, err := db.List().LPush("t", "a"); !errors.Is(err, tursokv.ErrWrongType) {
		t.Fatalf("LPUSH: got %v, want ErrWrongType", err)
	}
	if _, err := db.ZSet().Add("t", rzset.ScoreMember{Score: 1, Member: "a"}); !errors.Is(err, tursokv.ErrWrongType) {
		t.Fatalf("ZADD: got %v, want ErrWrongType", err)
	}
}

// A key written again keeps its expiry: a set member added to a live key with
// a TTL leaves the TTL in force.
func TestAWriteToALiveKeyKeepsItsExpiry(t *testing.T) {
	db := openKV(t)
	if _, err := db.Set().Add("s", "a"); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Key().Expire("s", time.Hour); err != nil || !ok {
		t.Fatalf("Expire: %v %v", ok, err)
	}
	if _, err := db.Set().Add("s", "b"); err != nil {
		t.Fatal(err)
	}
	if k, err := db.Key().Get("s"); err != nil || k.ETime == nil {
		t.Fatalf("the second write dropped the expiry: %+v, %v", k, err)
	}
}

// A key recreated after its expiry holds only what was written since: the
// expired key's values are gone.
func TestAKeyRecreatedAfterExpiryHoldsOnlyNewValues(t *testing.T) {
	db := openKV(t)
	if _, err := db.List().RPush("l", "old"); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Key().Expire("l", 20*time.Millisecond); err != nil || !ok {
		t.Fatalf("Expire: %v %v", ok, err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := db.List().RPush("l", "new"); err != nil {
		t.Fatal(err)
	}
	if got, err := db.List().LRange("l", 0, -1); err != nil || len(got) != 1 || got[0] != "new" {
		t.Fatalf("the recreated list reads %v, %v; want only the new value", got, err)
	}
}
