package tursokv_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
	"github.com/bbmumford/tursokv/internal/rzset"
)

func openKV(t *testing.T) *tursokv.DB {
	t.Helper()
	db, err := tursokv.Open(filepath.Join(t.TempDir(), "kv.db"), &tursokv.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func expiredKey(t *testing.T, db *tursokv.DB, key string) {
	t.Helper()
	if err := db.Str().SetEx(key, "old", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
}

// A write to a key whose expiry has passed creates the key afresh: the value
// written is the value read, for every value type.
func TestAWriteAfterExpiryIsVisible(t *testing.T) {
	db := openKV(t)

	expiredKey(t, db, "s")
	if _, err := db.Set().Add("s", "m"); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Set().Members("s"); err != nil || len(got) != 1 || got[0] != "m" {
		t.Fatalf("SADD after expiry reads back %v, %v", got, err)
	}

	expiredKey(t, db, "h")
	if _, err := db.Hash().Set("h", "f", "x"); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Hash().Get("h", "f"); err != nil || v != "x" {
		t.Fatalf("HSET after expiry reads back %q, %v", v, err)
	}

	expiredKey(t, db, "l")
	if _, err := db.List().RPush("l", "a"); err != nil {
		t.Fatal(err)
	}
	if got, err := db.List().LRange("l", 0, -1); err != nil || len(got) != 1 || got[0] != "a" {
		t.Fatalf("RPUSH after expiry reads back %v, %v", got, err)
	}

	expiredKey(t, db, "z")
	if _, err := db.ZSet().Add("z", rzset.ScoreMember{Score: 1, Member: "a"}); err != nil {
		t.Fatalf("ZADD after expiry: %v", err)
	}
	if got, err := db.ZSet().Range("z", 0, -1); err != nil || len(got) != 1 || got[0] != "a" {
		t.Fatalf("ZADD after expiry reads back %v, %v", got, err)
	}

	expiredKey(t, db, "str")
	if err := db.Str().Set("str", "new"); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Str().Get("str"); err != nil || v != "new" {
		t.Fatalf("SET after expiry reads back %q, %v", v, err)
	}
}

// SET replaces a key of any type, and the old value is gone.
func TestSetReplacesAKeyOfAnotherType(t *testing.T) {
	db := openKV(t)
	if _, err := db.Set().Add("k", "m"); err != nil {
		t.Fatal(err)
	}
	if err := db.Str().Set("k", "x"); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Str().Get("k"); err != nil || v != "x" {
		t.Fatalf("SET over a set reads back %q, %v", v, err)
	}
	if got, _ := db.Set().Members("k"); len(got) != 0 {
		t.Fatalf("the replaced set still answers %v", got)
	}
}

// A value-type write over a live key of another type never reports success
// while storing nothing readable.
func TestAWriteOverAnotherTypeIsNotLost(t *testing.T) {
	db := openKV(t)
	if err := db.Str().Set("t", "v"); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Set().Add("t", "m"); err == nil {
		got, _ := db.Set().Members("t")
		t.Fatalf("SADD over a string reported %d added, and the set reads %v", n, got)
	}
	if _, err := db.Hash().Set("t", "f", "x"); err == nil {
		t.Fatal("HSET over a string reported success")
	}
	if _, err := db.List().RPush("t", "a"); err == nil {
		t.Fatal("RPUSH over a string reported success")
	}
	if v, _ := db.Str().Get("t"); v != "v" {
		t.Fatalf("the string was changed to %q", v)
	}
}
