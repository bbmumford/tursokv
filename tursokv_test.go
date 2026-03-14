package tursokv_test

import (
	"os"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
)

func TestBasicStringOperations(t *testing.T) {
	// Use temp file for test
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Test SET/GET
	if err := db.Str().Set("name", "alice"); err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val, err := db.Str().Get("name")
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if val != "alice" {
		t.Errorf("expected 'alice', got '%s'", val)
	}

	// Test non-existent key
	val, err = db.Str().Get("nonexistent")
	if err != nil {
		t.Errorf("Get nonexistent failed: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty string for nonexistent key, got '%s'", val)
	}
}

func TestStringIncr(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// INCR on new key starts at 0
	val, err := db.Str().Incr("counter")
	if err != nil {
		t.Errorf("Incr failed: %v", err)
	}
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}

	// INCR again
	val, err = db.Str().Incr("counter")
	if err != nil {
		t.Errorf("Incr failed: %v", err)
	}
	if val != 2 {
		t.Errorf("expected 2, got %d", val)
	}

	// INCRBY
	val, err = db.Str().IncrBy("counter", 10)
	if err != nil {
		t.Errorf("IncrBy failed: %v", err)
	}
	if val != 12 {
		t.Errorf("expected 12, got %d", val)
	}

	// DECR
	val, err = db.Str().Decr("counter")
	if err != nil {
		t.Errorf("Decr failed: %v", err)
	}
	if val != 11 {
		t.Errorf("expected 11, got %d", val)
	}
}

func TestStringSetNX(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// SETNX on new key should succeed
	ok, err := db.Str().SetNX("lock", "holder1")
	if err != nil {
		t.Errorf("SetNX failed: %v", err)
	}
	if !ok {
		t.Error("expected SetNX to return true for new key")
	}

	// SETNX on existing key should fail
	ok, err = db.Str().SetNX("lock", "holder2")
	if err != nil {
		t.Errorf("SetNX failed: %v", err)
	}
	if ok {
		t.Error("expected SetNX to return false for existing key")
	}

	// Value should still be original
	val, _ := db.Str().Get("lock")
	if val != "holder1" {
		t.Errorf("expected 'holder1', got '%s'", val)
	}
}

func TestKeyExpiration(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Set with expiration
	if err := db.Str().SetEx("tempkey", "tempvalue", 100*time.Millisecond); err != nil {
		t.Errorf("SetEx failed: %v", err)
	}

	// Should exist immediately
	val, _ := db.Str().Get("tempkey")
	if val != "tempvalue" {
		t.Errorf("expected 'tempvalue', got '%s'", val)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be gone
	val, _ = db.Str().Get("tempkey")
	if val != "" {
		t.Errorf("expected empty string after expiration, got '%s'", val)
	}
}

func TestKeyOperations(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create some keys
	db.Str().Set("user:1", "alice")
	db.Str().Set("user:2", "bob")
	db.Str().Set("user:3", "charlie")
	db.Str().Set("product:1", "widget")

	// EXISTS
	count, err := db.Key().Exists("user:1", "user:2", "nonexistent")
	if err != nil {
		t.Errorf("Exists failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 existing keys, got %d", count)
	}

	// KEYS pattern
	keys, err := db.Key().Keys("user:*")
	if err != nil {
		t.Errorf("Keys failed: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("expected 3 keys matching 'user:*', got %d", len(keys))
	}

	// DELETE
	deleted, err := db.Key().Delete("user:1", "user:2")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted, got %d", deleted)
	}

	// Verify deleted
	count, _ = db.Key().Exists("user:1", "user:2")
	if count != 0 {
		t.Errorf("expected 0 existing after delete, got %d", count)
	}
}

func TestTransactions(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Write transaction
	err = db.Update(func(tx *tursokv.Tx) error {
		tx.Str().Set("tx:key1", "value1")
		tx.Str().Set("tx:key2", "value2")
		return nil
	})
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}

	// Read transaction
	var val1, val2 string
	err = db.View(func(tx *tursokv.Tx) error {
		var err error
		val1, err = tx.Str().Get("tx:key1")
		if err != nil {
			return err
		}
		val2, err = tx.Str().Get("tx:key2")
		return err
	})
	if err != nil {
		t.Errorf("View failed: %v", err)
	}
	if val1 != "value1" || val2 != "value2" {
		t.Errorf("expected 'value1'/'value2', got '%s'/'%s'", val1, val2)
	}
}

func TestMGetMSet(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// MSET
	err = db.Str().MSet(map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	})
	if err != nil {
		t.Errorf("MSet failed: %v", err)
	}

	// MGET
	vals, err := db.Str().MGet("a", "b", "c", "d")
	if err != nil {
		t.Errorf("MGet failed: %v", err)
	}
	if len(vals) != 4 {
		t.Errorf("expected 4 values, got %d", len(vals))
	}
	if vals[0] != "1" || vals[1] != "2" || vals[2] != "3" || vals[3] != "" {
		t.Errorf("unexpected values: %v", vals)
	}
}

func TestAppendStrlen(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "tursokv-test-*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := tursokv.Open(tmpFile.Name(), nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// APPEND to new key
	length, err := db.Str().Append("msg", "Hello")
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}
	if length != 5 {
		t.Errorf("expected length 5, got %d", length)
	}

	// APPEND to existing
	length, err = db.Str().Append("msg", " World")
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}
	if length != 11 {
		t.Errorf("expected length 11, got %d", length)
	}

	// Verify
	val, _ := db.Str().Get("msg")
	if val != "Hello World" {
		t.Errorf("expected 'Hello World', got '%s'", val)
	}

	// STRLEN
	length, err = db.Str().Strlen("msg")
	if err != nil {
		t.Errorf("Strlen failed: %v", err)
	}
	if length != 11 {
		t.Errorf("expected strlen 11, got %d", length)
	}
}

func TestInMemory(t *testing.T) {
	// Test in-memory database
	db, err := tursokv.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	defer db.Close()

	db.Str().Set("key", "value")
	val, _ := db.Str().Get("key")
	if val != "value" {
		t.Errorf("expected 'value', got '%s'", val)
	}
}
