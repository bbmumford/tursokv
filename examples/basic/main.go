// Example: Basic TursoKV usage demonstrating all data types
//
// This shows how to use TursoKV as a Redis-compatible key-value store
// backed by Turso/libraryQL with full ACID transactions.

package main

import (
	"fmt"
	"log"

	"github.com/bbmumford/tursokv"
	"github.com/bbmumford/tursokv/internal/rzset"
)

func main() {
	// Open database (local only, no cloud sync)
	db, err := tursokv.Open("example.db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// String operations
	stringExample(db)

	// Hash operations
	hashExample(db)

	// List operations
	listExample(db)

	// Set operations
	setExample(db)

	// Sorted set operations
	zsetExample(db)

	// Transaction example
	transactionExample(db)
}

func stringExample(db *tursokv.DB) {
	fmt.Println("=== String Operations ===")

	// Basic set/get
	db.Str().Set("name", "alice")
	name, _ := db.Str().Get("name")
	fmt.Printf("name: %s\n", name)

	// Increment
	db.Str().Set("counter", "0")
	count, _ := db.Str().Incr("counter")
	fmt.Printf("counter after incr: %d\n", count)

	// Multiple keys
	db.Str().MSet(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	values, _ := db.Str().MGet("key1", "key2")
	fmt.Printf("mget: %v\n", values)

	fmt.Println()
}

func hashExample(db *tursokv.DB) {
	fmt.Println("=== Hash Operations ===")

	// Set hash fields
	db.Hash().Set("user:1", "name", "Alice")
	db.Hash().Set("user:1", "email", "alice@example.com")
	db.Hash().Set("user:1", "age", "30")

	// Get single field
	name, _ := db.Hash().Get("user:1", "name")
	fmt.Printf("user:1 name: %s\n", name)

	// Get all fields
	user, _ := db.Hash().GetAll("user:1")
	fmt.Printf("user:1 all: %v\n", user)

	// Increment field
	newAge, _ := db.Hash().IncrBy("user:1", "age", 1)
	fmt.Printf("user:1 age after incr: %d\n", newAge)

	fmt.Println()
}

func listExample(db *tursokv.DB) {
	fmt.Println("=== List Operations ===")

	// Push items
	db.List().RPush("queue", "item1", "item2", "item3")
	db.List().LPush("queue", "item0")

	// Get range
	items, _ := db.List().LRange("queue", 0, -1)
	fmt.Printf("queue: %v\n", items)

	// Pop items
	first, _ := db.List().LPop("queue")
	last, _ := db.List().RPop("queue")
	fmt.Printf("popped first: %s, last: %s\n", first, last)

	// List length
	length, _ := db.List().LLen("queue")
	fmt.Printf("queue length: %d\n", length)

	fmt.Println()
}

func setExample(db *tursokv.DB) {
	fmt.Println("=== Set Operations ===")

	// Add members
	db.Set().Add("tags", "golang", "redis", "database")
	db.Set().Add("tags2", "golang", "python", "rust")

	// Get all members
	members, _ := db.Set().Members("tags")
	fmt.Printf("tags: %v\n", members)

	// Check membership
	isMember, _ := db.Set().IsMember("tags", "golang")
	fmt.Printf("is 'golang' in tags: %v\n", isMember)

	// Set operations
	union, _ := db.Set().Union("tags", "tags2")
	fmt.Printf("union: %v\n", union)

	inter, _ := db.Set().Inter("tags", "tags2")
	fmt.Printf("intersection: %v\n", inter)

	diff, _ := db.Set().Diff("tags", "tags2")
	fmt.Printf("difference: %v\n", diff)

	fmt.Println()
}

func zsetExample(db *tursokv.DB) {
	fmt.Println("=== Sorted Set Operations ===")

	// Add members with scores
	db.ZSet().Add("leaderboard",
		rzset.ScoreMember{Member: "alice", Score: 100},
		rzset.ScoreMember{Member: "bob", Score: 85},
		rzset.ScoreMember{Member: "charlie", Score: 92},
	)

	// Get top scores (highest first)
	top, _ := db.ZSet().RevRangeWithScores("leaderboard", 0, 2)
	fmt.Println("Leaderboard:")
	for i, sm := range top {
		fmt.Printf("  %d. %s: %.0f\n", i+1, sm.Member, sm.Score)
	}

	// Get rank
	rank, _, _ := db.ZSet().RevRank("leaderboard", "bob")
	fmt.Printf("bob's rank: %d (0-based)\n", rank)

	// Increment score
	newScore, _ := db.ZSet().IncrBy("leaderboard", "bob", 20)
	fmt.Printf("bob's new score: %.0f\n", newScore)

	fmt.Println()
}

func transactionExample(db *tursokv.DB) {
	fmt.Println("=== Transaction Example ===")

	// Atomic transaction
	err := db.Update(func(tx *tursokv.Tx) error {
		// All operations succeed or fail together
		tx.Str().Set("tx:key1", "value1")
		tx.Str().Set("tx:key2", "value2")

		// Read within transaction sees uncommitted changes
		v1, _ := tx.Str().Get("tx:key1")
		fmt.Printf("inside tx - key1: %s\n", v1)

		return nil // commit
	})

	if err != nil {
		log.Printf("transaction failed: %v", err)
	} else {
		fmt.Println("transaction committed")
	}

	// Read-only transaction
	db.View(func(tx *tursokv.Tx) error {
		v1, _ := tx.Str().Get("tx:key1")
		v2, _ := tx.Str().Get("tx:key2")
		fmt.Printf("after commit - key1: %s, key2: %s\n", v1, v2)
		return nil
	})
}
