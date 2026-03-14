// Example: TursoKV with Turso Cloud sync
//
// This demonstrates using embedded replicas with automatic sync to Turso Cloud,
// encryption at rest, and concurrent writes.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bbmumford/tursokv"
)

func main() {
	// Open with Turso Cloud sync
	db, err := tursokv.Open("local-replica.db", &tursokv.Options{
		// Turso Cloud connection (embedded replica mode)
		SyncURL:   "libsql://mydb-myorg.turso.io",
		AuthToken: "your-turso-auth-token",

		// Sync every 30 seconds
		SyncInterval: 30 * time.Second,

		// Enable encryption at rest (32-byte hex key)
		EncryptionKey: "0123456789abcdef0123456789abcdef",

		// Enable concurrent writes (MVCC)
		UseConcurrent: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Normal operations - all data persists locally AND syncs to cloud
	db.Str().Set("session:abc123", "user:42")

	// Manual sync if needed
	if err := db.Sync(context.Background()); err != nil {
		log.Printf("sync failed: %v", err)
	}

	// Checkpoint to optimize WAL
	if err := db.Checkpoint(context.Background()); err != nil {
		log.Printf("checkpoint failed: %v", err)
	}

	fmt.Println("Data written and synced to Turso Cloud")
}
