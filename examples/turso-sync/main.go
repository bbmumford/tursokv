// Example: TursoKV as an embedded replica of a Turso Cloud database.
//
// Writes land in the local file first; Sync pushes them to the remote and
// pulls the remote's changes, and a background loop does the same every
// SyncInterval. A synced replica takes writes serially and is stored
// unencrypted. Keep one writing replica per database: two replicas that both
// create keys can lose one another's writes without an error (see DB.Sync).

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bbmumford/tursokv"
)

func main() {
	db, err := tursokv.Open("local-replica.db", &tursokv.Options{
		SyncURL:   "libsql://mydb-myorg.turso.io",
		AuthToken: "your-turso-auth-token",

		// Push and pull every 30 seconds.
		SyncInterval: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("write mode:", db.WriteMode())

	if err := db.Str().Set("session:abc123", "user:42"); err != nil {
		log.Fatal(err)
	}

	// Push this write now instead of waiting for the loop.
	if err := db.Sync(context.Background()); err != nil {
		log.Printf("sync failed: %v", err)
	}

	if err := db.Checkpoint(context.Background()); err != nil {
		log.Printf("checkpoint failed: %v", err)
	}

	fmt.Println("Data written and synced to Turso Cloud")
}
