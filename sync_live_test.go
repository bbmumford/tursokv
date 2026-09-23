package tursokv_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bbmumford/tursokv"
)

// syncURLEnv names the sync server TestSyncConditionsLive runs against.
const syncURLEnv = "TURSOKV_SYNC_URL"

// replicaValue reads key on db, telling a missing key from an empty value.
func replicaValue(t *testing.T, db *tursokv.DB, key string) (string, bool) {
	t.Helper()
	n, err := db.Key().Exists(key)
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		return "", false
	}
	v, err := db.Str().Get(key)
	if err != nil {
		t.Fatal(err)
	}
	return v, true
}

func rkeyID(t *testing.T, db *tursokv.DB, key string) int64 {
	t.Helper()
	k, err := db.Key().Get(key)
	if err != nil {
		t.Fatalf("rkey id of %s: %v", key, err)
	}
	return k.ID
}

// TestSyncConditionsLive drives two replicas through tursokv.Open against a
// sync server and checks the conflict behaviour the Sync documentation
// states: the later push wins a row both replicas wrote; new keys created on
// both replicas collide through their locally assigned rkey ids; a push the
// server refuses fails one Sync and is discarded by the next; and Pull
// applies remote changes through the replica's one-connection pool. Keys are
// prefixed per run, so the server need not be empty.
func TestSyncConditionsLive(t *testing.T) {
	url := os.Getenv(syncURLEnv)
	if url == "" {
		t.Skipf("set %s to a sync server URL, for example http://127.0.0.1:8080 for `tursodb server.db --sync-server 127.0.0.1:8080`", syncURLEnv)
	}
	ctx := context.Background()
	dir := t.TempDir()
	p := fmt.Sprintf("r%d:", time.Now().UnixNano())
	open := func(name string) *tursokv.DB {
		db, err := tursokv.Open(filepath.Join(dir, name), &tursokv.Options{SyncURL: url, SyncInterval: -1})
		if err != nil {
			t.Fatalf("open replica %s: %v", name, err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	syncOK := func(name string, db *tursokv.DB) {
		t.Helper()
		if err := db.Sync(ctx); err != nil {
			t.Fatalf("%s.Sync: %v", name, err)
		}
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	a := open("a.db")
	if got := a.SQL().Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("replica pool holds %d connections, want 1", got)
	}
	must(a.Str().Set(p+"seed", "0"))
	syncOK("A", a)
	b := open("b.db")
	if v, ok := replicaValue(t, b, p+"seed"); !ok || v != "0" {
		t.Fatalf("B after Open: seed = %q (present %v); the bootstrap pull did not apply A's push", v, ok)
	}

	// The later push wins a row both replicas wrote: A counts to 10, B to 3,
	// and B pushes second.
	for i := range 10 {
		_, err := a.Str().Incr(p + "seed")
		must(err)
		if i < 3 {
			_, err = b.Str().Incr(p + "seed")
			must(err)
		}
	}
	syncOK("A", a)
	syncOK("B", b)
	syncOK("A", a)
	for name, db := range map[string]*tursokv.DB{"A": a, "B": b} {
		if v, _ := replicaValue(t, db, p+"seed"); v != "3" {
			t.Errorf("%s: seed = %s after A counted to 10 and B to 3, want 3 from B's later push", name, v)
		}
	}

	// Different new keys on the two replicas take the same local rkey id;
	// the later push replaces the earlier key's row everywhere, without error.
	must(a.Str().Set(p+"a", "va"))
	must(b.Str().Set(p+"b", "vb"))
	if ia, ib := rkeyID(t, a, p+"a"), rkeyID(t, b, p+"b"); ia != ib {
		t.Fatalf("new keys took rkey ids %d and %d; the collision needs equal ids", ia, ib)
	}
	syncOK("A", a)
	syncOK("B", b)
	syncOK("A", a)
	for name, db := range map[string]*tursokv.DB{"A": a, "B": b} {
		if _, ok := replicaValue(t, db, p+"a"); ok {
			t.Errorf("%s still holds %sa; the id collision did not remove it", name, p)
		}
		if v, _ := replicaValue(t, db, p+"b"); v != "vb" {
			t.Errorf("%s: %sb = %q, want vb", name, p, v)
		}
	}

	// A push the server refuses: B creates q with the id A gave x1, while A
	// also created q. B's first Sync fails on rkey's UNIQUE key; its next Sync
	// succeeds and B reads A's q. B's value row for that id lands on x1.
	must(a.Str().Set(p+"x1", "a"))
	must(a.Str().Set(p+"q", "fromA"))
	must(b.Str().Set(p+"q", "fromB"))
	if ix, iq := rkeyID(t, a, p+"x1"), rkeyID(t, b, p+"q"); ix != iq {
		t.Fatalf("A's x1 has id %d, B's q has id %d; the case needs equal ids", ix, iq)
	}
	syncOK("A", a)
	first := b.Sync(ctx)
	if first == nil || !strings.Contains(first.Error(), "UNIQUE constraint failed: rkey.key") {
		t.Fatalf("B's first Sync = %v, want the server's UNIQUE refusal", first)
	}
	if err := b.Sync(ctx); err != nil {
		t.Fatalf("B's second Sync = %v, want nil", err)
	}
	if v, _ := replicaValue(t, b, p+"q"); v != "fromA" {
		t.Errorf("B: q = %q after the refused push, want A's fromA", v)
	}

	// Pull applies remote changes through the held one-connection pool.
	must(a.Str().Set(p+"later", "fromA"))
	syncOK("A", a)
	syncOK("B", b)
	c := open("c.db")
	for name, db := range map[string]*tursokv.DB{"A": a, "B": b, "C (fresh)": c} {
		if v, _ := replicaValue(t, db, p+"later"); v != "fromA" {
			t.Errorf("%s: later = %q, want fromA", name, v)
		}
		if v, _ := replicaValue(t, db, p+"x1"); v != "fromB" {
			t.Errorf("%s: x1 = %q, want fromB, the value B's refused push carried", name, v)
		}
		if _, ok := replicaValue(t, db, p+"a"); ok {
			t.Errorf("%s holds %sa", name, p)
		}
	}
}
