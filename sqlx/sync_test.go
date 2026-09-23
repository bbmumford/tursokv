package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingSyncer records the order of sync calls and fails a call when its
// error is set.
type recordingSyncer struct {
	mu       sync.Mutex
	calls    []string
	pushErr  error
	pullErr  error
	inFlight int
	overlap  bool
}

func (r *recordingSyncer) record(call string) func() {
	r.mu.Lock()
	r.calls = append(r.calls, call)
	r.inFlight++
	if r.inFlight > 1 {
		r.overlap = true
	}
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		r.inFlight--
		r.mu.Unlock()
	}
}

func (r *recordingSyncer) Push(context.Context) error {
	defer r.record("push")()
	time.Sleep(time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pushErr
}

func (r *recordingSyncer) Pull(context.Context) (bool, error) {
	defer r.record("pull")()
	time.Sleep(time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	return false, r.pullErr
}

func (r *recordingSyncer) Checkpoint(context.Context) error {
	defer r.record("checkpoint")()
	return nil
}

func (r *recordingSyncer) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func TestSyncPushesThenPulls(t *testing.T) {
	errPush := errors.New("push refused")
	errPull := errors.New("pull refused")
	cases := []struct {
		name      string
		pushErr   error
		pullErr   error
		wantErr   error
		wantCalls []string
	}{
		{"both succeed", nil, nil, nil, []string{"push", "pull"}},
		{"push fails, no pull", errPush, nil, errPush, []string{"push"}},
		{"pull fails", nil, errPull, errPull, []string{"push", "pull"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingSyncer{pushErr: tc.pushErr, pullErr: tc.pullErr}
			db := &DB{sync: rec}
			if err := db.Sync(context.Background()); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Sync err = %v, want %v", err, tc.wantErr)
			}
			if got := rec.snapshot(); !reflect.DeepEqual(got, tc.wantCalls) {
				t.Fatalf("calls = %v, want %v", got, tc.wantCalls)
			}
		})
	}
}

// TestSyncLoopRunsOneRoundPerTick runs the background loop against a
// recorder: every round is exactly one push followed by one pull, callers'
// Sync never overlaps a round, failures reach the report function, and no
// round runs once stopSync has returned.
func TestSyncLoopRunsOneRoundPerTick(t *testing.T) {
	rec := &recordingSyncer{pushErr: errors.New("offline")}
	db := &DB{sync: rec}
	reports := make(chan error, 64)
	db.startSync(5*time.Millisecond, func(err error) { reports <- err })

	// Pushes keep failing until the loop itself has reported one: the calls
	// made here can reach three before the loop's first tick, and a loop
	// that never ran a failing round has nothing to report.
	deadline := time.Now().Add(5 * time.Second)
	for (len(rec.snapshot()) < 3 || len(reports) == 0) && time.Now().Before(deadline) {
		_ = db.Sync(context.Background())
		time.Sleep(2 * time.Millisecond)
	}
	rec.mu.Lock()
	rec.pushErr = nil
	rec.mu.Unlock()
	// Then wait for a round that pushed and pulled, however many calls the
	// failing phase already recorded.
	pulled := func() bool { return strings.Contains(strings.Join(rec.snapshot(), ","), "push,pull") }
	for (len(rec.snapshot()) < 12 || !pulled()) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	db.stopSync()
	stopped := rec.snapshot()
	time.Sleep(30 * time.Millisecond)
	if after := rec.snapshot(); len(after) != len(stopped) {
		t.Fatalf("calls after stopSync: %v", after[len(stopped):])
	}

	if rec.overlap {
		t.Fatal("two sync calls ran at once")
	}
	if len(reports) == 0 {
		t.Fatal("failed rounds were not reported")
	}
	// Failed rounds are a lone push; successful rounds are push then pull.
	for i := 0; i < len(stopped); i++ {
		if stopped[i] != "push" {
			t.Fatalf("call %d = %q in %v, want every round to start with push", i, stopped[i], stopped)
		}
		if i+1 < len(stopped) && stopped[i+1] == "pull" {
			i++
		}
	}
	if !strings.Contains(strings.Join(stopped, ","), "push,pull") {
		t.Fatalf("no round pulled after pushing: %v", stopped)
	}
}

func TestSyncCallsAreNoopsWithoutRemote(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "local.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := db.Push(ctx); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if changed, err := db.Pull(ctx); changed || err != nil {
		t.Fatalf("Pull = %v, %v", changed, err)
	}
	if err := db.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

// pathRecorder is a remote that refuses everything and records the requests
// the sync engine sends it.
type pathRecorder struct {
	mu    sync.Mutex
	paths []string
}

func (p *pathRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	p.paths = append(p.paths, r.Method+" "+r.URL.Path)
	p.mu.Unlock()
	http.Error(w, "refused by test", http.StatusServiceUnavailable)
}

func (p *pathRecorder) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.paths...)
}

// openOfflineReplica opens a synced DB through Open's own wiring with the
// engine's first pull skipped, so no remote has to serve the sync protocol.
func openOfflineReplica(t *testing.T, opts *Options) *DB {
	t.Helper()
	db, err := open(filepath.Join(t.TempDir(), "replica.db"), opts, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestSyncedReplicaEnforcesForeignKeys opens a real embedded replica. Its
// connections come from tursogo's sync connector with foreign keys off, and
// the pool Open builds must prove them on.
func TestSyncedReplicaEnforcesForeignKeys(t *testing.T) {
	remote := httptest.NewServer(&pathRecorder{})
	defer remote.Close()
	db := openOfflineReplica(t, &Options{SyncURL: remote.URL})

	if db.WriteMode() != WriteModeSerial {
		t.Fatalf("WriteMode = %v, want serial", db.WriteMode())
	}
	if got := db.DB().Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", got)
	}
	if _, err := db.Exec(fkSchema); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c, err := db.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	requireForeignKeysEnforced(t, ctx, c, "replica connection")
	var mode string
	if err := c.QueryRowContext(ctx, "PRAGMA journal_mode=mvcc").Scan(&mode); err == nil && mode == "mvcc" {
		t.Fatal("the replica accepted the MVCC journal mode; WriteModeConcurrent would not need refusing")
	}
	c.Close()

	if err := db.Update(func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO rkey (key, type) VALUES ('k', 1); INSERT INTO rstring (key_id, value) VALUES (last_insert_rowid(), 'v')")
		return err
	}); err != nil {
		t.Fatalf("Update on replica: %v", err)
	}
	var values int
	if err := db.QueryRow("SELECT COUNT(*) FROM rstring").Scan(&values); err != nil || values != 1 {
		t.Fatalf("rstring rows before the delete = %d (err %v), want 1", values, err)
	}
	if _, err := db.Exec("DELETE FROM rkey WHERE key = 'k'"); err != nil {
		t.Fatal(err)
	}
	var orphans int
	if err := db.QueryRow("SELECT COUNT(*) FROM rstring").Scan(&orphans); err != nil || orphans != 0 {
		t.Fatalf("rstring rows after deleting their key = %d (err %v), want 0", orphans, err)
	}
}

// TestSyncedReplicaSyncPushesFirst runs Sync against a remote that refuses
// every request: the engine's first request is the push of the local writes,
// Sync reports the push failure, and no pull is attempted after it.
func TestSyncedReplicaSyncPushesFirst(t *testing.T) {
	rec := &pathRecorder{}
	remote := httptest.NewServer(rec)
	defer remote.Close()
	db := openOfflineReplica(t, &Options{SyncURL: remote.URL})
	if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('pending', 1)"); err != nil {
		t.Fatal(err)
	}

	err := db.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "sync push") {
		t.Fatalf("Sync err = %v, want a push failure", err)
	}
	paths := rec.snapshot()
	t.Logf("Sync requests: %v", paths)
	if len(paths) == 0 {
		t.Fatal("Sync sent no request")
	}
	for _, p := range paths {
		if strings.Contains(p, "pull") {
			t.Fatalf("Sync pulled after a failed push: %v", paths)
		}
	}

	if _, err := db.Pull(context.Background()); err == nil {
		t.Fatal("Pull against a refusing remote succeeded")
	}
	var pulled bool
	for _, p := range rec.snapshot()[len(paths):] {
		pulled = pulled || strings.Contains(p, "pull")
	}
	t.Logf("Pull requests: %v", rec.snapshot()[len(paths):])
	if !pulled {
		t.Fatalf("Pull sent no pull request: %v", rec.snapshot())
	}
}

// TestSyncedReplicaReprovesAfterReconnect discards the replica's connection
// several times. Each replacement comes from the sync engine with foreign keys
// off and must be proved before use, and the replica's rows stay readable.
func TestSyncedReplicaReprovesAfterReconnect(t *testing.T) {
	remote := httptest.NewServer(&pathRecorder{})
	defer remote.Close()
	db := openOfflineReplica(t, &Options{SyncURL: remote.URL})
	if _, err := db.Exec(fkSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('kept', 1)"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 3 {
		c, err := db.DB().Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("discard %d: %v", i, err)
		}
	}
	c, err := db.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	requireForeignKeysEnforced(t, ctx, c, "reconnected replica connection")
	var n int
	if err := c.QueryRowContext(ctx, "SELECT COUNT(*) FROM rkey WHERE key = 'kept'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows after reconnect = %d (err %v), want 1", n, err)
	}
}

// TestSyncedReplicaBackgroundLoop opens replicas through Open's wiring
// against a remote that refuses every request. With a SyncInterval the one
// background loop pushes, reports each failure and stops at Close; without one,
// nothing reaches the remote.
func TestSyncedReplicaBackgroundLoop(t *testing.T) {
	t.Run("interval set", func(t *testing.T) {
		rec := &pathRecorder{}
		remote := httptest.NewServer(rec)
		defer remote.Close()
		reports := make(chan error, 256)
		db, err := open(filepath.Join(t.TempDir(), "loop.db"), &Options{
			SyncURL:      remote.URL,
			SyncInterval: 5 * time.Millisecond,
			SyncErrors:   func(err error) { reports <- err },
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('queued', 1)"); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for len(reports) < 3 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		closed := rec.snapshot()
		time.Sleep(30 * time.Millisecond)
		if after := rec.snapshot(); len(after) != len(closed) {
			t.Fatalf("requests after Close: %v", after[len(closed):])
		}
		if len(reports) < 3 {
			t.Fatalf("reported failures = %d, want at least 3", len(reports))
		}
		for len(reports) > 0 {
			if err := <-reports; !strings.Contains(err.Error(), "sync push") {
				t.Fatalf("reported %v, want a push failure", err)
			}
		}
		for _, p := range closed {
			if p != "POST /v2/pipeline" {
				t.Fatalf("loop sent %q; every round must start with the push: %v", p, closed)
			}
		}
	})
	t.Run("no interval", func(t *testing.T) {
		rec := &pathRecorder{}
		remote := httptest.NewServer(rec)
		defer remote.Close()
		db := openOfflineReplica(t, &Options{SyncURL: remote.URL})
		if _, err := db.Exec("INSERT INTO rkey (key, type) VALUES ('queued', 1)"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		if got := rec.snapshot(); len(got) != 0 {
			t.Fatalf("requests with no SyncInterval: %v", got)
		}
	})
}
