package sqlx

import (
	"context"
	"fmt"
	"time"
)

// syncer is the part of tursogo's TursoSyncDb that DB drives. Sync, Push,
// Pull, Checkpoint and the background loop call it; tests substitute a
// recorder to observe the order of calls.
type syncer interface {
	Push(ctx context.Context) error
	Pull(ctx context.Context) (bool, error)
	Checkpoint(ctx context.Context) error
}

// syncRoundTimeout bounds one background Sync, so a stalled request cannot
// hold the loop until Close.
const syncRoundTimeout = 30 * time.Second

// Sync sends local writes to the remote and then applies the remote's changes
// locally. It returns nil at once when no SyncURL is configured. Calls from
// callers and from the background loop run one at a time. A failed Push
// returns its error without running Pull.
//
// CONDITIONS, as measured with two tursokv replicas against a tursodb 0.7.2
// sync server; Turso Cloud has not been measured. A row both replicas wrote
// holds the later push's value on every replica. Each replica assigns rkey
// ids itself, so keys two replicas create between syncs can take the same
// id, and the later push replaces the earlier key's row everywhere with no
// error. A change the server refuses (rkey's UNIQUE key, for a key the other
// replica pushed first) fails that Sync; the next Sync returns nil and the
// refused change is gone from this replica, while the value row it carried
// is applied to whichever key holds its id on the server. Two replicas that
// both create keys can therefore lose one another's writes without an error.
func (db *DB) Sync(ctx context.Context) error {
	if db.sync == nil {
		return nil
	}
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	if err := db.sync.Push(ctx); err != nil {
		return fmt.Errorf("sync push: %w", err)
	}
	if _, err := db.sync.Pull(ctx); err != nil {
		return fmt.Errorf("sync pull: %w", err)
	}
	return nil
}

// Push sends local writes to the remote without pulling. It returns nil at
// once when no SyncURL is configured.
func (db *DB) Push(ctx context.Context) error {
	if db.sync == nil {
		return nil
	}
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	return db.sync.Push(ctx)
}

// Pull applies the remote's changes locally without pushing and reports
// whether any arrived. It returns false, nil at once when no SyncURL is
// configured.
func (db *DB) Pull(ctx context.Context) (bool, error) {
	if db.sync == nil {
		return false, nil
	}
	db.syncMu.Lock()
	defer db.syncMu.Unlock()
	return db.sync.Pull(ctx)
}

// Checkpoint moves the write-ahead log into the database file: through the
// sync engine for a synced database, so its sync metadata survives, and with
// PRAGMA wal_checkpoint(TRUNCATE) otherwise.
func (db *DB) Checkpoint(ctx context.Context) error {
	if db.sync != nil {
		return db.sync.Checkpoint(ctx)
	}
	_, err := db.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// startSync runs Sync every interval until stopSync, handing each failure to
// report when it is non-nil. Open starts it once, for a synced database with
// a positive SyncInterval; it is the only background sync loop.
func (db *DB) startSync(interval time.Duration, report func(error)) {
	ctx, cancel := context.WithCancel(context.Background())
	db.loopCancel = cancel
	db.loopDone = make(chan struct{})

	go func() {
		defer close(db.loopDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				roundCtx, roundCancel := context.WithTimeout(ctx, syncRoundTimeout)
				err := db.Sync(roundCtx)
				roundCancel()
				if err != nil && ctx.Err() == nil && report != nil {
					report(err)
				}
			}
		}
	}()
}

// stopSync stops the background loop and waits until it has returned, so
// Close never closes the pool under a running Sync.
func (db *DB) stopSync() {
	if db.loopCancel == nil {
		return
	}
	db.loopCancel()
	<-db.loopDone
	db.loopCancel = nil
}
