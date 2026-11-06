package shardedfilestore

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/tus/tusd/pkg/handler"
)

const MaintenanceLimit = 100
const MaintenanceBudget = 2 * time.Second

type ageCursor struct {
	timestamp int64
	id        string
}
type maintenanceState struct {
	mu          sync.Mutex
	pending     [2]string
	pendingTurn int
	expiryTurn  int
	recoverMore bool
	expireMore  bool
	expiry      [2]ageCursor
}

func newMaintenanceState() *maintenanceState {
	return &maintenanceState{expiry: [2]ageCursor{{timestamp: math.MinInt64}, {timestamp: math.MinInt64}}}
}

// Expire rechecks the deadline while holding the upload lock. Maintenance
// never waits for a busy upload or blob; its cursor advances and wraps later.
func (store *ShardedFileStore) Expire(id string, now time.Time) (bool, error) {
	return store.expireContext(context.Background(), id, now)
}
func (store *ShardedFileStore) expireContext(ctx context.Context, id string, now time.Time) (bool, error) {
	lock, err := store.NewLock(id)
	if err != nil {
		return false, err
	}
	if err = lock.Lock(); err != nil {
		return false, err
	}
	defer lock.Unlock()
	record, err := store.recordContext(ctx, id)
	if err != nil || record.Deleted {
		return false, err
	}
	if record.Lifecycle != stateDeleting {
		if record.Expires.Valid && record.Expires.Int64 > now.Unix() {
			return false, nil
		}
		if !record.Expires.Valid && record.Created > now.Unix()-86400 {
			return false, nil
		}
	}
	err = store.terminateContext(ctx, id)
	return err == nil, err
}

// Compatibility entry point, still bounded. The worker uses its own cancellable
// context. Each state has an independent keyset cursor, so bad early IDs cannot
// starve later work. At most limit IDs are visited in a call.
func (store *ShardedFileStore) RecoverPending() error {
	ctx, cancel := context.WithTimeout(context.Background(), MaintenanceBudget)
	defer cancel()
	return store.RecoverPendingContext(ctx, MaintenanceLimit)
}
func (store *ShardedFileStore) RecoverPendingContext(ctx context.Context, limit int) error {
	if limit <= 0 {
		return nil
	}
	ctx, cancel := maintenanceDeadline(ctx)
	defer cancel()
	if !store.maintenance.mu.TryLock() {
		return nil
	}
	defer store.maintenance.mu.Unlock()
	store.maintenance.recoverMore = false
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			store.maintenance.recoverMore = true
		}
	}()
	ctx = context.WithValue(ctx, maintenanceKey{}, true)
	turn := store.maintenance.pendingTurn
	store.maintenance.pendingTurn = 1 - turn
	states := [2]string{stateFinalizing, stateDeleting}
	for step := 0; step < 2; step++ {
		i := (turn + step) % 2
		state := states[i]
		n := limit / 2
		if i == turn {
			n += limit % 2
		}
		if n == 0 {
			continue
		}
		var ids []string
		if err := db.SelectContext(ctx, store.DBConn, &ids, `SELECT id FROM uploads
   WHERE deleted = 0 AND lifecycle = ? AND id > ? ORDER BY id LIMIT ?`, state, store.maintenance.pending[i], n); err != nil {
			return err
		}
		if len(ids) == n {
			store.maintenance.recoverMore = true
		}
		if len(ids) == 0 {
			store.maintenance.pending[i] = ""
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Advance even on error/lock contention. A later cycle wraps for retries.
			store.maintenance.pending[i] = id
			if err := store.recoverOne(ctx, id); err != nil && !errors.Is(err, handler.ErrFileLocked) {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				store.log.Error().Err(err).Str("id", id).Msg("Pending upload operation will be retried")
			}
		}
	}
	return nil
}
func (store *ShardedFileStore) recoverOne(ctx context.Context, id string) error {
	lock, err := store.NewLock(id)
	if err != nil {
		return err
	}
	if err = lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	record, err := store.recordContext(ctx, id)
	if err != nil {
		return err
	}
	switch record.Lifecycle {
	case stateDeleting:
		return store.terminateContext(ctx, id)
	case stateFinalizing:
		_, err := store.GetFileUpload(ctx, id)
		return err
	default:
		return nil // Another process may already have completed the work.
	}
}

type expiryCandidate struct {
	ID       string `db:"id"`
	Deadline int64  `db:"deadline"`
}

// Separate indexed range queries avoid the old OR and unbounded result list.
// Age and id form a keyset cursor, including equal timestamps without OFFSET.
func (store *ShardedFileStore) ExpireBatch(ctx context.Context, now time.Time, limit int) error {
	if limit <= 0 {
		return nil
	}
	ctx, cancel := maintenanceDeadline(ctx)
	defer cancel()
	if !store.maintenance.mu.TryLock() {
		return nil
	}
	defer store.maintenance.mu.Unlock()
	store.maintenance.expireMore = false
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			store.maintenance.expireMore = true
		}
	}()
	turn := store.maintenance.expiryTurn
	store.maintenance.expiryTurn = 1 - turn
	ctx = context.WithValue(ctx, maintenanceKey{}, true)
	for step := 0; step < 2; step++ {
		i := (turn + step) % 2
		n := limit / 2
		if i == turn {
			n += limit % 2
		}
		if n == 0 {
			continue
		}
		column, condition, deadline := "expires_at", "expires_at IS NOT NULL", now.Unix()
		if i == 1 {
			column, condition, deadline = "created_at", "expires_at IS NULL", now.Unix()-86400
		}
		cursor := store.maintenance.expiry[i]
		query := `SELECT id, ` + column + ` AS deadline FROM uploads WHERE deleted = 0 AND ` + condition + `
   AND ` + column + ` <= ? AND (` + column + `, id) > (?, ?) ORDER BY ` + column + `, id LIMIT ?`
		var rows []expiryCandidate
		if err := db.SelectContext(ctx, store.DBConn, &rows, query, deadline, cursor.timestamp, cursor.id, n); err != nil {
			return err
		}
		if len(rows) == n {
			store.maintenance.expireMore = true
		}
		if len(rows) == 0 {
			store.maintenance.expiry[i] = ageCursor{timestamp: math.MinInt64}
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			store.maintenance.expiry[i] = ageCursor{timestamp: row.Deadline, id: row.ID}
			removed, err := store.expireContext(ctx, row.ID, now)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if !errors.Is(err, handler.ErrFileLocked) {
					store.log.Error().Err(err).Str("id", row.ID).Msg("Failed to expire upload")
				}
			} else if removed {
				store.log.Info().Str("event", "expired").Str("id", row.ID).Msg("Terminated upload id")
			}
		}
	}
	return nil
}

// MoreMaintenance requests another bounded cycle soon instead of waiting an
// entire expiration interval to drain a backlog. Empty pages end a sweep.
func (store *ShardedFileStore) MoreMaintenance() bool {
	store.maintenance.mu.Lock()
	defer store.maintenance.mu.Unlock()
	return store.maintenance.recoverMore || store.maintenance.expireMore
}

func maintenanceDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, MaintenanceBudget)
}
