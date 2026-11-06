package shardedfilestore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/tus/tusd/pkg/handler"
)

func TestFormerLockSlotCollisionDoesNotInterfere(t *testing.T) {
	store := testStore(t)
	ids := []string{"405060a5976e64f6a48bbe80b611d54c", "84a74f4746db7cf28f9da7e0a0c0c061"}
	first, _ := store.NewLock(ids[0])
	second, _ := store.NewLock(ids[1])
	if err := first.Lock(); err != nil {
		t.Fatal(err)
	}
	defer first.Unlock()
	if err := second.Lock(); err != nil {
		t.Fatalf("different IDs interfered: %v", err)
	}
	second.Unlock()
}

func pendingUpload(t *testing.T, store *ShardedFileStore, content string) *FileUpload {
	t.Helper()
	u := testUpload(t, store, content)
	hash := sha256.Sum256([]byte(content))
	if err := db.UpdateRow(store.DBConn, `UPDATE uploads SET lifecycle=?,sha256sum=?,expires_at=? WHERE id=?`, stateFinalizing, hash[:], time.Now().Add(time.Hour).Unix(), u.info.ID); err != nil {
		t.Fatal(err)
	}
	return u
}
func TestRecoveryIsBoundedAndSkipsBusyBlobWithoutStarvation(t *testing.T) {
	store := testStore(t)
	var ids []string
	for i := 0; i < 5; i++ {
		u := pendingUpload(t, store, fmt.Sprint("pending-", i))
		ids = append(ids, u.info.ID)
	}
	sort.Strings(ids)
	record := assertState(t, store, ids[0], stateFinalizing)
	lock, err := store.newFileLock("blobs", fmt.Sprintf("%x", record.Hash), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	start := time.Now()
	if err = store.RecoverPendingContext(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("maintenance waited for a busy blob")
	}
	assertState(t, store, ids[0], stateFinalizing)
	assertState(t, store, ids[1], stateComplete)
	assertState(t, store, ids[2], stateFinalizing) // Limit 4 reserves 2 visits per state.
	if err = store.RecoverPendingContext(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, ids[2], stateComplete)
	assertState(t, store, ids[3], stateComplete)
	if err = store.RecoverPendingContext(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, ids[4], stateComplete)
	lock.Unlock()
	for i := 0; i < 3; i++ {
		if err = store.RecoverPendingContext(context.Background(), 4); err != nil {
			t.Fatal(err)
		}
	}
	assertState(t, store, ids[0], stateComplete)
}

func TestCancelledRecoveryDoesNotPublishOrDelete(t *testing.T) {
	store := testStore(t)
	u := pendingUpload(t, store, "preserve")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.RecoverPendingContext(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled recovery: %v", err)
	}
	assertState(t, store, u.info.ID, stateFinalizing)
	assertBody(t, store.incompleteBinPath(u.info.ID), "preserve")
}

func TestExpiryBatchIsBoundedAndAdvancesPastBusyUpload(t *testing.T) {
	store := testStore(t)
	var ids []string
	for i := 0; i < 5; i++ {
		u := testUpload(t, store, fmt.Sprint("expire-", i))
		if err := finish(store, u); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, u.info.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := db.UpdateRow(store.DBConn, `UPDATE uploads SET expires_at=? WHERE id=?`, int64(100), id); err != nil {
			t.Fatal(err)
		}
	}
	lock, _ := store.NewLock(ids[0])
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	if err := store.ExpireBatch(context.Background(), time.Now(), 4); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, ids[0], stateComplete)
	assertState(t, store, ids[1], stateDeleted)
	assertState(t, store, ids[2], stateComplete)
	if err := store.ExpireBatch(context.Background(), time.Now(), 4); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, ids[2], stateDeleted)
	assertState(t, store, ids[3], stateDeleted)
	lock.Unlock()
	for i := 0; i < 4; i++ {
		if err := store.ExpireBatch(context.Background(), time.Now(), 4); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		assertState(t, store, id, stateDeleted)
	}
}

func TestReadDescriptorSurvivesRevocationAndNoNewOpenIsAllowed(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "open descriptor")
	if err := finish(store, u); err != nil {
		t.Fatal(err)
	}
	_, file, err := store.OpenDownload(context.Background(), u.info.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err = store.Terminate(u.info.ID); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil || string(data) != "open descriptor" {
		t.Fatalf("started read: %q %v", data, err)
	}
	if _, next, err := store.OpenDownload(context.Background(), u.info.ID); !errors.Is(err, handler.ErrNotFound) {
		if next != nil {
			next.Close()
		}
		t.Fatalf("revoked download opened: %v", err)
	}
}

func TestLegacyMigrationDoesNotRewriteHistoricalRows(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "legacy")
	if err := finish(store, u); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP INDEX uploads_active_hash`, `DROP INDEX uploads_pending`, `DROP INDEX uploads_expiry`, `DROP INDEX uploads_incomplete_age`,
		`ALTER TABLE uploads DROP COLUMN lifecycle`, `DELETE FROM gorp_migrations WHERE id IN ('6','7')`,
		`CREATE TRIGGER reject_rewrite BEFORE UPDATE ON uploads BEGIN SELECT RAISE(FAIL,'historical rewrite forbidden'); END;`,
	} {
		if _, err := store.DBConn.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.migrateDB(); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := store.DBConn.DB.Get(&raw, `SELECT lifecycle FROM uploads WHERE id=?`, u.info.ID); err != nil {
		t.Fatal(err)
	}
	if raw != "legacy" {
		t.Fatalf("historical row rewritten: %q", raw)
	}
	assertState(t, store, u.info.ID, stateComplete)
	if _, err := store.DBConn.DB.Exec(`DROP TRIGGER reject_rewrite`); err != nil {
		t.Fatal(err)
	}
	if err := store.Terminate(u.info.ID); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseOneDatabaseUpgradesWithoutChangingLogicalStates(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "phase one")
	if err := finish(store, u); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DROP INDEX uploads_active_hash`, `DROP INDEX uploads_pending`, `DROP INDEX uploads_expiry`, `DROP INDEX uploads_incomplete_age`,
		`CREATE INDEX uploads_hash_active ON uploads(sha256sum,deleted,lifecycle)`, `DELETE FROM gorp_migrations WHERE id='7'`,
	} {
		if _, err := store.DBConn.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := store.migrateDB(); err != nil || n != 1 {
		t.Fatalf("upgrade: %d %v", n, err)
	}
	assertState(t, store, u.info.ID, stateComplete)
}

func TestMaintenanceQueriesUseIndexedSearches(t *testing.T) {
	store := testStore(t)
	queries := []string{
		`SELECT id FROM uploads WHERE deleted=0 AND lifecycle='finalizing' AND id>'' ORDER BY id LIMIT 50`,
		`SELECT id FROM uploads WHERE deleted=0 AND expires_at IS NOT NULL AND expires_at<=2000000000 AND (expires_at,id)>(0,'') ORDER BY expires_at,id LIMIT 50`,
		`SELECT id FROM uploads WHERE deleted=0 AND expires_at IS NULL AND created_at<=2000000000 AND (created_at,id)>(0,'') ORDER BY created_at,id LIMIT 50`,
		`SELECT EXISTS(SELECT 1 FROM uploads WHERE sha256sum=x'00' AND id!='x' AND deleted=0 AND lifecycle!='deleting' LIMIT 1)`,
	}
	for _, query := range queries {
		rows, err := store.DBConn.DB.Query("EXPLAIN QUERY PLAN " + query)
		if err != nil {
			t.Fatal(err)
		}
		search := false
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(detail, "SEARCH uploads") {
				search = true
			}
			if strings.Contains(detail, "SCAN uploads") || strings.Contains(detail, "TEMP B-TREE") {
				t.Errorf("bad plan: %s", detail)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !search {
			t.Errorf("no indexed search: %s", query)
		}
	}
}

func TestSingleItemBatchesAlternateRecoveryAndExpirationClasses(t *testing.T) {
	store := testStore(t)
	pending := pendingUpload(t, store, "pending-one")
	deleting := testUpload(t, store, "deleting-one")
	if err := db.UpdateRow(store.DBConn, `UPDATE uploads SET lifecycle=? WHERE id=?`, stateDeleting, deleting.info.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.RecoverPendingContext(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	assertState(t, store, pending.info.ID, stateComplete)
	assertState(t, store, deleting.info.ID, stateDeleted)
	expired := testUpload(t, store, "expired-one")
	if err := finish(store, expired); err != nil {
		t.Fatal(err)
	}
	abandoned := testUpload(t, store, "abandoned-one")
	if err := db.UpdateRow(store.DBConn, `UPDATE uploads SET expires_at=100 WHERE id=?`, expired.info.ID); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateRow(store.DBConn, `UPDATE uploads SET created_at=100 WHERE id=?`, abandoned.info.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.ExpireBatch(context.Background(), time.Now(), 1); err != nil {
			t.Fatal(err)
		}
	}
	assertState(t, store, expired.info.ID, stateDeleted)
	assertState(t, store, abandoned.info.ID, stateDeleted)
}
