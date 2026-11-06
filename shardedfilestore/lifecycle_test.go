package shardedfilestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/config"
	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/rs/zerolog"
	"github.com/tus/tusd/pkg/handler"
)

func testStore(t *testing.T) *ShardedFileStore {
	t.Helper()
	root := t.TempDir()
	log := zerolog.New(io.Discard)
	conn := db.ConnectToDB(&log, db.DBConfig{DriverName: "sqlite3", DSN: filepath.Join(root, "uploads.db")})
	t.Cleanup(func() { conn.DB.Close() })
	return New(filepath.Join(root, "files"), 2, time.Hour, 24*time.Hour, nil, conn, &log)
}

func testUpload(t *testing.T, store *ShardedFileStore, body string) *FileUpload {
	t.Helper()
	u, err := store.NewUpload(context.Background(), handler.FileInfo{Size: int64(len(body)), MetaData: handler.MetaData{"filetype": "text/plain", "RemoteIP": "127.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	upload := u.(*FileUpload)
	err = locked(store, upload.info.ID, func() error {
		_, err := upload.WriteChunk(context.Background(), 0, strings.NewReader(body))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return upload
}

func locked(store *ShardedFileStore, id string, fn func() error) error {
	lock, err := store.NewLock(id)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		err = lock.Lock()
		if !errors.Is(err, handler.ErrFileLocked) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return fn()
}

func finish(store *ShardedFileStore, u *FileUpload) error {
	return locked(store, u.info.ID, func() error { return u.FinishUpload(context.Background()) })
}

func assertState(t *testing.T, store *ShardedFileStore, id, state string) uploadRecord {
	t.Helper()
	r, err := store.record(id)
	if err != nil || r.Lifecycle != state {
		t.Fatalf("record %s: %+v, %v; want %s", id, r, err, state)
	}
	return r
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected missing %s, got %v", path, err)
	}
}

func assertBody(t *testing.T, path, want string) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil || string(body) != want {
		t.Fatalf("body %s: %q, %v; want %q", path, body, err, want)
	}
}

func TestComposerUsesLocksAndDisablesConcatenation(t *testing.T) {
	composer := handler.NewStoreComposer()
	testStore(t).UseIn(composer)
	if !composer.UsesLocker || composer.UsesConcater {
		t.Fatalf("unexpected capabilities: %s", composer.Capabilities())
	}
}

func TestConcurrentDeduplicationAndDeletion(t *testing.T) {
	store := testStore(t)
	const content = "identical content"
	uploads := make([]*FileUpload, 12)
	for i := range uploads {
		uploads[i] = testUpload(t, store, content)
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(uploads))
	for _, u := range uploads {
		wg.Add(1)
		go func(u *FileUpload) { defer wg.Done(); errs <- finish(store, u) }(u)
	}
	wg.Wait()
	for range uploads {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	hash := sha256.Sum256([]byte(content))
	path := store.completeBinPath(hash[:])
	assertBody(t, path, content)
	for _, u := range uploads {
		assertState(t, store, u.info.ID, stateComplete)
		assertMissing(t, store.incompleteBinPath(u.info.ID))
	}
	// One removal must preserve all remaining references.
	if err := store.Terminate(uploads[0].info.ID); err != nil {
		t.Fatal(err)
	}
	assertBody(t, path, content)
	for _, u := range uploads[1:] {
		wg.Add(1)
		go func(u *FileUpload) {
			defer wg.Done()
			errs <- locked(store, u.info.ID, func() error { return store.terminateLocked(u.info.ID) })
		}(u)
	}
	wg.Wait()
	for range uploads[1:] {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	assertMissing(t, path)
	for _, u := range uploads {
		r := assertState(t, store, u.info.ID, stateDeleted)
		if !r.Deleted {
			t.Fatal("deleted state without tombstone")
		}
	}
}

func TestFinalizationRacingWithLastReferenceDeletion(t *testing.T) {
	store := testStore(t)
	for i := 0; i < 12; i++ {
		content := fmt.Sprintf("shared-%d", i)
		old := testUpload(t, store, content)
		if err := finish(store, old); err != nil {
			t.Fatal(err)
		}
		fresh := testUpload(t, store, content)
		errs := make(chan error, 2)
		start := make(chan struct{})
		go func() { <-start; errs <- store.Terminate(old.info.ID) }()
		go func() { <-start; errs <- finish(store, fresh) }()
		close(start)
		for j := 0; j < 2; j++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		r := assertState(t, store, fresh.info.ID, stateComplete)
		assertBody(t, store.completeBinPath(r.Hash), content)
	}
}

func TestFailedRelocationRecoversAcrossReopenWithoutRepeatingCommands(t *testing.T) {
	store := testStore(t)
	counter := filepath.Join(filepath.Dir(store.BasePath), "command-runs")
	store.PreFinishCommands = []config.PreFinishCommand{{Pattern: "text/plain", Command: "/bin/sh", Args: []string{"-c", `printf x >> "$1"`, "sh", counter}, RejectOnNoneZeroExit: true}}
	u := testUpload(t, store, "recover me")
	obstruction := filepath.Join(store.BasePath, "complete")
	if err := os.WriteFile(obstruction, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := finish(store, u); err == nil {
		t.Fatal("relocation failure reported success")
	}
	before := assertState(t, store, u.info.ID, stateFinalizing)
	assertBody(t, store.incompleteBinPath(u.info.ID), "recover me")
	assertBody(t, counter, "x")
	if err := os.Remove(obstruction); err != nil {
		t.Fatal(err)
	}
	// Reopen the same database and storage as a new server instance would.
	log := zerolog.New(io.Discard)
	conn := db.ConnectToDB(&log, store.DBConn.DBConfig)
	defer conn.DB.Close()
	reopened := New(store.BasePath, store.PrefixShardLayers, time.Hour, 24*time.Hour, store.PreFinishCommands, conn, &log)
	if err := reopened.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	after := assertState(t, reopened, u.info.ID, stateComplete)
	if after.Expires != before.Expires {
		t.Fatal("recovery changed expiration")
	}
	assertBody(t, reopened.completeBinPath(after.Hash), "recover me")
	assertBody(t, counter, "x")
	assertMissing(t, reopened.incompleteBinPath(u.info.ID))
}

func TestMetadataFailureAfterRenameIsRecoverable(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "metadata failure")
	// A directory at the temporary metadata path forces an error after rename.
	if err := os.Mkdir(u.infoPath+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	if err := finish(store, u); err == nil {
		t.Fatal("metadata failure reported success")
	}
	r := assertState(t, store, u.info.ID, stateFinalizing)
	assertBody(t, store.completeBinPath(r.Hash), "metadata failure")
	assertMissing(t, store.incompleteBinPath(u.info.ID))
	if err := os.Remove(u.infoPath + ".tmp"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, u.info.ID, stateComplete)
}

func TestDatabaseFailureAfterPublishIsRecoverable(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "database failure")
	_, err := store.DBConn.DB.Exec(`CREATE TRIGGER fail_complete BEFORE UPDATE OF lifecycle ON uploads WHEN NEW.lifecycle = 'complete' BEGIN SELECT RAISE(FAIL, 'injected'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if err := finish(store, u); err == nil {
		t.Fatal("database failure reported success")
	}
	r := assertState(t, store, u.info.ID, stateFinalizing)
	assertBody(t, store.completeBinPath(r.Hash), "database failure")
	if _, err := store.DBConn.DB.Exec(`DROP TRIGGER fail_complete`); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, u.info.ID, stateComplete)
}

func TestFailedDeletionIsHiddenAndRetried(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "delete me")
	if err := finish(store, u); err != nil {
		t.Fatal(err)
	}
	// Force metadata removal to fail after the blob has already been removed.
	if err := os.Remove(u.infoPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(u.infoPath, 0700); err != nil {
		t.Fatal(err)
	}
	obstruction := filepath.Join(u.infoPath, "keep")
	if err := os.WriteFile(obstruction, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := store.Terminate(u.info.ID); err == nil {
		t.Fatal("failed deletion reported success")
	}
	assertState(t, store, u.info.ID, stateDeleting)
	if err := locked(store, u.info.ID, func() error {
		_, err := store.GetFileUpload(context.Background(), u.info.ID)
		if !errors.Is(err, handler.ErrNotFound) {
			return fmt.Errorf("deleting upload visible: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(obstruction); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, u.info.ID, stateDeleted)
	if err := store.Terminate(u.info.ID); err != nil {
		t.Fatalf("termination is not idempotent: %v", err)
	}
}

type interruptedReader struct{ done bool }

func (r *interruptedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, "done"), io.ErrUnexpectedEOF
}

func TestFullLastChunkWithReadErrorFinalizesOnRetry(t *testing.T) {
	store := testStore(t)
	u, err := store.NewUpload(context.Background(), handler.FileInfo{Size: 4})
	if err != nil {
		t.Fatal(err)
	}
	upload := u.(*FileUpload)
	if err := locked(store, upload.info.ID, func() error {
		n, err := upload.WriteChunk(context.Background(), 0, &interruptedReader{})
		if n != 4 || !errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("unexpected write: %d, %v", n, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, upload.info.ID, stateIncomplete)
	if err := locked(store, upload.info.ID, func() error {
		_, err := store.GetFileUpload(context.Background(), upload.info.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	r := assertState(t, store, upload.info.ID, stateComplete)
	assertBody(t, store.completeBinPath(r.Hash), "done")
}

func TestExpireRechecksDeadlineAndCleansLegacyTemporary(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "still alive")
	if _, err := store.DBConn.DB.Exec(`UPDATE uploads SET created_at = ? WHERE id = ?`, time.Now().Unix()-90000, u.info.ID); err != nil {
		t.Fatal(err)
	}
	if err := finish(store, u); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Expire(u.info.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	r := assertState(t, store, u.info.ID, stateComplete)
	if err := os.WriteFile(store.incompleteBinPath(u.info.ID), []byte("legacy residue"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Expire(u.info.ID, time.Unix(r.Expires.Int64+1, 0)); err != nil {
		t.Fatal(err)
	}
	assertState(t, store, u.info.ID, stateDeleted)
	assertMissing(t, store.incompleteBinPath(u.info.ID))
	assertMissing(t, store.completeBinPath(r.Hash))
}

func TestFailedCreationCleansUpAndDoesNotDeleteCollision(t *testing.T) {
	store := testStore(t)
	id := Uid()
	if err := os.MkdirAll(store.metaDir(id), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(store.infoPath(id)+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.NewUpload(context.Background(), handler.FileInfo{ID: id, Size: 4}); err == nil {
		t.Fatal("creation failure reported success")
	}
	assertState(t, store, id, stateDeleted)
	assertMissing(t, store.incompleteBinPath(id))
	u := testUpload(t, store, "keep")
	if _, err := store.NewUpload(context.Background(), handler.FileInfo{ID: u.info.ID, Size: 4}); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	assertState(t, store, u.info.ID, stateIncomplete)
	assertBody(t, store.incompleteBinPath(u.info.ID), "keep")
}

func TestInfoSnapshotsAndClosedDatabase(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "body")
	before, _ := u.GetInfo(context.Background())
	if err := finish(store, u); err != nil {
		t.Fatal(err)
	}
	if _, ok := before.MetaData["expires"]; ok {
		t.Fatal("completion mutated an event snapshot")
	}
	if !strings.Contains(before.Storage["Path"], "incomplete") {
		t.Fatal("completion mutated storage snapshot")
	}
	store.DBConn.DB.Close()
	if _, err := store.GetFileUpload(context.Background(), u.info.ID); err == nil {
		t.Fatal("closed DB did not return an error")
	}
}

func TestLocksAcrossProcesses(t *testing.T) {
	if root := os.Getenv("FILEUPLOADER_LOCK_TEST_ROOT"); root != "" {
		store := ShardedFileStore{BasePath: root, PrefixShardLayers: 2}
		lock, err := store.NewLock("abcdef0123456789abcdef0123456789")
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(lock.Lock(), handler.ErrFileLocked) {
			t.Fatal("child process acquired a held lock")
		}
		return
	}
	store := testStore(t)
	lock, err := store.NewLock("abcdef0123456789abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocksAcrossProcesses$")
	cmd.Env = append(os.Environ(), "FILEUPLOADER_LOCK_TEST_ROOT="+store.BasePath)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		t.Fatalf("child: %v\n%s", err, output.String())
	}
}

func TestMigrationPreservesExistingUploads(t *testing.T) {
	store := testStore(t)
	incomplete := testUpload(t, store, "incomplete")
	complete := testUpload(t, store, "complete")
	deleted := testUpload(t, store, "deleted")
	if err := finish(store, complete); err != nil {
		t.Fatal(err)
	}
	if err := store.Terminate(deleted.info.ID); err != nil {
		t.Fatal(err)
	}
	before := assertState(t, store, complete.info.ID, stateComplete)
	// Reproduce a version-5 database with real metadata and blob files intact.
	for _, query := range []string{
		`DROP INDEX uploads_active_hash`,
		`DROP INDEX uploads_pending`,
		`DROP INDEX uploads_expiry`,
		`DROP INDEX uploads_incomplete_age`,
		`ALTER TABLE uploads DROP COLUMN lifecycle`,
		`DELETE FROM gorp_migrations WHERE id IN ('6','7')`,
	} {
		if _, err := store.DBConn.DB.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	store.initDB()
	assertState(t, store, incomplete.info.ID, stateIncomplete)
	after := assertState(t, store, complete.info.ID, stateComplete)
	assertState(t, store, deleted.info.ID, stateDeleted)
	if !bytes.Equal(before.Hash, after.Hash) || before.Expires != after.Expires || before.Created != after.Created {
		t.Fatalf("migration changed existing data: before=%+v after=%+v", before, after)
	}
	assertBody(t, store.completeBinPath(after.Hash), "complete")
	assertBody(t, store.incompleteBinPath(incomplete.info.ID), "incomplete")
}

func TestRemoveWithDirsPropagatesErrorsAndRejectsSiblingPaths(t *testing.T) {
	base := t.TempDir()
	nonempty := filepath.Join(base, "nonempty")
	if err := os.Mkdir(nonempty, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonempty, "keep"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := RemoveWithDirs(nonempty, base); err == nil {
		t.Fatal("remove error was swallowed")
	}
	if err := RemoveWithDirs(base, base); err == nil {
		t.Fatal("allowed removal of storage root")
	}
	sibling := base + "-sibling"
	if err := os.WriteFile(sibling, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(sibling) })
	if err := RemoveWithDirs(sibling, base); err == nil {
		t.Fatal("allowed removal outside storage root")
	}
	assertBody(t, sibling, "keep")
}

func TestCorruptDeduplicationTargetDoesNotDiscardTemporary(t *testing.T) {
	store := testStore(t)
	u := testUpload(t, store, "good content")
	hash := sha256.Sum256([]byte("good content"))
	target := store.completeBinPath(hash[:])
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("corrupt content"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := finish(store, u); err == nil {
		t.Fatal("accepted a corrupt deduplication target")
	}
	assertState(t, store, u.info.ID, stateFinalizing)
	assertBody(t, store.incompleteBinPath(u.info.ID), "good content")
	assertBody(t, target, "corrupt content")
}
