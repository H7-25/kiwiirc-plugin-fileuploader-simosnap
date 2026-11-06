package expirer

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/kiwiirc/plugin-fileuploader/shardedfilestore"
	"github.com/rs/zerolog"
	"github.com/tus/tusd/pkg/handler"
)

type tickWriter struct{ tick chan struct{} }

func (w tickWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "gc_tick") {
		select {
		case w.tick <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

func TestStopCancelsRunningGCAndIsIdempotent(t *testing.T) {
	root := t.TempDir()
	log := zerolog.New(io.Discard)
	conn := db.ConnectToDB(&log, db.DBConfig{DriverName: "sqlite3", DSN: filepath.Join(root, "uploads.db")})
	defer conn.DB.Close()
	store := shardedfilestore.New(filepath.Join(root, "files"), 2, time.Hour, 24*time.Hour, nil, conn, &log)
	writer := tickWriter{tick: make(chan struct{}, 1)}
	gcLog := zerolog.New(writer).Level(zerolog.DebugLevel)
	// Hold the DB lock so an observed maintenance tick cannot finish its query.
	conn.DBLock.Lock()
	e := New(store, time.Millisecond, &gcLog)
	select {
	case <-writer.tick:
	case <-time.After(5 * time.Second):
		conn.DBLock.Unlock()
		e.Stop()
		t.Fatal("GC did not start")
	}
	done := make(chan struct{})
	go func() { e.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		conn.DBLock.Unlock()
		t.Fatal("Stop did not cancel the worker waiting for SQLite")
	}
	conn.DBLock.Unlock()
	e.Stop()
}

func TestBacklogDrainsWithBoundedCyclesBeforeNextRegularTick(t *testing.T) {
	root := t.TempDir()
	log := zerolog.New(io.Discard)
	conn := db.ConnectToDB(&log, db.DBConfig{DriverName: "sqlite3", DSN: filepath.Join(root, "uploads.db")})
	defer conn.DB.Close()
	store := shardedfilestore.New(filepath.Join(root, "files"), 2, time.Hour, 24*time.Hour, nil, conn, &log)
	for i := 0; i < 5; i++ {
		upload, err := store.NewUpload(context.Background(), handler.FileInfo{Size: 1})
		if err != nil {
			t.Fatal(err)
		}
		info, _ := upload.GetInfo(context.Background())
		lock, _ := store.NewLock(info.ID)
		if err = lock.Lock(); err != nil {
			t.Fatal(err)
		}
		_, err = upload.WriteChunk(context.Background(), 0, strings.NewReader("x"))
		if err == nil {
			err = upload.FinishUpload(context.Background())
		}
		lock.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if err = db.UpdateRow(conn, `UPDATE uploads SET expires_at=100 WHERE id=?`, info.ID); err != nil {
			t.Fatal(err)
		}
	}
	e := New(store, time.Hour, &log, Options{BatchLimit: 2, TimeBudget: time.Second, BacklogDelay: time.Millisecond})
	defer e.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var count int
		if err := db.QueryRow(conn, `SELECT count(*) FROM uploads WHERE deleted=1`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backlog stuck at %d/5", count)
		}
		time.Sleep(time.Millisecond)
	}
}
