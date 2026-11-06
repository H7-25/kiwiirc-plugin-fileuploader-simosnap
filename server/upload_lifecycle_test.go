package server

import (
	"errors"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kiwiirc/plugin-fileuploader/config"
	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/kiwiirc/plugin-fileuploader/shardedfilestore"
	"github.com/rs/zerolog"
	"github.com/tus/tusd/pkg/handler"
)

func lifecycleRouter(t *testing.T, commands ...config.PreFinishCommand) (*gin.Engine, *shardedfilestore.ShardedFileStore) {
	t.Helper()
	log := zerolog.New(io.Discard)
	root := t.TempDir()
	conn := db.ConnectToDB(&log, db.DBConfig{DriverName: "sqlite3", DSN: filepath.Join(root, "uploads.db")})
	t.Cleanup(func() { conn.DB.Close() })
	store := shardedfilestore.New(filepath.Join(root, "files"), 2, time.Hour, 24*time.Hour, commands, conn, &log)
	composer := handler.NewStoreComposer()
	store.UseIn(composer)
	h, err := handler.NewUnroutedHandler(handler.Config{BasePath: "/files/", StoreComposer: composer, MaxSize: 1024, Logger: stdlog.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	serv := &UploadServer{store: store, composer: composer, log: &log, cfg: *config.NewConfig()}
	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/files/", func(c *gin.Context) { c.Set("metadata", map[string]string{}) }, serv.postFile(h))
	r.PATCH("/files/:id", gin.WrapF(h.PatchFile))
	head, err := serv.headFile(handler.Config{BasePath: "/files/", MaxSize: 1024, Logger: stdlog.New(io.Discard, "", 0)}, store)
	if err != nil {
		t.Fatal(err)
	}
	r.HEAD("/files/:id", head)
	r.HEAD("/files/:id/:filename", rewritePath(head, "/files/"))
	r.GET("/files/:id", serv.getFile(h, store))
	r.GET("/files/:id/:filename", rewritePath(serv.getFile(h, store), "/files/"))
	r.DELETE("/files/:id", gin.WrapF(h.DelFile))
	return r, store
}

func tusRequest(r *gin.Engine, method, path string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Tus-Resumable", "1.0.0")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func createHTTPUpload(t *testing.T, r *gin.Engine) string {
	t.Helper()
	w := tusRequest(r, "POST", "/files/", nil, map[string]string{"Upload-Length": "4"})
	if w.Code != http.StatusCreated {
		t.Fatalf("POST: %d %s", w.Code, w.Body.String())
	}
	location := w.Header().Get("Location")
	return "/files/" + location[strings.LastIndex(location, "/")+1:]
}

type pausedBody struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
	reader  io.Reader
}

func (b *pausedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started); <-b.release })
	return b.reader.Read(p)
}

func TestHTTPConcurrentPatchAndMaintenanceShareLock(t *testing.T) {
	r, store := lifecycleRouter(t)
	path := createHTTPUpload(t, r)
	body := &pausedBody{started: make(chan struct{}), release: make(chan struct{}), reader: strings.NewReader("aa")}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		first <- tusRequest(r, "PATCH", path, body, map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
	}()
	select {
	case <-body.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first PATCH did not acquire the lock")
	}
	// Release the body even if an assertion fails.
	defer func() {
		select {
		case <-body.release:
		default:
			close(body.release)
		}
	}()
	second := tusRequest(r, "PATCH", path, strings.NewReader("xx"), map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
	if second.Code != 423 {
		t.Errorf("parallel PATCH: %d %s", second.Code, second.Body.String())
	}
	if err := store.Terminate(strings.TrimPrefix(path, "/files/")); !errors.Is(err, handler.ErrFileLocked) {
		t.Errorf("maintenance bypassed HTTP lock: %v", err)
	}
	get := tusRequest(r, "GET", path, nil, nil)
	if get.Code != 423 {
		t.Errorf("GET bypassed lock: %d", get.Code)
	}
	close(body.release)
	if w := <-first; w.Code != http.StatusNoContent {
		t.Fatalf("first PATCH: %d %s", w.Code, w.Body.String())
	}
	w := tusRequest(r, "PATCH", path, strings.NewReader("bb"), map[string]string{"Upload-Offset": "2", "Content-Type": "application/offset+octet-stream"})
	if w.Code != http.StatusNoContent {
		t.Fatalf("final PATCH: %d %s", w.Code, w.Body.String())
	}
	w = tusRequest(r, "GET", path, nil, nil)
	if w.Code != http.StatusOK || w.Body.String() != "aabb" {
		t.Fatalf("GET: %d %q", w.Code, w.Body.String())
	}
	w = tusRequest(r, "DELETE", path, nil, nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE: %d %s", w.Code, w.Body.String())
	}
	w = tusRequest(r, "GET", path, nil, nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted upload is visible: %d", w.Code)
	}
}

func TestHTTPFinalizationErrorAndRecovery(t *testing.T) {
	r, store := lifecycleRouter(t)
	path := createHTTPUpload(t, r)
	obstruction := filepath.Join(store.BasePath, "complete")
	if err := os.WriteFile(obstruction, []byte("obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	w := tusRequest(r, "PATCH", path, strings.NewReader("body"), map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("failed finalization: %d %s", w.Code, w.Body.String())
	}
	w = tusRequest(r, "HEAD", path, nil, nil)
	if w.Code != 423 {
		t.Fatalf("pending recovery must preserve resume URL: %d %s", w.Code, w.Body.String())
	}
	if err := os.Remove(obstruction); err != nil {
		t.Fatal(err)
	}
	w = tusRequest(r, "HEAD", path, nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Upload-Metadata"), "expires ") {
		t.Fatalf("recovery HEAD: %d %+v %s", w.Code, w.Header(), w.Body.String())
	}
	w = tusRequest(r, "GET", path, nil, nil)
	if w.Code != http.StatusOK || w.Body.String() != "body" {
		t.Fatalf("recovery GET: %d %q", w.Code, w.Body.String())
	}
}

func TestHTTPConcatenationIsRejectedBeforeCreation(t *testing.T) {
	r, store := lifecycleRouter(t)
	w := tusRequest(r, "POST", "/files/", nil, map[string]string{"Upload-Concat": "partial", "Upload-Length": "4"})
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("concatenation POST: %d", w.Code)
	}
	var count int
	if err := store.DBConn.DB.Get(&count, `SELECT count(*) FROM uploads`); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("rejected concatenation created an upload")
	}
}

func TestHTTPEmptyCreationIsPublishedWithAndWithoutChunk(t *testing.T) {
	for _, contentType := range []string{"", "application/offset+octet-stream"} {
		t.Run(contentType, func(t *testing.T) {
			r, store := lifecycleRouter(t)
			w := tusRequest(r, "POST", "/files/", strings.NewReader(""), map[string]string{"Upload-Length": "0", "Content-Type": contentType})
			if w.Code != http.StatusCreated {
				t.Fatalf("empty POST: %d %s", w.Code, w.Body.String())
			}
			location := w.Header().Get("Location")
			id := location[strings.LastIndex(location, "/")+1:]
			var state string
			if err := store.DBConn.DB.Get(&state, `SELECT lifecycle FROM uploads WHERE id = ?`, id); err != nil {
				t.Fatal(err)
			}
			if state != "complete" {
				t.Fatalf("empty upload state: %s", state)
			}
			w = tusRequest(r, "HEAD", "/files/"+id, nil, nil)
			if w.Code != http.StatusOK || !strings.Contains(w.Header().Get("Upload-Metadata"), "expires ") {
				t.Fatalf("empty HEAD: %d %+v", w.Code, w.Header())
			}
		})
	}
}
