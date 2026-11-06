package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/config"
)

type slowResponse struct {
	*httptest.ResponseRecorder
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *slowResponse) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started); <-w.release })
	return w.ResponseRecorder.Write(p)
}
func completeHTTP(t *testing.T, r http.Handler) string {
	t.Helper()
	req := httptest.NewRequest("POST", "/files/", nil)
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Length", "4")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 201 {
		t.Fatalf("POST: %d %s", w.Code, w.Body.String())
	}
	location := w.Header().Get("Location")
	path := "/files/" + location[strings.LastIndex(location, "/")+1:]
	req = httptest.NewRequest("PATCH", path, strings.NewReader("body"))
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 204 {
		t.Fatalf("PATCH: %d %s", w.Code, w.Body.String())
	}
	return path
}
func TestDownloadsAndHeadsContinueDuringSlowDownloadAndRevocation(t *testing.T) {
	r, _ := lifecycleRouter(t)
	path := completeHTTP(t, r)
	slow := &slowResponse{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { r.ServeHTTP(slow, httptest.NewRequest("GET", path, nil)); close(done) }()
	defer func() {
		select {
		case <-slow.release:
		default:
			close(slow.release)
		}
		<-done
	}()
	select {
	case <-slow.started:
	case <-time.After(3 * time.Second):
		t.Fatal("download did not start")
	}
	// All these reads overlap a still-open, deliberately stalled transmission.
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			method := "GET"
			if i%2 == 0 {
				method = "HEAD"
			}
			w := tusRequest(r, method, path, nil, nil)
			if w.Code != 200 {
				errs <- fmt.Errorf("%s: %d", method, w.Code)
			} else if method == "GET" && w.Body.String() != "body" {
				errs <- fmt.Errorf("wrong body: %q", w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	w := tusRequest(r, "DELETE", path, nil, nil)
	if w.Code != 204 {
		t.Fatalf("DELETE blocked on transmission: %d %s", w.Code, w.Body.String())
	}
	for _, method := range []string{"GET", "HEAD"} {
		w = tusRequest(r, method, path, nil, nil)
		if w.Code != 404 {
			t.Fatalf("%s opened revoked file: %d", method, w.Code)
		}
	}
	close(slow.release)
	<-done
	if slow.Code != 200 || slow.Body.String() != "body" {
		t.Fatalf("started download damaged: %d %q", slow.Code, slow.Body.String())
	}
}

func TestHTTPRangesReturnExactBytesAndRejectInvalidRanges(t *testing.T) {
	r, _ := lifecycleRouter(t)
	path := completeHTTP(t, r)
	for _, tc := range []struct {
		rangeHeader        string
		status             int
		body, contentRange string
	}{
		{"bytes=1-2", 206, "od", "bytes 1-2/4"},
		{"bytes=0-0", 206, "b", "bytes 0-0/4"},
		{"bytes=-2", 206, "dy", "bytes 2-3/4"},
		{"bytes=2-", 206, "dy", "bytes 2-3/4"},
		{"bytes=9-10", 416, "", "bytes */4"},
		{"bytes=", 416, "", "bytes */4"},
		{"bytes=-0", 416, "", "bytes */4"},
		{"bytes=--1", 416, "", "bytes */4"},
		{"bytes=3-1", 416, "", "bytes */4"},
		{"bytes=0-0,2-2", 416, "", "bytes */4"},
		{"bad", 416, "", "bytes */4"},
	} {
		t.Run(tc.rangeHeader, func(t *testing.T) {
			w := tusRequest(r, "GET", path, nil, map[string]string{"Range": tc.rangeHeader})
			if w.Code != tc.status || w.Body.String() != tc.body || w.Header().Get("Content-Range") != tc.contentRange {
				t.Fatalf("got %d %q %+v", w.Code, w.Body.String(), w.Header())
			}
		})
	}
}

func TestTransformedContentKeepsLogicalTusOffsetThroughRecovery(t *testing.T) {
	command := config.PreFinishCommand{Pattern: "*", Command: "/bin/sh", Args: []string{"-c", `printf X > "$1"`, "sh", "%FILE%"}, RejectOnNoneZeroExit: true}
	for _, failPublish := range []bool{false, true} {
		t.Run(fmt.Sprint(failPublish), func(t *testing.T) {
			r, store := lifecycleRouter(t, command)
			path := createHTTPUpload(t, r)
			if failPublish {
				if _, err := store.DBConn.DB.Exec(`CREATE TRIGGER fail_complete BEFORE UPDATE OF lifecycle ON uploads WHEN NEW.lifecycle='complete' BEGIN SELECT RAISE(FAIL,'injected'); END;`); err != nil {
					t.Fatal(err)
				}
			}
			w := tusRequest(r, "PATCH", path, strings.NewReader("body"), map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
			if failPublish {
				if w.Code != 500 {
					t.Fatalf("injected failure: %d", w.Code)
				}
				if _, err := store.DBConn.DB.Exec(`DROP TRIGGER fail_complete`); err != nil {
					t.Fatal(err)
				}
			} else if w.Code != 204 {
				t.Fatalf("PATCH: %d %s", w.Code, w.Body.String())
			}
			w = tusRequest(r, "HEAD", path, nil, nil)
			if w.Code != 200 || w.Header().Get("Upload-Offset") != "4" || w.Header().Get("Upload-Length") != "4" {
				t.Fatalf("logical HEAD: %d %+v", w.Code, w.Header())
			}
			// This is what a resuming tus-js-client sends/observes at the final offset.
			w = tusRequest(r, "PATCH", path, strings.NewReader(""), map[string]string{"Upload-Offset": "4", "Content-Type": "application/offset+octet-stream"})
			if w.Code != 204 {
				t.Fatalf("completed retry: %d %s", w.Code, w.Body.String())
			}
			w = tusRequest(r, "GET", path, nil, nil)
			if w.Code != 200 || w.Body.String() != "X" || w.Header().Get("Content-Length") != "1" {
				t.Fatalf("physical GET: %d %q %+v", w.Code, w.Body.String(), w.Header())
			}
		})
	}
}

func TestRejectedEmptyUploadPreserves406(t *testing.T) {
	command := config.PreFinishCommand{Pattern: "*", Command: "/bin/sh", Args: []string{"-c", "exit 1"}, RejectOnNoneZeroExit: true}
	r, _ := lifecycleRouter(t, command)
	w := tusRequest(r, "POST", "/files/", nil, map[string]string{"Upload-Length": "0"})
	if w.Code != 406 {
		t.Fatalf("rejection became %d: %s", w.Code, w.Body.String())
	}
}

func TestLazyCompletionAfterTransformUsesPhysicalDownloadLength(t *testing.T) {
	command := config.PreFinishCommand{Pattern: "*", Command: "/bin/sh", Args: []string{"-c", `printf X > "$1"`, "sh", "%FILE%"}, RejectOnNoneZeroExit: true}
	r, store := lifecycleRouter(t, command)
	path := createHTTPUpload(t, r)
	id := strings.TrimPrefix(path, "/files/")
	// Simulate a complete last chunk whose HTTP request died before FinishUpload.
	f, err := store.GetUpload(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := store.NewLock(id)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Lock(); err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteChunk(context.Background(), 0, strings.NewReader("body"))
	lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	w := tusRequest(r, "GET", path, nil, nil)
	if w.Code != 200 || w.Body.String() != "X" || w.Header().Get("Content-Length") != "1" {
		t.Fatalf("lazy transformed GET: %d %q %+v", w.Code, w.Body.String(), w.Header())
	}
}

func TestInvalidRangesDoNotLeakOpenFiles(t *testing.T) {
	r, _ := lifecycleRouter(t)
	path := completeHTTP(t, r)
	// Prevent finalizers from hiding a leak in an HTTP early-return path.
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	count := func() int {
		count := 0
		// /dev/fd cannot be enumerated on every supported Unix. Inspect low FDs
		// directly; the test opens far fewer than 4096 descriptors.
		for fd := uintptr(0); fd < 4096; fd++ {
			_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
			if errno == 0 {
				count++
			}
		}
		return count
	}
	tusRequest(r, "GET", path, nil, map[string]string{"Range": "bytes="})
	before := count()
	for i := 0; i < 100; i++ {
		w := tusRequest(r, "GET", path, nil, map[string]string{"Range": "bytes=0-0,2-2"})
		if w.Code != 416 {
			t.Fatal(w.Code)
		}
	}
	if extra := count() - before; extra > 2 {
		t.Fatalf("invalid Range leaked %d descriptors", extra)
	}
}

func TestDeferredLengthAndCreationWithUploadRemainSupported(t *testing.T) {
	r, _ := lifecycleRouter(t)
	w := tusRequest(r, "POST", "/files/", nil, map[string]string{"Upload-Defer-Length": "1"})
	if w.Code != 201 {
		t.Fatalf("deferred POST: %d %s", w.Code, w.Body.String())
	}
	location := w.Header().Get("Location")
	path := "/files/" + location[strings.LastIndex(location, "/")+1:]
	w = tusRequest(r, "PATCH", path, strings.NewReader("body"), map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
	if w.Code != 204 {
		t.Fatalf("deferred PATCH: %d %s", w.Code, w.Body.String())
	}
	w = tusRequest(r, "HEAD", path, nil, nil)
	if w.Code != 200 || w.Header().Get("Upload-Defer-Length") != "1" || w.Header().Get("Upload-Offset") != "4" {
		t.Fatalf("deferred HEAD: %d %+v", w.Code, w.Header())
	}
	w = tusRequest(r, "PATCH", path, strings.NewReader(""), map[string]string{"Upload-Offset": "4", "Upload-Length": "4", "Content-Type": "application/offset+octet-stream"})
	if w.Code != 204 {
		t.Fatalf("declared final PATCH: %d %s", w.Code, w.Body.String())
	}
	w = tusRequest(r, "GET", path+"/voice.webm", nil, nil)
	if w.Code != 200 || w.Body.String() != "body" {
		t.Fatalf("friendly GET: %d %q", w.Code, w.Body.String())
	}
	w = tusRequest(r, "HEAD", path+"/voice.webm", nil, nil)
	if w.Code != 200 || w.Header().Get("Upload-Offset") != "4" {
		t.Fatalf("friendly HEAD: %d %+v", w.Code, w.Header())
	}
	w = tusRequest(r, "POST", "/files/", strings.NewReader("body"), map[string]string{"Upload-Length": "4", "Content-Type": "application/offset+octet-stream"})
	if w.Code != 201 {
		t.Fatalf("creation with data: %d %s", w.Code, w.Body.String())
	}
}

func TestIncompleteDownloadDoesNotSendBytesAppendedAfterOpen(t *testing.T) {
	r, _ := lifecycleRouter(t)
	path := createHTTPUpload(t, r)
	w := tusRequest(r, "PATCH", path, strings.NewReader("bo"), map[string]string{"Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"})
	if w.Code != 204 {
		t.Fatal(w.Code)
	}
	slow := &slowResponse{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { r.ServeHTTP(slow, httptest.NewRequest("GET", path, nil)); close(done) }()
	defer func() {
		select {
		case <-slow.release:
		default:
			close(slow.release)
		}
		<-done
	}()
	select {
	case <-slow.started:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	w = tusRequest(r, "PATCH", path, strings.NewReader("dy"), map[string]string{"Upload-Offset": "2", "Content-Type": "application/offset+octet-stream"})
	if w.Code != 204 {
		t.Fatalf("append during download: %d %s", w.Code, w.Body.String())
	}
	close(slow.release)
	<-done
	if slow.Body.String() != "bo" || slow.Header().Get("Content-Length") != "2" {
		t.Fatalf("download exceeded captured size: %q %+v", slow.Body.String(), slow.Header())
	}
}
