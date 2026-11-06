package shardedfilestore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/tus/tusd/pkg/handler"
)

// Locks use the full key digest, never a shared slot. Keep the files in a
// separate tree and never unlink them while processes can still hold the inode.
type fileLock struct {
	path     string
	file     *os.File
	blocking bool
	shared   bool
}

func (store ShardedFileStore) newFileLock(kind, key string, blocking bool) (*fileLock, error) {
	hash := sha256.Sum256([]byte(key))
	dir := filepath.Join(store.BasePath, "locks", kind, fmt.Sprintf("%x", hash[:1]))
	if err := os.MkdirAll(dir, defaultDirectoryPerm); err != nil {
		return nil, err
	}
	return &fileLock{path: filepath.Join(dir, fmt.Sprintf("%x.lock", hash)), blocking: blocking}, nil
}

func (store ShardedFileStore) NewLock(id string) (handler.Lock, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	if len(id) < store.PrefixShardLayers {
		return nil, handler.ErrNotFound
	}
	return store.newFileLock("uploads", id, false)
}

func (lock *fileLock) Lock() error {
	if lock.blocking {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return lock.LockContext(ctx)
	}
	return lock.tryLock()
}

func (lock *fileLock) tryLock() error {
	file, err := os.OpenFile(lock.path, os.O_CREATE|os.O_RDWR, defaultFilePerm)
	if err != nil {
		return err
	}
	flags := syscall.LOCK_EX | syscall.LOCK_NB
	if lock.shared {
		flags = syscall.LOCK_SH | syscall.LOCK_NB
	}
	for {
		err = syscall.Flock(int(file.Fd()), flags)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return handler.ErrFileLocked
		}
		return err
	}
	lock.file = file
	return nil
}

func (lock *fileLock) LockContext(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := lock.tryLock()
		if !errors.Is(err, handler.ErrFileLocked) {
			return err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (store ShardedFileStore) acquireBlob(ctx context.Context, hash []byte) (*fileLock, error) {
	lock, err := store.newFileLock("blobs", fmt.Sprintf("%x", hash), false)
	if err != nil {
		return nil, err
	}
	// Maintenance uses try-lock semantics. Requests also get a bounded wait.
	if isMaintenance(ctx) {
		err = lock.Lock()
	} else {
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err = lock.LockContext(waitCtx)
	}
	return lock, err
}

func (lock *fileLock) Unlock() error {
	if lock.file == nil {
		return nil
	}
	err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil
	return errors.Join(err, closeErr)
}

func validateID(id string) error {
	// Generated IDs are hex. Also accept legacy IDs of up to 36 hex/hyphen
	// characters, but never allow path components from a request.
	if len(id) < 6 || len(id) > 36 {
		return handler.ErrNotFound
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' || c == '-') {
			return handler.ErrNotFound
		}
	}
	return nil
}
