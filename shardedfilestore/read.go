package shardedfilestore

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/tus/tusd/pkg/handler"
)

// GetReadUpload requires a shared upload lock and never mutates storage.
func (store ShardedFileStore) GetReadUpload(ctx context.Context, id string) (*FileUpload, error) {
	return store.getFileUpload(ctx, id, false)
}

// ReadUpload returns a snapshot and its shared lock. An interrupted upload may
// need exclusive recovery first; re-read under a new shared lock afterwards.
func (store ShardedFileStore) ReadUpload(ctx context.Context, id string) (*FileUpload, handler.Lock, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		raw, err := store.NewLock(id)
		if err != nil {
			return nil, nil, err
		}
		lock := raw.(*fileLock)
		lock.shared = true
		if err := lock.LockContext(waitCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = handler.ErrFileLocked
			}
			return nil, nil, err
		}
		upload, err := store.GetReadUpload(waitCtx, id)
		if err == nil {
			return upload, lock, nil
		}
		lock.Unlock()
		if !errors.Is(err, handler.ErrFileLocked) {
			return nil, nil, err
		}
		// Do not upgrade a held shared lock: two readers upgrading would deadlock.
		lock.shared = false
		if err := lock.LockContext(waitCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = handler.ErrFileLocked
			}
			return nil, nil, err
		}
		_, err = store.GetFileUpload(waitCtx, id)
		lock.Unlock()
		if err != nil {
			// HEAD is also tus-js-client's resume probe. A 500 there can make it
			// discard the existing URL and create another upload. Keep pending work
			// retryable as 423, while preserving deliberate protocol rejections.
			var protocolError handler.HTTPError
			if !errors.As(err, &protocolError) && !errors.Is(err, context.Canceled) {
				store.log.Warn().Err(err).Str("id", id).Msg("Upload publication pending; retry the same URL")
				return nil, nil, handler.ErrFileLocked
			}
			return nil, nil, err
		}
	}
}

// OpenDownload opens the descriptor before releasing the shared upload lock.
// DELETE cannot revoke access between the state check and open. Once open, an
// unlinked file remains readable on our supported Unix platforms. New opens
// after the deleting transition are rejected, even if physical cleanup fails.
func (store ShardedFileStore) OpenDownload(ctx context.Context, id string) (handler.FileInfo, *os.File, error) {
	upload, lock, err := store.ReadUpload(ctx, id)
	if err != nil {
		return handler.FileInfo{}, nil, err
	}
	defer lock.Unlock()
	file, err := upload.GetFile(ctx)
	if err != nil {
		return handler.FileInfo{}, nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return handler.FileInfo{}, nil, err
	}
	info, err := upload.GetInfo(ctx)
	if err != nil {
		file.Close()
		return handler.FileInfo{}, nil, err
	}
	// Downloads use physical size; TUS HEAD/PATCH use the original logical size.
	info.Offset = stat.Size()
	return info, file, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(p []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(p)
}

type maintenanceKey struct{}

func isMaintenance(ctx context.Context) bool { return ctx.Value(maintenanceKey{}) == true }
