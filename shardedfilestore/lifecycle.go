package shardedfilestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/tus/tusd/pkg/handler"
)

const (
	stateIncomplete = "incomplete"
	stateFinalizing = "finalizing"
	stateComplete   = "complete"
	stateDeleting   = "deleting"
	stateDeleted    = "deleted"
)

type uploadRecord struct {
	Hash      []byte
	Expires   sql.NullInt64
	Created   int64
	Deleted   bool
	Lifecycle string
}

func (store ShardedFileStore) record(id string) (uploadRecord, error) {
	return store.recordContext(context.Background(), id)
}
func (store ShardedFileStore) recordContext(ctx context.Context, id string) (uploadRecord, error) {
	var record uploadRecord
	err := db.QueryRowContext(ctx, store.DBConn, `SELECT sha256sum, expires_at, created_at, deleted, lifecycle FROM uploads WHERE id = ?`, id).
		Scan(&record.Hash, &record.Expires, &record.Created, &record.Deleted, &record.Lifecycle)
	if errors.Is(err, sql.ErrNoRows) {
		err = handler.ErrNotFound
	}
	if err == nil && record.Lifecycle == "legacy" {
		switch {
		case record.Deleted:
			record.Lifecycle = stateDeleted
		case record.Hash != nil:
			record.Lifecycle = stateComplete
		default:
			record.Lifecycle = stateIncomplete
		}
	}
	return record, err
}

// FileInfo is also handed to asynchronous event listeners. Never share mutable
// maps with them: completion adds expires and changes Storage.Path.
func copyInfo(info handler.FileInfo) handler.FileInfo {
	metadata := make(handler.MetaData, len(info.MetaData))
	for key, value := range info.MetaData {
		metadata[key] = value
	}
	storage := make(map[string]string, len(info.Storage))
	for key, value := range info.Storage {
		storage[key] = value
	}
	info.MetaData = metadata
	info.Storage = storage
	info.PartialUploads = append([]string(nil), info.PartialUploads...)
	return info
}

func atomicWriteInfo(path string, data []byte) (err error) {
	file, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func (upload *FileUpload) finishPending(ctx context.Context, record uploadRecord) error {
	lock, err := upload.store.acquireBlob(ctx, record.Hash)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return upload.publishLocked(ctx, record)
}

// The caller owns both the upload lock and blob lock, in that order. An intent
// row survives every failure until the final file and metadata have been saved.
func (upload *FileUpload) publishLocked(ctx context.Context, record uploadRecord) error {
	if len(record.Hash) != sha256.Size || !record.Expires.Valid {
		return errors.New("invalid finalization intent")
	}
	oldPath := upload.store.incompleteBinPath(upload.info.ID)
	newPath := upload.store.completeBinPath(record.Hash)
	if err := os.MkdirAll(filepath.Dir(newPath), defaultDirectoryPerm); err != nil {
		return err
	}
	if stat, err := os.Stat(newPath); err == nil {
		if !stat.Mode().IsRegular() {
			return errors.New("blob destination is not a regular file")
		}
		// Do not discard a temporary copy in favor of a corrupt existing blob.
		file, err := os.Open(newPath)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, readErr := io.Copy(hash, contextReader{ctx: ctx, reader: file})
		if err := errors.Join(readErr, file.Close()); err != nil {
			return err
		}
		if !bytes.Equal(hash.Sum(nil), record.Hash) {
			return errors.New("existing blob does not match its hash")
		}
	} else if os.IsNotExist(err) {
		if err := os.Rename(oldPath, newPath); err != nil {
			return err
		}
	} else {
		return err
	}

	upload.info.MetaData["expires"] = strconv.FormatInt(record.Expires.Int64, 10)
	upload.info.Storage["Path"] = newPath
	if err := upload.writeInfo(); err != nil {
		return err
	}
	// Remove a deduplicated temporary copy only after saving recoverable info.
	if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := db.UpdateRowContext(ctx, upload.store.DBConn, `UPDATE uploads SET lifecycle = ? WHERE id = ?`, stateComplete, upload.info.ID); err != nil {
		return err
	}
	upload.binPath = newPath
	return nil
}

func (store ShardedFileStore) terminateLocked(id string) error {
	return store.terminateContext(context.Background(), id)
}
func (store ShardedFileStore) terminateContext(ctx context.Context, id string) error {
	record, err := store.recordContext(ctx, id)
	if err != nil {
		return err
	}
	if record.Deleted {
		return nil
	}
	if record.Hash != nil {
		if len(record.Hash) != sha256.Size {
			return errors.New("invalid blob hash")
		}
		lock, err := store.acquireBlob(ctx, record.Hash)
		if err != nil {
			return err
		}
		defer lock.Unlock()
	}
	if record.Lifecycle != stateDeleting {
		if err := db.UpdateRowContext(ctx, store.DBConn, `UPDATE uploads SET lifecycle = ? WHERE id = ?`, stateDeleting, id); err != nil {
			return err
		}
	}
	if record.Hash != nil {
		var references bool
		if err := db.QueryRowContext(ctx, store.DBConn, `SELECT EXISTS(SELECT 1 FROM uploads
			WHERE sha256sum = ? AND id != ? AND deleted = 0 AND lifecycle != ? LIMIT 1)`, record.Hash, id, stateDeleting).
			Scan(&references); err != nil {
			return err
		}
		if !references {
			if err := RemoveWithDirs(store.completeBinPath(record.Hash), store.BasePath); err != nil {
				return err
			}
			store.log.Info().Str("event", "blob_deleted").Str("id", id).Msg("Removed unreferenced blob")
		}
	}
	// Always remove both possible locations. This also handles interrupted
	// finalization and legacy temporaries left after a failed relocation.
	for _, path := range []string{store.incompleteBinPath(id), store.infoPath(id) + ".tmp", store.infoPath(id)} {
		if err := RemoveWithDirs(path, store.BasePath); err != nil {
			return err
		}
	}
	return db.UpdateRowContext(ctx, store.DBConn, `UPDATE uploads SET deleted = 1, lifecycle = ? WHERE id = ?`, stateDeleted, id)
}
