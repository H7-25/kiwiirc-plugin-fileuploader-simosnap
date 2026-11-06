// Package shardedfilestore is a modified version of the tusd/filestore implementation.
// Splits file storage into subdirectories based on the hash prefix.
// based on https://github.com/tus/tusd/blob/6d987aa226e2e6cefca1df012da5599a57622b17/pkg/filestore/filestore.go
package shardedfilestore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/IGLOU-EU/go-wildcard"
	"github.com/rs/zerolog"
	"github.com/tus/tusd/pkg/handler"

	_ "github.com/go-sql-driver/mysql" // register mysql driver
	_ "github.com/mattn/go-sqlite3"    // register SQL driver

	"github.com/kiwiirc/plugin-fileuploader/config"
	"github.com/kiwiirc/plugin-fileuploader/db"
)

var defaultFilePerm = os.FileMode(0664)
var defaultDirectoryPerm = os.FileMode(0775)

// ShardedFileStore implements various handler.DataStore-related interfaces.
// See the interfaces for more documentation about the different methods.
type ShardedFileStore struct {
	BasePath             string        // Relative or absolute path to store files in.
	PrefixShardLayers    int           // Number of extra directory layers to prefix file paths with.
	ExpireTime           time.Duration // How long before an upload expires (seconds)
	ExpireIdentifiedTime time.Duration // How long before an upload expires with valid account (seconds)
	PreFinishCommands    []config.PreFinishCommand
	DBConn               *db.DatabaseConnection
	maintenance          *maintenanceState
	log                  *zerolog.Logger
}

// New creates a new file based storage backend. The directory specified will
// be used as the only storage entry. This method does not check
// whether the path exists, use os.MkdirAll to ensure.
// In addition, a locking mechanism is provided.
func New(basePath string, prefixShardLayers int, expireTime, expireIdentifiedTime time.Duration, PreFinishCommands []config.PreFinishCommand, dbConnection *db.DatabaseConnection, log *zerolog.Logger) *ShardedFileStore {
	store := &ShardedFileStore{
		BasePath:             basePath,
		PrefixShardLayers:    prefixShardLayers,
		ExpireTime:           expireTime,
		ExpireIdentifiedTime: expireIdentifiedTime,
		PreFinishCommands:    PreFinishCommands,
		DBConn:               dbConnection,
		log:                  log,
		maintenance:          newMaintenanceState(),
	}
	store.initDB()
	return store
}

// UseIn sets this store as the core data store in the passed composer and adds
// all possible extension to it.
func (store ShardedFileStore) UseIn(composer *handler.StoreComposer) {
	composer.UseCore(store)
	composer.UseTerminater(store)
	composer.UseLocker(store)
	composer.UseLengthDeferrer(store)
}

func (store ShardedFileStore) NewUpload(ctx context.Context, info handler.FileInfo) (result handler.Upload, err error) {
	if info.ID == "" {
		info.ID = Uid()
	}
	if err := validateID(info.ID); err != nil {
		return nil, err
	}
	lock, err := store.NewLock(info.ID)
	if err != nil {
		return nil, err
	}
	if err := lock.Lock(); err != nil {
		return nil, err
	}
	defer lock.Unlock()
	info = copyInfo(info)
	binPath := store.incompleteBinPath(info.ID)
	info.Storage = map[string]string{
		"Type": "filestore",
		"Path": binPath,
	}

	// Create the directory stucture if needed
	err = os.MkdirAll(store.metaDir(info.ID), defaultDirectoryPerm)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(store.incompleteBinDir(), defaultDirectoryPerm)
	if err != nil {
		return nil, err
	}

	// Metadata is exposed to users via headers, so remove RemoteIP
	remoteIP := info.MetaData["RemoteIP"]
	delete(info.MetaData, "RemoteIP")

	// create record in uploads table
	err = db.UpdateRow(store.DBConn,
		`INSERT INTO uploads(id, created_at, uploader_ip, jwt_account, jwt_issuer, lifecycle) VALUES (?, ?, ?, ?, ?, ?)`,
		info.ID, time.Now().Unix(), remoteIP, info.MetaData["account"], info.MetaData["issuer"], stateIncomplete,
	)
	if err != nil {
		return nil, err
	}

	// Keep failed creations discoverable by the maintenance worker if cleanup
	// itself fails. Never touch files belonging to a failed INSERT/colliding ID.
	defer func() {
		if err != nil {
			if cleanupErr := store.terminateLocked(info.ID); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()

	// Create .bin file with no content
	file, err := os.OpenFile(binPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		return nil, err
	}

	err = file.Close()
	if err != nil {
		return nil, err
	}

	upload := &FileUpload{
		info:     info,
		store:    store,
		infoPath: store.infoPath(info.ID),
		binPath:  binPath,
	}

	// writeInfo creates the file by itself if necessary
	err = upload.writeInfo()
	if err != nil {
		return nil, err
	}

	// TUS finishes a zero-length POST without taking its upload lock. Publish
	// it here while creation still owns that lock; the later call is a no-op.
	if !info.SizeIsDeferred && info.Size == 0 {
		if err := upload.FinishUpload(ctx); err != nil {
			return nil, err
		}
	}
	return upload, nil
}

// GetFileUpload is called with the upload lock held by TUS or our GET handler.
func (store ShardedFileStore) GetFileUpload(ctx context.Context, id string) (*FileUpload, error) {
	return store.getFileUpload(ctx, id, true)
}

func (store ShardedFileStore) getFileUpload(ctx context.Context, id string, recover bool) (*FileUpload, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	if len(id) < store.PrefixShardLayers {
		return nil, handler.ErrNotFound
	}
	record, err := store.recordContext(ctx, id)
	if err != nil {
		return nil, err
	}
	if record.Deleted || record.Lifecycle == stateDeleting {
		return nil, handler.ErrNotFound
	}
	info := handler.FileInfo{}
	data, err := ioutil.ReadFile(store.infoPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			err = handler.ErrNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	if info.ID != id {
		return nil, errors.New("upload metadata ID mismatch")
	}
	binPath := store.incompleteBinPath(id)
	if record.Lifecycle == stateComplete {
		if len(record.Hash) != sha256.Size {
			return nil, errors.New("invalid completed upload hash")
		}
		binPath = store.completeBinPath(record.Hash)
	} else if record.Lifecycle != stateIncomplete && record.Lifecycle != stateFinalizing {
		return nil, errors.New("invalid upload lifecycle")
	}
	upload := &FileUpload{info: copyInfo(info), binPath: binPath, store: store, infoPath: store.infoPath(id)}
	if record.Lifecycle == stateFinalizing {
		if !recover {
			return nil, handler.ErrFileLocked
		}
		if err := upload.finishPending(ctx, record); err != nil {
			return nil, err
		}
	}
	stat, err := os.Stat(upload.binPath)
	if err != nil {
		if os.IsNotExist(err) {
			err = handler.ErrNotFound
		}
		return nil, err
	}
	upload.info.Offset = stat.Size()
	// A last chunk can have reached disk even if its HTTP request failed. TUS
	// otherwise skips FinishUpload when the recovered offset already equals size.
	if record.Lifecycle == stateIncomplete && !info.SizeIsDeferred && stat.Size() == info.Size {
		if !recover {
			return nil, handler.ErrFileLocked
		}
		if err := upload.FinishUpload(ctx); err != nil {
			return nil, err
		}
		stat, err = os.Stat(upload.binPath)
		if err != nil {
			return nil, err
		}
	}
	// TUS tracks bytes received before transformations, not published blob size.
	if record.Lifecycle == stateComplete || upload.binPath != store.incompleteBinPath(id) {
		upload.info.Offset = upload.info.Size
	}
	return upload, nil
}

func (store ShardedFileStore) GetUpload(ctx context.Context, id string) (handler.Upload, error) {
	return store.GetFileUpload(ctx, id)
}

func (store ShardedFileStore) AsTerminatableUpload(upload handler.Upload) handler.TerminatableUpload {
	return upload.(*FileUpload)
}

func (store ShardedFileStore) AsLengthDeclarableUpload(upload handler.Upload) handler.LengthDeclarableUpload {
	return upload.(*FileUpload)
}

func (store ShardedFileStore) AsConcatableUpload(upload handler.Upload) handler.ConcatableUpload {
	return upload.(*FileUpload)
}

// infoPath returns the path to the .info file storing the upload's metadata.
func (store *ShardedFileStore) infoPath(id string) string {
	// <base-path>/meta/<id-shards>/<id>.info
	return filepath.Join(store.metaDir(id), id+".info")
}

type FileUpload struct {
	// info stores the current information about the upload
	info handler.FileInfo
	// store the fileupload's store
	store ShardedFileStore
	// infoPath is the path to the .info file
	infoPath string
	// binPath is the path to the binary file (which has no extension)
	binPath string
}

func (upload *FileUpload) GetInfo(ctx context.Context) (handler.FileInfo, error) {
	return copyInfo(upload.info), nil
}

func (upload *FileUpload) WriteChunk(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	record, err := upload.store.recordContext(ctx, upload.info.ID)
	if err != nil {
		return 0, err
	}
	if record.Deleted {
		return 0, handler.ErrNotFound
	}
	// Creation-with-upload of an empty file may call WriteChunk after
	// NewUpload has already finalized it under the creation lock.
	if record.Lifecycle == stateComplete && upload.info.Size == 0 && offset == 0 {
		return 0, nil
	}
	if record.Lifecycle != stateIncomplete {
		return 0, errors.New("upload is not writable")
	}
	file, err := os.OpenFile(upload.binPath, os.O_WRONLY|os.O_APPEND, defaultFilePerm)
	if err != nil {
		return 0, err
	}
	stat, err := file.Stat()
	if err != nil || stat.Size() != offset {
		file.Close()
		if err != nil {
			return 0, err
		}
		return 0, handler.ErrMismatchOffset
	}
	if !upload.info.SizeIsDeferred {
		remaining := upload.info.Size - offset
		if remaining < 0 {
			file.Close()
			return 0, errors.New("upload exceeds declared size")
		}
		src = io.LimitReader(src, remaining)
	}
	n, copyErr := io.Copy(file, src)
	syncErr := file.Sync()
	closeErr := file.Close()
	upload.info.Offset += n
	return n, errors.Join(copyErr, syncErr, closeErr)
}

func (upload *FileUpload) GetReader(ctx context.Context) (io.Reader, error) {
	return os.Open(upload.binPath)
}

func (upload *FileUpload) GetFile(ctx context.Context) (*os.File, error) {
	return os.Open(upload.binPath)
}

func (upload *FileUpload) Terminate(ctx context.Context) error {
	// TUS already owns the upload lock.
	return upload.store.terminateContext(ctx, upload.info.ID)
}

func (upload *FileUpload) ConcatUploads(ctx context.Context, uploads []handler.Upload) (err error) {
	file, err := os.OpenFile(upload.binPath, os.O_WRONLY|os.O_APPEND, defaultFilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, partialUpload := range uploads {
		fileUpload := partialUpload.(*FileUpload)

		src, err := os.Open(fileUpload.binPath)
		if err != nil {
			return err
		}

		_, copyErr := io.Copy(file, src)
		closeErr := src.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
	}

	return
}

func (upload *FileUpload) DeclareLength(ctx context.Context, length int64) error {
	upload.info.Size = length
	upload.info.SizeIsDeferred = false
	return upload.writeInfo()
}

// writeInfo updates the entire information. Everything will be overwritten.
func (upload *FileUpload) writeInfo() error {
	data, err := json.Marshal(upload.info)
	if err != nil {
		return err
	}
	return atomicWriteInfo(upload.infoPath, data)
}

func (upload *FileUpload) FinishUpload(ctx context.Context) error {
	record, err := upload.store.recordContext(ctx, upload.info.ID)
	if err != nil {
		return err
	}
	if record.Deleted || record.Lifecycle == stateDeleting {
		return handler.ErrNotFound
	}
	if record.Lifecycle == stateComplete {
		return nil
	}
	if record.Lifecycle == stateFinalizing {
		return upload.finishPending(ctx, record)
	}
	stat, err := os.Stat(upload.store.incompleteBinPath(upload.info.ID))
	if err != nil {
		return err
	}
	if upload.info.SizeIsDeferred || stat.Size() != upload.info.Size {
		return errors.New("cannot finalize an incomplete upload")
	}
	upload.store.log.Debug().
		Str("event", "upload_finished").
		Str("id", upload.info.ID).Msg("Finishing upload")

	oldPath := upload.store.incompleteBinPath(upload.info.ID)

	// execute completion commands
	fileType, ok := upload.info.MetaData["filetype"]
	if !ok {
		upload.store.log.Warn().
			Msg("Failed to get filetype metadata in ExecuteCommands")
	}

	absPath, err := filepath.Abs(oldPath)
	if err != nil {
		upload.store.log.Error().
			Err(err).
			Msg("Failed resolve path in ExecuteCommands")
		return err
	}

	for _, preFinish := range upload.store.PreFinishCommands {
		if !wildcard.Match(preFinish.Pattern, fileType) {
			continue
		}

		args := make([]string, 0)
		for _, arg := range preFinish.Args {
			arg = strings.ReplaceAll(arg, "%FILE%", absPath)
			args = append(args, arg)
		}

		cmd := exec.CommandContext(ctx, preFinish.Command, args...)
		var stdOut, stdErr bytes.Buffer
		cmd.Stdout = &stdOut
		cmd.Stderr = &stdErr
		if err := cmd.Run(); err != nil && preFinish.RejectOnNoneZeroExit {
			upload.store.log.Warn().
				Err(err).
				Strs("args", args).
				Str("command", preFinish.Command).
				Msg("Error with pre-finish command")

			upload.store.log.Debug().
				Str("stdout", stdOut.String()).
				Str("stderr", stdErr.String()).
				Msg("Error with pre-finish command")

			cleanupErr := upload.store.terminateContext(ctx, upload.info.ID)
			if cleanupErr != nil {
				return cleanupErr
			}
			return handler.NewHTTPError(errors.New("Upload has been reject by server"), 406)
		}
	}

	// calculate hash
	hash, err := upload.store.hashFile(ctx, upload.info.ID)
	if err != nil {
		upload.store.log.Error().
			Err(err).
			Msg("Failed to hash completed upload")
		return err
	}

	expires := durationToExpire(upload.store.ExpireTime)
	if upload.info.MetaData["account"] != "" {
		expires = durationToExpire(upload.store.ExpireIdentifiedTime)
	}
	upload.info.MetaData["expires"] = strconv.FormatInt(expires, 10)

	// Persist the intent before moving anything. Recovery never re-runs
	// pre-finish commands after this boundary.
	lock, err := upload.store.acquireBlob(ctx, hash)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	if err := db.UpdateRowContext(ctx, upload.store.DBConn, `
		UPDATE uploads SET sha256sum = ?, expires_at = ?, lifecycle = ? WHERE id = ?
	`, hash, expires, stateFinalizing, upload.info.ID); err != nil {
		return err
	}
	return upload.publishLocked(ctx, uploadRecord{Hash: hash, Expires: sql.NullInt64{Int64: expires, Valid: true}, Lifecycle: stateFinalizing})
}

// ADDED FUNCTIONS

// taken from https://github.com/tus/tusd/blob/42bfe35457f8bfc79a0af40a9f51c8112903737e/internal/uid/uid.go
func Uid() string {
	id := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, id)
	if err != nil {
		// This is probably an appropriate way to handle errors from our source
		// for random bits.
		panic(err)
	}
	return hex.EncodeToString(id)
}

// Terminate is the maintenance entry point. HTTP termination uses the same
// implementation with the upload lock already held by TUS.
func (store *ShardedFileStore) Terminate(id string) error {
	lock, err := store.NewLock(id)
	if err != nil {
		return err
	}
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()
	return store.terminateLocked(id)
}

func (store *ShardedFileStore) hashFile(ctx context.Context, id string) ([]byte, error) {
	f, err := os.Open(store.incompleteBinPath(id))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, contextReader{ctx: ctx, reader: f}); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

func isDirEmpty(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// RemoveWithDirs deletes the given path and its empty parent directories
// up to the given basePath
func RemoveWithDirs(path string, basePath string) (err error) {
	absBase, err := filepath.Abs(basePath)
	if err != nil {
		return
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return
	}

	rel, err := filepath.Rel(absBase, absPath)
	if err != nil {
		return err
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("Path %#v is not prefixed by basepath %#v", path, basePath)
	}

	if _, err = os.Stat(path); err == nil {
		err = os.Remove(path)
	} else if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	parent := path
	for {
		parent = filepath.Dir(parent)
		parentAbs, err := filepath.Abs(parent)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(absBase, parentAbs)
		if err != nil {
			return err
		}
		if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			break
		}

		empty, err := isDirEmpty(parent)
		if empty {
			err = os.Remove(parent)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// metaDir returns the directory that the info and lock files reside in for a given id
func (store *ShardedFileStore) metaDir(id string) string {
	// <base-path>/meta/<id-shards>
	shards := store.shards(id)
	return filepath.Join(store.BasePath, "meta", shards)
}

// generates a directory hierarchy
func (store *ShardedFileStore) shards(id string) string {
	if len(id) < store.PrefixShardLayers {
		panic("id is too short for requested number of shard layers")
	}
	shards := make([]string, store.PrefixShardLayers)
	for n, char := range id[:store.PrefixShardLayers] {
		shards[n] = string(char)
	}
	return filepath.Join(shards...)
}

func (store *ShardedFileStore) incompleteBinDir() string {
	return filepath.Join(store.BasePath, "incomplete")
}

func (store *ShardedFileStore) incompleteBinPath(id string) string {
	// during upload: <base-path>/incomplete/<id>.bin
	return filepath.Join(store.incompleteBinDir(), id+".bin")
}

func (store ShardedFileStore) completeBinPath(hashBytes []byte) string {
	// finished: <base-path>/complete/<hash-shards>/<hash>.bin
	hash := fmt.Sprintf("%x", hashBytes)
	shards := store.shards(hash)
	return filepath.Join(store.BasePath, "complete", shards, hash+".bin")
}

func durationToExpire(d time.Duration) int64 {
	timeStr := fmt.Sprintf("%.0f", d.Seconds())
	timeInt, _ := strconv.Atoi(timeStr)
	return time.Now().Unix() + int64(timeInt)
}
