package server

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/kiwiirc/plugin-fileuploader/shardedfilestore"
	"github.com/tus/tusd/pkg/handler"
)

type readStore struct {
	store *shardedfilestore.ShardedFileStore
}

func (s readStore) NewUpload(context.Context, handler.FileInfo) (handler.Upload, error) {
	return nil, handler.ErrNotImplemented
}
func (s readStore) GetUpload(ctx context.Context, id string) (handler.Upload, error) {
	return s.store.GetReadUpload(ctx, id)
}

// Retain tusd's HEAD protocol handling, but use shared locking and a read-only
// datastore. The main handler continues using exclusive locks for mutations.
func (serv *UploadServer) headFile(cfg handler.Config, store *shardedfilestore.ShardedFileStore) (gin.HandlerFunc, error) {
	composer := handler.NewStoreComposer()
	composer.UseCore(readStore{store: store})
	cfg.StoreComposer = composer
	h, err := handler.NewUnroutedHandler(cfg)
	if err != nil {
		return nil, err
	}
	return func(c *gin.Context) {
		id, err := extractIDFromPath(c.Request.URL.Path)
		if err != nil {
			serv.sendError(c.Writer, c.Request, err)
			return
		}
		_, lock, err := store.ReadUpload(c.Request.Context(), id)
		if err != nil {
			serv.sendError(c.Writer, c.Request, err)
			return
		}
		defer lock.Unlock()
		h.HeadFile(c.Writer, c.Request)
	}, nil
}
