package shardedfilestore

import (
	"context"
	"fmt"
	"time"

	"github.com/kiwiirc/plugin-fileuploader/db"

	migrate "github.com/rubenv/sql-migrate"
)

// MigrateDatabase applies only schema migrations, without touching upload files
// or running recovery. The verification command calls it on a new backup copy.
func MigrateDatabase(conn *db.DatabaseConnection, guestTTL, identifiedTTL time.Duration) (int, error) {
	return MigrateDatabaseContext(context.Background(), conn, guestTTL, identifiedTTL)
}

func MigrateDatabaseContext(ctx context.Context, conn *db.DatabaseConnection, guestTTL, identifiedTTL time.Duration) (int, error) {
	store := &ShardedFileStore{DBConn: conn, ExpireTime: guestTTL, ExpireIdentifiedTime: identifiedTTL}
	return store.migrateDBContext(ctx)
}

func (store *ShardedFileStore) migrateDB() (int, error) {
	return store.migrateDBContext(context.Background())
}

func (store *ShardedFileStore) migrateDBContext(ctx context.Context) (int, error) {
	indexes := []string{
		`DROP INDEX IF EXISTS uploads_hash_active;`,
		`CREATE INDEX uploads_active_hash ON uploads(sha256sum, lifecycle, id) WHERE deleted = 0;`,
		`CREATE INDEX uploads_pending ON uploads(lifecycle, id) WHERE deleted = 0;`,
		`CREATE INDEX uploads_expiry ON uploads(expires_at, id) WHERE deleted = 0 AND expires_at IS NOT NULL;`,
		`CREATE INDEX uploads_incomplete_age ON uploads(created_at, id) WHERE deleted = 0 AND expires_at IS NULL;`,
	}
	if store.DBConn.DriverName == "mysql" {
		// MySQL does not support partial indexes. The old hash index, if present,
		// remains valid; new index names also support upgrading a phase-one DB.
		indexes = []string{
			`CREATE INDEX uploads_active_hash ON uploads(sha256sum(32), deleted, lifecycle, id);`,
			`CREATE INDEX uploads_pending ON uploads(deleted, lifecycle, id);`,
			`CREATE INDEX uploads_expiry ON uploads(deleted, expires_at, id);`,
			`CREATE INDEX uploads_incomplete_age ON uploads(deleted, expires_at, created_at, id);`,
		}
	}
	migrations := &migrate.MemoryMigrationSource{
		Migrations: []*migrate.Migration{
			{
				Id: "1",
				Up: []string{
					`
					CREATE TABLE uploads(
						id VARCHAR(36) PRIMARY KEY,
						uploader_ip BLOB,
						sha256sum BLOB,
						created_at INTEGER(8)
					);`,
				},
				Down: []string{"DROP TABLE uploads;"},
			},
			{
				Id: "2",
				Up: []string{
					`
					ALTER TABLE uploads
						ADD deleted INTEGER(1) DEFAULT 0 NOT NULL
					;`,
				},
			},
			{
				Id: "3",
				Up: []string{
					`
					CREATE TABLE new_uploads(
						id VARCHAR(36) PRIMARY KEY,
						uploader_ip VARCHAR(45),
						sha256sum BLOB,
						created_at INTEGER(8),
						deleted INTEGER(1) DEFAULT 0 NOT NULL
					);`,
					`
					INSERT INTO new_uploads(id, sha256sum, created_at, deleted)
						SELECT id, sha256sum, created_at, deleted
						FROM uploads
					;`,
					`DROP TABLE uploads;`,
					`ALTER TABLE new_uploads RENAME TO uploads;`,
				},
			},
			{
				Id: "4",
				Up: []string{
					`
					CREATE TABLE new_uploads(
						id VARCHAR(36) PRIMARY KEY,
						uploader_ip VARCHAR(45),
						sha256sum BLOB,
						created_at INTEGER(8),
						deleted INTEGER(1) DEFAULT 0 NOT NULL,
						jwt_account TEXT,
						jwt_issuer TEXT
					);`,
					`
					INSERT INTO new_uploads(id, uploader_ip, sha256sum, created_at, deleted)
						SELECT id, uploader_ip, sha256sum, created_at, deleted
						FROM uploads
					;`,
					`DROP TABLE uploads;`,
					`ALTER TABLE new_uploads RENAME TO uploads;`,
				},
			},
			{
				Id: "5",
				Up: []string{
					`
					CREATE TABLE new_uploads(
						id VARCHAR(36) PRIMARY KEY,
						uploader_ip VARCHAR(45),
						sha256sum BLOB,
						created_at INTEGER(8),
						expires_at INTEGER(8),
						deleted INTEGER(1) DEFAULT 0 NOT NULL,
						jwt_account TEXT DEFAULT '' NOT NULL,
						jwt_issuer TEXT DEFAULT '' NOT NULL
					);`,
					`
					INSERT INTO new_uploads(id, uploader_ip, sha256sum, created_at, deleted, jwt_account, jwt_issuer, expires_at)
						SELECT id, uploader_ip, sha256sum, created_at, deleted,
						CASE WHEN jwt_account IS NULL THEN '' ELSE jwt_account END,
						CASE WHEN jwt_issuer IS NULL THEN '' ELSE jwt_issuer END,
						CASE WHEN jwt_account IS NOT NULL THEN created_at + ` + fmt.Sprintf("%.0f", store.ExpireIdentifiedTime.Seconds()) + `
						ELSE created_at + ` + fmt.Sprintf("%.0f", store.ExpireTime.Seconds()) + ` END
					 	as expires_at
						FROM uploads
					;`,
					`DROP TABLE uploads;`,
					`ALTER TABLE new_uploads RENAME TO uploads;`,
				},
			},
			{
				Id: "6",
				// Legacy rows are classified when read. Avoid rewriting millions of
				// historical rows just to materialize a state already implied by them.
				Up: []string{`ALTER TABLE uploads ADD lifecycle VARCHAR(16) NOT NULL DEFAULT 'legacy';`},
			},
			{Id: "7", Up: indexes},
		},
	}

	return migrate.ExecContext(ctx, store.DBConn.DB.DB, store.DBConn.DriverName, migrations, migrate.Up)
}

func (store *ShardedFileStore) initDB() {
	n, err := store.migrateDB()
	if err != nil {
		store.log.Fatal().Err(err).Msg("Failed to apply migrations")
	}
	if n > 0 {
		store.log.Info().Str("event", "schema_migrations").Int("count", n).Msg("Applied schema migrations")
	}
}
