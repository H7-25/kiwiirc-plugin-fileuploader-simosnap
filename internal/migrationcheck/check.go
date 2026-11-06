// Package migrationcheck verifies schema changes exclusively on a newly created
// SQLite backup. The source connection is always opened mode=ro.
package migrationcheck

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/kiwiirc/plugin-fileuploader/shardedfilestore"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type Report struct {
	Source           string              `json:"source_read_only"`
	Copy             string              `json:"migrated_copy"`
	SQLite           string              `json:"sqlite_version"`
	Rows             int64               `json:"rows"`
	Fingerprint      string              `json:"preserved_upload_fields_sha256"`
	Migrations       int                 `json:"migrations_applied"`
	BackupSeconds    float64             `json:"backup_seconds"`
	MigrationSeconds float64             `json:"migration_seconds"`
	BytesBefore      int64               `json:"copy_bytes_before_migration"`
	BytesAfter       int64               `json:"copy_bytes_after_migration"`
	QueryPlans       map[string][]string `json:"query_plans"`
}

func uri(path, mode string) string {
	u := url.URL{Scheme: "file", Path: path}
	return u.String() + "?mode=" + mode + "&_busy_timeout=5000"
}

// Verify refuses an existing destination (including symlinks), never loads the
// production TOML, and never opens an upload directory or invokes recovery.
func Verify(ctx context.Context, source, destination string) (report Report, err error) {
	source, err = filepath.Abs(source)
	if err != nil {
		return report, err
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return report, err
	}
	report.Source, report.Copy = source, destination
	if source == destination {
		return report, errors.New("source and destination must differ")
	}
	src, err := sql.Open("sqlite3", uri(source, "ro"))
	if err != nil {
		return report, err
	}
	defer src.Close()
	if err = src.PingContext(ctx); err != nil {
		return report, err
	}
	var version5 int
	if err = src.QueryRowContext(ctx, `SELECT count(*) FROM gorp_migrations WHERE id='5'`).Scan(&version5); err != nil || version5 != 1 {
		return report, fmt.Errorf("source must already contain migration 5: %v", err)
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return report, err
	}
	if err = file.Close(); err != nil {
		return report, err
	}
	dst, err := sqlx.Open("sqlite3", uri(destination, "rw"))
	if err != nil {
		return report, err
	}
	defer dst.Close()
	start := time.Now()
	err = backup(ctx, src, dst.DB)
	report.BackupSeconds = time.Since(start).Seconds()
	if err != nil {
		return report, err
	}
	if err = dst.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&report.SQLite); err != nil {
		return report, err
	}
	var fingerprint string
	report.Rows, fingerprint, err = digest(ctx, dst.DB)
	if err != nil {
		return report, err
	}
	if stat, e := os.Stat(destination); e == nil {
		report.BytesBefore = stat.Size()
	}
	start = time.Now()
	report.Migrations, err = shardedfilestore.MigrateDatabaseContext(ctx, &db.DatabaseConnection{DB: dst, DBConfig: db.DBConfig{DriverName: "sqlite3", DSN: uri(destination, "rw")}}, time.Hour, 24*time.Hour)
	report.MigrationSeconds = time.Since(start).Seconds()
	if err != nil {
		return report, err
	}
	rows, after, err := digest(ctx, dst.DB)
	if err != nil {
		return report, err
	}
	if rows != report.Rows || after != fingerprint {
		return report, errors.New("migration changed preserved upload data")
	}
	report.Fingerprint = after
	var integrity string
	if err = dst.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		return report, err
	}
	if integrity != "ok" {
		return report, fmt.Errorf("integrity_check: %s", integrity)
	}
	report.QueryPlans, err = Plans(ctx, dst.DB)
	if err != nil {
		return report, err
	}
	if stat, e := os.Stat(destination); e == nil {
		report.BytesAfter = stat.Size()
	}
	return report, nil
}

func backup(ctx context.Context, src, dst *sql.DB) error {
	source, err := src.Conn(ctx)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer destination.Close()
	return source.Raw(func(s any) error {
		return destination.Raw(func(d any) error {
			b, err := d.(*sqlite3.SQLiteConn).Backup("main", s.(*sqlite3.SQLiteConn), "main")
			if err != nil {
				return err
			}
			for {
				if err = ctx.Err(); err != nil {
					return errors.Join(err, b.Finish())
				}
				done, stepErr := b.Step(256)
				if stepErr != nil {
					return errors.Join(stepErr, b.Finish())
				}
				if done {
					return b.Finish()
				}
				// Yield and allow cancellation also if a busy source delays the backup.
				select {
				case <-ctx.Done():
					return errors.Join(ctx.Err(), b.Finish())
				case <-time.After(time.Millisecond):
				}
			}
		})
	})
}

func digest(ctx context.Context, conn *sql.DB) (int64, string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT id,uploader_ip,sha256sum,created_at,expires_at,deleted,jwt_account,jwt_issuer FROM uploads ORDER BY id`)
	if err != nil {
		return 0, "", err
	}
	defer rows.Close()
	hash := sha256.New()
	var count int64
	for rows.Next() {
		values := make([]any, 8)
		dest := make([]any, 8)
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return 0, "", err
		}
		for _, value := range values {
			var data []byte
			switch v := value.(type) {
			case nil:
				data = []byte{0}
			case []byte:
				data = append([]byte{1}, v...)
			default:
				data = []byte(fmt.Sprintf("%T:%v", value, value))
			}
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(data)))
			hash.Write(size[:])
			hash.Write(data)
		}
		count++
	}
	return count, hex.EncodeToString(hash.Sum(nil)), rows.Err()
}

// These are the same bounded/keyset predicate shapes used by maintenance.
func Plans(ctx context.Context, conn *sql.DB) (map[string][]string, error) {
	queries := map[string]string{
		"pending":    `SELECT id FROM uploads WHERE deleted=0 AND lifecycle='finalizing' AND id>'' ORDER BY id LIMIT 50`,
		"expiry":     `SELECT id,expires_at FROM uploads WHERE deleted=0 AND expires_at IS NOT NULL AND expires_at<=2000000000 AND (expires_at,id)>(0,'') ORDER BY expires_at,id LIMIT 50`,
		"incomplete": `SELECT id,created_at FROM uploads WHERE deleted=0 AND expires_at IS NULL AND created_at<=2000000000 AND (created_at,id)>(0,'') ORDER BY created_at,id LIMIT 50`,
		"references": `SELECT EXISTS(SELECT 1 FROM uploads WHERE sha256sum=x'00' AND id!='x' AND deleted=0 AND lifecycle!='deleting' LIMIT 1)`,
	}
	result := make(map[string][]string)
	for name, query := range queries {
		rows, err := conn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
				rows.Close()
				return nil, err
			}
			result[name] = append(result[name], detail)
			if strings.Contains(detail, "SCAN uploads") || strings.Contains(detail, "USE TEMP B-TREE") {
				rows.Close()
				return nil, fmt.Errorf("unbounded scan/sort in %s: %s", name, detail)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
