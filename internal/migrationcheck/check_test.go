package migrationcheck

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kiwiirc/plugin-fileuploader/db"
	"github.com/kiwiirc/plugin-fileuploader/shardedfilestore"
)

func versionFiveFixture(t *testing.T, count int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source-v5.db")
	conn, err := sqlx.Open("sqlite3", uri(path, "rwc"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	wrapper := &db.DatabaseConnection{DB: conn, DBConfig: db.DBConfig{DriverName: "sqlite3"}}
	if _, err := shardedfilestore.MigrateDatabase(wrapper, time.Hour, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`DROP INDEX uploads_active_hash`, `DROP INDEX uploads_pending`, `DROP INDEX uploads_expiry`, `DROP INDEX uploads_incomplete_age`, `ALTER TABLE uploads DROP COLUMN lifecycle`, `DELETE FROM gorp_migrations WHERE id IN ('6','7')`} {
		if _, err := conn.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	_, err = conn.Exec(`WITH RECURSIVE numbers(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM numbers WHERE i+1 < ?)
 INSERT INTO uploads(id,uploader_ip,sha256sum,created_at,expires_at,deleted,jwt_account,jwt_issuer)
 SELECT printf('%032x',i),'192.0.2.1',CASE WHEN i%100=0 THEN NULL ELSE randomblob(32) END,
 1700000000,CASE WHEN i%100=0 THEN NULL ELSE 1700003600 END,CASE WHEN i%20=0 THEN 0 ELSE 1 END,'','' FROM numbers`, count)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestVerificationMigratesOnlyNewCopyAndPreservesSource(t *testing.T) {
	source := versionFiveFixture(t, 100)
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "qualified-copy.db")
	report, err := Verify(context.Background(), source, destination)
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows != 100 || report.Migrations != 2 || len(report.Fingerprint) != 64 {
		t.Fatalf("report: %+v", report)
	}
	after, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("verification changed source database")
	}
	conn, err := sqlx.Open("sqlite3", uri(source, "ro"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var max string
	if err = conn.Get(&max, `SELECT max(id) FROM gorp_migrations`); err != nil {
		t.Fatal(err)
	}
	if max != "5" {
		t.Fatalf("source migrated to %q", max)
	}
}

func TestVerificationRefusesExistingDestinationAndSamePath(t *testing.T) {
	source := versionFiveFixture(t, 3)
	destination := filepath.Join(t.TempDir(), "existing.db")
	if err := os.WriteFile(destination, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), source, destination); err == nil {
		t.Fatal("overwrote existing destination")
	}
	if _, err := Verify(context.Background(), source, source); err == nil {
		t.Fatal("accepted source as destination")
	}
	body, _ := os.ReadFile(destination)
	if string(body) != "keep" {
		t.Fatal("changed existing destination")
	}
}

func TestMillionRowMigrationQualification(t *testing.T) {
	if os.Getenv("FILEUPLOADER_LARGE_MIGRATION") != "1" {
		t.Skip("opt-in disk-backed million-row qualification")
	}
	source := versionFiveFixture(t, 1000000)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	report, err := Verify(ctx, source, filepath.Join(t.TempDir(), "qualified-million.db"))
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows != 1000000 {
		t.Fatalf("lost rows: %d", report.Rows)
	}
	encoded, _ := json.MarshalIndent(report, "", "  ")
	t.Log(string(encoded))
}
