> Documento storico della fase precedente alla qualificazione reale. Per lo stato successivo vedere [SIMOSNAP-QUALIFICATION.md](SIMOSNAP-QUALIFICATION.md).

# Qualify the SQLite migration on a real snapshot

Do this in an isolated scratch environment using an existing consistent snapshot,
not the live production database. No production path is built into the tool.
The tool never loads the uploader TOML, opens upload files, runs commands or
starts recovery/expiration. It preserves historical rows, IDs and timestamps.

1. Obtain an authorized, consistent SQLite snapshot using SQLite's backup
   mechanism (include WAL correctly). Preserve a matching upload-directory
   backup separately for any later rollout/rollback. Do not copy a live main
   database file alone while ignoring its WAL.
2. Place the snapshot in the qualification environment. The source must already
   contain migration 5. Plan disk space for the original snapshot, the new copy,
   its indexes, transaction journal and temporary index-build space. A final
   size report is not a measurement of peak free-space requirements.
3. Use a destination filename that does not exist, on storage representative of
   the server. The program rejects an existing destination or source=destination.
4. From the repository, run the following with your snapshot/scratch paths:

```sh
go run ./cmd/verify-migration \
  -source /scratch/snapshot-v5.db \
  -copy /scratch/qualified-v7.db \
  -timeout 30m
```

The source is opened SQLite mode=ro. The driver backup API creates a consistent
copy. Only that newly created copy is migrated. On failure, keep the scratch copy
for inspection; it is not certified. Do not point the application at it merely
because a file exists.

A successful JSON report includes:

- the SQLite version actually linked by the uploader;
- upload count and a SHA-256 digest over all eight pre-existing upload fields,
  ordered by ID, compared before/after migration;
- applied migration count, backup/migration duration and final database sizes;
- integrity_check=ok (required internally);
- EXPLAIN QUERY PLAN output for pending, expiry, incomplete-age and reference
  queries. An upload-table full scan or temporary sort fails qualification.

Inspect wall time, host I/O and peak disk/journal space during the run. A single
fingerprint match does not prove acceptable service throughput. Repeat with
production-like active/deleted ratios, duplicate groups and representative
storage. Include a phase-one schema-6 snapshot if that version was used locally;
the schema-7 upgrade is supported and unit tested. Do not fabricate applied
migration records or run ad-hoc cleanup to make qualification pass.

After SQL qualification, use another isolated copy plus the matching file tree
to test application behavior in staging: normal/resumable/audio uploads, largest
configured transforms, slow simultaneous downloads, DELETE during transfer,
busy locks, interrupted publication, shutdown/restart and backlog drain. Check
Maintenance.TimeBudget against the largest blob verification time. Check any
parallelUploads overrides because concatenation remains unavailable.

Before a future rollout, stop every older uploader instance. New indexes are
created automatically at application startup if not already applied, so measure
that maintenance window and coordinate a single migrator. Avoid mixed binaries.
A rollback plan must restore a consistent database/file-tree pair or explicitly
reconcile journaled states; blindly starting the old binary is unsafe.

## Local synthetic evidence

The opt-in qualification test creates a disk-backed version-5 database with
1,000,000 rows, 95% tombstones and mixed completed/incomplete active rows:

```sh
FILEUPLOADER_LARGE_MIGRATION=1 go test -run '^TestMillionRowMigrationQualification$' -count=1 -v ./internal/migrationcheck
```

One local macOS run using SQLite 3.46.1 passed count/fingerprint preservation,
integrity checking and all four indexed plans. The database grew from
144,838,656 to 153,387,008 bytes; schema migration took approximately 0.28 seconds
(backup approximately 0.65 seconds). The entire test, including fixture creation
and all verification scans, took approximately 11 seconds. These figures describe
that synthetic fixture and local storage, not a production capacity guarantee.
No real production snapshot was available or accessed during development.
