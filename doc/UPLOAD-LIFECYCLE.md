# Upload lifecycle, locking and recovery

This implementation keeps upload IDs, URLs, configured TTLs and DELETE
ownership checks. It does not scan the filesystem or clean historical orphans.
AudioMsgButton and the other KiwiIRC UI components are unchanged.

## States and compatibility

Migration 6 adds lifecycle with a `legacy` default. Historical rows are not
rewritten: the reader derives their state from the existing deleted/hash fields.
New uploads explicitly insert `incomplete`. This also accepts databases created
by phase one, whose existing lifecycle values remain valid.

- `incomplete`: bytes may be appended under the upload lock.
- `finalizing`: hash and expiration are journaled; publication can be retried.
- `complete`: blob and metadata were published.
- `deleting`: access has been revoked; cleanup can be retried.
- `deleted`: cleanup succeeded, with the existing deleted=1 tombstone.

Migration 7 replaces the phase-one hash index with SQLite partial indexes for
active hash references, pending states, finite expiry and incomplete age. The
normal maintenance queries use bounded indexed keyset searches. Index creation
still reads the database and requires a maintenance window; qualify it first
using [MIGRATION-QUALIFICATION.md](MIGRATION-QUALIFICATION.md).

All older binaries, including phase one, must be stopped before this version
starts. The lock naming scheme changed, and old binaries do not honor the new
read/maintenance rules. Mixed-version operation is unsupported. Rollback to an
older binary is unsafe with journaled work pending; retain a consistent backup
of the database and upload directory.

## Lock ownership and downloads

Each ID/hash has its own full-key digest lock file, under a sharded directory
`Storage.Path/locks/{uploads,blobs}/<prefix>/<digest>.lock`. There are no shared
slots that make unrelated uploads interfere. These persistent coordination
files are not upload orphans. Never unlink a lock file while a server may hold
its inode. They consume inodes over time; include this in capacity planning.

Mutations acquire the upload lock exclusively, then the blob lock if needed.
TUS owns that upload lock for PATCH/DELETE. NewUpload acquires its own lock.
Internal FileUpload methods require that ownership and do not acquire it twice.

HEAD uses a shared lock and the normal tusd HEAD protocol handling through a
read-only datastore adapter. Multiple HEAD requests can run together. GET takes
a shared lock only while validating the state and opening the descriptor. It
releases it before any network transmission. A deleting/deleted row cannot be
opened. On supported Unix filesystems, deleting the pathname does not interrupt
an already-open descriptor: started downloads finish, while new opens fail.

An interrupted upload requiring finalization is recovered under a separate
exclusive acquisition. A shared lock is released before attempting that
acquisition; readers never upgrade held locks. Request lock waits are bounded
and cancellable. Temporary publication failures during resume return 423 so the
TUS client retries the same URL rather than treating it as an absent upload.

The target platform must support flock and Unix unlink/open-descriptor semantics
across every process sharing storage. Local tests run on macOS; validate Linux
and any network filesystem actually used by the deployment.

## TUS sizes and HTTP serving

For a completed upload, TUS Upload-Length/Upload-Offset represent the original
bytes received, even when pre-finish commands change the published file size.
GET/Range use a fresh stat of the opened descriptor and the physical content
size. Full GET is limited to the size observed at open, so later appends to an
incomplete upload cannot exceed its advertised Content-Length.

Range seeks use the exact start byte. Malformed, empty and unsupported multiple
ranges return 416, with Content-Range. The descriptor is closed on every return
path. Multiple ranges were already unsupported; no additional TUS extension
was disabled in phase two.

Concatenation remains disabled from phase one. Default KiwiIRC/Uppy uses
parallelUploads=1 and sequential chunks; concurrent different files do not
require concatenation. Production tusOptions/per-file overrides and saved
parallel uploads must be checked before release. Deferred lengths and
creation-with-upload remain supported and tested.

## Recovery and maintenance

Recovery runs in the background, not as an unbounded prerequisite for opening
the listener. Each phase has independent item/time limits, per-class keyset
cursors and rotating priority. Busy upload/blob locks are skipped immediately;
failures do not pin the cursor. Sweeps wrap later for retries.

Default settings, independent of TTLs:

```toml
[Maintenance]
BatchLimit = 100
TimeBudget = "2s"
BacklogDelay = "250ms"
```

BatchLimit applies separately to expiry and recovery in a cycle, divided across
their two classes. Each phase has its own TimeBudget. Full batches or an
exhausted time budget schedule another bounded cycle after BacklogDelay, rather
than waiting an entire Expiration.CheckInterval to drain a backlog. When a sweep
is exhausted, normal interval scheduling resumes. This prevents a new hard
throughput cap of one small batch per five minutes.

The expirer cancels its context at shutdown and waits for the worker to exit.
SQL statements, SQLite mutex waits, application lock waits and hash read loops
honor cancellation. A permanently stalled kernel/filesystem call cannot be
forcibly interrupted by this mechanism. Budget exhaustion is logged. Choose a
budget that can verify the largest supported blob on the real disk; otherwise
that item can repeatedly time out. Measure and tune before deployment.

Metadata is written to .info.tmp, synced, closed and renamed atomically. Errors
from relocation, metadata writes, SQL and removal are propagated. Missing
files are harmless on deletion retry. Other active references (including
finalizing uploads) protect shared blobs. Reference checks use EXISTS, not a
full count of every duplicate.

External commands can run again if the process stops before its finalization
intent is persisted. They must be idempotent. Process-recovery tests do not prove
power-loss durability: directory fsync ordering and external program writes
remain separate qualification work. Per-chunk fsync and duplicate hash
verification also require throughput measurement on the real storage.

## Frontend URL sharing

The current upload-success message format, audio tags, filename URL and legacy
HEAD 412 handling are retained. Optional metadata HEAD requests have a two-second
abort timeout and up to four attempts with delays 200/500/1000 ms. Temporary
statuses (408/423/425/429/5xx), network errors and timeouts retry; if exhausted,
the successful upload URL is still shared once without optional metadata.
Permanent failures retain the previous refusal to share. Consequently an
unavailable HEAD may omit the expiration tag/sidebar countdown, without losing
the URL.

## Verification and remaining scope

Run go test ./..., go test -race ./..., go vet ./..., and npm test from the
frontend directory. The Node tests exercise the actual source module using VM
modules and mocked responses; they do not replace a real browser/Uppy test.

TTL values, expiry access policy, DELETE authorization, historical orphan cleanup
and the generic embedded-server handler swap/drain implementation are unchanged.
A production release still needs the real snapshot qualification, Linux/storage
load tests, browser tests and a coordinated migration/rollback window. MySQL
migration/index behavior has not been qualified with a live server.
