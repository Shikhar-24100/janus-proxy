# Durable usage outbox and restart recovery

## Why this exists

The retry channel protects against temporary Redis failures while Janus is
running. Memory disappears on a hard process kill. The outbox adds a local
durability boundary: a completed accounting event is recoverable after its
file and directory entry have been successfully flushed to persistent storage.

An outbox is a collection of records waiting for delivery. PostgreSQL is still
the reporting database; Redis is still the persistent delivery queue. The local
outbox bridges the handoff to Redis. It stores only the existing accounting
metadata, never message content, generated answers, or credentials.

```mermaid
flowchart TD
  C[Authenticated client] --> A{Seat and outbox capacity available?}
  A -->|No| E[503 before quota or provider call]
  A -->|Yes| P[Quota, cache, provider and response]
  P --> O[Write event to temporary file and fsync]
  O --> R[Rename to final file and fsync directory]
  R --> Q[Deliver to persistent Redis]
  Q -->|Confirmed| D[Delete local file and fsync directory]
  D --> F[Release storage reservation and seat]
  Q -->|Uncertain| B[Keep file; pause admissions; retry]
  S[Restart: lock and validate outbox] --> B
  Q --> W[Existing worker commits to PostgreSQL]
```

## File lifecycle

Every event uses a SHA-256 filename derived from its original request ID.
Hashing keeps arbitrary IDs out of filesystem paths; it is not a content
checksum. Files contain validated JSON and are limited to 16 KiB.

Janus creates a temporary file, writes the event, calls `File.Sync`, closes it,
renames it to its final name, and syncs the directory. Flushing the file persists
its bytes; flushing the directory persists the final filename. Only then does
Janus try Redis. Confirmed Redis delivery permits removal of the local file,
followed by another directory flush. Operations for the same request ID are
serialized through a fixed set of locks. Independent records can flush
concurrently, bounded by admitted work; disk I/O never holds the metadata lock.

At startup, a Linux advisory lock prevents two processes from owning the same
directory. Janus validates every final record before starting recovery. A corrupt
or unexpected file stops startup and is retained for inspection. Temporary
files from incomplete writes are discarded: they never crossed the local
confirmation boundary. Recovered records are delivered before new chat
admissions reopen. No model is called during replay.

The same request ID survives every retry and restart. Redis's recent-handoff
marker reduces repeated deliveries; PostgreSQL's permanent primary key prevents
duplicate accounting if Redis receives an event again, including after the
marker expires. The SQL worker may still be writing after local removal; at that
point persistent Redis owns recovery.

## Configuration and limits

The normal Linux Compose stack and the load-test fixture enable:

```text
USAGE_OUTBOX_DIR=/var/lib/janus/outbox
USAGE_OUTBOX_MAX_MB=32
```

The normal stack uses the named `outbox-data` Docker volume. For a custom path,
its parent directory must already exist on persistent Linux storage. Each replica needs
its own directory/volume; do not share it between simultaneous replicas. Container
recreation preserves the volume. Removing volumes deliberately deletes recovery
data. Outbox data is excluded from Git and Docker build inputs.

The 32 MiB setting allows 2048 event slots of at most 16 KiB each. Admission
reserves a slot before invoking the chat handler. Existing records plus active
reservations consume capacity conservatively. A full outbox rejects arrivals
with the existing 503 response. Filesystem metadata, temporary files and filesystem
allocation overhead are additional disk usage: this is a logical event-payload
bound, not a partition quota or a guarantee that the host has free disk space.

A disk write/flush failure closes admission and retries the affected event.
That event is still only in memory until a subsequent write succeeds. Existing
streams follow their usual completion and cancellation behavior. File sync is
a blocking OS operation; the separate 200 ms Redis budget begins after local
persistence and cannot interrupt a disk sync.

Leaving `USAGE_OUTBOX_DIR` empty preserves the earlier in-memory handoff mode.
An enabled outbox requires Redis and PostgreSQL usage storage. The durable
implementation requires Linux filesystem locking and directory fsync; use
the Linux container stack from Windows. Native Windows mode leaves it disabled.

## Observability

Authenticated `/metrics` exposes `janus_outbox_records`, `janus_outbox_bytes`,
`janus_outbox_reserved`, `janus_outbox_capacity_bytes`, `janus_outbox_blocked`,
`janus_outbox_writes_total`, `janus_outbox_errors_total`, and
`janus_outbox_replayed_total`. Counters reset on restart; records remain on disk.
The `janus_outbox_write_duration_seconds` histogram measures local writes and
flushes, including lock waits. The existing usage-enqueue stage now includes
local persistence, Redis confirmation, and confirmed local deletion on its
fast path. Completion latency can rise; first-token streaming still precedes
the completion outbox write.

## Verification and remaining boundaries

Tests kill a real subprocess after local confirmation, reopen its outbox and
replay into Redis/PostgreSQL. They cover both a missing database record and an
already committed record. Further tests cover capacity reservations, admission
rejection before the handler, exclusive ownership, corruption, and injected
directory-flush failure preventing premature Redis delivery.

Concurrency tests verify independent disk flushes proceed together and
same-request publishers write once. Shutdown waits for active publishers before
releasing exclusive ownership. An unconfirmed record survives shutdown and
replays on reopening. Slow local flushing does not consume Redis's timeout.

A crash during generation or before local confirmation still leaves an
accounting gap. This is a completion outbox, not a durable request-start journal.
Disk/volume loss, storage that does not honor fsync, and hardware failure are
outside this guarantee. Backups, replication and provider invoice reconciliation
remain necessary production work. Saved events are metadata, not a way to
reconstruct or resume an interrupted model response.

## Local measurements

Separate-container fake-provider load phases ran for 10 seconds each. These
measure successful-response completion latency including upstream time and
client scheduling lag; deliberate 503 responses are counted separately.

| Gateway mode | Offered RPS | Accepted / offered | Rejected | Completion p50 / p99 | First-text p99 |
|---|---:|---:|---:|---:|---:|
| JSON | 200 | 2000 / 2000 | 0 | 16.59 / 54.79 ms | n/a |
| SSE | 200 | 2000 / 2000 | 0 | 19.01 / 44.95 ms | 8.26 ms |
| JSON | 1000 | 8757 / 10000 | 1243 | 33.10 / 59.02 ms | n/a |
| SSE | 1000 | 5829 / 10000 | 4171 | 46.33 / 151.11 ms | 87.62 ms |

All 18586 admitted requests had matching PostgreSQL request, attempt and token
totals. All outbox files, retained handoffs and Redis entries drained. There
were no transport drops, invalid responses, terminal handoff errors, worker
errors, outbox errors, or handoff retries in this run. It used no paid APIs.
Mean local write-stage times were 2.48/3.10 ms at 200 RPS (JSON/SSE), and
7.53/6.68 ms at 1000 RPS. These include local lock waits but exclude local deletion.

An earlier implementation serialized all disk flushes and admitted only 2495
JSON and 1974 SSE requests at 1000 RPS, with p99 of 260.67 and 317.31 ms.
Allowing independent files to flush concurrently improved those results.
The Redis budget also now starts after disk persistence.

The prior no-outbox 30-second run admitted 29744/30000 JSON and 29368/30000 SSE
requests at 1000 RPS, with p99 36.50/48.62 ms. The runs have different durations
and share the laptop's WSL resources, so this is not a controlled overhead
comparison. It does demonstrate a substantial throughput cost from durable
file writes. The 5-10 ms proxy-overhead goal remains unfinished. A batched
durable journal/group-commit design is a possible next optimization; production
capacity must be established with longer tests and representative storage.

Latest raw reports are in `.cache/loadtest/`; earlier outbox-serialized reports
are in `.cache/loadtest-outbox-serial/` and no-outbox reports in
`.cache/loadtest-before-outbox/`.
