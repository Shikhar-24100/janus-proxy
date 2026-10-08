# Group commit in the usage outbox

## Why batch writes?

Previously each event needed a file flush, rename/directory flush and durable
deletion. At 1000 offered RPS, the per-file outbox accepted 8757/10000 JSON and
5829/10000 streaming requests in the local 10-second phases. Their successful
completion p99 values were 59.02 and 151.11 ms. Disk pressure was substantial.

Group commit lets several callers share a disk flush. A dedicated goroutine
collects up to 32 operations for up to 1 ms after its first operation arrives.
It writes their frames together and flushes once. Callers wait for confirmation;
placing an operation in memory is not a durability acknowledgement. The channel
holds 64 operations and backpressures callers while admission seats remain held.
Disk I/O and earlier batches can add waiting beyond the collection window.

```text
request A -> save A --+
request B -> save B --+-> journal batch -> fsync -> confirm A, B, ack C
Redis confirmed C -> acknowledge C --+
```

A request first waits for its save, then delivers to Redis, then waits for its
journal acknowledgement. Those two operations may share batches with other
requests. An idle request can incur two collection windows and two flushes;
the benefit comes from concurrent work and avoiding per-request file metadata.

## Recovery and cleanup

The journal frame is `JNS1 | uint32 length | CRC32 | JSON operation`. The parser
bounds payload allocation, validates metadata, and applies save/ack operations
in order. CRC32 detects accidental damage; it is not authentication against
malicious modification. A partial trailing header/payload is truncated and
flushed. A complete corrupt frame fails startup instead of discarding history.

Complete unconfirmed frames may survive a crash and replay. Acknowledgements
are written only after Redis confirmation. Request IDs are stable, so PostgreSQL
still deduplicates repeated delivery. A failed live append is truncated back to
the last confirmed offset before retry, with a durable flush of that repair.

Compaction starts when the next batch crosses a threshold (up to 4 MiB) and
history is more than twice the estimated live payload/frame size. It flushes a
new file containing only pending saves, atomically renames it, then flushes the
directory. Both old and new versions describe the same pending set. If that
directory flush fails, appends pause until it succeeds. Compaction uses the same
writer, so it temporarily delays confirmations without losing ownership.

The configured 32 MiB remains a logical live-event allowance of 2048 slots;
admission reserves capacity before provider work. History/frame overhead means
the journal can approach roughly twice the live allowance plus one batch and
framing. Compaction also temporarily needs another live-sized file. Filesystem
metadata and allocation overhead are additional; this is not a filesystem quota.
Lowering capacity retains existing backlog for recovery rather than deleting it.

Old per-event JSON files migrate on startup: import, flush the journal and its
directory, then remove the old files. A crash between steps may repeat delivery,
which the permanent database request ID handles. The migration is one way;
deploying an old binary against this volume fails because it cannot read the
journal. Each replica still needs its own volume and exclusive Linux lock.

## Metrics and tests

Existing gauges show live records/bytes, reserved slots, capacity and blocked
state. New metrics are:

* `janus_outbox_operations_total`: committed saves plus acknowledgements.
* `janus_outbox_batches_total`: successful append batches.
* `janus_outbox_syncs_total`: successful file syncs, including startup/repair/checkpoints.
* `janus_outbox_journal_bytes`: current journal size, including delivered history.
* `janus_outbox_compactions_total`: confirmed journal replacements.
* `janus_outbox_repaired_tails_total`: incomplete tails repaired at startup.

Operation/batch deltas show actual batching. Sync counts may exceed batch counts
because of checkpoints or repairs. The outbox write histogram includes queueing,
collection, flush and any checkpoint waits. Counters reset on process restart.

Tests prove 32 queued saves share one flush and callers cannot return before
it, duplicate IDs write once, partial appends recover after a real process kill,
checksum corruption is retained, failed writes retry from a confirmed offset,
compaction preserves live events and delivery acknowledgements, directory-sync
failure recovers, and old files migrate. The existing Redis/PostgreSQL hard-kill,
deduplication, shutdown ownership and capacity tests remain in place.

Verification passed on Linux with real test Redis/PostgreSQL: the full suite,
the full suite under Go's race detector, and `go vet`. Windows cross-compilation
also passed; durable outbox operation still requires Linux. The local Linux
deployment was updated and its health endpoint and journal file verified without
making a paid provider request.

This remains a completion journal. A crash during generation or before local
confirmation can still leave missing usage. Disk loss, replication, request-start
journaling and provider invoice reconciliation remain separate production work.

## Measurements

The October 8 local fake-provider run used matching 10-second phases, 32 gateway
admission seats and offered rates of 200/1000 RPS. All response and PostgreSQL
request/attempt/token checks passed, including exactly one save and one journal
acknowledgement per successful gateway request.

| Mode | Offered RPS | Accepted / offered | Rejected | Completion p50 / p99 | First-text p99 |
|---|---:|---:|---:|---:|---:|
| JSON | 200 | 2000 / 2000 | 0 | 20.78 / 49.43 ms | n/a |
| SSE | 200 | 2000 / 2000 | 0 | 21.21 / 33.30 ms | 8.83 ms |
| JSON | 1000 | 9276 / 10000 | 724 | 27.27 / 55.84 ms | n/a |
| SSE | 1000 | 8999 / 10000 | 1001 | 29.44 / 52.93 ms | 17.81 ms |

At 1000 offered RPS, JSON committed 18552 operations in 2723 append batches;
SSE committed 17998 in 2580. That is about 6.8/7.0 operations per batch. Each
phase also completed one compaction. All 22275 admitted measured requests
reached PostgreSQL, with no transport, worker, outbox or handoff errors/retries.
Local records and retained handoffs drained. Redis had 1187/1577 entries at
client EOF in the two high-rate phases and drained afterward: the worker still
falls behind during these bursts, so this does not establish sustained capacity.

Compared with the earlier per-file run, high-rate SSE accepted 8999 instead of
5829 requests, completion p99 fell from 151.11 to 52.93 ms and first-text p99
fell from 87.62 to 17.81 ms. JSON acceptance rose from 8757 to 9276. At 200 RPS,
typical completion latency increased by 4.19 ms for JSON and 2.20 ms for SSE;
batch collection trades some idle/low-concurrency latency for shared flushes.

Successful-response percentiles include upstream time and scheduling lag and
exclude deliberate overload rejections. The runs share a laptop/WSL host and
occurred on different days; they are local observations, not isolated causal
measurements or a production guarantee. The 5-10 ms overhead goal remains open.
Raw reports are in `.cache/loadtest/`; the previous per-file results are in
`.cache/loadtest-before-group-commit/`.

Binary image builds now keep compiler caches and temporary files in RAM rather
than embedding them in image layers. This trades build-time memory for disk
space; it does not change the gateway's runtime memory configuration.
