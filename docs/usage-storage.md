# Durable usage history and background workers

Janus now records tenant usage in PostgreSQL, using a separate persistent Redis
Stream to buffer completed request events. Console logs and in-memory metrics
remain useful for operations; PostgreSQL supplies a queryable history.

## Why these components

| Component | Job in this version | Reason |
| --- | --- | --- |
| Go request handler | Validate, admit, route, stream and observe usage | Already handles the client request |
| Redis on 6380 | Tenant quotas and exact response cache | Existing ephemeral development instance |
| Redis on 6381 | Persistent usage queue | Consumer groups retain pending work for recovery |
| Go worker goroutine | Read usage events, save them, acknowledge them | Database writes run independently of client handling |
| PostgreSQL 16 on 5432 | Usage requests and provider attempts | Transactions, unique constraints and SQL reporting |
| pgx v5 | PostgreSQL connection pool and driver | Native Go PostgreSQL integration |

PostgreSQL is our choice for this stage: the data has clear relationships and
we want correctness and simple tenant/day queries. ClickHouse is an alternative
for a much larger analytics workload, not a requirement for this learning step.
Redis is the waiting room; PostgreSQL is the long-term usage record. Neither
server is embedded inside Janus.

## What async means

A goroutine is independently scheduled Go code in the same process. Starting a
worker with `go` lets request handlers and database processing make progress
independently; it does not create another OS service or guarantee disk durability.

```text
Client -> tenant authentication -> quota/cache -> provider JSON or SSE
       <- response body / streamed events
                         |
                 handler completion
                         |
             finish quota reconciliation
                         |
               construct usage event
                         |
              durable local journal save
                         |
              bounded Redis enqueue (200 ms budget)
              then local journal acknowledgement
                         v
              Redis Stream: janus:usage:v1
                         |
        background goroutine: XREADGROUP / reclaim pending entries
                         |
              PostgreSQL transaction
              + request row
              + provider attempt rows
                         |
                  successful COMMIT
                         |
                  Redis XACK + XDEL
```

Database writes do not run in the request handler. Queue handoff does: the local
journal confirms a save before Redis delivery, then confirms its acknowledgement.
The 200 ms budget bounds the Redis attempt, including after client cancellation;
it does not bound local disk queueing or flushes. Writing the
response body does not guarantee net/http has already flushed or closed it, so
this handoff can still add completion latency. Streaming text is forwarded as
before; queue handoff happens after streaming and quota settlement.
An unconfirmed fast-path handoff now transfers the event and its admission seat
to a bounded retry goroutine. New admissions pause until it confirms delivery.
The HTTP response can finish while that retained job retries.

Our existing console-log channel is separate and best effort. A Go channel holds
data in process memory and loses it on a crash; it is not the durable usage queue.
The worker uses at most two PostgreSQL connections, one worker per Janus process,
and batches of at most 64 queued messages. Available messages are sent together
through pgx in one PostgreSQL transaction; the worker does not wait to fill a batch.
After commit, one Redis script acknowledges and removes all valid entries in that
batch. Failed batches stay pending and replay safely; malformed neighbors stay
pending while valid entries can proceed. Usage Redis has a bounded pool of 16
connections shared by publishers and the worker, replacing the earlier four.

See [worker tuning and stage metrics](usage-worker-tuning.md) for the measurements
behind the batch size and recovery scan schedule.

## Delivery, retry and duplicates

Reading through the consumer group assigns an entry to a worker and places it
in the pending list. A database failure leaves it unacknowledged. The worker
retries with pauses of 1, 2, 4, 8 and then at most 10 seconds.

After a worker crash, another worker can reclaim entries idle for at least
30 seconds. The worker completes recovery scan pages, then schedules the next
scan about one second later; intervening cycles read new work directly. Blocking
reads, processing and retry delays can postpone the next scan. A database commit
can succeed just before a crash prevents acknowledgement. That event will be
delivered again: this is at-least-once delivery, not exactly-once transport.

`request_id` is a PostgreSQL primary key, and the request and its attempts are
inserted in one transaction. `ON CONFLICT DO NOTHING` makes repeat deliveries
produce one stored request. Request IDs have a random process prefix and a
counter. The worker acknowledges and deletes the stream entry only after the
transaction succeeds. This stream is owned by one consumer group; don't attach
another group expecting retained history after acknowledgement.

Malformed entries stay pending for inspection and increment an error counter;
they are not silently dropped. Valid later entries can still be processed.
The stream accepts at most 100000 outstanding entries and local Redis has a
64 MiB memory limit with `noeviction`. If full, new handoffs fail visibly rather
than trimming unprocessed events. Monitor and repair these failures.

## What we store and how to read it

`janus_usage_requests` stores request ID, tenant ID, completion timestamp, selected
route, status/outcome, streaming flag, cache result, duration and optional TTFT.

`janus_usage_attempts` stores each primary/fallback attempt's route, outcome,
upstream status, duration and nullable prompt/completion/total token counts.
The key is `(request_id, attempt_index)`.

Example:

```text
Alice request A: primary success -> prompt 83 + completion 52 = 135 tokens
Alice request B: cache hit       -> no provider attempts, zero new tokens
Daily report: 2 requests, 1 cache hit, 135 known tokens
```

A fallback request is still one client request with up to two provider attempts.
Both attempts contribute reported tokens. An attempted provider call without
trustworthy usage has NULL token values, not zero. A skipped circuit has no
provider usage. Reports show known token totals plus unknown-attempt counts;
known totals are incomplete when that count is nonzero.

No prompts, answers, credentials or model strings enter these usage tables.
Dollar costs and model pricing are not implemented in this step. This history
is a foundation for accounting, not an authoritative provider invoice.

## Local setup and running

PostgreSQL is installed in Ubuntu WSL. The `janus_usage` database and dedicated
login role are configured; the generated database password is in ignored `.env`.
Local `DATABASE_URL` uses loopback and `sslmode=disable`; use appropriate TLS
and credentials for a remote deployment. Janus initializes the version-one
schema at startup. Later schema changes need versioned migration handling.

On a fresh Ubuntu WSL setup, install and create the database once:

```powershell
wsl.exe -d Ubuntu -u root -- bash -lc 'apt-get update && apt-get install -y postgresql postgresql-client'
wsl.exe -d Ubuntu -u root -- pg_ctlcluster 16 main start
wsl.exe -d Ubuntu -u postgres -- createuser --pwprompt janus_usage
wsl.exe -d Ubuntu -u postgres -- createdb --owner=janus_usage janus_usage
```

Then put `DATABASE_URL=postgres://janus_usage:YOUR_URL_ENCODED_PASSWORD@127.0.0.1:5432/janus_usage?sslmode=disable`
and `USAGE_REDIS_URL=redis://127.0.0.1:6381/0` in ignored `.env`.
Our current machine is already configured; do not recreate its role/database.

In separate terminals:

```powershell
.\start-redis.ps1  # Existing ephemeral quota/cache Redis on 6380
.\start-usage.ps1  # Start PostgreSQL and persistent usage Redis on 6381; keep open
.\run.ps1         # Start Janus and its worker
```

The `.env` settings `DATABASE_URL` and `USAGE_REDIS_URL` must both be set to enable
storage, or both empty to disable it. Initial database/queue setup must succeed
before Janus starts. A database outage after startup leaves events in the queue
while requests continue, subject to available queue capacity and handoff success.

The usage Redis writes an append-only file under the WSL user's
`~/.local/share/janus-usage-redis`, using `appendfsync always`. This persists
commands before acknowledging them, with an associated disk latency cost.
PostgreSQL data lives in `/var/lib/postgresql/16/main`. Do not delete these data
directories when restarting services. Our original quota/cache Redis remains
ephemeral; its restart doesn't erase the separate usage queue or database.

After sending chat requests, run:

```powershell
.\usage-report.ps1
```

The report script targets the local WSL database using peer authentication and
shows seven days of daily totals in UTC. It preaggregates provider attempts so
joining them does not multiply client request counts. It can lag briefly because
the worker saves events asynchronously: this is eventual consistency.

## Observe and verify

Authenticated `/metrics` exposes:

- `janus_usage_enqueued_total`: confirmed queue handoffs.
- `janus_usage_enqueue_errors_total`: terminal invalid/unconfirmed events at shutdown.
- `janus_usage_pending_handoffs`: retained usage jobs awaiting confirmation.
- `janus_usage_enqueue_retries_total`: failures triggering another attempt.
- `janus_usage_persisted_total`: saved/acknowledged deliveries, including duplicate retries.
- `janus_usage_worker_errors_total`: read, save or acknowledgement failures.
- `janus_usage_invalid_events_total`: invalid-entry encounters, including repeats.

These counters reset on Janus restart; the database history does not. Inspect
queue backlog/pending work locally with:

```powershell
wsl.exe -d Ubuntu -- redis-cli -p 6381 XLEN janus:usage:v1
wsl.exe -d Ubuntu -- redis-cli -p 6381 XPENDING janus:usage:v1 postgres-v1
```

For integration tests, explicitly set `REDIS_TEST_URL` and `POSTGRES_TEST_URL`
(the latter can be loaded from local `DATABASE_URL`) and run `go test -p 1
-timeout 45s ./...`. Tests create unique queue keys and tenant/request IDs and
clean only their own records. Fake providers make no billed API calls. Coverage
includes retries, reclaiming crashed consumers, full queues, invalid events,
concurrent duplicate inserts, commit-before-ack recovery and daily-report maths.

## Remaining reliability limits

With the Linux outbox enabled, an event is recoverable after its journal save
is confirmed, before persistent Redis accepts it. Without the outbox,
recovery begins at persistent Redis acceptance. A crash during generation or
before local confirmation can still leave a missing completion event. An enqueue timeout can be ambiguous:
Redis might have accepted it. Janus now retains the event and its admission seat
in a bounded retry channel, pauses new admissions and retries confirmation.
Recent handoff markers reduce duplicate Redis deliveries; PostgreSQL request IDs
prevent duplicate accounting. Pending journal records replay on restart; other
retained jobs are not crash durable. See [outbox recovery](durable-outbox.md)
and [overload and retry design](overload-protection.md).

Gateway shutdown stops admission, allows up to five seconds for active handlers,
then cancels remaining work. The worker stops without deleting unfinished events;
the next run reclaims them. A hard process kill can interrupt the handoff.

This improves persistence and retries but is not lossless billing. Backups,
replication, disk failure handling, backlog alerts, dead-letter administration,
retention, durable request-start records and provider invoice reconciliation
remain future work. Local AOF and outbox durability trade latency for safety;
see [the measured outbox cost](durable-outbox.md). The proxy-overhead target
remains unfinished.

References: [PostgreSQL INSERT and ON CONFLICT](https://www.postgresql.org/docs/16/sql-insert.html),
[Redis consumer groups](https://redis.io/docs/latest/develop/use-cases/streaming/),
[Redis persistence policies](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/).
