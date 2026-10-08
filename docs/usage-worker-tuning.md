# Usage worker batching and recovery scans

The usage worker is a goroutine inside Janus. It moves completed request events
from the separate usage Redis server into PostgreSQL. It runs independently of
HTTP handlers, but it shares Redis connections and host resources with them.

```text
HTTP completion -> local durable journal -> usage Redis Stream
                                               |
                                      background worker
                                      read up to 64 events
                                               |
                                     PostgreSQL transaction
                                               |
                                        successful commit
                                               |
                                      Redis XACK + XDEL
```

## What changed

Previously the worker reclaimed abandoned deliveries, read up to 16 messages,
saved one transaction, and acknowledged the entries on every cycle. Even when
there were no abandoned deliveries, each cycle paid for the recovery command.

The worker now reads/reclaims up to 64 messages per batch. It processes whatever
is available immediately; it never waits to fill 64. Once a pending-entry scan
finishes, it schedules another scan about one second later and reads new work
directly in between. Nonzero scan cursors continue on subsequent cycles rather
than waiting a second between recovery pages. Reads and processing can delay
scans, so the interval is not a hard recovery deadline. The normal 30-second
minimum idle time before reclaiming a delivery is unchanged.

One worker and the existing database pool remain sufficient for this experiment.
No database durability settings or Redis persistence settings were relaxed.
Valid events are saved in one transaction before acknowledgement. Failure keeps
entries pending; PostgreSQL request IDs make replay idempotent. Malformed entries
stay pending for inspection while valid neighbors proceed. Larger batches can
increase memory, transaction time and work replayed after a failed acknowledgement;
the batch bound and existing operation timeouts remain in place.

For intuition, if a cycle takes 20 ms, 16 events per cycle allows roughly
`16 / 0.020 = 800 events/second`. Increasing the bound helps when backlog fills
the batch, but a larger transaction may also take longer. We measure the result
rather than assuming a fourfold throughput improvement.

## Measuring each stage

The metrics endpoint exports histogram sums, counts and buckets for:

| Metric prefix | What it measures |
|---|---|
| `janus_usage_claim_duration_seconds` | Reclaiming idle pending entries |
| `janus_usage_read_duration_seconds` | Reading new entries, including idle blocking |
| `janus_usage_store_duration_seconds` | Validation/preparation in the store, connection acquisition, transaction and commit |
| `janus_usage_ack_duration_seconds` | Redis acknowledgement and deletion |

Timings include failed calls. `janus_usage_worker_events_total` counts valid
events submitted to the store, including retries. Divide its delta by the store
histogram count delta to get the mean attempted batch size. Dividing a stage's
sum delta by its count delta gives mean seconds per call. These are batch-level
timings, not per-event timings or HTTP request overhead. Read time includes idle
waiting, so a high read mean with an empty queue need not indicate a bottleneck.

## Local comparison

Both runs use fake providers, 30-second phases, 1000 offered RPS and 32 HTTP
admission seats. Deliberate overload responses are counted separately; successful
latency includes upstream time and client scheduling. Fixtures share the laptop's
WSL resources. This is a local comparison, not a production capacity guarantee.

In the instrumented 16-event baseline, the SSE worker's mean batch was 15.97.
Mean claim/read/store/ack times were 4.01/4.58/6.59/4.26 ms. This showed both
fixed Redis work and saturated batches contributing to the backlog.

| Mode | Worker | Accepted / offered | Completion p50 / p99 | Queue at client EOF |
|---|---|---:|---:|---:|
| JSON | 16, reclaim each cycle | 29288 / 30000 | 22.31 / 42.71 ms | 97 |
| JSON | 64, periodic reclaim | 23616 / 30000 | 33.24 / 87.94 ms | 3 |
| SSE | 16, reclaim each cycle | 26481 / 30000 | 29.71 / 64.56 ms | 5279 |
| SSE | 64, periodic reclaim | 27526 / 30000 | 27.06 / 63.03 ms | 20 |

The tuned run passed all response/accounting checks. All 51142 admitted measured
requests reached PostgreSQL with matching request, attempt and token totals.
There were no worker, handoff, outbox or invalid-event errors, and queues drained
after traffic. In the SSE phase, the sampled queue maximum was 26 versus 5241
in the baseline, and first-text p99 was 16.32 versus 19.77 ms.

Recovery calls fell from 2720/1658 (JSON/SSE) to 30/29. The tuned SSE worker made
2034 store calls with a mean batch of 13.53, with mean read/store/ack times of
4.83/5.72/4.04 ms. A larger upper bound does not imply every batch is larger:
keeping up with arrivals often leaves less queued work available to collect.

HTTP results are mixed. JSON acceptance and latency worsened, and its direct
control p99 also rose from 12.56 to 167.43 ms. Direct SSE p99 was similar in the
successful repeat (13.64 versus 13.41 ms). The first tuned attempt failed because
its direct SSE control had 314 inflight-limit drops and p99 of 284.21 ms; that
run is preserved, not treated as a successful performance comparison. The cause
of these control spikes is unestablished. Queue behavior improved in these local
runs, but a general HTTP latency improvement and sustained production capacity
have not been established. The 5-10 ms overhead target remains unfinished.

Raw baseline reports: `.cache/loadtest-worker-baseline/`; successful tuned repeat:
`.cache/loadtest-worker-tuned/`; failed first attempt: `.cache/loadtest-worker-attempt1/`.
The earlier 10-second journal reports are preserved in `.cache/loadtest-before-worker/`.

## Correctness checks

Tests cover recovery over more than two full pages plus new deliveries, revisiting
young pending entries, transaction rollback, database failure, malformed neighbors,
replay after a committed transaction, and shutdown. The load harness checks every
admitted request's PostgreSQL request/attempt/token totals, journal save/ack counts,
queue draining, and absence of worker/outbox errors.
The full Redis/PostgreSQL suite passed normally and under Go's race detector;
`go vet` passed. Load traffic used no paid provider APIs.

The local Linux stack was updated and Windows health/authenticated metrics
verified while WSL was active. On this host, the distro restarted between short
WSL invocations and Windows connections were refused while it was down. Keep
Ubuntu/WSL running for local use; host lifetime settings were not changed here.
