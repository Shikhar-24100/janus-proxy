# Latency percentiles and the local benchmark

p50 and p99 describe a distribution of request times. Sort the measurements
from fastest to slowest. With 1000 samples, the nearest-rank p50 is sample 500
and p99 is sample 990. At least 50% and 99% of samples finish at or below those
respective times. p99 is not the worst request, and not a percentage increase.

For example, p50=5 ms and p99=80 ms means the middle of the distribution is quick
while the slow end is much worse. An average can conceal that slow end. Ties can
put more than the stated percentage at or below a percentile.

## Run it

The older same-process `core`/`full` harness leaves the local outbox disabled,
including its Linux variant. To measure the current Linux stack with durable
outbox writes, use `loadtest.ps1` and the [separate-container fixture](load-testing.md).
Current disk-write measurements are in [the outbox notes](durable-outbox.md).

The benchmark starts its own HTTP fake provider and Janus servers on random
loopback ports. It never calls Groq/OpenAI or reads provider API keys, and does
not use the normally running Janus on 8080.

```powershell
# Full mode: start Redis and PostgreSQL first, in their separate terminals.
.\start-redis.ps1
.\start-usage.ps1

# Benchmark terminal: real quotas/cache + persistent usage queue and PostgreSQL.
.\benchmark.ps1 -Mode full -Requests 500 -Concurrency 1,16

# Core mode: works without WSL/Redis/PostgreSQL; deliberately excludes their cost.
.\benchmark.ps1 -Mode core -Requests 500 -Concurrency 1,16
```

Use `-Requests 1000` or more for more tail samples. Repeated runs help distinguish
persistent slowness from a one-off scheduling pause. Script defaults are full
mode, 500 measured requests per scenario, concurrency 1 and 16. Counts must be
100-5000; concurrency 1-128. Failed service startup is not silently replaced
with a core benchmark.

`-GoCPUs` defaults to 2 and explicitly sets GOMAXPROCS only for the run, restoring
the previous value afterward. Compare `-GoCPUs 1`, `2`, and `4` independently of
build concurrency (`-p 1`). More CPUs do not guarantee lower latency.

Results are written to ignored `.cache/perf/latest-core.*` or `latest-full.*`:

- Markdown: readable summary table.
- JSON: summary, UTC measurement timestamp, Go version, OS and GOMAXPROCS.
- Stage operation means: admission, settlement and durable usage enqueue,
  including connection acquisition and failures, excluding warm-ups.
- CSV: every sample's duration, first-text time and error.

Do not publish raw conclusions without the mode and concurrency. Core results
cannot demonstrate the full gateway's 5-10 ms overhead target.

## What the scenarios mean

| Scenario | Includes |
| --- | --- |
| direct-json | Client to fake provider, buffered JSON |
| direct-sse | Client to fake provider, streaming |
| janus-core-json / sse | Janus tenant auth, validation, routing, telemetry and forwarding; quota checks replaced by a no-op |
| janus-redis-json / sse | Real Redis admission and settlement; durable usage disabled |
| janus-full-json / sse | Real Redis plus persistent usage enqueue and PostgreSQL worker |
| janus-cache-hit | Prepopulated exact answer, real RPM/cache, durable usage; no provider generation |

The fake JSON provider waits 10 ms before returning a fixed small answer. SSE
first sends a role event, waits 3 ms before visible text, then waits 7 ms before
usage and DONE. Operating system scheduling changes the actual wait durations.
This isolates a repeatable workload; it does not model a real LLM's latency,
large responses, long streams or internet conditions.

TTFT is measured from starting the client request until visible SSE text is
parsed. Headers, role-only events and usage events do not count. End-to-end
latency continues until the response body reaches EOF, including usage handoff
when enabled. Every JSON body is checked, and SSE must include text and DONE.

Warm-up requests are excluded (20 or the concurrency level, whichever is larger).
Successful measurements determine percentiles; all failures are counted and
make the run fail. Throughput is completed successful requests per wall-clock
second. The load generator retains two idle connections per host across all
comparisons. Janus now retains up to 64 idle connections per provider host and
128 overall per provider transport, reducing redialing after concurrent bursts.
These are idle limits, not limits on active requests.

## Interpreting results correctly

Compare direct and Janus results for the same response mode and concurrency.
For example, direct p50=10 ms and Janus p50=14 ms gives a 4 ms difference between
their medians under that workload. It is not a precise decomposition of where
each individual request spent those 4 ms.

In particular, subtracting two p99 values does not produce the p99 of per-request
gateway overhead. These are separate distributions. Timer noise, connection
setup, GC, scheduling and background work can affect their tails differently.
Cache hits should be evaluated separately because they skip provider generation.

This is a closed-loop test: each client starts its next request after the previous
one finishes. It tests fixed concurrency, not a fixed arrival rate, and can
underrepresent queuing under overload (coordinated omission). It is a useful local
baseline, not a production load-test or latency guarantee. A few hundred requests
give only a handful of samples in the slowest 1%; p99 is noisy at that size.

Full mode uses unique Redis quota/cache namespaces, its own usage stream, and
temporary tenant rows in the configured database. It never consumes normal
usage events. It waits up to 30 seconds for each usage scenario's queue to drain,
reports backlog and worker/enqueue failures, and cleans only its benchmark state.
Counters include warm-ups; worker/enqueue error counters are cumulative for the
run. Using production database/Redis infrastructure is not recommended for this
local harness: its load still competes for shared resources.

## Memory and CPU investigation

JSON includes allocated bytes per request for the whole Go harness. Client, fake
provider, gateway and worker run in the same process, so this is not isolated
Janus memory usage or peak RSS. It excludes PostgreSQL and Redis server memory.

Optional profiles can show which Go functions use CPU or allocate memory:

```powershell
.\benchmark.ps1 -Mode core -Requests 500 -Concurrency 1,16 -Profile
.\.tools\go\bin\go.exe tool pprof -top .cache/perf/core.test.exe .cache/perf/core-cpu.pprof
.\.tools\go\bin\go.exe tool pprof -top -alloc_space .cache/perf/core.test.exe .cache/perf/core-heap.pprof
```

Profiling adds overhead, so use unprofiled runs for latency comparisons.
Profiled runs write `latest-core-profiled.*` (or full), preserving the unprofiled
report and explicitly marking profiler overhead. Avoid compiling tests or running
other load generators during a latency measurement.
A future separate-process load generator can measure gateway CPU/RSS independently.

## October 6 optimization pass

Stage measurements at concurrency 16 and GOMAXPROCS=2 showed durable enqueue
averaging 21.42 ms for JSON, compared with 1.90 ms admission and 1.87 ms
settlement. Changes target that waiting and repeated connection/allocation work:

- The durable Redis pool is bounded at 16 connections, previously four.
- The worker saves up to 16 available events in one PostgreSQL transaction using
  pgx batching, then acknowledges/deletes them in one Redis script. It never
  acknowledges before commit or waits to fill a batch. PostgreSQL still has at
  most two connections and there is still one worker.
- Provider transports retain 64 idle connections per host, previously two.
- Streaming copy buffers use a sync.Pool; each buffer is cleared before return,
  including aborted streams. Go can discard unused pooled buffers during GC.
- Fixed stage histograms expose admission, settlement and enqueue in `/metrics`.
  The benchmark reports operation means; they do not add up to an end-to-end
  percentile. Provider attempts remain timed separately in logs.

Quota admission remains atomic and reconciliation idempotent. Usage Redis still
uses appendfsync=always. The reliable handoff still runs on handler completion;
its cost has not been hidden by moving events into an in-memory queue.

Pre-change two-CPU runs had full JSON p50 34.35-38.42 ms and p99 63.15-80.91 ms
at concurrency 16. Two post-change runs before buffer reuse had JSON p50
22.74-25.56 ms and p99 40.40-54.33 ms. Enqueue mean fell to 6.66-7.71 ms.
Full SSE TTFT p50 changed from 12.53-14.10 ms to 7.50-8.63 ms.
The worker still accumulated backlog: JSON had 103-149 entries immediately after
load, down from 475-480; all drained. Cache bursts retained larger backlogs.

Results are not uniformly better: quota-only JSON p50 was 17.32 ms in the
instrumented baseline and 19.45-20.16 ms afterward, and cache-hit p99 was
34.41 ms before versus 52.70-61.84 ms afterward in these two-CPU runs. Cache
tail latency and quota/WSL variability need further investigation. Fixed
concurrency also changes achieved arrival rate when the gateway speeds up.

A four-CPU post-change run had full JSON p50/p99 25.46/45.69 ms and SSE TTFT
p50/p99 6.05/12.35 ms. More CPUs did not consistently improve every scenario;
the benchmark defaults to two rather than assuming the largest value wins.
All these runs used 500 measured requests per scenario and had zero request,
enqueue and worker errors. Before/after changes were measured as a group;
these results do not isolate each individual change's contribution.

The final unprofiled run, including buffer reuse, measured 7000 requests at
GOMAXPROCS=2, concurrency 1 and 16, with zero request/enqueue/worker failures
and all queues drained. Concurrent full JSON p50 improved about 24% against
the instrumented two-CPU baseline; the slow tail remains variable.

| Final measurement | Concurrency 1 | Concurrency 16 |
| --- | ---: | ---: |
| Direct JSON p50 / p99 ms | 11.29 / 12.28 | 11.66 / 22.40 |
| Full Janus JSON p50 / p99 ms | 18.13 / 95.45 | 29.37 / 50.60 |
| Direct SSE TTFT p50 / p99 ms | 4.30 / 5.56 | 4.12 / 15.19 |
| Full Janus SSE TTFT p50 / p99 ms | 5.74 / 8.75 | 11.24 / 35.73 |
| Durable enqueue mean ms, JSON | 4.75 | 9.53 |
| Full JSON backlog after load | 1 | 228 |

The final single-client JSON p99 spiked to 95.45 ms versus 24.96 ms in the first
optimized run. Its cause was not established. Do not present only the favorable
runs: local Windows/WSL timing and durable disk-write tails still need study.
The median JSON difference from direct is approximately 6.85 ms at concurrency
1 and 17.71 ms at concurrency 16. This does not establish the full 5-10 ms goal.

Sampled whole-harness allocation profiles for the same 500-request, concurrency
16 scenarios fell from about 138 MB to 105 MB after buffer reuse. Stream copying
was the largest allocation source before reuse and disappeared from the top
allocation sources afterward. These are sampled allocated bytes across the
whole run, not peak RAM or isolated gateway memory; profiled timings are excluded
from the comparison above.

Tests verify failed batches remain pending, poison entries survive while valid
neighbors commit, duplicate deliveries preserve request/attempt totals, and a
SQL error rolls back the entire transaction. Existing streaming/cancellation,
tenant, fallback and quota integration tests pass, as does go vet.

Next measurements should run the gateway and load generator separately, place
Janus and Redis in the same Linux environment, inspect Redis pool waits and
disk latency, and use longer controlled arrival-rate loads. Cache p99 and worker
drain throughput remain important targets; lowering durability to win the
benchmark is not part of this pass.

## Linux container baseline

For open-loop tests of the actual runtime container with a separate provider and
load generator, see [separate-container load testing](load-testing.md).

The container stack adds `.\containers.ps1 -Action benchmark`, which runs this
same harness inside Linux with real dependency containers. Reports are written
to `.cache/perf-linux/latest-full.*`, preserving the Windows artifacts.
The first full run completed 7000 measured requests with no request, enqueue or
worker errors and empty queues after draining. At GOMAXPROCS=2, full JSON p50
was 13.04 ms versus direct 10.62 ms at concurrency 1, and 13.76 ms versus direct
10.92 ms at concurrency 16. Differences between medians are 2.42/2.84 ms for this
workload. It still uses a same-process load generator/mock gateway, not the
independently running container on port 8081. See [container notes](containers.md)
for setup, exact measurements and remaining separate-process testing.

## WSL startup history

On the October 6, 2026 session, Ubuntu WSL could not start with
`Wsl/Service/E_UNEXPECTED`. No distributions were running; a subsystem reset did
not resolve it, and this session could not restart the Windows WSL service.
Ubuntu started successfully on a later retry during the October 6 startup
repair session. PostgreSQL and both Redis instances were restored, and Janus
health, metrics and saved usage history were verified. The earlier Windows
failure's root cause was not established. See [startup checks](startup.md).
Core-mode artifacts remain labeled separately from full measurements;
do not interpret the core baseline as having met the full latency target.

The restored full-mode run completed 7000 measured requests (500 per scenario,
concurrency 1 and 16), with no request, enqueue or worker failures and empty
queues after draining. Windows Go used GOMAXPROCS=1, the local fake provider,
WSL Redis and PostgreSQL, and appendfsync=always for usage Redis.

| JSON path | Concurrency | p50 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Direct | 1 | 11.34 | 13.08 |
| Janus with Redis, usage disabled | 1 | 15.85 | 19.67 |
| Janus with durable usage | 1 | 22.62 | 30.59 |
| Direct | 16 | 14.49 | 24.32 |
| Janus with Redis, usage disabled | 16 | 49.79 | 88.89 |
| Janus with durable usage | 16 | 56.17 | 173.93 |

These results show substantial additional latency in this local setup, especially
under concurrency. They do not establish the desired 5-10 ms overhead target.
Investigate Redis/WSL round trips, connection contention, gateway scheduling and
durable enqueue cost before choosing optimizations; this run alone does not
identify the exact bottleneck. Streaming TTFT, cache-hit results and raw samples
are in ignored `.cache/perf/latest-full.{md,json,csv}`.
