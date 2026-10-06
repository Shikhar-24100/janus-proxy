# Latency percentiles and the local benchmark

p50 and p99 describe a distribution of request times. Sort the measurements
from fastest to slowest. With 1000 samples, the nearest-rank p50 is sample 500
and p99 is sample 990. At least 50% and 99% of samples finish at or below those
respective times. p99 is not the worst request, and not a percentage increase.

For example, p50=5 ms and p99=80 ms means the middle of the distribution is quick
while the slow end is much worse. An average can conceal that slow end. Ties can
put more than the stated percentage at or below a percentile.

## Run it

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

Results are written to ignored `.cache/perf/latest-core.*` or `latest-full.*`:

- Markdown: readable summary table.
- JSON: summary, UTC measurement timestamp, Go version, OS and GOMAXPROCS.
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
second. HTTP connection reuse uses the usual two-idle-connections-per-host
default, including the gateway's upstream client.

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
