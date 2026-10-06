# Request logs and metrics

Logs answer: what happened to this request? Metrics answer: what is happening
across requests? Both live inside Janus; Redis still holds admission quotas.

```text
Chat request -> telemetry wrapper creates request ID and starts timer
             -> authentication -> validation -> Redis -> routing -> provider
             -> SSE observer marks first forwarded text and final usage
             -> quota settlement
             -> update in-memory metrics
             -> enqueue JSON request log -> background console writer

GET /metrics + Janus key -> read metrics and breaker snapshots
```

## What we measure

| Measurement | Meaning |
| --- | --- |
| Request counter | Completed chat handlers, including rejected requests; fixed route/status-class/outcome labels |
| Provider attempt counter | Each primary/fallback attempt, including skipped open circuits |
| Reported token counters | Valid prompt, completion, and total usage for each route |
| Unknown usage counter | Contacted providers without trustworthy final usage |
| Fallback selections | Requests selecting fallback, even if its circuit blocks the call |
| In-flight gauge | Unfinished chat observations; use admission active for held seats |
| Admission metrics | Configured/held seats and early overload rejections |
| Usage retry metrics | Pending handoffs, retry triggers and terminal failures |
| Duration histogram | Time from chat handler entry until quota settlement finishes |
| TTFT histogram | Time until Janus flushes the first recognized non-empty text delta |
| Circuit state gauge | Closed=0, open=1, half-open=2 for each configured route |
| Logging counters | Events dropped due to a full queue, or failed log writes |
| Cache counters | Hit/miss/bypass/error results and separate read/write errors |
| Stage histograms | Quota admission, quota settlement and durable usage enqueue, including connection acquisition and failures |

For total token usage, select `kind="total"`. That already includes prompt and
completion tokens; summing all three kinds would double-count usage.

TTFT includes authentication, Redis, and failed primary attempts before fallback.
The initial header flush, role-only events, usage events, and reasoning-only
events do not count as visible text. We observe a complete SSE data event, so
fragmented events can delay recognition. This measures a gateway flush, not the
exact moment the user's screen renders text. Non-streaming calls and streams
with no recognized text have no TTFT sample.

Total duration includes upstream generation, slow client writes, and bounded
Redis reconciliation. It is not isolated proxy overhead; a benchmark against
direct provider calls is still needed to assess the 5-10 ms overhead goal.
Provider attempt durations are present in request logs.
Duration is captured before optional durable usage enqueue, so its bounded
handoff latency is not included in that measurement.

The three stage families are `janus_quota_admission_duration_seconds`,
`janus_quota_settlement_duration_seconds`, and
`janus_usage_enqueue_duration_seconds`. Each exposes `_bucket`, `_sum`, and
`_count`. For example, enqueue `_sum / _count * 1000` gives its mean operation
duration in milliseconds. Admission includes extra cache-miss/fallback token
reservations; counts are operations, not necessarily one per request. Enqueue
counts only tenant-attributed requests when usage storage is enabled.
Overload rejections do not enqueue usage. This histogram measures the initial
handoff/retention step; background retry time is visible through pending jobs,
not added to the same histogram. See [admission and retry design](overload-protection.md).
Settlement does not run when provider usage is unknown. Cache, HTTP and provider generation
contribute additional time outside these three stages. Sub-millisecond buckets
make fast local operations visible.

## Counters, gauges, and histograms

A counter accumulates events: three successful chats increment the request
counter three times. A gauge describes current state: two running chats make
in-flight equal two, which falls back to zero after they finish.

Histograms put durations into cumulative buckets. For durations 0.1, 0.2, and
0.4 seconds, a 0.25-second bucket contains two samples; a 0.5-second bucket
contains all three. Sum is 0.7 seconds and count is three:

```text
average = sum / count = 0.7 / 3 = about 0.233 seconds
```

Buckets can support approximate latency percentiles when scraped into Prometheus.
Our endpoint exposes Prometheus text format 0.0.4 with cumulative buckets,
sum, count, and an infinity bucket. It does not install or run Prometheus or
Grafana. Format reference:
[official exposition specification](https://prometheus.io/docs/instrumenting/exposition_formats/).

## Logs and failure interpretation

Every chat gets a server-generated `X-Request-ID`. The matching JSON console
event contains status, outcome, selected route, stream flag, duration, optional
TTFT, and at most two provider attempt records with upstream status, outcome,
duration, and known token usage. Unknown usage is null, not zero.

Logs also include `cache` (HIT/MISS/BYPASS/ERROR) and `cache_write_error`.
Authenticated requests include the trusted `tenant_id`; rejected credentials
have no tenant attribution. Tenant IDs are kept out of metric labels.
A hit selects route `cache` and has no provider attempts. Although its JSON body
contains the original generation's usage, it adds no fresh provider token usage.
Duration includes cache operations; non-streaming hits have no TTFT sample.

An HTTP 200 stream can still fail after headers were sent. The log and request
metric therefore distinguish `success`, `error`, `interrupted`, and `canceled`.
A canceled call without response headers records status zero. Telemetry records
stream-abort panics and rethrows them so net/http still aborts the connection.
Both provider attempts are retained even when fallback succeeds. This explains
why request success can coexist with a primary provider failure.

The queue holds 256 completed request events. Request handlers never wait for
console writes. If it fills, drop the event and increment `janus_log_dropped_total`;
metrics still update. A writer failure increments `janus_log_write_errors_total`.
This is best-effort operational logging, not durable billing or a message broker.
Optional usage storage has a separate persistent Redis queue and PostgreSQL worker;
it does not rely on this console-log channel. See [usage storage](usage-storage.md).

## Access and limits

`GET /metrics` requires the administrative `JANUS_API_KEY`. Other tenant keys
cannot read aggregate metrics. It does not consume RPM/TPM,
and neither health checks nor metric scrapes generate chat logs or latency samples.
Unauthorized chat requests do count as failed chat requests.

Logs exclude prompts, answers, client headers, provider keys, request bodies,
URLs, and model strings. Metrics use fixed internal labels: no request IDs,
keys, prompts, or arbitrary models become labels. Console logs and metrics use
the standard library; optional usage persistence adds pgx for PostgreSQL.
Registry locks protect short updates and snapshots; provider breaker locks are
held only while reading their state. Metrics reset when Janus restarts and are
separate for each process. Optional PostgreSQL history persists across restarts.
Dollar pricing, dashboards and a tracing backend remain future work.

Files: `telemetry.go` owns logs, metrics, and the HTTP wrapper; `provider.go`
records attempts; `stream.go` records flush/completion; `tokens.go` validates
usage and recognizes text events; `main.go` wires the endpoints together.
