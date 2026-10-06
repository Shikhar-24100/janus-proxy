# Janus architecture and progress

Janus runs on Windows. Redis is a separate server in Ubuntu WSL on port 6380;
it is not embedded inside Go. Groq is the remote LLM provider.

```text
Client (PowerShell, curl, browser app)
  | POST /v1/chat/completions + Janus API key
  v
Router -> telemetry wrapper (request ID, timer) -> tenant authentication
  | missing/wrong key -> 401; public /health bypasses quotas
  v
Attach trusted tenant to context; validate JSON and tenant output allowance
  | concurrency gate before validation: full/pending usage handoff -> 503
  | admitted seat stays held until completion and usage confirmation
  | invalid -> 400; oversized body -> 413
  v
Estimate input + output allowance
  | individual reservation larger than TPM capacity -> 400
  v
Quota admission <----> Redis
  | RPM: refillable request bucket
  | TPM: rolling 60-second ledger of token charges
  | exhausted -> 429 + Retry-After; unavailable -> 503
  | opted-in non-streaming: spend RPM, then exact cache lookup
  | cache hit -> stored JSON to client; zero TPM and no provider call
  | cache miss/error -> reserve TPM; denial still spends RPM
  | ordinary/streaming requests -> combined atomic RPM + TPM admission
  v
Primary circuit breaker (inside Janus memory)
  | open / another probe running -> try configured fallback
  v
Primary HTTP client (Groq key) -> Groq
  | qualifying failure before streaming starts
  v
Fallback router -> reserve more TPM if primary was attempted
  | skipped primary -> reuse original allocation
  | insufficient token quota -> 429; Redis unavailable -> 503
  v
Fallback breaker + HTTP client (separate key) -> configured fallback model
  |
  +-> complete JSON -> bounded read -> client response
  +-> SSE -> read / observe usage / write / flush -> client stream
  |
  v
Eligible opted-in primary JSON -> store response in Redis with TTL
Handler completion -> settle actual usage in Redis
  | unknown usage -> keep reservation until original window expires
  v
Update metrics -> enqueue JSON request log -> background console writer
               -> Linux: fsync completion event into persistent local outbox
               -> bounded persistent Redis usage enqueue (separate 6381 server)
                  -> confirmed: remove local outbox record; restart replays unfinished records
                  -> unconfirmed: retain event/seat, pause admissions, retry
                  -> background worker -> PostgreSQL request + attempt transaction
                  -> commit -> acknowledge/delete queue entry

GET /metrics + Janus key -> in-memory counters/histograms + circuit snapshots
```

RPM and TPM govern admission, not response event speed or event count. Admitted
streams are forwarded subject to client cancellation, provider failure, timeout,
and the output allowance sent to the provider.

Redis quota state is scoped to a stable tenant ID's SHA-256 fingerprint. One Lua
script checks both quotas atomically across gateway instances for ordinary calls.
Opted-in cache calls split RPM and TPM admission so hits need no TPM; a miss
rejected by TPM still spends RPM. RPM holds 60 permits by default and refills at one per
second. TPM defaults to 60000 tokens allocated in a rolling admission window.

Each request has a unique ID. Actual usage replaces reserved usage once.
Duplicate settlement has no effect. Reservations and settled charges age out
60 seconds after admission. Late responses cannot refund a newer window.
Long-running calls still belong to their admission window. Idle Redis records
expire. The development Redis loses state when restarted.

The bounded SSE observer handles events split across network reads. Events,
chunks, and visible words are not tokens. Provider usage includes reported
reasoning tokens in completion totals. The first input estimator is a byte
heuristic; accurate model tokenizers remain future work.

## Rough progress estimate

About **65% of the first production-focused version** after adding durable usage storage, tenant management, exact caching,
logs and metrics. This is an effort estimate, not a measured percentage or
production-readiness claim. Optional semantic caching is outside this scope.

| Area | Status |
| --- | --- |
| API and validation | Basic text-only subset |
| Provider calls | Primary + second Groq key fallback; live JSON/SSE verified |
| SSE streaming | Forwarding, flushing, cancellation, failure tests |
| Authentication | Configured tenant keys; separate metrics administrator |
| RPM | Atomic Redis bucket |
| TPM | Rolling reservations and actual usage settlement |
| Tokenization | Byte heuristic; model tokenizer still needed |
| Exact caching | Opt-in non-streaming primary answers; Redis TTL and metrics |
| Provider resilience | Separate breakers and one fallback; load balancing still needed |
| Tenant management | Startup registry, isolated quotas/cache, rotation and disabling; dollar budgets and live administration remain |
| Durable usage pipeline | Persistent completion outbox on Linux, Redis Stream, retrying PostgreSQL worker, deduplication and daily reports; crashes before local confirmation remain uncovered |
| Operations | Logs/metrics, stage timings, Windows/Linux benchmarks, Linux Compose, separate-container open-loop loads, concurrency admission, usage retries and completion recovery; deployment hardening, request-start journaling and production soak tests remain |

Tests cover concurrency, refunds, duplicate/late settlement, underestimated
usage, missing usage, fragmented SSE, and output bounds. Settlement is currently
a bounded call at handler completion, not a durable worker. Crash recovery,
persistence for quota Redis, replication, and lossless billing remain future work.

See [token accounting maths](token-accounting.md) for worked examples.
See [circuit breaker design](circuit-breaker.md) for provider failure handling.
See [fallback routing](fallback-routing.md) for multi-attempt accounting.
See [observability](observability.md) for TTFT, logs, metrics, and limitations.
See [caching](caching.md) for eligibility, quota behavior, and test commands.
See [tenants](tenants.md) for identity, configuration, isolation, and rotation.
See [usage storage](usage-storage.md) for PostgreSQL, async workers and reliability limits.
See [performance](performance.md) for p50/p99, benchmark modes and interpretation.
See [Linux containers](containers.md) for the Compose topology, storage and startup.
See [separate-container load testing](load-testing.md) for fixed-rate traffic,
gateway CPU/memory, usage verification and observed overload limits.
See [overload protection](overload-protection.md) for admission seats and usage retries.
See [durable outbox](durable-outbox.md) for disk persistence and restart recovery.
