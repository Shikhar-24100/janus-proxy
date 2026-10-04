# Janus architecture and progress

Janus runs on Windows. Redis is a separate server in Ubuntu WSL on port 6380;
it is not embedded inside Go. Groq is the remote LLM provider.

```text
Client (PowerShell, curl, browser app)
  | POST /v1/chat/completions + Janus API key
  v
Router -> authentication
  | missing/wrong key -> 401; public /health bypasses quotas
  v
Decode and validate JSON; choose output allowance
  | invalid -> 400; oversized body -> 413
  v
Estimate input + output allowance
  | individual reservation larger than TPM capacity -> 400
  v
Atomic quota admission <----> Redis
  | RPM: refillable request bucket
  | TPM: rolling 60-second ledger of token charges
  | exhausted -> 429 + Retry-After; unavailable -> 503
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
Handler completion -> settle actual usage in Redis
  | unknown usage -> keep reservation until original window expires
```

RPM and TPM govern admission, not response event speed or event count. Admitted
streams are forwarded subject to client cancellation, provider failure, timeout,
and the output allowance sent to the provider.

Redis state is scoped to the shared Janus key's SHA-256 fingerprint. One Lua
script checks both quotas atomically across gateway instances. Rejection spends
neither quota. RPM holds 60 request permits by default and refills at one per
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

About **45% of the first production-focused version** after adding two-provider
fallback. This is an effort estimate, not a measured percentage or
production-readiness claim. Optional semantic caching is outside this scope.

| Area | Status |
| --- | --- |
| API and validation | Basic text-only subset |
| Provider calls | Primary + second Groq key fallback; live JSON/SSE verified |
| SSE streaming | Forwarding, flushing, cancellation, failure tests |
| Authentication | One shared key; no tenant registry |
| RPM | Atomic Redis bucket |
| TPM | Rolling reservations and actual usage settlement |
| Tokenization | Byte heuristic; model tokenizer still needed |
| Exact caching | Not built |
| Provider resilience | Separate breakers and one fallback; load balancing still needed |
| Tenant management | Individual keys, budgets, revocation still needed |
| Durable usage pipeline | Event delivery and analytics storage still needed |
| Operations | Metrics, load tests, measured latency, hardening still needed |

Tests cover concurrency, refunds, duplicate/late settlement, underestimated
usage, missing usage, fragmented SSE, and output bounds. Settlement is currently
a bounded call at handler completion, not a durable worker. Crash recovery,
persistence, replication, and durable billing remain future work.

See [token accounting maths](token-accounting.md) for worked examples.
See [circuit breaker design](circuit-breaker.md) for provider failure handling.
See [fallback routing](fallback-routing.md) for multi-attempt accounting.
