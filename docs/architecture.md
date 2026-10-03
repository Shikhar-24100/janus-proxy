# Janus architecture and progress

The gateway runs on Windows; development Redis runs in Ubuntu WSL.

```text
Client (PowerShell, curl, or app)
  | POST /v1/chat/completions + Janus API key
  v
Janus router
  | /health returns 200 without authentication or Redis admission
  v
Authentication middleware
  | wrong or missing key -> 401
  v
Request rate-limit middleware <----> Redis token bucket
  | exhausted -> 429 + Retry-After
  | Redis unavailable -> 503
  v
JSON decoding and validation
  | invalid input -> 400 / oversized body -> 413
  v
Provider HTTP client (uses separate Groq key)
  v
Groq / one OpenAI-compatible provider
  |
  +-> complete JSON -> bounded read -> client response
  +-> SSE -> read / write / flush -> client stream
```

Redis stores request quota state shared across gateway instances, scoped to the
configured Janus key's fingerprint. The current token bucket holds 60 request
permits by default and refills at 60 per minute. Authentication runs first, so
unauthorized requests never reach Redis or Groq. Accepted streaming calls consume
one request permit each. These permits are not model tokens.

## Rough progress estimate

About **30% of the first production-focused version**, after adding Redis RPM
limits. This is a planning estimate based on remaining effort, not a measured
percentage or a claim of production readiness. The optional semantic cache is
outside this first version.

| Area | Current status |
| --- | --- |
| HTTP API and request validation | Basic text-only subset works |
| Provider calls | One compatible upstream; live Groq verified |
| SSE streaming | Forwarding, flushing, cancellation, failure tests |
| Client authentication | One shared key; no tenant registry yet |
| Redis RPM limiting | Atomic shared token bucket; concurrent integration test |
| TPM and accounting | Not built: estimates, reservations, actual usage settlement |
| Exact caching | Not built: tenant-safe keys, TTL, successful-response storage |
| Resilient routing | Not built: capability checks, circuit breakers, safe fallback |
| Tenant management | Not built: individual keys, budgets, revocation |
| Durable usage pipeline | Not built: event delivery and analytics storage |
| Operations and performance | Not built: metrics, load tests, latency measurements, deployment hardening |

Next: explain the Redis bucket, then build token estimation and TPM reservation
as a separate step. Streaming usage reconciliation must account for cancellations
and missing provider usage; do not treat SSE chunks as tokens.
