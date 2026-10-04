# Provider circuit breaker

The breaker protects future calls when our configured provider endpoint and
credential keep failing. It does not throttle response events or retry calls.

```text
Closed: allow calls
  | five consecutive qualifying failures
  v
Open: reject with 503 + Retry-After
  | 30 seconds pass, next request arrives
  v
Half-open: allow one trial, reject other requests
  | success -> Closed, failure -> Open for another 30 seconds
  | cancellation/local failure -> Open for another cooldown
```

Example: four 503 responses followed by a valid response reset the failure count
to zero. Five subsequent failures open the circuit. A sixth request is rejected
inside Janus without contacting Groq. After 30 seconds, one request tests recovery.
That trial includes the whole response, including streaming completion.

## Files and concurrency

- `breaker.go`: state, failure streak, cooldown, mutex, and generation tickets.
- `provider.go`: admission before HTTP and one deferred outcome report per call.
- `stream.go`: observes completion and distinguishes upstream reads from client
  write failures. Forwarded SSE bytes stay unchanged.
- `breaker_test.go`: state transitions, stale outcomes, one concurrent probe,
  provider classification, recovery, and quota release.

The mutex prevents two simultaneous requests from claiming the probe. It is
released before HTTP starts. A generation ticket identifies the state that
admitted a request. Once the breaker transitions, older call outcomes are
ignored: a late success cannot close a circuit opened by newer failures.
Closed-state outcomes are counted in completion order.

## Outcomes

HTTP 429/5xx, network errors, upstream timeout, redirects, malformed/oversized
JSON, invalid SSE content type, and streams lacking recognized completion are
provider failures. A normal JSON response or complete SSE is success. Valid
non-429 4xx means the provider is reachable and resets the failure streak.
Cancellation or downstream write failure is neutral, not a provider failure.
A neutral probe cannot establish recovery, so it releases its slot and starts
another cooldown. Token usage can be missing on a healthy complete response:
the breaker can recover while TPM remains conservatively reserved.

## Quotas and limitations

```text
authenticate -> validate -> RPM + TPM admission -> breaker -> provider
```

Blocked by primary breaker: reuse the original TPM allocation for configured
fallback. Without fallback, release TPM to zero. RPM remains spent. An uncertain
upstream failure keeps its own TPM allocation reserved; an additional fallback
allocation is required. No provider switch occurs after streaming begins.

One breaker lives in each Provider object in Janus memory. Restarting Janus
resets it, and multiple gateway processes do not share breaker state. Existing
calls can finish after opening. Defaults are fixed at five failures and 30
seconds. Optional fallback routing is implemented. Provider-specific backoff,
load balancing, and metrics remain future work. The retry hint during a running probe is one second, not
a promised recovery time.
