# Reproducible MVP demo

Run from the repository root with Docker Compose available:

```powershell
.\demo.ps1
```

Or on Linux/macOS:

```sh
docker compose -f compose.demo.yaml run --rm --build demo
docker compose -f compose.demo.yaml down --volumes
```

The first build downloads Go modules and service images. Later runs reuse those
build layers. This is a console walkthrough of real assertions, not a web UI or
a paid model demonstration. HTTP providers run on local test servers. Redis and
PostgreSQL run as separate disposable services on an internal Docker network.
The demo reads no `.env` files and publishes no host ports. The Windows runner
stops only the `janus-demo` stack afterward; normal Janus volumes are separate.

## What to show

Read the named scenarios in the output in this order (the Go runner may execute
them in source order). Each scenario must print `PASS`; the run ends with `ok`.
The shell script requires Redis/PostgreSQL URLs so these scenarios cannot silently
skip because those settings are absent.

| Feature | Executable scenario | What its assertions demonstrate |
|---|---|---|
| Authentication | `TestAuthenticationRunsBeforeRateLimit` | Rejected credentials do not consume quota or reach the provider |
| Streaming | `TestStreamArrivesBeforeProviderFinishes` | Text reaches the HTTP client while the provider is still generating |
| Rate limiting | `TestRedisHTTPAdmission` | Redis-backed HTTP admission returns quota headers and rejects excess requests |
| Exact cache | `TestRedisCacheHitAndExpiry` | Repeated opt-in JSON requests avoid a provider call; expiry allows a new call |
| Fallback | `TestFallbackRouting` | Eligible primary failures route to a secondary; client errors do not |
| Observability | `TestTelemetryFallbackUsageAndPrivacy` | Attempts/usage appear in metrics while keys and prompt content stay out of logs |
| Tenants | `TestRedisTenantIsolationRotationAndLogs` | Tenant quotas/cache remain isolated and key rotation preserves identity |
| Durable history | `TestPostgresUsageDeduplicationAndDailyReport` | Replayed deliveries do not double stored usage or reports |
| Crash recovery | `TestOutboxHardCrashRecovery` | A killed process's confirmed journal event replays into Redis/PostgreSQL |

For a short presentation, show the architecture in the root README, run this
demo, and then explain the crash boundary: completion records are protected
after durable local confirmation, but a crash during generation can still leave
an accounting gap. Finish with the [measured results and limitations](release.md).

## Optional live provider walkthrough

After following the normal container setup, use `examples/request.json` and
`examples/request-stream.json` against port 8081. Set the model for your configured
provider and authenticate with a tenant key. Metrics use the administrator's
`JANUS_API_KEY`. This separate walkthrough uses your provider configuration and
is not part of the credential-free demo or CI.

To view streaming from PowerShell without printing the key:

```powershell
$env:JANUS_CLIENT_KEY = 'your-local-tenant-key'
curl.exe -N http://localhost:8081/v1/chat/completions `
  -H "Authorization: Bearer $env:JANUS_CLIENT_KEY" `
  -H 'Content-Type: application/json' `
  --data-binary '@examples/request-stream.json'
```

See [streaming/observability](observability.md), [cache behavior](caching.md),
[tenant setup](tenants.md), and [container operations](containers.md) for detailed
commands. Do not commit real keys or local tenant files.
