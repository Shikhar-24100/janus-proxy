# Janus Proxy

A Go LLM gateway built one working step at a time.

Currently supports a local health endpoint and streaming or non-streaming chat requests to
an OpenAI-compatible primary and optional fallback, client authentication, and Redis RPM/TPM quotas
with provider usage reconciliation.

Provider calls also use an in-memory circuit breaker to stop repeatedly calling
an upstream that is failing.

## Run

Requires Go 1.24 or newer and Redis. Our local Go installation already meets this.
For this Windows setup, Ubuntu WSL already has Redis installed. First open a
terminal and keep it running:

```powershell
.\start-redis.ps1
```

That script runs a dedicated, ephemeral Redis on loopback port 6380. It leaves
existing Redis instances alone and keeps WSL active for localhost forwarding.
Stopping or restarting this development Redis loses its quota state. For another
Redis deployment, configure `REDIS_URL` instead of using this script.

If Windows reports low memory or an insufficient paging file while compiling,
use `$env:GOMAXPROCS = '1'` before running the local startup script. For checks,
use `go test -p 1 ./...` and `go vet -p 1 ./...` to reduce build concurrency.
This is a local development workaround, not a production performance setting.

For local Groq setup, copy `.env.example` to `.env`, set your provider key and
your separate `JANUS_API_KEY` there, and run in a second terminal:

```powershell
.\run.ps1
```

The script loads supported settings from `.env` (overriding values in that terminal)
and uses the project-local Go toolchain if available, otherwise Go from PATH.
The `.env` format is plain `NAME=value`, without quotes or inline comments.
The real `.env` is ignored by Git; `.env.example` contains placeholders only.

You can also configure the Go program directly:

```powershell
$env:OPENAI_API_KEY = 'your-provider-key'
$env:JANUS_API_KEY = 'your-own-janus-client-key'
go run .
```

If using the project-local Go toolchain instead:

```powershell
$env:GOCACHE = Join-Path (Get-Location) '.cache\go-build'
.\.tools\go\bin\go.exe run .
```

The server listens on `127.0.0.1:8080`. Stop it with Ctrl+C.
Environment variables must be set in the same terminal before starting the server.
The Go program itself does not load `.env`; `run.ps1` handles that for local runs.

`OPENAI_BASE_URL` defaults to `https://api.openai.com/v1`. You can set it to
another trusted OpenAI-compatible provider's API base URL, including its version
path. Janus appends `/chat/completions`. Remote endpoints require HTTPS;
loopback HTTP endpoints are allowed for development.

Without `JANUS_API_KEY`, startup fails so chat access cannot accidentally be public.
Without a primary key, `/health` still works and chat can use a configured fallback;
without either provider key, valid authenticated chat requests return 503.
Keys belong in local environment variables, never in source or request JSON.

## Client authentication

Clients send `Authorization: Bearer <JANUS_API_KEY>` to Janus. The authentication
middleware checks that key before decoding the chat body or calling a provider.
Missing, malformed, duplicate, or incorrect credentials return a JSON 401 error
with `WWW-Authenticate: Bearer`. `/health` stays public.

The Janus client key and provider key are separate credentials. Janus constructs
a new upstream request with `OPENAI_API_KEY`, rather than forwarding the client's
Authorization header. Key comparison uses fixed-size SHA-256 hashes and a
constant-time comparison. Keys are not logged.

This first version has one shared client key. Tenant identities, individual keys,
rotation, and per-tenant quotas are future work. Authorization headers need HTTPS
when exposing a gateway beyond local development; this server still binds only
to loopback. Restart Janus after changing its configured key.

## VS Code and a local Go toolchain

If the Go extension cannot find `go`, set `go.alternateTools.go` in your local
`.vscode/settings.json` to the absolute path of `.tools/go/bin/go.exe`.
That settings file is ignored by Git because paths are machine-specific.
Reload VS Code after changing the configuration. New integrated terminals can
use `go` if `.tools/go/bin` is added to their PATH; existing terminals retain
their previous environment.

An unsaved editor tab can show an older version of a file changed on disk.
Copy any notes or edits you want to keep, then use `File: Revert File` on that
tab to load the saved version. Saving the stale tab instead would overwrite
the newer file on disk.

## Redis request rate limiting

`REDIS_URL` selects Redis (default `redis://127.0.0.1:6379/0`; our local `.env`
uses port 6380). `RPM_LIMIT` defaults to 60 and must be a positive integer up to
1000000. Janus checks Redis at startup and refuses to start if it cannot connect.

After authentication and body validation, a token bucket controls RPM admission. The bucket
holds up to `RPM_LIMIT` request slots and refills that many per minute. At 60 RPM,
an idle bucket permits a burst of 60 calls and then replenishes one slot per
second. This is an average refill rate plus burst capacity, not a strict cap in
every rolling 60-second interval. These slots are request permits, not LLM tokens.

A Lua script uses Redis's clock and atomically reads, refills, checks, and updates
the bucket. All gateway instances using the same Redis database and configured
client key share the same bucket. The Redis key contains a SHA-256 fingerprint,
not the raw client key. Idle buckets expire after two minutes.

Exhausted quota returns JSON 429 with `Retry-After` in whole seconds. Successful
checks include `X-RateLimit-Limit` and `X-RateLimit-Remaining`. Redis failures
return 503 before calling the provider. Admission checks have a 750 ms budget;
mutating commands are not automatically retried because they might have executed.

Unauthenticated requests, invalid bodies, and `/health` do not consume quota.
Valid admitted chat attempts consume one RPM slot, including upstream failures.
Streaming consumes one request slot at admission, not one per event. A single
Lua script admits both RPM and TPM together; rejection consumes neither.

Redis provides shared state across Janus restarts. Restarting this ephemeral
development Redis resets that state. Production persistence, replication, and
outage policies need further work. Redis limiter reference:
[Redis rate limiter documentation](https://redis.io/docs/latest/develop/use-cases/rate-limiter/).

## Token quota and reconciliation

`TPM_LIMIT` defaults to 60000 tokens in a rolling 60-second admission window.
`MAX_OUTPUT_TOKENS` defaults to 1024 and must be smaller than `TPM_LIMIT`.
Both settings accept positive integers up to 100000000. Clients can request
`max_completion_tokens` between 1 and `MAX_OUTPUT_TOKENS`; omission uses that
configured maximum. Janus sends the allowance to the provider, which can end
generation at that cap. Quota reconciliation does not cut streams.

Before calling the provider, Janus reserves estimated input plus the output
allowance. The first estimator uses UTF-8 byte lengths and message overhead.
It is a heuristic, not a model tokenizer: it often overestimates text and can
underestimate provider framing. Accurate tokenizers remain future work.

Redis tracks reservations by request ID and admission time. After successful
responses with valid prompt/completion/total usage, Janus replaces the reserved
charge with actual usage. Smaller usage releases capacity; larger usage adds
debt that can block future admissions. Settlement is idempotent. Charges age
out 60 seconds after admission; late settlement never credits a newer window.

Streams request `stream_options.include_usage`. A bounded SSE observer recognizes
top-level `usage` and Groq's `x_groq.usage` without changing forwarded bytes.
Settlement requires clean EOF, `[DONE]`, and valid usage. Missing usage or an
uncertain upstream failure retains the reservation until it ages out. No
upstream attempt releases TPM to zero, but the RPM permit remains spent.

Settlement runs at handler completion with a bounded Redis call, including
after client cancellation. It is not a durable worker pipeline. A crash or
failed settlement leaves the conservative reservation.

`X-TokenLimit-Limit`, `X-TokenLimit-Remaining`, and `X-TokenLimit-Reserved`
show admission values before refunds. On rejection, Reserved is the requested
allowance, not an accepted charge. Exhausted TPM returns 429 with `Retry-After`;
a single request exceeding total capacity returns 400 so it can be reduced.

See [token accounting maths](docs/token-accounting.md) and the
[Groq API reference](https://console.groq.com/docs/api-reference).

## Send a request

`request.json` contains a small Groq chat request. When changing providers, set
its `model` to a model ID available to your account.
In a second PowerShell terminal:

```powershell
Invoke-RestMethod -Uri http://localhost:8080/health

# Load only the Janus client key in this terminal, without displaying it.
$janusKey = (Get-Content .env | Where-Object { $_ -like 'JANUS_API_KEY=*' }).Split('=', 2)[1]

$response = Invoke-RestMethod `
  -Uri http://localhost:8080/v1/chat/completions `
  -Method Post `
  -Headers @{ Authorization = "Bearer $janusKey" } `
  -ContentType 'application/json' `
  -InFile request.json

$response.choices[0].message.content
```

PowerShell summarizes nested objects. To print the answer, store the result in
`$response` and read `$response.choices[0].message.content`. To inspect all fields,
use `$response | ConvertTo-Json -Depth 20`.

## Streaming

Start Janus with `.\run.ps1`. In a second terminal, use the native curl executable:

```powershell
$janusKey = (Get-Content .env | Where-Object { $_ -like 'JANUS_API_KEY=*' }).Split('=', 2)[1]

curl.exe --no-buffer --silent --show-error `
  http://localhost:8080/v1/chat/completions `
  -H "Authorization: Bearer $janusKey" `
  -H "Content-Type: application/json" `
  --data-binary "@request-stream.json"
```

`request-stream.json` sets `stream: true`. A normal request returns one complete
JSON answer. A streaming request returns Server-Sent Events (SSE) over the same
HTTP response as the provider generates them. Each event is separated by a blank
line, typically with `data: {...}` containing JSON. Text fragments appear under
`choices[0].delta.content`; some events contain roles, reasoning, finish reasons,
or usage instead. `data: [DONE]` is the provider's completion marker.

Janus checks for a successful `text/event-stream` response, then reads into a
32 KiB buffer, writes the available bytes, and flushes. Flushing sends buffered
bytes toward the client immediately. A network read is not necessarily one
token or one event: events and UTF-8 characters can span reads. Janus preserves
all bytes and framing; clients assemble and parse the events. `--no-buffer`
also tells curl to display incoming data without waiting for its output buffer.

Streaming reduces the wait for the first visible text, not necessarily the time
to generate the entire answer. It uses bounded memory instead of collecting the
whole response. Slow client writes naturally pause upstream reads (backpressure).

Provider JSON errors before streaming begins retain their status and body.
After SSE headers have been sent, Janus cannot replace the HTTP status or switch
to a JSON error. A read/write failure aborts the response without appending a
fake completion marker. Clients must treat a stream without `[DONE]` as
incomplete. Client disconnection cancels the upstream request. The current
60-second timeout applies to the whole provider call, including streaming;
separate idle and total deadlines are a future refinement. Janus observes usage
and completion markers alongside forwarding to settle token reservations.

Tests use a gated fake provider to prove bytes arrive before generation finishes,
preserve a split UTF-8 character and SSE framing, check cancellation, and check
interrupted streams and errors. Provider protocol:
[Groq streaming documentation](https://console.groq.com/docs/text-chat).

## Current request flow

```text
Client -> router -> authentication -> JSON decoding and validation
       -> Redis atomic RPM check + TPM reservation -> primary circuit breaker
       -> primary provider call -> qualifying failure before streaming?
       -> optional fallback TPM allocation + breaker + provider call
       <- provider's JSON response and HTTP status <-
       -> reconcile TPM from actual provider usage in Redis
```

Supported input fields: `model`, `messages`, `stream` (defaults to false),
`max_completion_tokens`, and `stream_options` with `include_usage`.
Messages support `system`, `user`, and `assistant` roles with non-empty string
content. Other fields, tool calls, and multimodal content are not
implemented yet. Provider response bodies are forwarded without changing their
JSON, so completion data and usage are preserved.

Request bodies are limited to 1 MiB; buffered JSON provider responses to 4 MiB.
SSE responses use a fixed-size buffer without the 4 MiB total-body limit.
Provider calls have a 60-second timeout and use the incoming request's context
for cancellation. JSON provider errors keep their status (including 429) and
`Retry-After`; network failures return 502 and timeouts return 504. Redirects
are rejected. Non-streaming responses are buffered before sending to the client.

## Provider circuit breaker

Each configured Provider owns a breaker scoped to its endpoint and credential.
Five consecutive qualifying failures open it for 30 seconds. An open circuit
returns a Janus JSON 503 with `Retry-After`, without calling the provider.
When cooldown expires, the next incoming request becomes the single half-open
probe; other calls receive 503 with `Retry-After: 1` while it is running. No
background health request is generated. Recovery is not guaranteed by that
retry hint.

Network errors, upstream timeouts, HTTP 429/5xx, invalid or oversized JSON,
unexpected redirects, and invalid or incomplete SSE count as failures. Valid
non-429 4xx responses demonstrate reachability and reset the failure streak;
their original status/body are preserved. Client cancellations and stream writes
that fail toward the client do not count as provider outages. Concurrent results
count in completion order within the current breaker generation.

A streaming probe stays in flight until the stream ends. Clean EOF with a
recognized `[DONE]` means breaker success even if token usage is missing;
TPM settlement separately requires valid usage. Upstream read errors still
abort an active stream; the breaker only affects future requests. A canceled
probe returns to open for another cooldown so the trial slot cannot get stuck.
Results from calls admitted before a state transition cannot alter the new state.

The breaker is guarded by a Go mutex and lives in each Janus process, not Redis.
Restarting Janus resets it; separate instances have separate breakers. The mutex
is held only for short state changes, never while waiting on HTTP. Calls already
in flight can continue after the circuit opens. The router can select the optional
fallback while a primary circuit is open; it does not retry the same provider.

Quota admission runs first. With no fallback, a breaker-blocked call releases
TPM because no upstream attempt happened; RPM remains spent. With fallback,
the unused original TPM allocation is reused for that provider attempt.
See [breaker examples and design](docs/circuit-breaker.md).

## Groq to OpenAI fallback

The primary settings remain `OPENAI_BASE_URL` and `OPENAI_API_KEY`; in our local
setup these point to Groq. A non-empty `FALLBACK_API_KEY` enables one secondary
OpenAI-compatible endpoint. `FALLBACK_BASE_URL` defaults to
`https://api.openai.com/v1`; `FALLBACK_MODEL` defaults to `gpt-4o-mini`.
Leave the fallback key empty to disable it. `run.ps1` loads all three settings.

The primary receives the client's model unchanged. The fallback receives the
same messages, stream settings, and output allowance with its configured model
substituted. Responses retain the selected provider's actual model and JSON/SSE
bytes. `X-Janus-Route: primary` or `fallback` identifies the selected route.
This intentionally allows a different model to answer; quality can differ.

Fallback can follow an open circuit, missing primary key, network failure,
upstream timeout, 429/5xx, invalid buffered response, redirect, or invalid SSE
content type before streaming begins. Other provider 4xx errors are returned
unchanged. Client cancellation stops routing. There are at most two attempts,
one per provider, with separate breakers and credentials. No provider error is
sent to the client before deciding whether to fall back. Once primary SSE
headers have been sent, an interrupted stream is aborted with no provider switch.
If both providers fail, return the fallback's error/status and Retry-After.

One client request spends one RPM permit. If primary was attempted, reconcile
its usage separately, retaining its full reservation when usage is unknown.
Then reserve another TPM allocation before calling fallback, without charging
RPM again. Insufficient TPM returns a Janus 429; Redis failure returns 503.
If primary was skipped, reuse the unspent original allocation. The original
X-TokenLimit headers describe initial admission, not total fallback spend.

Each provider attempt has its own 60-second timeout, so two slow attempts can
take roughly 120 seconds. A shorter end-to-end routing deadline, capability
mapping, load balancing, and durable cost accounting remain future work.

For budget-conscious learning, the selected `gpt-4o-mini` model lists $0.15 per
million input tokens and $0.60 per million output tokens. A 1000-input,
500-output call is approximately $0.00045 at those uncached standard rates.
Verify current prices in the
[official model documentation](https://developers.openai.com/api/docs/models/gpt-4o-mini).
Janus does not enforce dollar budgets; TPM is a token allocation limit.

The ordinary test suite uses fake providers. `TestLiveOpenAIFallback` requires
explicit `JANUS_LIVE_FALLBACK=1`, `FALLBACK_API_KEY`, and `REDIS_TEST_URL`.
It simulates a primary failure and sends up to two billed OpenAI calls, each
with a 32-token output cap. It never loads `.env` itself or prints credentials.
See [fallback design and accounting examples](docs/fallback-routing.md).

This is a local development gateway. Tenant management, accurate tokenization,
caching, wider provider routing, and durable usage analytics are future steps.

## Check

```powershell
go test ./...
go vet ./...
```

To also exercise the real Redis script and concurrent admission across two
gateway clients, start Redis, then run:

```powershell
$env:REDIS_TEST_URL = 'redis://127.0.0.1:6380/0'
go test -timeout 30s ./...
```

Replace `go` with `.\.tools\go\bin\go.exe` if using the local toolchain.
Tests use local fake providers and make no paid API calls.
