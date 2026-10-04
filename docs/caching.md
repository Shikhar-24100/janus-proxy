# Exact response caching

Redis remains a separate server. Janus reuses the existing Redis connection;
quota keys and response keys occupy separate namespaces in the same database.

```text
Client -> authenticate -> validate request -> spend one RPM permit
                                         -> opted-in non-streaming cache lookup
                                            | HIT -> return stored JSON (zero TPM)
                                            | MISS / cache error
                                            v
                                         reserve TPM -> breaker -> provider
                                            -> return response -> cache eligible answer
                                            -> reconcile actual usage -> logs / metrics
```

Without opt-in, or for streaming, the existing combined RPM/TPM admission runs.
For opted-in requests, RPM admission runs before lookup and TPM is reserved only
on a miss. A TPM-rejected miss therefore still spends one RPM permit. Hits still
require authentication, validation, and available RPM, even when TPM is exhausted.

## What is stored

Send `X-Janus-Cache: true` with a non-streaming request to opt in. Requests without
this header generate fresh answers. The current text request schema is unchanged;
temperature, tools, and semantic similarity are not supported by this cache.

Keys use SHA-256 of the normalized supported request, including model, messages,
roles, output allowance, and stream settings. JSON spacing and field order do
not affect matching; changing a message or model does. A separate fingerprint
isolates the authenticated tenant ID and provider credentials/routing configuration.
Raw credentials and prompts do not appear in keys. Response text is stored in
Redis, so Redis still needs appropriate access controls.

Only successful HTTP 200 primary responses with complete, nonempty assistant
text and `finish_reason: stop` are stored. Errors, refusals, truncated answers,
tool calls, streaming responses, and fallback answers are excluded. Entries
are limited to 256 KiB. The response body, including its original ID, timestamp,
and usage, is reused unchanged. That usage describes the original generation;
the cache hit creates no new provider usage or TPM charge.

`CACHE_TTL_SECONDS` defaults to 300 (five minutes). Valid values are 0 through
86400; zero disables caching. TTL means time to live: Redis deletes an entry
after that time. Hits do not extend its lifetime. Entries survive a Janus restart
with the same configuration while Redis is running. Restarting our ephemeral
development Redis loses both cache entries and quota state.

## Failure handling and limits

Cache reads and writes each have a 100 ms context budget. Read errors or invalid
entries continue to fresh generation if quota admission works. Writes happen
after the response body is successfully written and cannot change that answer.
The write remains a bounded handler operation rather than a durable async job.
A complete Redis outage still prevents quota admission and returns 503.

TTL and entry size do not bound total Redis memory. Before production, decide
memory limits and eviction policy or separate cache storage from quota storage;
evicting quota keys could reset limits. Concurrent misses can generate duplicate
answers; request coalescing remains future work. Exact caching intentionally
reuses an earlier answer, so clients should opt in only when that is acceptable.

## Try it

Keep Redis and Janus running. In another PowerShell terminal:

```powershell
$janusKey = (Get-Content .env | Where-Object { $_ -like 'JANUS_API_KEY=*' }).Split('=', 2)[1]
$headers = @{ Authorization = "Bearer $janusKey"; 'X-Janus-Cache' = 'true' }
$first = Invoke-WebRequest -UseBasicParsing -Uri http://localhost:8080/v1/chat/completions -Method Post -Headers $headers -ContentType 'application/json' -InFile request.json
$second = Invoke-WebRequest -UseBasicParsing -Uri http://localhost:8080/v1/chat/completions -Method Post -Headers $headers -ContentType 'application/json' -InFile request.json
$first.Headers['X-Janus-Cache']
$second.Headers['X-Janus-Cache']
$second.Headers['X-TokenLimit-Reserved']
($second.Content | ConvertFrom-Json).choices[0].message.content
```

For an eligible primary answer, expect MISS, then HIT, then 0 reserved tokens.
The second response has `X-Janus-Route: cache`. BYPASS means ineligible or disabled;
ERROR means lookup failed and normal generation was attempted.

Metrics expose `janus_cache_requests_total` by hit/miss/bypass/error and
`janus_cache_errors_total` by read/write. Logs include `cache` and
`cache_write_error`. Hits have no provider attempts and do not increase reported
token counters. A hit rate can be calculated as hits / (hits + misses + read
errors), excluding bypasses.
