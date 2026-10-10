# Tenant management

A tenant is an application or customer using Janus. Authentication resolves its
client API key into a trusted tenant identity. The request context carries that
identity through validation, quotas, caching, provider routing, and logs.

```text
Client Bearer key -> SHA-256 lookup in startup registry
                 -> unknown / disabled / malformed: 401
                 -> attach authenticated tenant to Go request context
                 -> tenant output limit and request validation
                 -> tenant RPM -> tenant cache -> tenant TPM on a miss
                 -> shared provider routing / breakers
                 -> settle tenant usage -> request log with tenant_id
```

The primary and fallback provider credentials remain gateway configuration.
Tenant keys are never forwarded upstream. Clients cannot select a tenant through
a request body or header other than their authenticated key.

## Configure

Without `TENANTS_CONFIG`, Janus creates tenant `default` using `JANUS_API_KEY`,
`RPM_LIMIT`, `TPM_LIMIT`, and `MAX_OUTPUT_TOKENS`. Existing client requests work.

With `TENANTS_CONFIG=tenants.local.json`, the JSON file supplies the entire chat
registry. Include a `default` entry referencing `JANUS_API_KEY` if that key should
continue granting chat access. Otherwise it grants only administrative metrics
access. `JANUS_API_KEY` remains required for `GET /metrics`; other tenant keys
cannot read the gateway's aggregate metrics.

Copy `examples/tenants.example.json` and set each tenant's environment variable in `.env`.
Our `run.ps1` accepts tenant credential names ending in `_JANUS_API_KEY`, such as
`ALICE_JANUS_API_KEY`. The Go application can read other uppercase variable names
when you set them directly in its environment. The JSON stores variable names,
never the actual keys. `.env` and `tenants.local.json` are ignored by Git.

Each tenant requires:

| Field | Meaning |
| --- | --- |
| id | Stable, unique 1-64 character identity; letters, digits, underscores and hyphens, starting with a letter or digit |
| api_key_env | Uppercase environment variable containing its unique client credential |
| rpm | Request permits per minute, including burst capacity; 1-1000000 |
| tpm | Token allocation in the rolling admission window; 1-100000000 |
| max_output_tokens | Default and maximum per-request output; positive and smaller than TPM |
| enabled | Required boolean; false rejects new requests with 401 |

Configuration is strictly decoded, capped at 1 MiB and 1000 tenants. Duplicate
IDs or credentials, unknown fields, missing keys, whitespace in credentials,
and invalid limits fail startup without printing credential values. Disabled
tenants still require valid configuration and credentials.

## Redis identity and rotation

Tenant limiters share one Redis client connection pool. They use different quota
keys derived from their stable ID, under `janus:quota:tenant:{fingerprint}`.
The Lua admission and settlement logic is unchanged. Instances with the same
tenant IDs and Redis database share limits; unrelated deployments should use
separate Redis databases or distinct IDs.

Cache scopes include tenant identity and provider configuration. Identical
prompts from different tenants cannot reuse each other's answers. A hit still
spends tenant RPM and zero TPM. Missing cache entries use that tenant's output
allowance and token quota, including fallback reservations.

Rotate a key by changing its environment variable and restarting Janus. The old
key stops working, while quota and cache state survive because the ID remains
unchanged. Change `enabled` to false and restart to revoke new access. Active
requests are not canceled. There is one credential per tenant in this version;
overlapping rotation and live reload are future work. Lowering limits applies
to existing usage; it does not erase that usage.

Migrating from the previous single-key implementation changes Redis namespaces
once, starting a fresh quota and cache scope. Subsequent restarts and client key
rotations retain state while Redis persists. Our development Redis remains
ephemeral, so restarting it clears state.

## Logs and limits

Authenticated chat logs include `tenant_id`; rejected credentials have no trusted
tenant ID. Keys, prompts and answers remain excluded. Metrics retain fixed labels
without tenant IDs, avoiding unbounded series growth. The optional PostgreSQL
pipeline supplies durable tenant usage history; lossless billing remains future work.
Breakers, provider credentials, connection pools,
and gateway capacity are shared: separate quotas do not eliminate every source
of resource contention between tenants.

Configuration changes require restart. This version has no tenant administration
API, database, dollar budgets, self-service key creation or tenant concurrency
limit. Never reuse an ID for a different customer while its Redis state survives.

## Local test

The local setup includes default, Alice and Bob, with credentials in your ignored
`.env`. Keep Redis running and restart Janus with `.\run.ps1` after edits.

```powershell
$aliceKey = (Get-Content .env | Where-Object { $_ -like 'ALICE_JANUS_API_KEY=*' }).Split('=', 2)[1]
$response = Invoke-WebRequest -UseBasicParsing -Uri http://127.0.0.1:8080/v1/chat/completions -Method Post -Headers @{ Authorization = "Bearer $aliceKey"; 'X-Janus-Cache' = 'true' } -ContentType 'application/json' -InFile examples/request.json
$response.Headers['X-RateLimit-Limit']
$response.Headers['X-TokenLimit-Limit']
($response.Content | ConvertFrom-Json).choices[0].message.content
```

Alice should see RPM 30 and TPM 20000. Repeat with `BOB_JANUS_API_KEY` to see
RPM 60 and TPM 50000. Each tenant's first eligible answer is a separate cache
miss; subsequent identical calls can hit only that tenant's cache.

Redis integration tests verify independent RPM, TPM debt and caches; rejection
before quota spending; output caps; disabled tenants; administrator-only metrics;
rotation preserving exhausted quotas; and tenant attribution in logs.
They also verify that SSE is forwarded and flushed with usage settled against
the authenticated tenant, and that fallback charges cannot alter another tenant's ledger.
