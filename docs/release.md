# Portfolio MVP release

This release freezes a useful learning/portfolio scope. It is not a claim that
the original production-grade gateway specification is complete. The original
scope remains roughly 75% by effort estimate; optional future features are not
needed to reproduce this MVP.

## Included

- OpenAI-compatible text chat subset with JSON and flushed SSE forwarding.
- Tenant authentication/isolation, Redis RPM and rolling TPM reservations.
- Primary/secondary routing with separate circuit breakers and safe streaming boundaries.
- Opt-in Redis exact cache for eligible non-streaming primary responses.
- Request/attempt metrics, TTFT, structured logs and bounded admission/retries.
- Linux completion journal with group commit, recovery and compaction.
- Persistent usage Redis Stream, PostgreSQL deduplication/reporting, and a batched worker.
- Portable fake-provider demo, colocated tests, container builds and GitHub CI.

## Release verification

The final pass checks formatting, PowerShell syntax, complete Redis/PostgreSQL
integration tests, race detection, static checks, Linux/Windows compilation,
the standalone demo, and container startup. Hosted CI status is shown by the
README badge; a workflow definition alone is not evidence of a successful run.
Normal verification reads no provider credentials and makes no paid provider calls.

## Measurements we can defend

In the 10-second local journal comparison at 1000 offered RPS, SSE accepted
8999/10000 requests versus 5829 previously, with successful completion p99
52.93 versus 151.11 ms. The later 30-second worker comparison accepted
27526/30000 SSE requests versus 26481, and reduced queue-at-client-EOF from
5279 to 20. All accepted requests in the successful runs matched PostgreSQL
request/attempt/token totals. See [journal measurements](group-commit.md) and
[worker measurements](usage-worker-tuning.md).

These are separate experiments, not interchangeable runs. Percentiles exclude
deliberate overload rejections and include upstream/client scheduling time.
JSON results were mixed; direct control phases also had spikes, and one run
failed its response checks. A general HTTP latency improvement, the 5–10 ms
overhead target, and sustained production capacity remain unestablished.

## Deferred work

| Gap | Consequence |
|---|---|
| Model-aware tokenization | Input reservations currently use a byte heuristic |
| Durable request-start/invoice reconciliation | Crashes before completion confirmation can leave missing usage |
| Replication, backups and storage failure recovery | Local fsync/Redis persistence do not protect against volume loss |
| More API schemas and providers | Tools, multimodal content and native Anthropic/Gemini adapters are unsupported |
| Live tenant administration and dollar budgets | Tenant configuration is loaded at startup; stored tokens are not a billing ledger |
| Load balancing and semantic caching | One primary/fallback and exact caching only |
| Deployment hardening and soak tests | TLS termination, secret management and production operating limits need deployment work |

On this Windows host, keep Ubuntu/WSL running for its Docker Engine and localhost
forwarding. Native Windows mode has no durable local outbox. Do not describe this
project as production-ready or claim the latency target is met.
