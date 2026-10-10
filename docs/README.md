# Documentation

Start with the root [README](../README.md) and [demo walkthrough](demo.md).

| Area | Notes |
|---|---|
| Release | [Checklist, scope and limitations](release.md) |
| Architecture | [Request lifecycle and component boundaries](architecture.md) |
| Setup | [Linux containers](containers.md), [Windows/WSL startup](startup.md) |
| Quotas | [RPM/TPM mathematics](token-accounting.md) |
| Routing | [Circuit breakers](circuit-breaker.md), [fallback](fallback-routing.md) |
| Tenants | [Identity, isolation and rotation](tenants.md) |
| Cache | [Opt-in exact cache](caching.md) |
| Observability | [Metrics, TTFT and logs](observability.md) |
| Usage history | [Redis/PostgreSQL pipeline](usage-storage.md), [worker tuning](usage-worker-tuning.md) |
| Local durability | [Recovery boundaries](durable-outbox.md), [journal batching](group-commit.md) |
| Load and overload | [Load fixtures](load-testing.md), [admission/retries](overload-protection.md), [benchmark interpretation](performance.md) |

Earlier benchmark sections describe historical implementations. Use the worker
tuning and journal batching notes for the latest measured comparisons; raw local
reports are ignored build artifacts and are not required to run the demo.
