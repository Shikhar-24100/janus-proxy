# Janus in Linux containers

This step packages the existing gateway and its dependencies into a repeatable
local environment. It does not add another gateway feature or establish a
production latency guarantee. The Windows development stack remains independent.

## The pieces, from the beginning

An **image** is a packaged filesystem and startup recipe. Our Janus image holds
the compiled Go program and HTTPS certificate authorities. A **container** is a
running instance of an image, with its own processes and network namespace.
Linux containers share a Linux kernel; on this laptop that kernel is provided by
Ubuntu WSL. There is not a separate virtual machine for each container.

**Docker Engine** starts and manages containers. **Docker Compose** reads one
configuration file and manages the related containers, network and volumes.
Compose does not replace Redis/PostgreSQL or embed them in Janus.

```text
Windows browser / PowerShell client
        |
        | http://localhost:8081
        v
Docker published port -> Janus container, port 8080
        |                    |
        |                    +-> Groq / OpenAI over HTTPS
        |
        +-> quota-redis:6379 -> tenant RPM/TPM + exact cache
        |
        +-> outbox-data volume -> fsync completion event
        |                    |
        +-> usage-redis:6379 <- confirmed delivery; delete local event
                               durable stream -> Go worker inside Janus
                                                    |
                                                    v
                                               postgres:5432

All four services above run inside the same Linux Docker environment.
The provider remains an external service; client-to-Janus traffic still crosses
the Windows/WSL boundary when the client runs on Windows.
```

## Files and their responsibilities

Janus mounts the persistent `outbox-data` volume at `/var/lib/janus/outbox`.
Container recreation preserves confirmed local handoffs waiting for Redis.
The [outbox notes](durable-outbox.md) explain recovery and storage limits.

| File | What it does |
| --- | --- |
| `Dockerfile` | Builds Linux Go code, then creates a runtime image with the binary and CA certificates |
| `.dockerignore` | Allows only Go/module/migration/query build inputs; private config never enters the build context |
| `compose.yaml` | Defines the four services, DNS names, internal connections, health checks and persistent volumes |
| `containers.ps1` | Creates local configuration once and provides startup/test/benchmark commands |
| `.env.container` (ignored) | Runtime provider and tenant keys, initially copied from `.env` |
| `.env.compose` (ignored) | Generated database password and host HTTP port |
| `.container/tenants.json` (ignored) | Read-only tenant config, copied from the current config or generated for the default tenant |
| `.cache/perf-linux/` (ignored) | Linux benchmark artifacts, separate from Windows reports |

The build is **multi-stage**: the `build` stage has Go and source code; `binary`
compiles Janus; `runtime` copies just the result. The compiler and build cache
are not shipped in the runtime image. The `test` target can run unit tests, and
the Compose `tools` profile uses the build stage for integration tests and the
existing performance harness. Runtime Janus runs as UID 10001 with a read-only
filesystem and writable temporary directory.

The source allowlist names each root Go file explicitly. Broad negated globs
caused Docker to traverse large Windows tool/cache directories during context
preparation. When adding a new Go file, add its `!filename.go` entry too; the
wrapper checks this before building. The verified build context was about 199 KiB.

Image tags specify Go 1.27.1, Redis 7.4.11 and PostgreSQL 16.15. They are explicit
version tags rather than `latest`; a fully immutable deployment would also pin
image digests and plan updates. PostgreSQL remains major version 16 to match our
existing design. Redis uses two separate instances for separate lifetimes.

## Why localhost changes

Inside Janus's container, `localhost` means that container. It cannot identify
a neighboring Redis container. Compose supplies DNS names, so Janus uses
`redis://quota-redis:6379/0`, `redis://usage-redis:6379/0`, and `postgres:5432`.
Both Redis containers can use port 6379 because they have separate namespaces.
Redis and PostgreSQL have no published host ports in this setup.

`LISTEN_ADDR` lets Janus bind to `0.0.0.0:8080` inside the container, so traffic
from Docker's network can reach it. The default outside containers stays
`127.0.0.1:8080`. Compose publishes the container's 8080 as **host loopback
8081**. That separate host port avoids the existing Windows Janus on 8080.

## Startup and saved data

Compose starts the three dependencies and waits for their health checks before
starting Janus. Janus also checks Redis and initializes its PostgreSQL schema.
`/health` remains process liveness, not a continuous database readiness check.
Health checks do not automatically repair all runtime dependency failures.

Quota/cache Redis has no persistence. Recreating it resets quotas and cache.
It uses noeviction so memory pressure cannot silently remove quota records;
requests can fail when full. Usage Redis keeps appendonly=yes,
appendfsync=always and noeviction. Its `/data` directory has a named volume.
PostgreSQL's data directory has another named volume. Replacing a container
does not replace these volumes. Volumes are not backups or replication.

This stack has separate history from the older WSL PostgreSQL. Initialization
does not migrate or delete existing usage records. Changing POSTGRES_PASSWORD
in the environment does not change the password in an already initialized
database; password rotation must update PostgreSQL too.

The Go usage worker still lives inside Janus: batching, commit-before-ack and
deduplication all stay in place. Containers do not add exactly-once billing or
close the pre-enqueue crash gap.

## Commands on this laptop

We installed Ubuntu's Docker Engine, Compose v2 and Buildx packages inside WSL.
The wrapper uses `wsl -d Ubuntu -u root -- docker ...` when Windows Docker is not
available; it uses native Docker when installed. Engine access is administrative,
while the Janus process itself runs as a non-root container user.

```powershell
# Once: copies existing local settings and generates a database password.
# Already completed on this laptop; init refuses to overwrite existing files.
.\containers.ps1 -Action init

# Build/start all four containers and wait for health checks.
.\containers.ps1 -Action up
.\containers.ps1 -Action ps
Invoke-RestMethod http://localhost:8081/health
.\containers.ps1 -Action report

# Integration tests use fake providers and real container Redis/PostgreSQL.
# They do not receive the provider credentials.
.\containers.ps1 -Action test

# Full Linux harness: quotas/cache + durable queue/worker/DB.
.\containers.ps1 -Action benchmark -Requests 500 -Concurrency 1,16 -GoCPUs 2

# Read the most recent gateway logs, or stop the stack keeping volumes.
.\containers.ps1 -Action logs
.\containers.ps1 -Action down
```

For another machine, install a Linux-capable Docker Engine with Compose v2 (for
example Docker Desktop with WSL integration), populate `.env`, then run init/up.
If this laptop's WSL Docker service is stopped, start it with
`wsl -d Ubuntu -u root -- systemctl start docker`.

`init` copies configuration once; later edits to `.env` do not propagate. Edit
`.env.container` or `.container/tenants.json`, then run `up` again. Environment
changes cause Compose to recreate Janus; if only the mounted tenant JSON changes,
restart Janus explicitly with Compose or run down/up, because the registry loads
at startup. `up --build` recompiles changed Go code. Credentials are runtime
environment values, visible to Docker administrators, not a secret-manager
integration. Do not print expanded `docker compose config`: it contains secrets.

The wrapper's `down` keeps durable volumes. Avoid `down -v` unless intentionally
discarding the container stack's usage history. The tools profile is one-shot;
ordinary `up` starts only the four services. No external image publishing occurs.
`report` reads the container database; the older `usage-report.ps1` still reads
the separate WSL database configured in `.env`.

## What this means for performance

Moving Janus, Redis and PostgreSQL into Linux removes the Windows-to-WSL
connection boundary between gateway and dependencies. It also changes CPU,
networking, timer and disk behavior, so improvements need measurement.
Named volumes keep database/AOF files in Docker's Linux storage, rather than a
Windows source-directory bind mount. Source and reports can remain on Windows;
the runtime image does not read source files while serving requests.

The Linux benchmark uses the existing **same-process** client, fake provider,
gateway and worker in a tools container, plus the real dependency containers.
It does not benchmark the separately running Janus container on 8081. Its
separate artifact directory prevents confusing the results with Windows runs.
The next load-testing step is an external fake provider and load generator with
controlled arrival rates, independent gateway CPU measurements and longer runs.
This container step alone does not establish the 5-10 ms overhead target.

## Verified on October 6, 2026

All four containers became healthy. Host `/health` returned status=ok,
authenticated `/metrics` returned 200, and unauthenticated metrics returned 401.
Janus runs as UID/GID 10001 with HTTPS certificates installed. The runtime image
is linux/amd64, approximately 8.9 MB by Docker's image size report, and contains
no Go compiler, source directory or local credential files.

The Linux integration suite passed against the container dependencies, along
with go vet. Temporary Redis/PostgreSQL probes survived Compose down/up without
deleting volumes and were removed afterward. The report command returned an
empty history after test cleanup, as expected for this new database.

The first full Linux benchmark used Go 1.27.1, GOMAXPROCS=2, 500 samples per
scenario and concurrency 1/16: 7000 measured requests, zero request/enqueue/worker
errors, and every queue drained. No paid provider requests were made.

| Measurement | Concurrency 1 | Concurrency 16 |
| --- | ---: | ---: |
| Direct JSON p50 / p99 ms | 10.62 / 11.42 | 10.92 / 12.36 |
| Full Janus JSON p50 / p99 ms | 13.04 / 19.17 | 13.76 / 19.23 |
| Difference between JSON medians ms | 2.42 | 2.84 |
| Direct streaming TTFT p50 ms | 3.45 | 3.81 |
| Full Janus streaming TTFT p50 ms | 4.18 | 4.20 |
| Full JSON queue backlog after load | 1 | 10 |

The local median differences fall within the original 5-10 ms goal for this
small workload. They are not per-request overhead percentiles or a guarantee
for real providers, long streams, sustained arrival rates or the runtime container.
This first run changes OS, filesystem and network together; it cannot attribute
all improvement specifically to Windows/WSL round trips. Reports are
`.cache/perf-linux/latest-full.*`. The newer
[separate-container load test](load-testing.md) exercises the actual runtime
image and records its CPU/memory and usage backlog under fixed-rate traffic.

Official references:

- [Multi-stage Docker builds](https://docs.docker.com/build/building/multi-stage/)
- [Compose startup order and health checks](https://docs.docker.com/compose/how-tos/startup-order/)
- [Docker volumes](https://docs.docker.com/engine/storage/volumes/)
- [Docker networking](https://docs.docker.com/engine/network/)
