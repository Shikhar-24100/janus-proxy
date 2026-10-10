# Development

Keep changes focused and include verification that exercises the changed behavior.
Run commands from the repository root. Go implementation and tests live together
in `internal/gateway`; `cmd/janus/main.go` is the executable entrypoint.

```sh
gofmt -w cmd internal
go vet ./...
go test ./...
go build ./cmd/...
```

Without `REDIS_TEST_URL` and `POSTGRES_TEST_URL`, integration tests skip. Use
`containers.ps1 -Action test` for the full Linux suite, or supply disposable
Redis/PostgreSQL URLs and run `go test -race -count=1 ./...`. Do not point integration
tests at production services: fixtures create/delete isolated keys and usage rows.
The credential-free `demo.ps1` is a quick review path.

Native Go execution reads exported environment variables, not `.env` itself.
`run.ps1` loads the local file. Container settings are copied once by `init` into
ignored configuration; existing copies are never overwritten automatically.

Update launch/build paths and the Docker input allowlist when moving source files.
Avoid committing credentials, machine settings, generated reports, or binaries.
The opt-in live fallback test can call a billed provider and must remain disabled
in routine tests and CI. Prefer fake-provider tests for reproducibility.

Before presenting a latency improvement, include accepted/rejected counts, duration,
direct-provider controls, accounting checks and relevant caveats. Local laptop
results do not establish production capacity. Keep README claims consistent with
[release boundaries](docs/release.md).
