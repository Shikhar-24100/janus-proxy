# Janus Proxy

A Go LLM gateway built one working step at a time.

Currently supports a local health endpoint and non-streaming chat requests to
one OpenAI-compatible upstream. No external Go dependencies.

## Run

Requires Go 1.22 or newer. In PowerShell:

```powershell
$env:OPENAI_API_KEY = 'your-provider-key'
go run .
```

If using the project-local Go toolchain instead:

```powershell
$env:GOCACHE = Join-Path (Get-Location) '.cache\go-build'
.\.tools\go\bin\go.exe run .
```

The server listens on `127.0.0.1:8080`. Stop it with Ctrl+C.
Environment variables must be set in the same terminal before starting the server.
There is no automatic `.env` file loading.

`OPENAI_BASE_URL` defaults to `https://api.openai.com/v1`. You can set it to
another trusted OpenAI-compatible provider's API base URL, including its version
path. Janus appends `/chat/completions`. Remote endpoints require HTTPS;
loopback HTTP endpoints are allowed for development.

Without an API key, `/health` still works, but valid chat requests return 503.
Keys belong in local environment variables, never in source or request JSON.

## Send a request

Edit `request.json` and replace `demo-model` with a model available to your
provider. It is a placeholder used by local tests, not a real model name.
In a second PowerShell terminal:

```powershell
Invoke-RestMethod -Uri http://localhost:8080/health

Invoke-RestMethod `
  -Uri http://localhost:8080/v1/chat/completions `
  -Method Post `
  -ContentType 'application/json' `
  -InFile request.json
```

## Current request flow

```text
Client -> router -> JSON decoding -> validation -> provider HTTP call
       <- provider's JSON response and HTTP status <-
```

Supported input fields: `model`, `messages`, and `stream: false`.
Messages support `system`, `user`, and `assistant` roles with non-empty string
content. Other fields, tool calls, multimodal content, and streaming are not
implemented yet. Provider response bodies are forwarded without changing their
JSON, so completion data and usage are preserved.

Request bodies are limited to 1 MiB; provider responses to 4 MiB.
Provider calls have a 60-second timeout and use the incoming request's context
for cancellation. JSON provider errors keep their status (including 429) and
`Retry-After`; network failures return 502 and timeouts return 504. Redirects
are rejected. Non-streaming responses are buffered before sending to the client.

This is a local development gateway. Client authentication, quotas, streaming,
fallback, caching, and usage accounting are future steps.

## Check

```powershell
go test ./...
go vet ./...
```

Replace `go` with `.\.tools\go\bin\go.exe` if using the local toolchain.
Tests use local fake providers and make no paid API calls.
