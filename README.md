# Janus Proxy

A Go LLM gateway built one working step at a time.

Currently supports a local health endpoint and streaming or non-streaming chat requests to
one OpenAI-compatible upstream. No external Go dependencies.

## Run

Requires Go 1.22 or newer. In PowerShell:

For local Groq setup, copy `.env.example` to `.env`, set your key there, and run:

```powershell
.\run.ps1
```

The script loads the two settings from `.env` (overriding values in that terminal)
and uses the project-local Go toolchain if available, otherwise Go from PATH.
The `.env` format is plain `NAME=value`, without quotes or inline comments.
The real `.env` is ignored by Git; `.env.example` contains placeholders only.

You can also configure the Go program directly:

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
The Go program itself does not load `.env`; `run.ps1` handles that for local runs.

`OPENAI_BASE_URL` defaults to `https://api.openai.com/v1`. You can set it to
another trusted OpenAI-compatible provider's API base URL, including its version
path. Janus appends `/chat/completions`. Remote endpoints require HTTPS;
loopback HTTP endpoints are allowed for development.

Without an API key, `/health` still works, but valid chat requests return 503.
Keys belong in local environment variables, never in source or request JSON.

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

## Send a request

`request.json` contains a small Groq chat request. When changing providers, set
its `model` to a model ID available to your account.
In a second PowerShell terminal:

```powershell
Invoke-RestMethod -Uri http://localhost:8080/health

Invoke-RestMethod `
  -Uri http://localhost:8080/v1/chat/completions `
  -Method Post `
  -ContentType 'application/json' `
  -InFile request.json
```

PowerShell summarizes nested objects. To print the answer, store the result in
`$response` and read `$response.choices[0].message.content`. To inspect all fields,
use `$response | ConvertTo-Json -Depth 20`.

## Streaming

Start Janus with `.\run.ps1`. In a second terminal, use the native curl executable:

```powershell
curl.exe --no-buffer --silent --show-error `
  http://localhost:8080/v1/chat/completions `
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
separate idle and total deadlines are a future refinement. Janus currently
forwards SSE without interpreting token usage or detecting completion markers.

Tests use a gated fake provider to prove bytes arrive before generation finishes,
preserve a split UTF-8 character and SSE framing, check cancellation, and check
interrupted streams and errors. Provider protocol:
[Groq streaming documentation](https://console.groq.com/docs/text-chat).

## Current request flow

```text
Client -> router -> JSON decoding -> validation -> provider HTTP call
       <- provider's JSON response and HTTP status <-
```

Supported input fields: `model`, `messages`, and `stream` (defaults to false).
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

This is a local development gateway. Client authentication, quotas,
fallback, caching, and usage accounting are future steps.

## Check

```powershell
go test ./...
go vet ./...
```

Replace `go` with `.\.tools\go\bin\go.exe` if using the local toolchain.
Tests use local fake providers and make no paid API calls.
