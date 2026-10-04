# Groq to OpenAI fallback

```text
Client -> authenticate -> validate -> initial RPM + TPM allocation
       -> primary breaker -> Groq
                            | eligible failure, before streaming
                            v
                   settle primary allocation if usage known
                   reserve fallback TPM (no second RPM)
                            v
                   fallback breaker -> OpenAI -> client
```

If the primary was never contacted, reuse the initial token allocation instead
of making another. If a failed primary attempt has unknown usage, keep its
reservation: an error does not prove the provider did no work.

## Configuration and behavior

`FALLBACK_API_KEY` enables the secondary endpoint. Its defaults are
`FALLBACK_BASE_URL=https://api.openai.com/v1` and `FALLBACK_MODEL=gpt-4o-mini`.
The primary retains the client's model. The fallback substitutes its configured
model but preserves messages, stream settings, and output cap. Do not commit
real keys. These providers both support our current text-only chat subset.

`router.go` selects one of two routes and delays sending errors until the route
decision is final. `provider.go` performs an attempt and reports errors without
committing them. Successful buffered bodies and SSE remain unchanged.
`X-Janus-Route` identifies primary or fallback. The response model is the actual
provider model, not an invented alias.

Fallback is allowed for network errors, upstream timeout, 429/5xx, unavailable
primary configuration, open breaker, invalid buffered JSON, redirects, or an
invalid SSE content type detected before headers are sent. Other 4xx errors are
returned to the client. Cancellation stops routing. Once SSE headers are sent,
an upstream failure aborts that stream: no second answer is inserted into it.

Each provider has its own breaker and HTTP timeout. Two attempts may take up
to roughly 120 seconds. No parallel hedging, repeated provider attempts, or
third provider exists. If fallback also fails, its error becomes the response.

## Accounting example

Suppose the prompt estimate plus output allowance is 600 tokens.

| Scenario | Token charge | RPM permits |
| --- | ---: | ---: |
| Primary circuit open; fallback uses 250 | 250 | 1 |
| Primary failed, usage unknown; fallback uses 250 | 600 + 250 = 850 | 1 |
| Primary reports 100 tokens despite failure; fallback uses 250 | 100 + 250 = 350 | 1 |

After an attempted primary, fallback needs a fresh 600-token reservation before
it can run. That is checked atomically in Redis, without spending another RPM
permit. If there is not enough capacity, Janus returns 429 before contacting
fallback. Initial quota headers remain initial-admission snapshots.

`quota.go` uses the same reservation script with an explicit flag controlling
whether RPM is spent. Each provider attempt has an independent reservation ID
and timestamp. `tokens.go` settles the current attempt once before replacing its
state with the fallback attempt. Deferred cleanup handles the final attempt,
including aborted streams. Unknown usage and failed reconciliation remain
conservative until their original rolling windows expire.

Tests cover routing, credentials/model substitution, original error preservation,
cancelation, stream boundaries, separate usage charges, open-circuit reuse, and
fallback quota denial. The opt-in live test caps each of two calls at 32 output
tokens. Ordinary tests never contact OpenAI.

Dollar budgets are separate from token quota; Janus currently does not enforce
a dollar spend cap. Prices and model access should be checked against the
[official OpenAI model page](https://developers.openai.com/api/docs/models/gpt-4o-mini).
