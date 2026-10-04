# RPM and TPM maths

RPM counts admitted requests. TPM allocates model tokens to requests admitted
within the last 60 seconds. Neither counts SSE events.

## RPM refill

```text
refilled permits = min(R, previous permits + elapsed seconds * R / 60)
admit when refilled permits >= 1
remaining permits = refilled permits - 1
```

At 60 RPM, refill is 1 permit/second. An idle bucket allows a burst of 60.
If empty, 0.5 seconds restores half a permit: still reject. One second restores
a full permit. This is an average rate plus burst capacity, not a strict rolling
RPM cap. TPM below uses a rolling ledger.

## Reserve and settle

```text
reservation = estimated input tokens + maximum output tokens
admit when active charges + reservation <= TPM limit
actual = provider prompt tokens + provider completion tokens
new active total = old active total - reservation + actual
refund = reservation - actual
```

Example: TPM limit 1000, initially empty:

| Action | Active charge | Available |
| --- | ---: | ---: |
| Input estimate 100 + output allowance 500; reserve 600 | 600 | 400 |
| Another request asks for 500; reject | 600 | 400 |
| First finishes: actual input 90 + output 160 = 250 | 250 | 750 |
| Another request reserves 500; accept | 750 | 250 |

Refund = 600 - 250 = 350. The first call still costs one RPM permit. The rejected
request spends neither RPM nor TPM.

## Verified live Groq examples

| Response | Input estimate | Output cap | Reserved | Actual input | Actual output | Actual total | Released |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| JSON | 102 | 256 | 358 | 83 | 68 | 151 | 207 |
| SSE | 109 | 512 | 621 | 82 | 122 | 204 | 417 |

These reserved 979, then settled to 355; Redis was checked and stored 355.
Results vary on subsequent calls. Reported reasoning tokens are already included
in completion tokens: do not add them again.

## Initial estimator

```text
estimate = 32 + sum(UTF-8 bytes of content + bytes of role + 16)
```

One user message containing Hello estimates 32 + 5 + 4 + 16 = 57 input tokens.
An output allowance of 200 reserves 257. Byte counts are not actual tokens.
This heuristic often over-reserves but can underestimate provider formatting.
Settlement uses reported usage.

## Uncertainty and time

- Reserve 600, actual 700: refund is -100, so charge 100 more. Future admissions
  may be blocked; the admitted stream is not cut.
- Interrupted or missing final usage: retain the reservation until it ages out.
  Partial text cannot prove billed usage.
- No upstream attempt: settle TPM to zero; RPM remains spent.
- Admit at 12:00:00: the charge ages out at 12:01:00. Settlement at 12:01:05
  cannot refund capacity in the newer window.
- Duplicate settlement for a request ID does nothing.
- A crash or failed settlement retains the conservative reservation.

The output allowance is sent to Groq. The provider may stop generation at that
cap. This differs from delaying or rate-limiting individual SSE events.
