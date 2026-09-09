# aver-python

Record what your decisioning system saw and did, so the decision can be
explained later — to a regulator, an auditor, or a customer who was declined.

You call it after a credit decision is computed. It captures the inputs your
model actually read, the versions of the model and policy that read them, and
the action you took. Recording is fire-and-forget: it never blocks the approval
and never raises into your code path.

**This library is open source on purpose.** It sits inside your decision path
and reads every input your model saw, including PII before redaction. Your
security team is going to ask to read it, and "closed source, trust us" is not
an answer. Everything that touches your data is in this repository, in about
830 lines. The redaction happens here, in `aver/redact.py`, before anything
crosses the process boundary — you can verify that rather than take our word
for it.

```bash
pip install aver
```

Python 3.9+. Depends on `httpx` and the standard library. Nothing else.

---

## Quickstart

```python
import os
from aver import AverClient

aver = AverClient(
    api_key=os.environ["AVER_API_KEY"],
    stream_id="consumer-pl",
    policy_version="credit-policy-v4.2",
    redact=["applicant.pan", "applicant.name", "bureau_report.*"],
)

with aver.decision(session_id=application_id) as d:
    d.observe("application_form", form_data)
    d.observe("cibil_report", bureau_response)
    d.observe("dti_computation", dti_result)

    d.model(
        version="scorecard-v7",
        artifact_hash="sha256:9f2b...",
        feature_set="fs-2026-03",
        rule_config_hash="sha256:44ab...",
    )

    d.record_action({"type": "deny", "reason_code": "DTI_EXCEEDED", "amount": 450000})
```

The block returns in microseconds. Delivery happens on a background thread.

---

## What this sends

This is the section to read before you approve this dependency. The list is
exhaustive.

**Every record contains exactly these fields, and nothing else:**

| Field | What it is |
|---|---|
| `schema_version` | Wire format version. Currently `"1"`. |
| `idempotency_key` | UUID4 generated at enqueue and carried on the record itself, so a retry — or a redelivery in a differently-composed batch — cannot become a second ledger entry. |
| `decision_id` | UUID4 identifying this decision. |
| `parent_decision_id` | The prior step in a multi-step chain, or `null`. |
| `stream_id` | The stream you configured. |
| `session_id` | The identifier **you** passed — usually your application id. |
| `inputs` | The `role` and `value` of each thing you passed to `observe()`, **after redaction**. |
| `model_version`, `model_artifact_hash`, `feature_set_version`, `rule_config_hash` | Whatever you passed to `model()`. Strings you supply. |
| `policy_version` | The string you configured or passed. |
| `action` | The dict you passed to `record_action()`, verbatim. If your block raised, this becomes `{"type": "error", ...}` and your action is preserved under `attempted_action`. |
| `recorded_at` | UTC timestamp, taken when you recorded — not when we delivered. |

**Every request also carries these HTTP headers:**

- `Authorization: Bearer <your api key>`
- `User-Agent: aver-python/0.1.0 python/3.11.4` — your SDK version and your
  Python version. This is the only environment detail that leaves your process,
  and it is there so support can tell which SDK a problem came from.

**What is never sent:**

- Nothing else from your process. No environment variables, no hostname, no
  process id, no working directory, no installed package list.
- No stack traces. If your `with` block raises, we record
  `{"type": "error", "detail": str(exc)}` — the exception's message, not its
  traceback and not its locals. If you had already called `record_action()`,
  that action is kept as `attempted_action`: "we decided to deny, and then
  something broke" is the record you most need. If your exception messages
  themselves contain PII, that string is yours to control.
- **This library never logs the values you pass it.** The `aver` logger emits
  counts, statuses and reasons. One honest caveat: when your endpoint rejects a
  batch, the first 200 characters of *its* response body go to the ERROR log so
  a broken integration is diagnosable. If your Aver deployment echoes records
  back in validation errors, that excerpt can contain unmarked field values —
  fields you chose not to redact, since marked ones are already masked or
  encrypted before they are sent. `stats()["last_error"]` deliberately carries
  only the status (`"HTTP 400"`), never the body, because health endpoints
  expose it.
- No telemetry, no analytics, no crash reporting, no phone-home. The only
  outbound connection this library makes is a POST of your records to the
  `base_url` you configured.

A value that is not JSON-encodable (a `Decimal`, a `date`, an ORM object) is
sent as `str(value)` rather than costing you the record.

---

## Redaction

Mark fields with dotted paths. Three forms:

```
"applicant.pan"          that field
"bureau_report.*"        every field one level under
"accounts[].balance"     that field in every element of the array
"[].balance"             that field in every element, when the input
                         value is itself an array
```

Paths are matched against the *value* of each input, so one path covers
whichever inputs contain that structure.

```python
aver = AverClient(
    api_key=...,
    stream_id="consumer-pl",
    redact=["applicant.pan", "applicant.name", "accounts[].balance"],
)

bureau = {
    "applicant": {"pan": "ABCDE1234F", "name": "Asha Rao", "age": 34},
    "accounts": [{"id": "a1", "balance": 125000}],
    "dti": 0.61,
}

with aver.decision(session_id="app-77") as d:
    d.observe("cibil_report", bureau)
    d.record_action({"type": "deny", "reason_code": "DTI_EXCEEDED"})
```

What leaves the process:

```json
{
  "applicant": {"pan": "[REDACTED]", "name": "[REDACTED]", "age": 34},
  "accounts": [{"id": "a1", "balance": "[REDACTED]"}],
  "dti": 0.61
}
```

`bureau` itself is unchanged — redaction runs on a deep copy, because you are
probably still using that dict.

**A redaction path that matches nothing logs a warning on the first record.**
A typo that silently ships PII is the worst thing this library could do, so it
is noisy about it:

```
WARNING aver: redaction path(s) matched nothing in the first record:
        applicant.pan_number — check for typos; unmatched fields are sent unredacted
```

### Client-side encryption

If masking loses too much — you want the values back during a dispute, but
Aver must never be able to read them — encrypt instead. AES-256-GCM under a
key you hold and we never see:

```python
aver = AverClient(
    api_key=...,
    stream_id="consumer-pl",
    redact=["applicant.pan"],
    redact_mode="encrypt",
    encryption_key=os.environ["AVER_FIELD_KEY"],  # 32 bytes, or base64 of 32 bytes
)
```

The field travels as `{"__aver__": "enc:aes-256-gcm", "nonce": ..., "ciphertext": ...}`.
Aver hashes the ciphertext, so the chain still proves what your model saw
without Aver being able to read it. Recover a value with the key you hold:

```python
from aver import decrypt_field
pan = decrypt_field(record["inputs"][0]["value"]["applicant"]["pan"], key)
```

Encryption needs one extra dependency, so it is opt-in:

```bash
pip install 'aver[encrypt]'
```

---

## When Aver is unreachable

Your loan approvals do not care. Concretely:

| Situation | What happens |
|---|---|
| Network down, timeout, 404, 429, 5xx | Retried with exponential backoff — 1s, 2s, 4s … capped at 60s. `Retry-After` is respected on any of them, not just 429. A 404 is retried too: an ingress can return one transiently mid-deploy, and that must not destroy records. Nothing raises. |
| 400, 401, 403, 413, 422 | Permanent. Logged at ERROR, counted in `dropped`, not retried. A broken integration should be visible, not hidden behind infinite retries. |
| Process exits | An `atexit` handler flushes synchronously with a 10-second budget. Anything still undelivered after that is logged at ERROR. |
| **Buffer full** | **`AverBufferFull` is raised into your code.** |
| **Recording after `close()`** | **`AverClientClosed` is raised into your code.** |

Those two are the only exceptions this library will ever put in your decision
path. Both mean a record was lost.

That last row is deliberate. The buffer holds 10,000 records by default. If it
fills, records are being lost, and a silently dropped audit record is worse
than an error — you would believe you have coverage you do not have. Catch it
if you must, but catch it knowing what it means:

```python
try:
    aver.record(...)
except (AverBufferFull, AverClientClosed):
    metrics.increment("aver.records_lost")   # your call, but do not swallow it silently
```

### Checking it works

```python
aver.stats()
# {"queued": 12, "sent": 45231, "failed": 3, "dropped": 0,
#  "last_error": "connection timeout", "last_success_at": "2026-09-07T09:14:22Z"}
```

`dropped` should be zero. If it is not, records are missing from your audit
trail. The `aver` logger reports the same events: INFO on start and stop,
WARNING on retry, ERROR on any permanent drop.

For tests and batch jobs, drain explicitly:

```python
aver.flush(timeout=5.0)   # True if it drained, False if the timeout expired
```

---

## Self-hosted (BYOC)

Running Aver in your own VPC? One line:

```python
aver = AverClient(api_key=..., stream_id=..., base_url="https://aver.internal.bank.example")
```

Nothing else changes, and no traffic goes anywhere else.

---

## Other forms

### Direct call, for existing pipelines

```python
aver.record(
    session_id=application_id,
    inputs=[
        {"role": "application_form", "value": form_data},
        {"role": "cibil_report", "value": bureau_response},
    ],
    model_version="scorecard-v7",
    model_artifact_hash="sha256:9f2b...",
    feature_set_version="fs-2026-03",
    rule_config_hash="sha256:44ab...",
    policy_version="credit-policy-v4.2",
    action={"type": "approve", "amount": 300000},
)
```

### Multi-step chains

```python
with aver.decision(session_id=app_id) as d:
    d.observe("application_form", form)
    d.record_action({"type": "fetch_bureau"})
step0 = d.decision_id           # available after the block; None before

with aver.decision(session_id=app_id, parent=step0) as d:
    ...
```

`parent` takes a `decision_id` string or the recorder object from a prior block.

### Async

```python
from aver import AsyncAverClient

aver = AsyncAverClient(api_key=..., stream_id="consumer-pl")

async with aver.decision(session_id=application_id) as d:
    d.observe("application_form", form_data)      # local, nothing to await
    d.record_action({"type": "approve"})

await aver.flush(timeout=5.0)
```

Identical surface and identical guarantees. `observe` and `record` stay
synchronous because they do no I/O. Delivery runs on a background thread rather
than an asyncio task, deliberately: your event loop spends nothing on audit
traffic, and a loop blocked by your own work cannot stall the audit trail.

---

## What this library does not do

Being strict about this is what keeps it small enough to read in an afternoon.

- **No hashing or chaining client-side.** The server owns record integrity. A
  client that computes its own hashes is a client that can forge them.
- **No local persistence.** No SQLite fallback, no disk spooling. If you need
  durability across process restarts, run BYOC.
- **No framework integration.** No Django middleware, no FastAPI dependency,
  no imports of either. See [`examples/`](examples/) for how to wire it up in
  about five lines.
- **No schema validation of your inputs.** The server validates against your
  stream's `input_schema` and reports completeness. Two validators means two
  sources of truth.
- **No retry of permanently-failed records.** Logged and dropped. Silent
  infinite retry hides a broken integration.

---

## Compatibility

SemVer. Python 3.9 and up. The wire format is versioned server-side via
`schema_version` — the SDK sends what it sends and the server accepts known
versions. No breaking change without a major bump and a migration note.

Full record format: <https://docs.aver.dev/record-format>

## License

Apache 2.0. See [LICENSE](LICENSE).
