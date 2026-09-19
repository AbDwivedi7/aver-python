# Design notes

Decisions that are not obvious from reading the code, and the gaps we know
about. The README is for customers; this is for whoever changes the library
next.

---

## Known gaps

### A record dropped in the transport is not counted in `dropped`

`Transport.send` serialises each record separately, so a record that cannot be
encoded — a circular reference is the realistic case — is skipped and the rest
of the batch is delivered. That is the right trade: one bad record must not
cost the other forty-nine.

But the buffer counts the whole batch as sent, so the skipped record is logged
at ERROR and is *not* reflected in `stats()["dropped"]`. A platform team
watching only the counter would not see it.

This is a real inconsistency with the library's own rule that data loss must be
loud in both places. Closing it means the transport reporting per-record
outcomes back to the buffer, which is more coupling than the problem currently
justifies. Revisit if it ever fires in practice.

---

## Decisions worth not re-litigating

**Delivery runs on a thread, even in `AsyncAverClient`.** Not an asyncio task.
Telemetry then costs the event loop nothing, and a loop blocked by the caller's
own work cannot stall the audit trail. `record` and `observe` stay synchronous
because they do no I/O.

**Idempotency is per record, never per batch.** `_requeue` puts undelivered
records back and the next take composes a *different* batch. Any key scoped to
the request — an `Idempotency-Key` header, say — changes membership underneath
the server, and a retried record lands as a second ledger entry. The key lives
on the record, from enqueue, and survives rebatching.
**This requires the server to dedupe on `records[].idempotency_key`.**

**`decision_id` is ours to mint, and the ledger keeps it.** Recording is
fire-and-forget, so this client cannot learn a server-assigned id without the
blocking call it promises never to make. While the service minted its own, the
id `record()` returned named no ledger entry: a caller storing it against the
loan application had a foreign key to nothing, `reconstruct` answered 404, and
`parent=` linking was rejected outright — none of it visible until a dispute.
The service now takes the id on the wire (see its DESIGN.md for the costs it
accepted). What that buys here is that `record()`'s return value is worth
storing. What it demands is that the id really is unique: it is `uuid4()`, minted
once in `client.record`, and a reused one is a permanent 400.

**The wire format is owned by the service, and pinned by a golden fixture.**
For its whole life this SDK posted to `/v1/records` (the service routes
`/v1/decisions`), put `stream_id` on each record (the service reads it on the
envelope) and sent inputs under `value` (the service reads `data`). Three
mismatches, each fatal alone, and a fully green suite throughout — because
every test asserted the SDK's shape against itself. `tests/test_wire_contract.py`
and its golden file are written *from the service's contract*, never
regenerated from this SDK's output: a golden file produced by the code it
checks agrees with that code's bugs, which is how all three shipped. The
envelope, not the record, carries `stream_id`, because the service reads one
stream per request and scopes the api key to it.

**The deep copy in `Redactor.apply` is unconditional.** It is a snapshot, not
redaction scaffolding: it is what stops the caller mutating a value after we
recorded it, since the flusher serialises later on another thread. Do not make
it conditional on whether redaction paths are configured. The `action` dict is
copied in `client.record` for the same reason.

**404 is retryable; 422 is not.** An ingress can return 404 transiently while
routes reconfigure. The two mistakes are not symmetric: a genuinely permanent
404 treated as retryable costs backoff and visible buffer pressure, while a
transient one treated as permanent destroys records during a routine deploy.

**`close()` sets `_closing` before notifying, under the lock.** A notified
waiter re-checks its condition the moment it re-acquires the lock. If the flag
were still unset it would sleep out the rest of the flush interval and miss the
drain entirely — losing records with nothing in `dropped`.

**`_sleep` waits on the closing event rather than `time.sleep`.** A 60s backoff
must not add 60s to the caller's shutdown.

**The size probe for large inputs runs once, on the first input a client
sees.** Measuring costs a full serialisation, around 40% on top of the deep
copy, on the caller's decision path. Once per process is nothing; once per
record is a tax on every decision.

An earlier version gated the probe on the deep copy having been *slow*, which
is cheaper still but has a blind spot: `copy.deepcopy` treats strings as
atomic, so a payload whose bulk is long text — a bureau report with large
free-text fields — can be 1.4MB on the wire and copy in under a millisecond.
Measured: 1.4MB of shared strings copies in 0.87ms, while 1.0MB spread over
6000 small dicts takes 17ms. Size is what costs the customer storage, so size
is what gets measured.

Known limitation of the current form: a client whose first record is small will
not notice later records growing. Probing every record to catch that would
reintroduce the per-record tax the single probe exists to avoid.

**`_types.py` does not validate at runtime.** The server validates against the
stream's `input_schema` and reports completeness. Two validators means two
sources of truth.

### `stats()["last_error"]` is a category, never a message

`AverTransportError.summary` is composed only from fields we control: a status
code, or a caller-supplied `kind`, falling back to a fixed string. It never
falls through to the exception message, because `stats()` is documented as
something a platform team surfaces on a dashboard and the message can quote the
server's response body — which may echo the record we sent.

The full detail, body excerpt included, still goes to the log. That is the
customer's own log, and diagnosing a permanent 400 without it is guesswork.

---

## The line budget

`1_implementation.md` §1.6 caps library code at 850 lines, raised from 800
after the second and third review rounds. The library currently sits just
under it — run the counter over `aver/*.py`.

The ceiling is a proxy for "no server logic has leaked into the client", not a
target in its own right. That is why it was raised rather than met: the growth
was hardening plus the comments explaining why each decision is what it is,
and in a library a bank's security team reads line by line, those comments are
the most valuable text in the file. Nothing the server owns — hashing,
chaining, schema validation — has moved inward, and that is the thing to check
before raising it again.

If it does need to come down, the large-input warning in `redact.py` is the
most discretionary piece. Do not get under the number by deleting the
`_types.py` TypedDicts: that optimises the measurement rather than the thing
being measured.
