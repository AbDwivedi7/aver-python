"""The contract between this SDK and the Aver service.

Every other test in this suite asserts that the SDK does what the SDK does.
That is worth having, but it cannot catch the failure this file exists for:
for the whole life of the library the two sides did not share a wire format,
and nothing exercised them together. The SDK posted to ``/v1/records`` (the
service routes ``/v1/decisions``), put ``stream_id`` on each record (the
service reads it on the envelope), and sent inputs under ``value`` (the
service reads ``data``). Three mismatches, each fatal alone, and a green
suite the whole time.

So the golden file next to this module — ``v1_decisions_request.golden.json``
— is **written from the service, not captured from this SDK**, and the service
keeps a copy at ``internal/ingest/testdata/`` which its own suite decodes into
``ingest.Request`` and validates. A rename of a json tag in ``ingest.go``
breaks the service's build; a change to what this SDK emits breaks this one.

**The two copies are kept identical by hand, and nothing checks that they
are.** ``GOLDEN_SHA256`` below pins this copy against this repository and
nothing else: it catches the file being edited without the constant being
updated, and it cannot see the other repository at all. Edit the file and the
constant together, here or there, and both suites stay green while the two
fixtures differ — which is the state this pair exists to prevent. Changing the
fixture means changing four things: both files and both constants. That is a
convention, not a mechanism, and it is spelled out because relying on it as
though it were one is how the mismatches below survived a green suite.

Its sources, in the ``aver`` service repository:

* ``internal/httpapi/api.go``   — the route: ``POST /v1/decisions``
* ``internal/ingest/ingest.go`` — ``Request``, ``Record`` and ``Input`` struct
  tags, and ``validate()`` for what may not be empty
* ``README.md``                 — the worked ``curl`` example this fixture
  deliberately mirrors field for field, so the two can be diffed by eye

Do not regenerate it from the SDK's output. A golden file produced by the code
it checks agrees with that code's bugs, which is exactly how three of them
shipped. When the service changes, change this file from the service's
contract and let the SDK fail until it matches.

``decision_id`` is minted here and is the ledger's id. It used to be minted on
both sides — this SDK generated one and returned it, the service generated its
own and kept that — so the id a caller stored on an application row named no
record, and ``reconstruct`` answered 404 for it. Nobody would have found that
out at integration time; they would have found it out during a dispute. The
service now takes the client's id, which is what makes the value ``record()``
returns worth storing, and what makes a parent link resolve.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path

import pytest

import aver

# -- the service's contract, transcribed by hand ------------------------------

INGEST_PATH = "/v1/decisions"

ENVELOPE_KEYS = {"stream_id", "records"}

#: Every key ``ingest.Record`` declares a json tag for.
RECORD_KEYS_READ = {
    "idempotency_key",
    "decision_id",
    "session_id",
    "parent_decision_id",
    "model_version",
    "model_artifact_hash",
    "feature_set_version",
    "policy_version",
    "rule_config_hash",
    "action",
    "inputs",
}

#: The subset ``validate()`` refuses to accept empty. Each one missing is a
#: 400, which this SDK classifies as permanent — so it discards the batch.
RECORD_KEYS_REQUIRED = {
    "idempotency_key",
    "decision_id",
    "session_id",
    "model_version",
    "action",
}

#: Keys the SDK sends that the service's decoder discards. Go ignores unknown
#: fields, so these cost nothing on the wire — but the set is pinned so that
#: *adding* one is a decision somebody makes on purpose rather than a field
#: that quietly goes nowhere.
RECORD_KEYS_IGNORED = {"schema_version", "recorded_at"}

INPUT_KEYS_READ = {"role", "content_type", "data"}
INPUT_KEYS_REQUIRED = {"role", "data"}

# -- normalising what changes every run ---------------------------------------

_UUID = re.compile(r"\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_TIMESTAMP = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\Z")

#: field -> (placeholder in the golden file, shape it must still have)
VOLATILE = {
    "idempotency_key": ("11111111-1111-4111-8111-111111111111", _UUID),
    "decision_id": ("22222222-2222-4222-8222-222222222222", _UUID),
    "recorded_at": ("2026-09-18T09:14:22.031Z", _TIMESTAMP),
}

GOLDEN = Path(__file__).parent / "fixtures" / "v1_decisions_request.golden.json"

#: Should equal ``goldenSHA256`` in the service's
#: ``internal/ingest/sdk_contract_test.go``. Should, by convention: no test
#: compares them, and neither repository can see the other's value.
GOLDEN_SHA256 = "9ece1d86eb4c21a929c8c529fec9e53e33653ad5ad1de404ec95a0abad6f8cbd"

SESSION_ID = "d71ddc40-3c57-4fc0-a9fc-d3ec6f3ffb25"


def normalise(body):
    """Swap the per-run fields for placeholders, checking their shape first.

    The shape check is the point: replacing them unconditionally would let the
    golden file keep passing while ``recorded_at`` turned into, say, a float.
    """
    out = copy.deepcopy(body)
    for record in out.get("records", []):
        for key, (placeholder, pattern) in VOLATILE.items():
            if key not in record:
                continue
            actual = record[key]
            assert isinstance(actual, str) and pattern.match(actual), (
                "{0} is {1!r}, which is not a shape the service can parse"
                .format(key, actual)
            )
            record[key] = placeholder
    return out


def record_worked_example(stub_server):
    """Record the service README's example through the SDK's public API."""
    client = aver.AverClient(
        api_key="k",
        stream_id="consumer-pl",
        policy_version="policy-17",
        redact=["applicant.pan"],
        base_url=stub_server.url,
        flush_interval=0.02,
    )
    try:
        with client.decision(session_id=SESSION_ID) as d:
            d.observe("cibil_report", {"score": 742, "applicant": {"pan": "ABCDE1234F"}})
            d.observe("application_form", {"amount": 250000, "tenor_months": 36})
            d.model(
                version="scorecard-4.2",
                artifact_hash="sha256:a1b2",
                feature_set="fs-2024-11",
                rule_config_hash="sha256:c3d4",
            )
            d.record_action(
                {"decision": "approve", "limit": 250000, "customer_ref": "LN-2024-8817"}
            )
        assert client.flush(timeout=5.0), "the batch never left the buffer"
    finally:
        client.close(timeout=2.0)

    assert len(stub_server.requests) == 1, "expected exactly one POST"
    return stub_server.requests[0]


@pytest.fixture
def request_sent(stub_server):
    return record_worked_example(stub_server)


def test_golden_has_not_changed_without_the_pin_changing():
    """Catches an edit to this copy that forgot the constant below.

    Not that the service's copy matches: nothing here can see it.
    """
    actual = hashlib.sha256(GOLDEN.read_bytes()).hexdigest()
    assert actual == GOLDEN_SHA256, (
        "the golden fixture changed.\n  got: {0}\n want: {1}\n\nIf that is "
        "intended, make the identical change to the service's "
        "internal/ingest/testdata/v1_decisions_request.golden.json and update "
        "the pinned hash in both suites.".format(actual, GOLDEN_SHA256)
    )


class TestTheRequestTheServiceReceives:
    def test_body_matches_the_golden_request(self, request_sent):
        """The whole body, field for field, against the service's contract."""
        expected = json.loads(GOLDEN.read_text())
        assert normalise(request_sent["body"]) == expected

    def test_posts_to_the_published_ingest_route(self, request_sent):
        """``/v1/records`` was a 404 on every attempt, and 404 is retryable."""
        assert request_sent["path"] == INGEST_PATH

    def test_stream_id_travels_once_on_the_envelope(self, request_sent):
        body = request_sent["body"]
        assert set(body) == ENVELOPE_KEYS
        assert body["stream_id"] == "consumer-pl"
        # Not on the records: the service decodes it only at the top level, and
        # repeating it fifty times would be fifty copies going nowhere.
        for record in body["records"]:
            assert "stream_id" not in record


class TestFieldsTheServiceReads:
    def test_no_record_key_is_unknown_to_the_service(self, request_sent):
        for record in request_sent["body"]["records"]:
            unknown = set(record) - RECORD_KEYS_READ - RECORD_KEYS_IGNORED
            assert not unknown, (
                "sending {0}, which the service neither reads nor is known to "
                "ignore".format(sorted(unknown))
            )

    def test_every_required_record_field_is_present_and_non_empty(self, request_sent):
        for record in request_sent["body"]["records"]:
            for key in sorted(RECORD_KEYS_REQUIRED):
                assert key in record, "{0} is required and absent".format(key)
                assert record[key] not in (None, "", {}, []), (
                    "{0} is {1!r}; the service rejects the batch with a 400 and "
                    "this SDK treats 400 as permanent".format(key, record[key])
                )

    def test_idempotency_key_fits_the_column(self, request_sent):
        for record in request_sent["body"]["records"]:
            assert len(record["idempotency_key"]) <= 255

    def test_every_input_is_shaped_the_way_the_service_reads_it(self, request_sent):
        inputs = request_sent["body"]["records"][0]["inputs"]
        assert inputs, "the example has two inputs"
        for item in inputs:
            assert set(item) <= INPUT_KEYS_READ, (
                "input keys {0} — the service reads {1}".format(
                    sorted(item), sorted(INPUT_KEYS_READ)
                )
            )
            for key in sorted(INPUT_KEYS_REQUIRED):
                assert key in item, "inputs[].{0} is required".format(key)
            # `len(in.Data) == 0` is the check that rejects the record. An
            # input read under the wrong key arrives as JSON null, which is
            # two bytes and passes a naive truthiness test — assert against
            # the encoded length the way the service does.
            assert len(json.dumps(item["data"])) > 0
            assert item["data"] is not None, "inputs[].data is null"
            assert item["role"], "inputs[].role must not be empty"

    def test_redaction_survives_the_rename(self, request_sent):
        """The PAN is masked under `data`, not left unredacted under `value`."""
        cibil = request_sent["body"]["records"][0]["inputs"][0]
        assert cibil["data"]["applicant"]["pan"] == "[REDACTED]"
        assert cibil["data"]["score"] == 742
        assert "ABCDE1234F" not in json.dumps(request_sent["body"])


class TestKnownDivergences:
    """Contract breaks that are real, reproducible, and not yet fixed.

    ``strict`` on purpose: when one of these is fixed it fails here until the
    marker comes off, so a fix cannot land without this file being read.
    """

    @pytest.mark.xfail(
        strict=True,
        reason="the SDK allows model_version=None; the service requires it "
        "non-empty and answers 400, which this SDK discards the batch on",
    )
    def test_model_version_is_sent_even_when_the_caller_omits_it(self, stub_server):
        client = aver.AverClient(
            api_key="k", stream_id="s", base_url=stub_server.url, flush_interval=0.02
        )
        try:
            # No .model() call — the caller never said what produced this.
            client.record(session_id=SESSION_ID, inputs=[], action={"type": "approve"})
            assert client.flush(timeout=5.0)
        finally:
            client.close(timeout=2.0)

        record = stub_server.requests[0]["body"]["records"][0]
        assert record["model_version"], "model_version is required by the service"


class TestTheIDTheCallerKeeps:
    """``record()``'s return value has to name the record on the ledger.

    The obvious integration is to store it on the loan application row. While
    the service minted its own id, that column was a foreign key to nothing and
    ``reconstruct`` answered 404 for it — discovered, if ever, during a dispute.
    """

    def test_the_returned_id_is_the_one_on_the_wire(self, stub_server):
        client = aver.AverClient(
            api_key="k", stream_id="s", base_url=stub_server.url, flush_interval=0.02
        )
        try:
            returned = client.record(
                session_id=SESSION_ID,
                inputs=[],
                action={"decision": "approve"},
                model_version="scorecard-4.2",
            )
            assert client.flush(timeout=5.0)
        finally:
            client.close(timeout=2.0)

        sent = stub_server.requests[0]["body"]["records"][0]["decision_id"]
        assert returned == sent, (
            "record() handed the caller an id it did not send; the service "
            "keeps the one on the wire"
        )

    def test_a_parent_link_names_the_previous_decisions_id(self, stub_server):
        """The child's parent_decision_id is the parent's own decision_id.

        The service resolves it against ids it has stored, and since it stores
        the client's, this link is one it can follow. It could not before: the
        id here named nothing on the ledger and the child was rejected 400.
        """
        client = aver.AverClient(
            api_key="k", stream_id="s", base_url=stub_server.url, flush_interval=0.02
        )
        try:
            with client.decision(session_id=SESSION_ID) as first:
                first.model(version="scorecard-4.2")
                first.record_action({"decision": "refer"})
            assert client.flush(timeout=5.0)

            with client.decision(session_id=SESSION_ID, parent=first) as second:
                second.model(version="scorecard-4.2")
                second.record_action({"decision": "approve"})
            assert client.flush(timeout=5.0)
        finally:
            client.close(timeout=2.0)

        parent = stub_server.requests[0]["body"]["records"][0]
        child = stub_server.requests[1]["body"]["records"][0]
        assert child["parent_decision_id"] == parent["decision_id"]
        assert child["parent_decision_id"] == first.decision_id
