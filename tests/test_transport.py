"""Transport: status classification, headers, and the real socket path."""

from __future__ import annotations

import json
import time

import httpx
import pytest

import aver
from aver.errors import AverTransportError
from aver.transport import Transport


def transport_returning(*responses, capture=None):
    """A Transport wired to an httpx mock that replays ``responses``."""
    calls = iter(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        if capture is not None:
            capture.append(request)
        return next(calls)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    return Transport(
        "test-key", "consumer-pl", base_url="https://example.test", client=client
    )


BATCH = [{"decision_id": "d1", "session_id": "s1"}]

#: What ``BATCH`` looks like once the transport has wrapped it. The stream sits
#: on the envelope because that is where the service reads it.
ENVELOPE = {"stream_id": "consumer-pl", "records": BATCH}


class TestClassification:
    def test_2xx_is_success(self):
        transport_returning(httpx.Response(202)).send(BATCH)

    @pytest.mark.parametrize("status", [400, 401, 403, 413, 422])
    def test_permanent_statuses(self, status):
        with pytest.raises(AverTransportError) as caught:
            transport_returning(httpx.Response(status, text="nope")).send(BATCH)
        assert caught.value.retryable is False
        assert caught.value.status_code == status

    @pytest.mark.parametrize("status", [404, 408, 425, 429, 500, 502, 503, 504])
    def test_retryable_statuses(self, status):
        """404 included: an ingress mid-deploy must not cost us records."""
        with pytest.raises(AverTransportError) as caught:
            transport_returning(httpx.Response(status)).send(BATCH)
        assert caught.value.retryable is True

    @pytest.mark.parametrize(
        "exc",
        [
            httpx.ConnectError("refused"),
            httpx.ReadTimeout("too slow"),
            httpx.RemoteProtocolError("garbage"),
        ],
    )
    def test_network_failures_are_retryable(self, exc):
        def handler(request):
            raise exc

        client = httpx.Client(transport=httpx.MockTransport(handler))
        transport = Transport(
            "k", "consumer-pl", base_url="https://example.test", client=client
        )
        with pytest.raises(AverTransportError) as caught:
            transport.send(BATCH)
        assert caught.value.retryable is True

    def test_retry_after_is_read_on_429(self):
        response = httpx.Response(429, headers={"Retry-After": "12"})
        with pytest.raises(AverTransportError) as caught:
            transport_returning(response).send(BATCH)
        assert caught.value.retry_after == 12.0

    def test_retry_after_honoured_on_503_not_just_429(self):
        """A 503 during a maintenance window carries Retry-After too."""
        response = httpx.Response(503, headers={"Retry-After": "7"})
        with pytest.raises(AverTransportError) as caught:
            transport_returning(response).send(BATCH)
        assert caught.value.retry_after == 7.0
        assert caught.value.retryable is True

    def test_unparseable_retry_after_is_ignored(self):
        response = httpx.Response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
        with pytest.raises(AverTransportError) as caught:
            transport_returning(response).send(BATCH)
        assert caught.value.retry_after is None

    def test_error_body_is_truncated_in_the_message(self):
        with pytest.raises(AverTransportError) as caught:
            transport_returning(httpx.Response(500, text="x" * 5000)).send(BATCH)
        assert len(str(caught.value)) < 300


class TestRequestShape:
    def test_headers_and_body(self):
        captured = []
        transport_returning(httpx.Response(200), capture=captured).send(BATCH)
        request = captured[0]
        assert request.url.path == "/v1/decisions"
        assert request.headers["Authorization"] == "Bearer test-key"
        # Dedupe is per record in the body, never a batch-scoped header: the
        # buffer recomposes batches on requeue, so a header key would shift.
        assert "Idempotency-Key" not in request.headers
        assert request.headers["User-Agent"].startswith(
            "aver-python/{0} python/".format(aver.__version__)
        )
        assert json.loads(request.content) == ENVELOPE

    def test_base_url_for_byoc(self):
        captured = []
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda r: (captured.append(r), httpx.Response(200))[1]
            )
        )
        Transport(
            "k", "s", base_url="https://aver.internal.bank.example/", client=client
        ).send(BATCH)
        assert str(captured[0].url) == (
            "https://aver.internal.bank.example/v1/decisions"
        )

    def test_one_unserialisable_record_does_not_sink_the_batch(self, caplog):
        """default=str handles most things; a cycle still raises.

        Encoding the batch as one document would let that single record take
        the other forty-nine down with it.
        """
        circular = {"decision_id": "bad", "inputs": []}
        circular["inputs"].append({"role": "self", "data": circular})

        batch = [
            {"decision_id": "good-1", "session_id": "s1"},
            circular,
            {"decision_id": "good-2", "session_id": "s2"},
        ]
        captured = []
        transport_returning(httpx.Response(200), capture=captured).send(batch)

        body = json.loads(captured[0].content)  # must still be valid JSON
        ids = [r["decision_id"] for r in body["records"]]
        assert ids == ["good-1", "good-2"]
        assert "could not be serialised" in caplog.text
        assert "bad" in caplog.text

    def test_batch_of_only_bad_records_sends_nothing(self):
        circular = {"decision_id": "bad"}
        circular["self"] = circular
        captured = []
        with pytest.raises(AverTransportError) as caught:
            transport_returning(httpx.Response(200), capture=captured).send([circular])
        assert captured == [], "sent a request with no records in it"
        # Permanent, and an error rather than a clean return: a bare return
        # here was indistinguishable from a delivered batch, and the buffer
        # counted every record in it as sent.
        assert caught.value.retryable is False
        assert caught.value.summary == "UnserialisableBatch"

    def test_permanent_error_carries_the_written_count(self):
        """A batch that failed part-way says how much of it landed."""
        body = {"results": [{"seq": 0}, {"seq": 1}], "error": {"code": "x"}}
        with pytest.raises(AverTransportError) as caught:
            transport_returning(httpx.Response(400, json=body)).send(BATCH)
        assert caught.value.written == 2
        # The count, never the body: stats() surfaces `summary`.
        assert caught.value.summary == "HTTP 400"

    @pytest.mark.parametrize(
        "response",
        [
            httpx.Response(400, text="not json at all"),
            httpx.Response(400, json={"error": {"code": "invalid_record"}}),
            httpx.Response(400, json=["results", 2]),
            httpx.Response(400, json={"results": "two"}),
        ],
        ids=["not-json", "no-results", "not-an-object", "results-not-a-list"],
    )
    def test_a_body_that_says_nothing_reports_nothing_written(self, response):
        """No usable `results` must read as "nothing was written", not zero."""
        with pytest.raises(AverTransportError) as caught:
            transport_returning(response).send(BATCH)
        assert caught.value.written is None

    def test_sent_indices_skip_records_that_could_not_be_encoded(self):
        """The written count indexes the wire, not the caller's batch.

        One unserialisable record shifts every position behind it. Without the
        mapping the buffer blames the wrong record for the rejection and drops
        a good one.
        """
        circular = {"decision_id": "bad"}
        circular["self"] = circular
        batch = [
            {"decision_id": "d0"},
            circular,
            {"decision_id": "d2"},
            {"decision_id": "d3"},
        ]
        with pytest.raises(AverTransportError) as caught:
            transport_returning(
                httpx.Response(400, json={"results": [{"seq": 0}]})
            ).send(batch)
        assert caught.value.sent_indices == (0, 2, 3)
        assert caught.value.written == 1

    def test_send_reports_only_the_records_that_went_out(self):
        """The count is how the buffer learns a record was dropped in here."""
        circular = {"decision_id": "bad"}
        circular["self"] = circular
        batch = [{"decision_id": "good"}, circular]
        delivered = transport_returning(httpx.Response(200)).send(batch)
        assert delivered == 1, "handed 2 records, put 1 on the wire"

    def test_undecodable_values_are_stringified_not_dropped(self):
        """A Decimal in a bureau payload must not cost us the record."""
        from decimal import Decimal

        captured = []
        transport_returning(httpx.Response(200), capture=captured).send(
            [{"decision_id": "d1", "inputs": [{"role": "r", "data": Decimal("1.5")}]}]
        )
        body = json.loads(captured[0].content)
        # A string, not 1.5. The record survives, but the service's canonical
        # encoder is handed "1.5" and never sees a number — so a DTI of 0.61 is
        # sealed into the ledger as text. Reported separately; pinned here so
        # the behaviour is at least not a surprise.
        assert body["records"][0]["inputs"][0]["data"] == "1.5"


class TestAgainstRealServer:
    def test_end_to_end_delivery(self, stub_server):
        client = aver.AverClient(
            api_key="k",
            stream_id="consumer-pl",
            policy_version="credit-policy-v4.2",
            redact=["applicant.pan"],
            base_url=stub_server.url,
            flush_interval=0.02,
        )
        try:
            with client.decision(session_id="app-77") as d:
                d.observe("form", {"applicant": {"pan": "ABCDE1234F", "amount": 5}})
                d.model(version="scorecard-v7", artifact_hash="sha256:9f2b")
                d.record_action({"type": "approve", "amount": 300000})
            assert client.flush(timeout=5.0)
        finally:
            client.close(timeout=2.0)

        body = stub_server.requests[0]["body"]
        assert body["stream_id"] == "consumer-pl"  # on the envelope, once
        records = body["records"]
        assert len(records) == 1
        record = records[0]
        assert record["session_id"] == "app-77"
        assert record["policy_version"] == "credit-policy-v4.2"
        assert record["model_version"] == "scorecard-v7"
        assert record["action"] == {"type": "approve", "amount": 300000}
        assert record["inputs"][0]["data"]["applicant"]["pan"] == "[REDACTED]"
        assert record["inputs"][0]["data"]["applicant"]["amount"] == 5
        assert record["recorded_at"].endswith("Z")
        assert client.stats()["sent"] == 1

    def test_stats_never_carries_the_response_body(self, stub_server):
        """stats() gets surfaced on dashboards; error bodies can echo records."""
        marker = "PAN-ABCDE1234F-ECHOED-BY-SERVER"
        stub_server.set_status(500, body={"error": "rejected", "echo": marker})
        client = aver.AverClient(
            api_key="k",
            stream_id="s",
            base_url=stub_server.url,
            flush_interval=0.02,
            batch_size=1,
        )
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            deadline = time.time() + 3
            while client.stats()["failed_batches"] == 0 and time.time() < deadline:
                time.sleep(0.01)
        finally:
            client.close(timeout=0.5)

        stats = client.stats()
        assert stats["failed_batches"] > 0, "the 500 never landed"
        assert marker not in json.dumps(stats)
        assert stats["last_error"] == "HTTP 500"

    def test_server_500_does_not_reach_the_caller(self, stub_server):
        stub_server.set_status(500)
        client = aver.AverClient(
            api_key="k",
            stream_id="s",
            base_url=stub_server.url,
            flush_interval=0.02,
            batch_size=1,
        )
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            client.flush(timeout=0.3)
        finally:
            client.close(timeout=0.3)
        assert client.stats()["failed_batches"] > 0
        assert client.stats()["sent"] == 0
