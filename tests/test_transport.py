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
    return Transport("test-key", base_url="https://example.test", client=client)


BATCH = [{"decision_id": "d1", "session_id": "s1"}]


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
        transport = Transport("k", base_url="https://example.test", client=client)
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
        assert request.url.path == "/v1/records"
        assert request.headers["Authorization"] == "Bearer test-key"
        # Dedupe is per record in the body, never a batch-scoped header: the
        # buffer recomposes batches on requeue, so a header key would shift.
        assert "Idempotency-Key" not in request.headers
        assert request.headers["User-Agent"].startswith(
            "aver-python/{0} python/".format(aver.__version__)
        )
        assert json.loads(request.content) == {"records": BATCH}

    def test_base_url_for_byoc(self):
        captured = []
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda r: (captured.append(r), httpx.Response(200))[1]
            )
        )
        Transport(
            "k", base_url="https://aver.internal.bank.example/", client=client
        ).send(BATCH)
        assert str(captured[0].url) == (
            "https://aver.internal.bank.example/v1/records"
        )

    def test_one_unserialisable_record_does_not_sink_the_batch(self, caplog):
        """default=str handles most things; a cycle still raises.

        Encoding the batch as one document would let that single record take
        the other forty-nine down with it.
        """
        circular = {"decision_id": "bad", "inputs": []}
        circular["inputs"].append({"role": "self", "value": circular})

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
        transport_returning(httpx.Response(200), capture=captured).send([circular])
        assert captured == [], "sent a request with no records in it"

    def test_undecodable_values_are_stringified_not_dropped(self):
        """A Decimal in a bureau payload must not cost us the record."""
        from decimal import Decimal

        captured = []
        transport_returning(httpx.Response(200), capture=captured).send(
            [{"decision_id": "d1", "inputs": [{"role": "r", "value": Decimal("1.5")}]}]
        )
        body = json.loads(captured[0].content)
        assert body["records"][0]["inputs"][0]["value"] == "1.5"


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

        records = stub_server.records()
        assert len(records) == 1
        record = records[0]
        assert record["session_id"] == "app-77"
        assert record["stream_id"] == "consumer-pl"
        assert record["policy_version"] == "credit-policy-v4.2"
        assert record["model_version"] == "scorecard-v7"
        assert record["action"] == {"type": "approve", "amount": 300000}
        assert record["inputs"][0]["value"]["applicant"]["pan"] == "[REDACTED]"
        assert record["inputs"][0]["value"]["applicant"]["amount"] == 5
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
            while client.stats()["failed"] == 0 and time.time() < deadline:
                time.sleep(0.01)
        finally:
            client.close(timeout=0.5)

        stats = client.stats()
        assert stats["failed"] > 0, "the 500 never landed"
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
        assert client.stats()["failed"] > 0
        assert client.stats()["sent"] == 0
