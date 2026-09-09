"""The hard constraints from the spec, one test class each.

If any of these fail, the library is not safe to put in a lender's decision
path, whatever else works.
"""

from __future__ import annotations

import json
import threading
import time

import pytest

from aver import AverBufferFull, AverClient
from aver.errors import AverTransportError
from conftest import FakeTransport, fail_times


def make_client(transport=None, **kwargs):
    kwargs.setdefault("flush_interval", 0.02)
    return AverClient(
        api_key="test-key",
        stream_id="consumer-pl",
        transport=transport or FakeTransport(),
        **kwargs
    )


class TestNeverBlocks:
    """Constraint 1: recording must not slow the caller's decision down."""

    def test_record_returns_while_transport_hangs(self):
        transport = FakeTransport(behaviour=lambda _n: time.sleep(5))
        client = make_client(transport, batch_size=1)
        try:
            client.record(
                session_id="app-1",
                inputs=[{"role": "form", "value": {"a": 1}}],
                action={"type": "approve"},
            )
            # Let the flusher pick it up and get stuck in the 5s send.
            time.sleep(0.1)
            start = time.perf_counter()
            for i in range(100):
                client.record(
                    session_id="app-{0}".format(i),
                    inputs=[{"role": "form", "value": {"a": i}}],
                    action={"type": "approve"},
                )
            elapsed = time.perf_counter() - start
            assert elapsed / 100 < 0.001, "record() averaged {0:.4f}s".format(
                elapsed / 100
            )
        finally:
            client.close(timeout=0.1)

    def test_realistic_payload_stays_well_inside_budget(self):
        """Regression guard, not a target.

        The deep copy runs on the caller's thread — the one place the
        non-blocking guarantee is a matter of degree rather than architecture.
        It must not drift as payloads grow.
        """
        import random
        import string

        blob = "".join(random.choices(string.ascii_letters, k=200))
        big = {
            "accounts": [
                {"id": "acc-%d" % i, "balance": i * 37, "notes": blob}
                for i in range(2200)
            ],
            "bureau_report": {"history": [{"m": i} for i in range(500)]},
        }
        assert len(json.dumps(big)) > 500_000, "payload is not big enough to guard"

        transport = FakeTransport()
        client = make_client(transport, max_records=100)
        try:
            start = time.perf_counter()
            client.record(
                session_id="app-1",
                inputs=[{"role": "cibil", "value": big}],
                action={"type": "approve"},
            )
            elapsed = time.perf_counter() - start
            assert elapsed < 0.05, "record() took {0:.1f}ms".format(elapsed * 1000)
        finally:
            client.close(timeout=1.0)

    def test_decision_block_returns_while_transport_hangs(self):
        transport = FakeTransport(behaviour=lambda _n: time.sleep(5))
        client = make_client(transport, batch_size=1)
        try:
            start = time.perf_counter()
            with client.decision(session_id="app-1") as d:
                d.observe("form", {"a": 1})
                d.record_action({"type": "deny"})
            assert time.perf_counter() - start < 0.05
        finally:
            client.close(timeout=0.1)


class TestNeverRaises:
    """Constraint 2: no transport failure reaches the caller."""

    @pytest.mark.parametrize(
        "behaviour",
        [
            pytest.param(fail_times(1000), id="always-retryable"),
            pytest.param(
                fail_times(1000, retryable=False, status=400), id="permanent-400"
            ),
            pytest.param(
                fail_times(1000, retryable=False, status=401), id="permanent-401"
            ),
            pytest.param(fail_times(1000, status=500), id="server-500"),
            pytest.param(fail_times(1000, status=503), id="server-503"),
            pytest.param(
                lambda _n: (_ for _ in ()).throw(ValueError("malformed JSON")),
                id="unexpected-exception",
            ),
            pytest.param(lambda _n: time.sleep(0.2), id="slow"),
        ],
    )
    def test_nothing_propagates(self, behaviour):
        client = make_client(FakeTransport(behaviour=behaviour), batch_size=1)
        try:
            for i in range(5):
                client.record(
                    session_id="app-{0}".format(i),
                    inputs=[{"role": "form", "value": {"i": i}}],
                    action={"type": "approve"},
                )
                with client.decision(session_id="ctx-{0}".format(i)) as d:
                    d.observe("form", {"i": i})
                    d.record_action({"type": "deny"})
            client.flush(timeout=0.3)
            assert client.stats()["queued"] >= 0  # still answering, not wedged
        finally:
            client.close(timeout=0.2)

    def test_exception_after_an_action_preserves_that_action(self):
        """The most valuable record in the set: decided, then something broke.

        The decision was never written anywhere else, so overwriting it with
        the error loses it permanently.
        """
        transport = FakeTransport()
        client = make_client(transport)
        try:
            with pytest.raises(RuntimeError):
                with client.decision(session_id="app-1") as d:
                    d.observe("form", {"a": 1})
                    d.record_action({"type": "deny", "reason_code": "DTI_EXCEEDED"})
                    raise RuntimeError("notification service down")

            client.flush(timeout=1.0)
            action = transport.delivered_records()[0]["action"]
            assert action["type"] == "error"
            assert "notification service down" in action["detail"]
            assert action["attempted_action"]["reason_code"] == "DTI_EXCEEDED"
            assert action["attempted_action"]["type"] == "deny"
        finally:
            client.close(timeout=0.5)

    def test_exception_with_no_action_has_no_attempted_action(self):
        transport = FakeTransport()
        client = make_client(transport)
        try:
            with pytest.raises(RuntimeError):
                with client.decision(session_id="app-1") as d:
                    d.observe("form", {"a": 1})
                    raise RuntimeError("bureau down")
            client.flush(timeout=1.0)
            action = transport.delivered_records()[0]["action"]
            assert action["type"] == "error"
            assert "attempted_action" not in action
        finally:
            client.close(timeout=0.5)

    def test_caller_exception_reraises_unchanged(self):
        transport = FakeTransport()
        client = make_client(transport)
        sentinel = ValueError("bureau timeout")
        try:
            with pytest.raises(ValueError) as caught:
                with client.decision(session_id="app-1") as d:
                    d.observe("form", {"a": 1})
                    raise sentinel
            assert caught.value is sentinel

            client.flush(timeout=1.0)
            record = transport.delivered_records()[0]
            # A decision that crashed is still a decision that needs explaining.
            assert record["action"]["type"] == "error"
            assert "bureau timeout" in record["action"]["detail"]
            assert record["inputs"][0]["role"] == "form"
        finally:
            client.close(timeout=0.5)


class TestFailsLoudlyOnDataLoss:
    """Constraint 3: overflow is the one thing the caller must hear about."""

    def test_overflow_raises(self):
        # batch_size above capacity keeps the flusher asleep for the whole
        # test, so the bound is reached deterministically.
        client = make_client(
            FakeTransport(), max_records=10, batch_size=1000, flush_interval=3600
        )
        try:
            for i in range(10):
                client.record(
                    session_id="app-{0}".format(i),
                    inputs=[],
                    action={"type": "approve"},
                )
            with pytest.raises(AverBufferFull):
                client.record(
                    session_id="overflow", inputs=[], action={"type": "approve"}
                )
            assert client.stats()["dropped"] == 1
        finally:
            client.close(timeout=0.1)

    def test_overflow_inside_decision_block_reaches_caller(self):
        client = make_client(
            FakeTransport(), max_records=1, batch_size=1000, flush_interval=3600
        )
        try:
            client.record(session_id="app-0", inputs=[], action={"type": "approve"})
            with pytest.raises(AverBufferFull):
                with client.decision(session_id="app-1") as d:
                    d.record_action({"type": "deny"})
        finally:
            client.close(timeout=0.1)

    def test_caller_exception_wins_over_buffer_full(self, caplog):
        """Both went wrong; the caller's exception is the one they must see."""
        client = make_client(
            FakeTransport(), max_records=1, batch_size=1000, flush_interval=3600
        )
        try:
            client.record(session_id="app-0", inputs=[], action={"type": "approve"})
            with pytest.raises(ZeroDivisionError):
                with client.decision(session_id="app-1") as d:
                    d.observe("form", {"a": 1})
                    1 / 0
            assert "AverBufferFull" in caplog.text
        finally:
            client.close(timeout=0.1)

    def test_permanent_failure_counts_as_dropped(self):
        transport = FakeTransport(fail_times(1000, retryable=False, status=400))
        client = make_client(transport, batch_size=1)
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            deadline = time.time() + 2
            while client.stats()["dropped"] == 0 and time.time() < deadline:
                time.sleep(0.01)
            stats = client.stats()
            assert stats["dropped"] == 1
            assert stats["sent"] == 0
            # Summary only: stats() must not carry the server's response body.
            assert stats["last_error"] == "HTTP 400"
            # Permanent means permanent: exactly one attempt, no retry storm.
            assert transport.attempt_count == 1
        finally:
            client.close(timeout=0.2)


class TestIdempotency:
    """A retried record must not become a second ledger entry."""

    def test_same_key_across_retries(self):
        transport = FakeTransport(behaviour=fail_times(2))
        client = make_client(transport, batch_size=1)
        try:
            client.record(
                session_id="app-1",
                inputs=[{"role": "form", "value": {"a": 1}}],
                action={"type": "approve"},
            )
            assert client.flush(timeout=3.0)

            assert transport.attempt_count == 3
            keys = {a.records[0]["idempotency_key"] for a in transport.attempts}
            assert len(keys) == 1, "record key changed between retries"
        finally:
            client.close(timeout=0.5)

    def test_key_survives_requeue_and_rebatching(self, monkeypatch):
        """The case a batch-scoped key could not cover.

        Retrying the *same* batch is the easy half. The hard half is a requeue:
        the failed batch goes back to the front of the queue and the next take
        composes a **different** batch — different membership, different
        boundaries. A key scoped to the request changes underneath the server
        there, and the record lands as a second ledger entry.

        Constructed so that record A provably travels in two differently-sized
        batches: [A, B] on the failing attempt, then [A, B, C, D] after C and
        D arrive during the backoff and close() forces the requeue and drain.
        """
        monkeypatch.setattr("aver.buffer.INITIAL_BACKOFF", 0.30)
        monkeypatch.setattr("aver.buffer.MAX_BACKOFF", 0.30)

        transport = FakeTransport(behaviour=fail_times(1))
        client = make_client(transport, batch_size=4, flush_interval=0.05)
        client.record(session_id="A", inputs=[], action={"type": "approve"})
        client.record(session_id="B", inputs=[], action={"type": "approve"})
        time.sleep(0.15)  # flusher takes [A, B], fails, enters backoff
        client.record(session_id="C", inputs=[], action={"type": "approve"})
        client.record(session_id="D", inputs=[], action={"type": "approve"})
        client.close(timeout=2.0)  # interrupts the backoff: requeue, then drain

        carrying_a = [
            a for a in transport.attempts
            if any(r["session_id"] == "A" for r in a.records)
        ]
        sizes = sorted(len(a.records) for a in carrying_a)
        # Guard the construction itself: if A only ever rode one batch shape,
        # this test is not exercising rebatching and proves nothing.
        assert sizes == [2, 4], "A rode batches {0}, expected [2, 4]".format(sizes)

        by_session = {}
        for attempt in transport.attempts:
            for record in attempt.records:
                by_session.setdefault(record["session_id"], set()).add(
                    record["idempotency_key"]
                )
        for session, keys in by_session.items():
            assert len(keys) == 1, "{0} changed key across batches".format(session)

        # The same guarantee stated directly: every delivered record carries
        # the key it was given on its very first attempt.
        first_attempt = {
            r["session_id"]: r["idempotency_key"]
            for r in transport.attempts[0].records
        }
        delivered = {
            r["session_id"]: r["idempotency_key"]
            for r in transport.delivered_records()
        }
        for session, key in first_attempt.items():
            assert delivered[session] == key, "{0} was redelivered under a new " \
                "key; the server would write it twice".format(session)
        assert sorted(delivered) == ["A", "B", "C", "D"]

    def test_distinct_records_get_distinct_keys(self):
        transport = FakeTransport()
        client = make_client(transport)
        try:
            for i in range(20):
                client.record(
                    session_id="app-{0}".format(i),
                    inputs=[],
                    action={"type": "approve"},
                )
            assert client.flush(timeout=2.0)
            records = transport.delivered_records()
            assert len({r["idempotency_key"] for r in records}) == 20
            assert len({r["decision_id"] for r in records}) == 20
        finally:
            client.close(timeout=0.5)


class TestThreadSafety:
    def test_fifty_threads_lose_nothing(self):
        transport = FakeTransport()
        client = make_client(transport, max_records=5000)
        threads = []
        per_thread = 20
        try:

            def worker(tid: int) -> None:
                for i in range(per_thread):
                    with client.decision(session_id="t{0}-{1}".format(tid, i)) as d:
                        d.observe("form", {"tid": tid, "i": i})
                        d.record_action({"type": "approve"})

            for tid in range(50):
                t = threading.Thread(target=worker, args=(tid,))
                threads.append(t)
                t.start()
            for t in threads:
                t.join()

            assert client.flush(timeout=10.0)
            records = transport.delivered_records()
            sessions = [r["session_id"] for r in records]
            assert len(sessions) == 50 * per_thread
            assert len(set(sessions)) == 50 * per_thread  # none lost, none doubled
        finally:
            client.close(timeout=1.0)


class TestFlushAndStats:
    def test_stats_shape_and_counters(self):
        transport = FakeTransport(behaviour=fail_times(1))
        client = make_client(transport, batch_size=1)
        try:
            assert set(client.stats()) == {
                "queued",
                "sent",
                "failed",
                "dropped",
                "last_error",
                "last_success_at",
            }
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            assert client.flush(timeout=3.0)
            stats = client.stats()
            assert stats["sent"] == 1
            assert stats["failed"] == 1
            assert stats["dropped"] == 0
            assert stats["queued"] == 0
            assert stats["last_success_at"] is not None
        finally:
            client.close(timeout=0.5)

    def test_logger_reports_start_flush_retry_and_drop(self, caplog):
        """The four levels the platform team is told to watch."""
        import logging

        with caplog.at_level(logging.INFO, logger="aver"):
            transport = FakeTransport(behaviour=fail_times(1))
            client = make_client(transport, batch_size=1)
            try:
                client.record(
                    session_id="app-1", inputs=[], action={"type": "approve"}
                )
                assert client.flush(timeout=3.0)
            finally:
                client.close(timeout=0.5)

        by_level = {
            level: [r.message for r in caplog.records if r.levelname == level]
            for level in ("INFO", "WARNING", "ERROR")
        }
        assert any("delivery started" in m for m in by_level["INFO"])
        assert any("flushed 1 record" in m for m in by_level["INFO"])
        assert any("retrying" in m for m in by_level["WARNING"])
        assert not by_level["ERROR"]

    def test_last_error_is_safe_when_there_is_no_status_code(self):
        """The branch the status-code test cannot reach.

        With a status code, ``summary`` short-circuits to "HTTP nnn" and the
        message is never consulted — so that path proves nothing about the
        message. A network error or an unexpected exception has no status
        code, and ``summary`` used to fall through to the full message there.
        """
        marker = "PAN-ABCDE1234F-IN-THE-EXCEPTION-TEXT"

        def network_error(_n):
            raise AverTransportError(
                "ConnectError: failed posting {0}".format(marker),
                retryable=False,
                kind="ConnectError",
            )

        client = make_client(FakeTransport(behaviour=network_error), batch_size=1)
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            deadline = time.time() + 2
            while client.stats()["dropped"] == 0 and time.time() < deadline:
                time.sleep(0.01)
            stats = client.stats()
            assert stats["dropped"] == 1
            assert marker not in json.dumps(stats)
            assert stats["last_error"] == "ConnectError"
        finally:
            client.close(timeout=0.2)

    def test_last_error_is_safe_for_an_unexpected_exception(self):
        """A transport bug must not leak its message into stats() either."""
        marker = "PAN-ABCDE1234F-IN-A-BUG"

        def boom(_n):
            raise ValueError("serialiser blew up on {0}".format(marker))

        client = make_client(FakeTransport(behaviour=boom), batch_size=1)
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            deadline = time.time() + 2
            while client.stats()["dropped"] == 0 and time.time() < deadline:
                time.sleep(0.01)
            stats = client.stats()
            assert marker not in json.dumps(stats)
            assert stats["last_error"] == "ValueError"
        finally:
            client.close(timeout=0.2)

    def test_last_error_never_carries_the_response_body(self):
        """stats() gets exposed on health endpoints; bodies can echo records."""
        echoed = (
            'invalid record: {"inputs": [{"value": {"employer": "Acme Bank"}}]}'
        )

        def behaviour(_n):
            raise AverTransportError(
                "HTTP 400 (permanent): " + echoed, retryable=False, status_code=400
            )

        client = make_client(FakeTransport(behaviour=behaviour), batch_size=1)
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            deadline = time.time() + 2
            while client.stats()["dropped"] == 0 and time.time() < deadline:
                time.sleep(0.01)
            assert client.stats()["last_error"] == "HTTP 400"
            assert "Acme Bank" not in client.stats()["last_error"]
        finally:
            client.close(timeout=0.2)

    def test_input_values_never_reach_the_log(self, caplog):
        """Logging inputs would defeat the redaction the caller configured."""
        import logging

        with caplog.at_level(logging.DEBUG, logger="aver"):
            transport = FakeTransport(behaviour=fail_times(1, retryable=False))
            client = make_client(transport, batch_size=1)
            try:
                client.record(
                    session_id="app-1",
                    inputs=[{"role": "form", "value": {"pan": "ABCDE1234F"}}],
                    action={"type": "deny", "reason_code": "SECRET_RULE"},
                )
                client.flush(timeout=1.0)
            finally:
                client.close(timeout=0.5)
        assert "ABCDE1234F" not in caplog.text
        assert "SECRET_RULE" not in caplog.text

    def test_flush_returns_false_on_timeout(self):
        client = make_client(
            FakeTransport(behaviour=lambda _n: time.sleep(5)), batch_size=1
        )
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            assert client.flush(timeout=0.05) is False
        finally:
            client.close(timeout=0.1)

    def test_retry_after_is_respected(self, monkeypatch):
        from aver.buffer import RecordBuffer

        waits = []
        original = RecordBuffer._sleep

        def spy(self, seconds, deadline):
            waits.append(seconds)
            return original(self, 0.0, deadline)  # record it, don't actually wait

        monkeypatch.setattr(RecordBuffer, "_sleep", spy)

        def behaviour(attempt):
            if attempt == 1:
                raise AverTransportError(
                    "throttled", retryable=True, status_code=429, retry_after=0.25
                )

        transport = FakeTransport(behaviour=behaviour)
        client = make_client(transport, batch_size=1)
        try:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
            assert client.flush(timeout=3.0)
        finally:
            client.close(timeout=0.5)
        # 0.25 came from Retry-After, not from the 0.01 backoff schedule.
        assert waits and waits[0] == pytest.approx(0.25)
