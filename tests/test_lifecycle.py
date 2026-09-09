"""Shutdown, chaining, and the async surface."""

from __future__ import annotations

import subprocess
import sys
import textwrap
import threading
import time

import pytest

from aver import AsyncAverClient, AverClient, AverClientClosed
from aver.errors import AverConfigError
from conftest import FakeTransport


class TestAtExit:
    def test_record_survives_interpreter_exit_without_flush(self, stub_server):
        """No flush(), no close() — the atexit handler has to do it."""
        script = textwrap.dedent(
            """
            import sys
            sys.path.insert(0, {repo!r})
            from aver import AverClient

            aver = AverClient(api_key="k", stream_id="s", base_url={url!r})
            with aver.decision(session_id="exiting-app") as d:
                d.observe("form", {{"amount": 42}})
                d.record_action({{"type": "approve"}})
            # deliberately falls off the end: no flush, no close
            """
        ).format(repo=str(__import__("pathlib").Path(__file__).parent.parent), url=stub_server.url)

        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=30
        )
        assert result.returncode == 0, result.stderr

        records = stub_server.records()
        assert len(records) == 1
        assert records[0]["session_id"] == "exiting-app"
        assert records[0]["action"] == {"type": "approve"}

    def test_close_wakes_a_parked_flusher(self):
        """Regression: close() must not lose records to the flush interval.

        The flusher parks in ``cv.wait(flush_interval)``. If ``close()``
        notifies before setting the closing flag, the waiter re-checks a flag
        that is still unset and sleeps out the rest of the interval — the
        drain never runs and the records vanish with nothing in ``dropped``.
        ``SlowEvent`` widens that scheduling window so the race is
        deterministic instead of one-in-a-thousand.
        """

        class SlowEvent(threading.Event):
            def set(self):
                time.sleep(0.25)
                super().set()

        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=5.0
        )
        client._buffer._closing = SlowEvent()
        time.sleep(0.3)  # let the flusher park in its wait
        client.record(session_id="APP-CRITICAL", inputs=[], action={"type": "approve"})
        client.close(timeout=1.0)

        delivered = transport.delivered_records()
        assert len(delivered) == 1, "record lost to the flush interval"
        assert delivered[0]["session_id"] == "APP-CRITICAL"
        assert client.stats()["dropped"] == 0

    def test_close_is_idempotent(self):
        transport = FakeTransport()
        client = AverClient(api_key="k", stream_id="s", transport=transport)
        client.close(timeout=0.5)
        client.close(timeout=0.5)
        assert transport.closed

    def test_records_after_close_raise(self, caplog):
        """Losing a record here is as definitive as buffer overflow."""
        transport = FakeTransport()
        client = AverClient(api_key="k", stream_id="s", transport=transport)
        client.close(timeout=0.5)
        with pytest.raises(AverClientClosed):
            client.record(session_id="late", inputs=[], action={"type": "approve"})
        assert client.stats()["dropped"] == 1
        assert "already closed" in caplog.text

    def test_decision_block_after_close_raises(self):
        client = AverClient(api_key="k", stream_id="s", transport=FakeTransport())
        client.close(timeout=0.5)
        with pytest.raises(AverClientClosed):
            with client.decision(session_id="late") as d:
                d.record_action({"type": "approve"})

    def test_client_as_context_manager_flushes(self):
        transport = FakeTransport()
        with AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        ) as client:
            client.record(session_id="app-1", inputs=[], action={"type": "approve"})
        assert len(transport.delivered_records()) == 1


class TestChaining:
    def test_parent_accepts_a_decision_id(self):
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            with client.decision(session_id="app-1") as d:
                d.observe("form", {"a": 1})
                d.record_action({"type": "fetch_bureau"})
            step0 = d.decision_id
            assert step0 is not None

            with client.decision(session_id="app-1", parent=step0) as d2:
                d2.record_action({"type": "approve"})

            assert client.flush(timeout=2.0)
            records = {r["decision_id"]: r for r in transport.delivered_records()}
            assert records[d2.decision_id]["parent_decision_id"] == step0
            assert records[step0]["parent_decision_id"] is None
        finally:
            client.close(timeout=0.5)

    def test_parent_accepts_the_recorder_object(self):
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            with client.decision(session_id="app-1") as first:
                first.record_action({"type": "fetch_bureau"})
            with client.decision(session_id="app-1", parent=first) as second:
                second.record_action({"type": "approve"})
            assert client.flush(timeout=2.0)
            records = {r["decision_id"]: r for r in transport.delivered_records()}
            assert (
                records[second.decision_id]["parent_decision_id"] == first.decision_id
            )
        finally:
            client.close(timeout=0.5)

    def test_nested_blocks_do_not_put_a_repr_in_the_chain(self, caplog):
        """A recorder only has an id once it leaves its block.

        Nesting instead of sequencing used to record
        ``"<aver.decision.DecisionRecorder object at 0x...>"`` as the parent —
        a chain link that can never be resolved.
        """
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            with client.decision(session_id="app-1") as outer:
                outer.record_action({"type": "fetch_bureau"})
                with client.decision(session_id="app-1", parent=outer) as inner:
                    inner.record_action({"type": "approve"})
            assert client.flush(timeout=2.0)
            parents = [r["parent_decision_id"] for r in transport.delivered_records()]
            assert parents == [None, None]
            assert "has not left its block" in caplog.text
        finally:
            client.close(timeout=0.5)

    def test_decision_id_is_none_before_the_block_exits(self):
        client = AverClient(api_key="k", stream_id="s", transport=FakeTransport())
        try:
            with client.decision(session_id="app-1") as d:
                assert d.decision_id is None
                d.record_action({"type": "approve"})
            assert d.decision_id is not None
        finally:
            client.close(timeout=0.5)

    def test_block_without_an_action_is_still_recorded(self, caplog):
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            for i in range(3):
                with client.decision(session_id="app-{0}".format(i)) as d:
                    d.observe("form", {"a": i})
            assert client.flush(timeout=2.0)
            records = transport.delivered_records()
            assert len(records) == 3
            assert records[0]["action"] == {"type": "unspecified"}
            # Loud enough to notice, quiet enough not to flood their logs.
            assert len([r for r in caplog.records if "unspecified" in r.message]) == 1
        finally:
            client.close(timeout=0.5)


class TestConfig:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"api_key": "", "stream_id": "s"},
            {"api_key": "k", "stream_id": ""},
        ],
    )
    def test_missing_required_config_raises_at_construction(self, kwargs):
        with pytest.raises(AverConfigError):
            AverClient(transport=FakeTransport(), **kwargs)

    def test_bad_redact_path_raises_at_construction(self):
        with pytest.raises(AverConfigError):
            AverClient(
                api_key="k", stream_id="s", redact=[""], transport=FakeTransport()
            )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"flush_interval": 0},
            {"flush_interval": -1},
            {"batch_size": 0},
            {"max_records": 0},
        ],
    )
    def test_degenerate_tuning_refused(self, kwargs):
        """flush_interval=0 spins a core; batch_size=0 spins and delivers nothing."""
        with pytest.raises(AverConfigError):
            AverClient(
                api_key="k", stream_id="s", transport=FakeTransport(), **kwargs
            )

    def test_closed_client_is_collectable(self):
        """close() must release the atexit reference, or clients accumulate."""
        import gc
        import weakref

        refs = []
        for _ in range(5):
            client = AverClient(api_key="k", stream_id="s", transport=FakeTransport())
            refs.append(weakref.ref(client))
            client.close(timeout=0.2)
            del client
        gc.collect()
        assert all(ref() is None for ref in refs)

    def test_uncopyable_input_is_dropped_loudly_not_sent(self, caplog):
        """We cannot snapshot it, so we must not send it — and must say so."""
        transport = FakeTransport()
        client = AverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            result = client.record(
                session_id="app-1",
                inputs=[{"role": "conn", "value": threading.Lock()}],
                action={"type": "approve"},
            )
            assert result is None
            assert client.flush(timeout=1.0)
            assert transport.delivered_records() == []
            assert client.stats()["dropped"] == 1
            assert "dropped decision" in caplog.text
        finally:
            client.close(timeout=0.5)


class TestAsyncParity:
    async def test_async_context_manager(self):
        transport = FakeTransport()
        client = AsyncAverClient(
            api_key="k",
            stream_id="s",
            redact=["applicant.pan"],
            transport=transport,
            flush_interval=0.02,
        )
        try:
            async with client.decision(session_id="app-1") as d:
                d.observe("form", {"applicant": {"pan": "ABCDE1234F"}})
                d.model(version="scorecard-v7")
                d.record_action({"type": "approve"})
            assert await client.flush(timeout=2.0)
            record = transport.delivered_records()[0]
            assert record["session_id"] == "app-1"
            assert record["inputs"][0]["value"]["applicant"]["pan"] == "[REDACTED]"
            assert d.decision_id is not None
        finally:
            await client.close(timeout=0.5)

    async def test_exception_inside_async_block_is_recorded_and_reraised(self):
        transport = FakeTransport()
        client = AsyncAverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        )
        try:
            with pytest.raises(RuntimeError):
                async with client.decision(session_id="app-1") as d:
                    d.observe("form", {"a": 1})
                    raise RuntimeError("bureau down")
            assert await client.flush(timeout=2.0)
            action = transport.delivered_records()[0]["action"]
            assert action["type"] == "error"
            assert "bureau down" in action["detail"]
        finally:
            await client.close(timeout=0.5)

    async def test_record_does_not_await(self):
        """Recording stays synchronous: nothing to await, nothing to forget."""
        transport = FakeTransport()
        async with AsyncAverClient(
            api_key="k", stream_id="s", transport=transport, flush_interval=0.02
        ) as client:
            result = client.record(
                session_id="app-1", inputs=[], action={"type": "approve"}
            )
            assert isinstance(result, str)  # an id, not a coroutine
            assert await client.flush(timeout=2.0)
        assert len(transport.delivered_records()) == 1

    async def test_sync_with_is_refused(self):
        client = AsyncAverClient(
            api_key="k", stream_id="s", transport=FakeTransport()
        )
        try:
            with pytest.raises(TypeError):
                with client:
                    pass
        finally:
            await client.close(timeout=0.5)
