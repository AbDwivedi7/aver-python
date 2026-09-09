"""Bounded buffer and background delivery.

``put`` is the only method on the caller's decision path, and all it does is
append under a lock. Everything expensive — connections, retries, backoff —
happens on the flusher thread.

The buffer is deliberately *not* a ``deque(maxlen=...)``. A maxlen deque
evicts silently, and a silently dropped record is the failure mode this
library exists to prevent: the customer believes they have audit coverage
they do not have. We check the bound ourselves and raise.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List, Optional, Sequence

from ._types import Record, Stats
from .errors import (
    AverBufferFull, AverClientClosed, AverConfigError, AverTransportError,
)

log = logging.getLogger("aver")

MAX_RECORDS = 10_000
BATCH_SIZE = 50
FLUSH_INTERVAL = 2.0
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0
CLOSE_TIMEOUT = 10.0

_POLL = 0.005


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class RecordBuffer:
    """Holds records until the flusher thread delivers them."""

    def __init__(
        self,
        transport,
        *,
        max_records: int = MAX_RECORDS,
        batch_size: int = BATCH_SIZE,
        flush_interval: float = FLUSH_INTERVAL,
        max_backoff: float = MAX_BACKOFF,
    ) -> None:
        # Degenerate values are not merely useless: flush_interval=0 spins the
        # flusher on a full core, and batch_size=0 spins it *and* delivers
        # nothing at all. Both are plausible typos, so refuse them at
        # construction where the caller's tests will catch it.
        if max_records < 1 or batch_size < 1 or flush_interval <= 0:
            raise AverConfigError(
                "max_records and batch_size must be >= 1 and flush_interval "
                "> 0; got max_records={0}, batch_size={1}, flush_interval={2}"
                .format(max_records, batch_size, flush_interval)
            )
        self._transport = transport
        self._max = max_records
        self._batch_size = batch_size
        self._interval = flush_interval
        self._max_backoff = max_backoff

        self._q: Deque[Record] = deque()
        self._cv = threading.Condition()
        self._closing = threading.Event()
        self._flush_now = False
        self._inflight = 0
        self._closed = False
        self._close_deadline = 0.0

        self._sent = 0
        self._failed = 0
        self._dropped = 0
        self._last_error: Optional[str] = None
        self._last_success_at: Optional[str] = None

        self._thread = threading.Thread(
            target=self._run, name="aver-flusher", daemon=True
        )
        self._thread.start()

    # -- caller side -----------------------------------------------------

    def put(self, record: Record) -> None:
        """Queue a record.

        Raises rather than losing it: ``AverClientClosed`` if the client is
        already shut down, ``AverBufferFull`` if delivery is not keeping up.
        """
        with self._cv:
            if self._closed:
                self._dropped += 1
                self._last_error = "client closed"
                log.error(
                    "aver: record dropped, client already closed "
                    "(session_id=%s)",
                    record.get("session_id"),
                )
                raise AverClientClosed(
                    "aver client is closed: this record was not recorded"
                )
            if len(self._q) >= self._max:
                self._dropped += 1
                self._last_error = "buffer full ({0} records)".format(self._max)
                raise AverBufferFull(
                    "aver buffer is full ({0} records): delivery is not keeping "
                    "up and records are being lost".format(self._max)
                )
            self._q.append(record)
            if len(self._q) >= self._batch_size:
                self._cv.notify()

    def note_dropped(self, reason: str, count: int = 1) -> None:
        """Account for a record lost before it ever reached the queue."""
        with self._cv:
            self._dropped += count
            self._last_error = reason

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Block until the buffer drains. True if it drained, False on timeout.

        ``timeout=None`` waits indefinitely, so it will not return while Aver
        is unreachable and records are still being retried. Pass a timeout in
        anything that must make progress regardless.
        """
        end = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._cv:
                if not self._q and not self._inflight:
                    return True
                if not self._thread.is_alive():
                    return False
                self._flush_now = True
                self._cv.notify_all()
            if end is not None and time.monotonic() >= end:
                return False
            time.sleep(_POLL)

    def close(self, timeout: float = CLOSE_TIMEOUT) -> None:
        """Stop accepting records and give the flusher ``timeout`` to drain."""
        with self._cv:
            if self._closed:
                return
            self._closed = True
            self._close_deadline = time.monotonic() + timeout
            # Set the flag *before* notifying, and while holding the lock. A
            # notified waiter re-checks its condition the moment it re-acquires
            # the lock; if the flag were still unset it would go back to sleep
            # for the rest of the flush interval and miss the drain entirely.
            self._closing.set()
            self._cv.notify_all()
        self._thread.join(timeout + 1.0)
        if self._thread.is_alive():
            log.error(
                "aver: delivery thread did not stop within %.1fs; "
                "%d record(s) may be undelivered",
                timeout,
                len(self._q),
            )
        self._transport.close()

    def stats(self) -> Stats:
        with self._cv:
            return {
                "queued": len(self._q) + self._inflight,
                "sent": self._sent,
                "failed": self._failed,
                "dropped": self._dropped,
                "last_error": self._last_error,
                "last_success_at": self._last_success_at,
            }

    # -- flusher thread --------------------------------------------------

    def _run(self) -> None:
        log.info(
            "aver: delivery started (batch=%d, interval=%.1fs, capacity=%d)",
            self._batch_size,
            self._interval,
            self._max,
        )
        while not self._closing.is_set():
            batch = self._take_batch()
            if batch:
                self._deliver(batch, None)
        self._drain()

    def _take_batch(self) -> List[Record]:
        """Wait for a full batch or the flush interval, whichever comes first."""
        with self._cv:
            deadline = time.monotonic() + self._interval
            while not self._closing.is_set() and not self._flush_now:
                if len(self._q) >= self._batch_size:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._cv.wait(remaining)
            self._flush_now = False
            return self._take_locked()

    def _take_locked(self) -> List[Record]:
        n = min(len(self._q), self._batch_size)
        batch = [self._q.popleft() for _ in range(n)]
        self._inflight += n
        return batch

    def _deliver(self, batch: Sequence[Record], deadline: Optional[float]) -> bool:
        """Send one batch, retrying with backoff. Never raises.

        No batch-level key: each record carries its own ``idempotency_key``
        from enqueue, which survives requeues and rebatching. See
        ``Transport.send``.
        """
        delay = INITIAL_BACKOFF
        while True:
            try:
                self._transport.send(batch)
            except AverTransportError as exc:
                retryable, hint, detail = exc.retryable, exc.retry_after, str(exc)
                brief = exc.summary
            except Exception as exc:  # a transport bug must not kill the thread
                retryable, hint = False, None
                detail = "{0}: {1}".format(type(exc).__name__, exc)
                brief = type(exc).__name__  # stats() gets the class, not the text
            else:
                with self._cv:
                    self._sent += len(batch)
                    self._inflight -= len(batch)
                    self._last_success_at = _utcnow()
                    total, pending = self._sent, len(self._q)
                    self._cv.notify_all()
                log.info(
                    "aver: flushed %d record(s) (total=%d, queued=%d)",
                    len(batch),
                    total,
                    pending,
                )
                return True

            with self._cv:
                self._failed += 1
                self._last_error = brief  # never the response body — see errors.py

            if not retryable:
                log.error(
                    "aver: dropping %d record(s), permanent failure: %s",
                    len(batch),
                    detail,
                )
                self._discard(batch)
                return False

            wait = delay if hint is None else max(0.0, hint)
            log.warning(
                "aver: delivery failed (%s), retrying %d record(s) in %.0fs",
                detail,
                len(batch),
                wait,
            )
            delay = min(delay * 2, self._max_backoff)
            if not self._sleep(wait, deadline):
                self._requeue(batch)
                return False

    def _sleep(self, seconds: float, deadline: Optional[float]) -> bool:
        """Back off. False means stop retrying this batch now."""
        if deadline is None:
            # A 60s backoff must not add 60s to the caller's shutdown.
            return not self._closing.wait(seconds)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(seconds, remaining))
        return time.monotonic() < deadline

    def _requeue(self, batch: Sequence[Record]) -> None:
        """Put an undelivered batch back at the front, preserving order."""
        with self._cv:
            self._inflight -= len(batch)
            room = max(0, self._max - len(self._q))
            keep = list(batch[:room])
            lost = len(batch) - len(keep)
            for record in reversed(keep):
                self._q.appendleft(record)
            self._dropped += lost
            self._cv.notify_all()
        if lost:
            log.error("aver: buffer full, dropped %d undelivered record(s)", lost)

    def _discard(self, batch: Sequence[Record]) -> None:
        with self._cv:
            self._inflight -= len(batch)
            self._dropped += len(batch)
            self._cv.notify_all()

    def _drain(self) -> None:
        """Final pass at shutdown, bounded by the close deadline."""
        deadline = self._close_deadline
        while time.monotonic() < deadline:
            with self._cv:
                batch = self._take_locked()
            if not batch:
                break
            if not self._deliver(batch, deadline) and time.monotonic() >= deadline:
                break

        with self._cv:
            lost = len(self._q)
            self._dropped += lost
            self._q.clear()
            self._inflight = 0
            self._cv.notify_all()
        if lost:
            log.error(
                "aver: shutdown timed out, %d record(s) never delivered", lost
            )
        else:
            log.info("aver: delivery stopped, buffer empty (sent=%d)", self._sent)

