"""The async client.

Identical surface, identical guarantees. ``record`` and ``observe`` are local
and stay synchronous — there is nothing to await. Only ``flush`` and ``close``
become coroutines, because those are the two calls that genuinely wait.

Delivery still runs on a background thread rather than an asyncio task, and
that is deliberate: telemetry then costs the event loop nothing, and a loop
blocked by the caller's own work cannot stall the audit trail. Same buffer,
same backoff, same atexit drain.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from .buffer import CLOSE_TIMEOUT
from .client import AverClient
from .decision import DecisionRecorder


class AsyncDecisionRecorder(DecisionRecorder):
    """``async with`` form of :class:`~aver.decision.DecisionRecorder`."""

    async def __aenter__(self) -> "AsyncDecisionRecorder":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return self.__exit__(exc_type, exc, tb)


class AsyncAverClient(AverClient):
    """Async-friendly client. See :class:`~aver.client.AverClient`."""

    def decision(
        self,
        session_id: str,
        *,
        parent: Any = None,
        policy_version: Optional[str] = None,
    ) -> AsyncDecisionRecorder:
        return AsyncDecisionRecorder(
            self, session_id, parent=parent, policy_version=policy_version
        )

    async def flush(self, timeout: Optional[float] = None) -> bool:  # type: ignore[override]
        """Drain the buffer without blocking the event loop."""
        return await asyncio.to_thread(self._buffer.flush, timeout)

    async def close(self, timeout: float = CLOSE_TIMEOUT) -> None:  # type: ignore[override]
        """Flush and stop, off the event loop."""
        await asyncio.to_thread(self._close_sync, timeout)

    async def __aenter__(self) -> "AsyncAverClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self.close()
        return False

    def __enter__(self):
        raise TypeError("AsyncAverClient requires 'async with', not 'with'")

    def __exit__(self, exc_type, exc, tb):
        # Inherited, this would call the async close() and leave an un-awaited
        # coroutine behind, closing nothing.
        raise TypeError("AsyncAverClient requires 'async with', not 'with'")

    # -- internals -------------------------------------------------------

    def _close_sync(self, timeout: float = CLOSE_TIMEOUT) -> None:
        AverClient.close(self, timeout)

    def _at_exit(self) -> None:
        # atexit runs with no event loop, so it takes the synchronous path.
        try:
            self._close_sync(CLOSE_TIMEOUT)
        except Exception:  # pragma: no cover - nothing useful to do at exit
            pass

