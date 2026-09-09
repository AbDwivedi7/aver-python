"""The synchronous client.

Two rules govern everything in this file:

* ``record`` and ``decision`` never block on the network.
* Two exceptions can reach the caller at record time — ``AverBufferFull`` and
  ``AverClientClosed`` — and both mean a record was lost. ``AverConfigError`` is
  raised only at client construction. Everything else is caught by the background
  flusher and logged.
"""

from __future__ import annotations

import atexit
import copy
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Sequence

from . import buffer as _buffer
from ._types import SCHEMA_VERSION, Action, Input, Record, Stats
from .buffer import CLOSE_TIMEOUT, RecordBuffer
from .decision import DecisionRecorder
from .errors import AverConfigError
from .redact import Redactor
from .transport import DEFAULT_BASE_URL, Transport

log = logging.getLogger("aver")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parent_id(parent: Any) -> Optional[str]:
    """Accept a decision id, a recorder from a prior block, or nothing."""
    if parent is None:
        return None
    if hasattr(parent, "decision_id"):
        # A recorder only gets its id on the way out of its block. Nesting
        # instead of sequencing would otherwise put a repr — with a memory
        # address in it — into the chain, which can never be resolved.
        if parent.decision_id is None:
            log.warning("aver: parent recorder has not left its block yet; "
                        "recording this decision without a parent link")
        return parent.decision_id
    return str(parent)


class AverClient:
    """Records decisions to Aver. One per process is plenty."""

    def __init__(
        self,
        api_key: str,
        stream_id: str,
        *,
        policy_version: Optional[str] = None,
        redact: Optional[Iterable[str]] = None,
        redact_mode: str = "mask",
        encryption_key: Optional[Any] = None,
        base_url: str = DEFAULT_BASE_URL,
        max_records: int = _buffer.MAX_RECORDS,
        batch_size: int = _buffer.BATCH_SIZE,
        flush_interval: float = _buffer.FLUSH_INTERVAL,
        transport: Optional[Any] = None,
    ) -> None:
        if not api_key:
            raise AverConfigError("api_key is required")
        if not stream_id:
            raise AverConfigError("stream_id is required")

        self.stream_id = stream_id
        self.policy_version = policy_version
        self._redactor = Redactor(redact, mode=redact_mode, key=encryption_key)
        self._transport = transport or Transport(api_key, base_url=base_url)
        self._buffer = RecordBuffer(
            self._transport,
            max_records=max_records,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )
        self._warned: set = set()
        self._closed = False
        atexit.register(self._at_exit)

    # -- public API ------------------------------------------------------

    def decision(
        self,
        session_id: str,
        *,
        parent: Any = None,
        policy_version: Optional[str] = None,
    ) -> DecisionRecorder:
        """Open a decision block. The record is queued when the block exits."""
        return DecisionRecorder(
            self, session_id, parent=parent, policy_version=policy_version
        )

    def record(
        self,
        session_id: str,
        inputs: Sequence[Input],
        action: Action,
        *,
        model_version: Optional[str] = None,
        model_artifact_hash: Optional[str] = None,
        feature_set_version: Optional[str] = None,
        rule_config_hash: Optional[str] = None,
        policy_version: Optional[str] = None,
        parent_decision_id: Any = None,
    ) -> Optional[str]:
        """Queue one decision. Returns immediately with its decision id."""
        policy = self.policy_version if policy_version is None else policy_version
        try:
            payload: Record = {
                "schema_version": SCHEMA_VERSION,
                # Generated once, here, and reused on every retry of this
                # record. This is what makes at-least-once delivery safe
                # against the server's seq chain: a retry must not become a
                # second ledger entry.
                "idempotency_key": str(uuid.uuid4()),
                "decision_id": str(uuid.uuid4()),
                "parent_decision_id": _parent_id(parent_decision_id),
                "stream_id": self.stream_id,
                "session_id": str(session_id),
                # Redaction happens here, before the record is queued, so the
                # background thread never holds unredacted values.
                "inputs": self._redactor.redact_inputs(inputs),
                "model_version": model_version,
                "model_artifact_hash": model_artifact_hash,
                "feature_set_version": feature_set_version,
                "rule_config_hash": rule_config_hash,
                "policy_version": policy,
                # Snapshotted like the inputs are. The flusher serialises this
                # later, so holding the caller's dict by reference would let a
                # mutation after the block change what we recorded — and change
                # it only sometimes, depending on which ran first.
                "action": copy.deepcopy(action),
                "recorded_at": _now(),
            }
        except Exception as exc:
            # Redaction or copying failed. Dropping the record is the only
            # safe option: sending it could ship the PII we were asked to
            # remove. Loud in the log and in stats(), silent to the caller.
            reason = "record build failed: {0}: {1}".format(type(exc).__name__, exc)
            self._buffer.note_dropped(reason)
            log.error(
                "aver: dropped decision for session %s — %s", session_id, reason
            )
            return None

        # The only call here that may raise: AverBufferFull or AverClientClosed.
        self._buffer.put(payload)
        return payload["decision_id"]

    def flush(self, timeout: Optional[float] = None) -> bool:
        """Drain the buffer. True if it drained, False if ``timeout`` expired."""
        return self._buffer.flush(timeout)

    def stats(self) -> Stats:
        """Delivery counters, for the platform team's dashboard."""
        return self._buffer.stats()

    def close(self, timeout: float = CLOSE_TIMEOUT) -> None:
        """Flush and stop. Called automatically at interpreter exit."""
        if self._closed:
            return
        self._closed = True
        self._buffer.close(timeout)
        # The atexit registry holds a strong reference to this client. Drop it
        # once we are closed, so a caller who builds clients more often than
        # they should does not accumulate them for the life of the process.
        atexit.unregister(self._at_exit)

    def __enter__(self) -> "AverClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False

    # -- internals -------------------------------------------------------

    def _warn_once(self, key: str, message: str, *args: Any) -> None:
        """Integration mistakes should be visible without flooding the log."""
        if key not in self._warned:
            self._warned.add(key)
            log.warning(message, *args)

    def _at_exit(self) -> None:
        try:
            self.close(CLOSE_TIMEOUT)
        except Exception:  # pragma: no cover - nothing useful to do at exit
            pass

