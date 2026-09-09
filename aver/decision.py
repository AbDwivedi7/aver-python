"""The context-manager form.

``observe``, ``model`` and ``record_action`` are local bookkeeping — they do
no I/O and cannot fail. The record is built and queued once, on block exit.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List, Optional

from ._types import Action, Input
from .errors import AverBufferFull, AverClientClosed

if TYPE_CHECKING:  # pragma: no cover
    from .client import AverClient

log = logging.getLogger("aver")

#: What we record when a block exits without calling ``record_action``.
UNSPECIFIED_ACTION: Action = {"type": "unspecified"}


class DecisionRecorder:
    """Collects one decision. Queued on exit, whether or not it succeeded."""

    def __init__(
        self,
        client: "AverClient",
        session_id: str,
        *,
        parent: Any = None,
        policy_version: Optional[str] = None,
    ) -> None:
        self._client = client
        self._session_id = session_id
        self._parent = parent
        self._policy_version = policy_version
        self._inputs: List[Input] = []
        self._action: Optional[Action] = None
        self._model_version: Optional[str] = None
        self._model_artifact_hash: Optional[str] = None
        self._feature_set_version: Optional[str] = None
        self._rule_config_hash: Optional[str] = None
        self._emitted = False
        #: Set when the record is built, on block exit. ``None`` before that.
        self.decision_id: Optional[str] = None

    def observe(self, role: str, value: Any) -> None:
        """Note something the decision system saw, under the role it played."""
        self._inputs.append({"role": role, "value": value})

    def model(
        self,
        version: Optional[str] = None,
        artifact_hash: Optional[str] = None,
        feature_set: Optional[str] = None,
        rule_config_hash: Optional[str] = None,
    ) -> None:
        """Identify what produced the decision."""
        if version is not None:
            self._model_version = version
        if artifact_hash is not None:
            self._model_artifact_hash = artifact_hash
        if feature_set is not None:
            self._feature_set_version = feature_set
        if rule_config_hash is not None:
            self._rule_config_hash = rule_config_hash

    def record_action(self, action: Action) -> None:
        """Note what the system decided to do."""
        self._action = action

    # -- context manager -------------------------------------------------

    def __enter__(self) -> "DecisionRecorder":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is not None:
            # A decision that crashed is still a decision that needs
            # explaining, so it is recorded before the exception continues.
            # If an action was already recorded and something *after* it
            # raised, that action is the most valuable thing in the record —
            # the system decided to deny, and then something broke. Keep both.
            failure: Action = {"type": "error", "detail": str(exc)}
            if self._action is not None:
                failure["attempted_action"] = self._action
            self._action = failure
        try:
            self.emit()
        except (AverBufferFull, AverClientClosed) as lost:
            if exc is None:
                raise
            # Both went wrong. The caller's exception is the one they must
            # see, so ours is logged rather than raised over the top of it.
            log.error(
                "aver: decision for session %s not recorded (%s)",
                self._session_id,
                type(lost).__name__,
            )
        return False  # never swallow the caller's exception

    def emit(self) -> Optional[str]:
        """Build and queue the record. Idempotent; returns the decision id."""
        if self._emitted:
            return self.decision_id
        self._emitted = True
        action = self._action
        if action is None:
            self._client._warn_once(
                "no-action",
                "aver: decision block for session %s exited without "
                "record_action(); recorded as 'unspecified'",
                self._session_id,
            )
            action = dict(UNSPECIFIED_ACTION)
        self.decision_id = self._client.record(
            session_id=self._session_id,
            inputs=self._inputs,
            action=action,
            model_version=self._model_version,
            model_artifact_hash=self._model_artifact_hash,
            feature_set_version=self._feature_set_version,
            rule_config_hash=self._rule_config_hash,
            policy_version=self._policy_version,
            parent_decision_id=self._parent,
        )
        return self.decision_id

