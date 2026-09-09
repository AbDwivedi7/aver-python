"""Wire format, described with TypedDicts.

These are documentation and type-checker hints. Nothing here validates at
runtime: the server validates records against the stream's ``input_schema``
and reports completeness. Duplicating that here would create a second source
of truth that drifts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:  # pragma: no cover - typing shim for 3.9/3.10
    from typing import TypedDict
except ImportError:  # pragma: no cover
    from typing_extensions import TypedDict  # type: ignore[assignment]

#: Wire format version the SDK emits. The server accepts known versions; the
#: SDK does not negotiate.
SCHEMA_VERSION = "1"

#: An action is an open dict. ``type`` is the only key the server relies on.
Action = Dict[str, Any]


class Input(TypedDict):
    """One thing the decision system saw, under the role it played."""

    role: str
    value: Any


class Record(TypedDict, total=False):
    """A single decision, as sent to Aver."""

    schema_version: str
    idempotency_key: str
    decision_id: str
    parent_decision_id: Optional[str]
    stream_id: str
    session_id: str
    inputs: List[Input]
    model_version: Optional[str]
    model_artifact_hash: Optional[str]
    feature_set_version: Optional[str]
    rule_config_hash: Optional[str]
    policy_version: Optional[str]
    action: Action
    recorded_at: str


class Stats(TypedDict):
    """What ``AverClient.stats()`` returns."""

    queued: int
    sent: int
    failed: int
    dropped: int
    last_error: Optional[str]
    last_success_at: Optional[str]
