"""Aver — record what your decisioning system saw and did.

    from aver import AverClient

    aver = AverClient(api_key=..., stream_id="consumer-pl")

    with aver.decision(session_id=application_id) as d:
        d.observe("application_form", form_data)
        d.model(version="scorecard-v7")
        d.record_action({"type": "deny", "reason_code": "DTI_EXCEEDED"})

Recording is fire-and-forget: it never blocks the decision path and never
raises into it, with two exceptions — ``AverBufferFull`` and
``AverClientClosed``, which both mean records are being lost.
"""

# Defined before the imports below, deliberately: transport.py resolves the
# User-Agent version with a lazy `from . import __version__`, and moving
# this after the imports turns that into a circular import.
__version__ = "0.1.0"

from .aclient import AsyncAverClient, AsyncDecisionRecorder
from .client import AverClient
from .decision import DecisionRecorder
from .errors import (
    AverBufferFull, AverClientClosed, AverConfigError, AverError,
    AverTransportError,
)
from .redact import decrypt_field

__all__ = [
    "AverClient", "AsyncAverClient", "DecisionRecorder", "AsyncDecisionRecorder",
    "AverError", "AverBufferFull", "AverClientClosed", "AverConfigError",
    "AverTransportError", "decrypt_field", "__version__",
]
