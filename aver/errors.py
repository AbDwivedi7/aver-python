"""Exceptions.

Two exceptions can reach the caller at record time — ``AverBufferFull`` and
``AverClientClosed`` — and both mean a record was lost. ``AverConfigError`` is
raised only at client construction. Everything else is caught by the background
flusher and logged.
"""

from __future__ import annotations

from typing import Optional


class AverError(Exception):
    """Base class for every exception this library defines."""


class AverConfigError(AverError, ValueError):
    """Invalid client configuration.

    Raised from ``AverClient.__init__`` only. A misconfigured client is a
    programmer error, caught in the caller's tests, not a runtime condition on
    the decision path.
    """


class AverBufferFull(AverError, RuntimeError):
    """The buffer is full and the record was not accepted.

    One of the two exceptions that reach the caller at record time; see the
    module docstring. It means records are being dropped, which is worse than
    an error the caller can see: they would otherwise believe they have audit
    coverage they do not have.
    """


class AverClientClosed(AverError, RuntimeError):
    """A record was offered to a client that is already closed.

    One of the two exceptions that reach the caller at record time; see the
    module docstring. Losing a record this way is as definitive as buffer
    overflow — an atexit ordering issue, a worker outliving its client — so it
    is raised rather than swallowed.
    """


class AverTransportError(AverError):
    """A delivery attempt failed. Never escapes the flusher thread."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
        kind: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code
        self.retry_after = retry_after
        # `message` may quote the server's response body, which can echo back
        # the record we sent. That belongs in the log, not in stats(), which
        # callers routinely expose on a health endpoint. `summary` is composed
        # only from fields we control — a status code or a caller-supplied
        # category — and never falls through to free text.
        self.summary = (
            "HTTP {0}".format(status_code)
            if status_code
            else (kind or "delivery error")
        )
