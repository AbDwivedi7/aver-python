"""HTTP delivery.

This module knows how to make one attempt and how to classify the result.
It does not know about retries or backoff — that loop lives in ``buffer.py``,
which owns the records and therefore owns the decision to keep them.
"""

from __future__ import annotations

import json
import logging
import platform
from typing import Optional, Sequence

import httpx

from ._types import Record
from .errors import AverTransportError

log = logging.getLogger("aver")

DEFAULT_BASE_URL = "https://api.aver.dev"
RECORDS_PATH = "/v1/records"

CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 10.0

#: Permanent. The request is wrong, or we are not allowed to make it, or the
#: payload will never fit. Retrying cannot change any of those. 422 is here
#: for the same reason: re-sending identical bytes cannot fix a body the
#: server has already rejected.
#:
#: 404 is deliberately *not* here. An ingress can return it transiently while
#: routes reconfigure, and the two mistakes are not symmetric: a permanent 404
#: treated as retryable costs backoff and visible buffer pressure, while a
#: transient 404 treated as permanent destroys records during a routine deploy.
PERMANENT_STATUS = frozenset({400, 401, 403, 413, 422})


class Transport:
    """One HTTP attempt per call. Raises ``AverTransportError`` on failure."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        version: Optional[str] = None,
        client: Optional[httpx.Client] = None,
    ) -> None:
        if version is None:
            # Support needs to know which SDK a bank is running; a default
            # placeholder here would quietly lie about that.
            from . import __version__ as version
        self._url = base_url.rstrip("/") + RECORDS_PATH
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(READ_TIMEOUT, connect=CONNECT_TIMEOUT),
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        )
        self._headers = {
            "Authorization": "Bearer " + api_key,
            "Content-Type": "application/json",
            "User-Agent": "aver-python/{0} python/{1}".format(
                version, platform.python_version()
            ),
        }

    def send(self, batch: Sequence[Record]) -> None:
        """Deliver a batch.

        Deduplication is per record, on ``records[].idempotency_key``, not on
        a request header: the buffer recomposes batches when a delivery is
        requeued, so any batch-scoped key changes membership underneath the
        server and a retried record would land as a second ledger entry.
        """
        # Per record, not per batch. default=str covers Decimals, dates, UUIDs
        # and ORM objects, but a circular reference still raises — and encoding
        # the batch as one document would let that single record take the other
        # forty-nine down with it.
        encoded = []
        for record in batch:
            try:
                encoded.append(json.dumps(record, default=str))
            except (TypeError, ValueError) as exc:
                log.error(
                    "aver: record %s could not be serialised and was dropped: "
                    "%s: %s",
                    record.get("decision_id"),
                    type(exc).__name__,
                    exc,
                )
        if not encoded:
            return
        body = ('{"records":[' + ",".join(encoded) + "]}").encode()
        try:
            response = self._client.post(
                self._url, content=body, headers=self._headers
            )
        except httpx.HTTPError as exc:  # connection errors, timeouts, protocol errors
            raise AverTransportError(
                "{0}: {1}".format(type(exc).__name__, exc),
                retryable=True,
                kind=type(exc).__name__,
            ) from exc

        status = response.status_code
        if status < 300:
            return
        if status in PERMANENT_STATUS:
            raise AverTransportError(
                "HTTP {0} (permanent): {1}".format(status, _summarise(response)),
                retryable=False,
                status_code=status,
            )
        raise AverTransportError(
            "HTTP {0}: {1}".format(status, _summarise(response)),
            retryable=True,
            status_code=status,
            # Not just 429: a 503 during a deploy or maintenance window
            # commonly carries Retry-After too, and backing off on our own
            # schedule ignores a server that told us exactly when to return.
            retry_after=_retry_after(response),
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


def _retry_after(response: httpx.Response) -> Optional[float]:
    """Seconds to wait, if the server told us. Only the delta-seconds form."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw.strip()))
    except ValueError:
        return None


def _summarise(response: httpx.Response, limit: int = 200) -> str:
    """A short, safe excerpt of an error body for the log.

    Bounded because an HTML error page from a proxy is not worth a log line
    the size of a webpage.
    """
    try:
        text = response.text
    except Exception:  # pragma: no cover - defensive
        return "<unreadable body>"
    text = " ".join(text.split())
    return text[:limit] + ("…" if len(text) > limit else "")

