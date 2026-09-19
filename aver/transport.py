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

#: The service's ingest route. Not ``/v1/records``: the published API is
#: ``POST /v1/decisions`` and nothing else is routed there, so the old path was
#: a 404 on every attempt. Because 404 is retryable (see below) that never
#: surfaced as an error — it became unbounded backoff, a buffer filling behind
#: it, and eventually ``AverBufferFull`` raised into the decision path.
DECISIONS_PATH = "/v1/decisions"

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
        stream_id: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        version: Optional[str] = None,
        client: Optional[httpx.Client] = None,
    ) -> None:
        if version is None:
            # Support needs to know which SDK a bank is running; a default
            # placeholder here would quietly lie about that.
            from . import __version__ as version
        self._url = base_url.rstrip("/") + DECISIONS_PATH
        # The stream belongs to the request, not to the record: the service
        # reads one ``stream_id`` per body and checks the api key is scoped to
        # it. It lives here rather than on each record because a transport
        # serves exactly one client, and a client serves exactly one stream.
        self._stream_id = stream_id
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

    def send(self, batch: Sequence[Record]) -> int:
        """Deliver a batch. Returns how many records actually went on the wire.

        The count is not decoration. A record that cannot be serialised is
        dropped here, and the caller has no other way to learn it: counting the
        whole batch as delivered is how ``sent`` came to include records that
        never left the process.

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
        # Which positions in `batch` are in `encoded`, in order. The service
        # reports a partial failure as a count of records it wrote, and that
        # count indexes the wire, not the caller's batch. One record dropped
        # here shifts every position after it, so the mapping travels with the
        # error rather than being reconstructed from a length.
        kept = []
        for index, record in enumerate(batch):
            try:
                blob = json.dumps(record, default=str)
            except (TypeError, ValueError) as exc:
                log.error(
                    "aver: record %s could not be serialised and was dropped: "
                    "%s: %s",
                    record.get("decision_id"),
                    type(exc).__name__,
                    exc,
                )
                continue
            encoded.append(blob)
            kept.append(index)
        if not encoded:
            # Not a success. This used to be a bare ``return``, indistinguish-
            # able from a delivered batch: the buffer ran its success branch,
            # added fifty to ``sent``, stamped a fresh ``last_success_at`` and
            # left ``dropped`` at zero. Every observable said the audit trail
            # was healthy while fifty records sat unencodable in memory.
            # Permanent, because re-encoding the same objects cannot succeed.
            raise AverTransportError(
                "no record in the batch could be serialised",
                retryable=False,
                kind="UnserialisableBatch",
            )
        # ``stream_id`` at the top level, once, not repeated on every record:
        # that is the shape the service decodes, and a body without it is
        # rejected as an invalid record before the batch is even read.
        body = (
            '{"stream_id":'
            + json.dumps(self._stream_id)
            + ',"records":['
            + ",".join(encoded)
            + "]}"
        ).encode()
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
            return len(encoded)
        if status in PERMANENT_STATUS:
            # A batch that fails part-way is not all-or-nothing. The service
            # commits each record in its own transaction and returns the ones
            # it wrote in ``results``; the record after them is the one it
            # refused, and the records behind that were never attempted. The
            # buffer needs all three groups to recover, so the count and the
            # wire mapping travel with the error.
            raise AverTransportError(
                "HTTP {0} (permanent): {1}".format(status, _summarise(response)),
                retryable=False,
                status_code=status,
                written=_written_count(response),
                sent_indices=kept,
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


def _written_count(response: httpx.Response) -> Optional[int]:
    """How many records the service says it wrote before rejecting the batch.

    ``None`` means it did not say — a body that is not JSON, or carries no
    ``results`` — and the caller must assume nothing was written.

    The count, and nothing else. ``results`` echoes identifiers back and the
    body can quote a record outright, so only the length leaves this function;
    see ``errors.AverTransportError.summary`` for the same rule.
    """
    try:
        body = response.json()
    except Exception:  # not JSON, truncated, or an HTML page from a proxy
        return None
    if not isinstance(body, dict):
        return None
    results = body.get("results")
    if not isinstance(results, list):
        return None
    return len(results)


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

