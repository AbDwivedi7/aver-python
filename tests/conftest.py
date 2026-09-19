"""Test doubles.

``FakeTransport`` records every *attempt*, not just every success, because
most of what this library promises is about what happens when delivery fails.
"""

from __future__ import annotations

import copy
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional

import pytest

from aver.errors import AverTransportError


class Attempt:
    def __init__(self, records: List[Dict[str, Any]]) -> None:
        self.records = records


class FakeTransport:
    """Stands in for ``aver.transport.Transport``.

    ``behaviour`` is called with the 1-based attempt number and may sleep or
    raise ``AverTransportError`` to simulate a failing endpoint.
    """

    def __init__(self, behaviour: Optional[Callable[[int], None]] = None) -> None:
        self._behaviour = behaviour
        self._lock = threading.Lock()
        self.attempts: List[Attempt] = []
        self.delivered: List[Dict[str, Any]] = []
        self.closed = False

    def send(self, batch) -> int:
        """Deliver, and report how many records went out — like the real one."""
        snapshot = [copy.deepcopy(dict(r)) for r in batch]
        with self._lock:
            self.attempts.append(Attempt(snapshot))
            n = len(self.attempts)
        if self._behaviour is not None:
            self._behaviour(n)
        with self._lock:
            self.delivered.extend(snapshot)
        return len(snapshot)

    def close(self) -> None:
        self.closed = True

    # -- assertions helpers ---------------------------------------------

    @property
    def attempt_count(self) -> int:
        with self._lock:
            return len(self.attempts)

    def delivered_records(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.delivered)

    def payload_text(self) -> str:
        """Everything ever put on the wire, as one searchable string."""
        with self._lock:
            attempts = list(self.attempts)
        return json.dumps([a.records for a in attempts], default=str)


def fail_times(n: int, *, retryable: bool = True, status: Optional[int] = None):
    """Behaviour: fail the first ``n`` attempts, then succeed."""

    def behaviour(attempt: int) -> None:
        if attempt <= n:
            raise AverTransportError(
                "simulated failure {0}".format(attempt),
                retryable=retryable,
                status_code=status,
            )

    return behaviour


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    """Keep the retry tests in milliseconds instead of seconds.

    This works because ``_deliver`` reads INITIAL_BACKOFF as a module global at
    call time. If it is ever hoisted into ``RecordBuffer.__init__`` (as
    ``self._initial_backoff``, say), this fixture silently stops taking effect
    and the retry tests start waiting whole seconds. Patch the attribute then,
    or pass the value through the constructor.
    """
    monkeypatch.setattr("aver.buffer.INITIAL_BACKOFF", 0.01)
    monkeypatch.setattr("aver.buffer.MAX_BACKOFF", 0.05)


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        server: Any = self.server
        with server.lock:
            server.requests.append(
                {
                    # The path is part of the contract, so it is captured like
                    # the body is. Nothing asserted on it until the wire
                    # contract test existed, and the SDK spent its whole life
                    # posting to a route the service does not serve.
                    "path": self.path,
                    "headers": dict(self.headers),
                    "body": json.loads(body or b"{}"),
                }
            )
            status = server.next_status
            payload = json.dumps(server.next_body).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args: Any) -> None:
        pass  # keep pytest output readable


class StubServer:
    """A real HTTP endpoint, for the tests that must exercise real sockets."""

    def __init__(self) -> None:
        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._httpd.lock = threading.Lock()  # type: ignore[attr-defined]
        self._httpd.requests = []  # type: ignore[attr-defined]
        self._httpd.next_status = 200  # type: ignore[attr-defined]
        self._httpd.next_body = {}  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._httpd.server_address[:2]
        return "http://{0}:{1}".format(host, port)

    @property
    def requests(self) -> List[Dict[str, Any]]:
        with self._httpd.lock:  # type: ignore[attr-defined]
            return list(self._httpd.requests)  # type: ignore[attr-defined]

    def records(self) -> List[Dict[str, Any]]:
        return [r for req in self.requests for r in req["body"].get("records", [])]

    def set_status(self, status: int, body: Optional[Dict[str, Any]] = None) -> None:
        with self._httpd.lock:  # type: ignore[attr-defined]
            self._httpd.next_status = status  # type: ignore[attr-defined]
            self._httpd.next_body = body or {}  # type: ignore[attr-defined]

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()


@pytest.fixture
def stub_server():
    server = StubServer()
    try:
        yield server
    finally:
        server.stop()
