"""Field redaction, applied before anything leaves the process.

Paths are dotted, with two wildcards::

    "applicant.pan"          that field
    "bureau_report.*"        every field one level under
    "accounts[].balance"     that field in every element of the array

A path is matched against the *value* of each input, not against its role, so
one path covers whichever inputs actually contain that structure.

Two modes:

``mask``
    Replace with ``"[REDACTED]"``. Irreversible and simple.

``encrypt``
    AES-256-GCM under a key the customer holds. The ciphertext travels to
    Aver; only the customer can read it. Aver still hashes what it receives,
    so the chain still proves what the model saw without Aver being able to
    read it. Requires the ``aver[encrypt]`` extra — the base install stays on
    httpx and stdlib.

Everything here runs on a deep copy. The caller is almost certainly still
using that dict.
"""

from __future__ import annotations

import base64
import copy
import json
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from ._types import Input
from .errors import AverConfigError

log = logging.getLogger("aver")

MASK = "[REDACTED]"

#: Warn once above this. Large inputs slow the decision path and cost storage.
MAX_INPUT_BYTES = 1_000_000
ENCRYPTED_MARKER = "enc:aes-256-gcm"

#: (name, is_array) per path segment. ``accounts[]`` -> ("accounts", True).
_Segment = Tuple[str, bool]


def _parse(path: str) -> List[_Segment]:
    segments: List[_Segment] = []
    for raw in path.split("."):
        name = raw.strip()
        is_array = name.endswith("[]")
        if is_array:
            name = name[:-2]
        # A bare "[]" is legal only as the first segment, where it means the
        # input's value is itself the array. Anywhere else an empty name is a
        # typo like "a..b".
        if not name and not (is_array and not segments):
            raise AverConfigError("invalid redaction path: {0!r}".format(path))
        segments.append((name, is_array))
    if not segments:
        raise AverConfigError("invalid redaction path: {0!r}".format(path))
    return segments


class Redactor:
    """Applies a set of redaction paths to input values."""

    def __init__(
        self,
        paths: Optional[Iterable[str]] = None,
        *,
        mode: str = "mask",
        key: Optional[Any] = None,
    ) -> None:
        if mode not in ("mask", "encrypt"):
            raise AverConfigError(
                "redact_mode must be 'mask' or 'encrypt', got {0!r}".format(mode)
            )
        self.mode = mode
        self.paths: List[str] = [p for p in (paths or [])]
        self._compiled = [(p, _parse(p)) for p in self.paths]
        self._transform = (
            _mask if mode == "mask" else _encryptor(_coerce_key(key))
        )
        self._matched: set = set()
        self._warned = False
        self._probed_size = False

    @property
    def active(self) -> bool:
        return bool(self._compiled)

    def redact_inputs(self, inputs: Sequence[Input]) -> List[Input]:
        """Deep-copy and redact every input in a record."""
        out = []
        for item in inputs:
            value = self.apply(item.get("value"))
            if not self._probed_size:
                self._warn_if_large(item.get("role", "?"), value)
            out.append({"role": item.get("role", "?"), "value": value})
        self._warn_unmatched()
        return out  # type: ignore[return-value]

    def _warn_if_large(self, role: str, value: Any) -> None:
        """Say something, once, about an input big enough to be felt.

        Measuring size costs a full serialisation — around 40% on top of the
        deep copy — so this runs on the first input this client ever sees and
        never again, whatever it finds. One serialisation per process is
        nothing; one per record is a tax on every decision.

        Timing the copy instead would be cheaper still, but ``deepcopy``
        treats strings as atomic: a bureau report whose bulk is long text
        fields can be megabytes on the wire and almost free to copy, and would
        never be measured. Size is what costs the customer storage, so size is
        what gets measured.

        The trade is that a client whose first record is small will not notice
        later ones growing. See DESIGN.md.
        """
        self._probed_size = True
        # `value` is the *redacted* clone, deliberately. What costs the
        # customer storage is what goes on the wire, and masking a PAN down to
        # "[REDACTED]" genuinely shrinks it. Measuring the caller's original
        # would report a size we never send. This looks like a bug; it is not.
        size = len(json.dumps(value, default=str))
        if size >= MAX_INPUT_BYTES:
            log.warning("aver: input %r is %.1f MB — large inputs slow the "
                        "decision path and cost storage", role, size / 1e6)

    def apply(self, value: Any) -> Any:
        """Return a redacted deep copy of ``value``. Never mutates the input."""
        # Unconditional, even with no redaction paths configured. This copy is
        # a *snapshot*, not redaction scaffolding: it is what stops the caller
        # mutating a value after we recorded it, since the flusher serialises
        # later on another thread. Do not make it conditional on self.active.
        clone = copy.deepcopy(value)
        for path, segments in self._compiled:
            if _walk(clone, segments, 0, self._transform):
                self._matched.add(path)
        return clone

    def _warn_unmatched(self) -> None:
        """Say something, once, about paths that matched nothing.

        A typo in a redaction path that silently ships PII is the worst thing
        this library could do. We cannot check paths at startup — there is no
        data yet — so we check against the first record instead.
        """
        if self._warned or not self._compiled:
            return
        self._warned = True
        missing = [p for p in self.paths if p not in self._matched]
        if missing:
            log.warning(
                "aver: redaction path(s) matched nothing in the first record: "
                "%s — check for typos; unmatched fields are sent unredacted",
                ", ".join(sorted(missing)),
            )


def _walk(
    node: Any, segments: List[_Segment], index: int, transform: Callable[[Any], Any]
) -> bool:
    """Apply ``transform`` at every point matching ``segments[index:]``."""
    name, is_array = segments[index]
    if not name:  # leading "[]": the value itself is the array
        return _walk_list(node, segments, index, transform)
    if not isinstance(node, dict):
        return False
    last = index == len(segments) - 1
    keys = list(node.keys()) if name == "*" else ([name] if name in node else [])

    hit = False
    for key in keys:
        child = node[key]
        if is_array:
            hit = _walk_list(child, segments, index, transform) or hit
        elif last:
            node[key] = transform(child)
            hit = True
        else:
            hit = _walk(child, segments, index + 1, transform) or hit
    return hit


def _walk_list(
    node: Any, segments: List[_Segment], index: int, transform: Callable[[Any], Any]
) -> bool:
    """Apply ``transform`` across every element of an array."""
    if not isinstance(node, list):
        return False
    last = index == len(segments) - 1
    hit = False
    for i, item in enumerate(node):
        if last:
            node[i] = transform(item)
            hit = True
        else:
            hit = _walk(item, segments, index + 1, transform) or hit
    return hit


def _mask(_value: Any) -> str:
    return MASK


def _coerce_key(key: Optional[Any]) -> bytes:
    if key is None:
        raise AverConfigError(
            "redact_mode='encrypt' requires encryption_key (32 raw bytes or "
            "base64 of 32 bytes)"
        )
    if isinstance(key, str):
        try:
            key = base64.b64decode(key, validate=True)
        except Exception as exc:
            raise AverConfigError("encryption_key is not valid base64") from exc
    if not isinstance(key, (bytes, bytearray)) or len(key) != 32:
        raise AverConfigError(
            "encryption_key must be 32 bytes for AES-256-GCM, got {0}".format(
                len(key) if isinstance(key, (bytes, bytearray)) else type(key).__name__
            )
        )
    return bytes(key)


def _encryptor(key: bytes) -> Callable[[Any], Dict[str, str]]:
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as exc:  # pragma: no cover - depends on install extras
        raise AverConfigError(
            "redact_mode='encrypt' needs the encrypt extra: pip install 'aver[encrypt]'"
        ) from exc

    import os

    aead = AESGCM(key)

    def encrypt(value: Any) -> Dict[str, str]:
        plaintext = json.dumps(value, default=str).encode("utf-8")
        nonce = os.urandom(12)
        ciphertext = aead.encrypt(nonce, plaintext, None)
        return {
            "__aver__": ENCRYPTED_MARKER,
            "nonce": base64.b64encode(nonce).decode("ascii"),
            "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        }

    return encrypt


def decrypt_field(field: Dict[str, str], key: Any) -> Any:
    """Reverse ``encrypt`` mode. For the customer, who holds the key."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    aead = AESGCM(_coerce_key(key))
    plaintext = aead.decrypt(
        base64.b64decode(field["nonce"]), base64.b64decode(field["ciphertext"]), None
    )
    return json.loads(plaintext.decode("utf-8"))

