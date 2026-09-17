"""BQIP-ID-V1 framing and structured observation identity, without venue recipes."""

import hashlib
import re
import struct
from collections.abc import Iterable
from uuid import UUID, uuid4

from bqip.contracts.values import U32_MAX, U64_MAX, CaptureId, ContractError

DOMAIN = b"BQIP-ID-V1"


def preimage(components: Iterable[tuple[str, bytes]]) -> bytes:
    output = bytearray(DOMAIN)
    for label, value in components:
        encoded = label.encode("utf-8", errors="strict")
        if len(encoded) > U32_MAX or len(value) > U32_MAX:
            raise ContractError("IDENTITY_COMPONENT_TOO_LONG")
        output.extend(struct.pack(">I", len(encoded)))
        output.extend(encoded)
        output.extend(struct.pack(">I", len(value)))
        output.extend(value)
    return bytes(output)


def logical_hash(components: Iterable[tuple[str, bytes]]) -> bytes:
    return hashlib.sha256(preimage(components)).digest()


def raw_hash(payload: bytes) -> bytes:
    return hashlib.sha256(payload).digest()


def integer_bytes(value: int) -> bytes:
    if type(value) is not int:
        raise ContractError("INTEGER_REQUIRED")
    return str(value).encode("ascii")


def enum_bytes(symbolic_token: str) -> bytes:
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", symbolic_token):
        raise ContractError("INVALID_ENUM_TOKEN")
    return symbolic_token.encode("ascii")


class CaptureSequencer:
    """Single owner per collector process. Reconnects do not reset this counter."""

    def __init__(self, collector_instance_id: UUID | None = None) -> None:
        identity = uuid4() if collector_instance_id is None else collector_instance_id
        if not isinstance(identity, UUID):
            raise ContractError("INVALID_COLLECTOR_UUID")
        self._collector_instance_id = identity
        self._next_seq: int | None = 0

    @property
    def collector_instance_id(self) -> UUID:
        return self._collector_instance_id

    def next_id(self) -> CaptureId:
        if self._next_seq is None:
            raise ContractError("CAPTURE_SEQUENCE_EXHAUSTED")
        current = self._next_seq
        self._next_seq = current + 1 if current < U64_MAX else None
        return CaptureId(self.collector_instance_id, current)

    @staticmethod
    def new_connection_id() -> UUID:
        return uuid4()

    @staticmethod
    def new_session_id() -> UUID:
        return uuid4()
