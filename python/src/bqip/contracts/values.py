"""Exact domain primitives. Protobuf wire presence must be checked by its binding."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import IntEnum
from uuid import UUID

U32_MAX = (1 << 32) - 1
U64_MAX = (1 << 64) - 1
I64_MIN = -(1 << 63)
I64_MAX = (1 << 63) - 1


class ContractError(ValueError):
    """An input is not a valid value of the AC-001 contract."""


def bounded_int(value: int, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise ContractError("INTEGER_OUT_OF_RANGE")
    return value


def timestamp_ns(value: int) -> int:
    return bounded_int(value, I64_MIN, I64_MAX)


def validate_text(value: str, *, allow_empty: bool = True) -> None:
    """Enforce the UTF-8 string domain shared with Protobuf/Rust String."""
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ContractError("INVALID_CONTRACT_TEXT")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError as error:
        raise ContractError("INVALID_UNICODE") from error


@dataclass(frozen=True)
class DecimalValue:
    coefficient: str
    scale: int

    def __post_init__(self) -> None:
        bounded_int(self.scale, 0, U32_MAX)
        if not re.fullmatch(r"(?:0|-?[1-9][0-9]*)", self.coefficient):
            raise ContractError("NON_CANONICAL_COEFFICIENT")
        if self.coefficient == "0" and self.scale != 0:
            raise ContractError("NON_CANONICAL_ZERO")
        if self.scale and self.coefficient.endswith("0"):
            raise ContractError("NON_CANONICAL_SCALE")

    @classmethod
    def normalize(cls, coefficient: str, scale: int) -> DecimalValue:
        bounded_int(scale, 0, U32_MAX)
        if not re.fullmatch(r"-?[0-9]+", coefficient):
            raise ContractError("INVALID_COEFFICIENT")
        negative = coefficient.startswith("-")
        digits = coefficient.removeprefix("-").lstrip("0")
        if not digits:
            return cls("0", 0)
        while scale and digits.endswith("0"):
            digits = digits[:-1]
            scale -= 1
        return cls(("-" if negative else "") + digits, scale)

    @classmethod
    def parse(cls, text: str) -> DecimalValue:
        if not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", text):
            raise ContractError("INVALID_DECIMAL_TEXT")
        whole, separator, fraction = text.partition(".")
        return cls.normalize(whole + fraction, len(fraction) if separator else 0)

    def identity_bytes(self) -> bytes:
        return f"{self.coefficient}:{self.scale}".encode("ascii")


@dataclass(frozen=True)
class CaptureId:
    collector_instance_id: UUID
    capture_seq: int

    def __post_init__(self) -> None:
        if not isinstance(self.collector_instance_id, UUID):
            raise ContractError("INVALID_COLLECTOR_UUID")
        bounded_int(self.capture_seq, 0, U64_MAX)


class ReplayMode(IntEnum):
    UNKNOWN = 0
    LIVE = 1
    VERSION_PINNED_RESEARCH = 2
    AS_LIVED_FORENSIC = 3


class PayloadKind(IntEnum):
    UNKNOWN = 0
    TEXT = 1
    BINARY = 2


class RawSegmentState(IntEnum):
    UNKNOWN = 0
    OPEN = 1
    SEALED_LOCAL = 2
    UPLOADED = 3
    VERIFIED_REMOTE = 4
    MANIFEST_COMMITTED = 5
    GC_ELIGIBLE = 6
    RECOVERED_PARTIAL = 7
    FAILED = 8


@dataclass(frozen=True)
class ProcessingContext:
    processing_timestamp_ns: int
    code_commit: str
    schema_version: int
    run_id: UUID
    replay_mode: ReplayMode

    def __post_init__(self) -> None:
        timestamp_ns(self.processing_timestamp_ns)
        bounded_int(self.schema_version, 1, U32_MAX)
        validate_text(self.code_commit, allow_empty=False)
        if not isinstance(self.run_id, UUID):
            raise ContractError("INVALID_PROCESSING_CONTEXT")
        if not isinstance(self.replay_mode, ReplayMode) or self.replay_mode == ReplayMode.UNKNOWN:
            raise ContractError("INVALID_REPLAY_MODE")


def semantic_context_equal(left: ProcessingContext, right: ProcessingContext) -> bool:
    """Compare pinned transformation identity; timestamps and run IDs are provenance."""
    return (left.code_commit, left.schema_version, left.replay_mode) == (
        right.code_commit, right.schema_version, right.replay_mode
    )
