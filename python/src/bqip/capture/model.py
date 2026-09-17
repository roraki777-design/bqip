"""Immutable domain validation, separate from generated Protobuf messages."""

from dataclasses import dataclass, field
from uuid import UUID

from bqip.contracts.values import (
    U32_MAX,
    U64_MAX,
    CaptureId,
    ContractError,
    PayloadKind,
    ProcessingContext,
    bounded_int,
    timestamp_ns,
    validate_text,
)
from bqip.identity import raw_hash


@dataclass(frozen=True)
class RawCaptureEvent:
    capture_id: CaptureId
    connection_id: UUID
    session_id: UUID
    transport: str
    source: str
    endpoint: str
    channel: str
    receive_timestamp_ns: int
    monotonic_ns_since_collector_start: int
    payload_kind: PayloadKind
    payload_length: int
    raw_content_hash: bytes
    exchange_timestamp_ns: int | None = None
    venue_event_id: bytes | None = None
    processing_context: ProcessingContext | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.capture_id, CaptureId):
            raise ContractError("INVALID_CAPTURE_ID")
        if not isinstance(self.connection_id, UUID) or not isinstance(self.session_id, UUID):
            raise ContractError("INVALID_UUID")
        for value in (self.transport, self.source, self.endpoint, self.channel):
            validate_text(value, allow_empty=False)
        timestamp_ns(self.receive_timestamp_ns)
        if self.exchange_timestamp_ns is not None:
            timestamp_ns(self.exchange_timestamp_ns)
        bounded_int(self.monotonic_ns_since_collector_start, 0, U64_MAX)
        bounded_int(self.payload_length, 0, U32_MAX)
        if not isinstance(self.payload_kind, PayloadKind) or self.payload_kind == PayloadKind.UNKNOWN:
            raise ContractError("INVALID_PAYLOAD_KIND")
        if not isinstance(self.raw_content_hash, bytes) or len(self.raw_content_hash) != 32:
            raise ContractError("INVALID_SHA256_LENGTH")
        if self.venue_event_id is not None and not isinstance(self.venue_event_id, bytes):
            raise ContractError("INVALID_VENUE_EVENT_ID")
        if (self.processing_context is not None
                and not isinstance(self.processing_context, ProcessingContext)):
            raise ContractError("INVALID_PROCESSING_CONTEXT")

    def validate_payload(self, payload: bytes) -> None:
        if self.payload_length != len(payload):
            raise ContractError("PAYLOAD_LENGTH_MISMATCH")
        if self.raw_content_hash != raw_hash(payload):
            raise ContractError("RAW_HASH_MISMATCH")
