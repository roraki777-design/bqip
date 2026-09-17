"""Operational and quality values; no realtime behavior is implemented here."""

from dataclasses import dataclass
from enum import IntEnum
from uuid import UUID

from bqip.contracts.values import CaptureId, ContractError, ProcessingContext, timestamp_ns


class OperationalKind(IntEnum):
    UNKNOWN = 0
    CONNECTION_OPENED = 1
    CONNECTION_CLOSED = 2
    VENUE_GAP = 3
    LOCAL_CAPACITY_DROP = 4
    PROCESS_CRASH_GAP = 5
    RESYNC_STARTED = 6
    SNAPSHOT_SELECTED = 7
    RESYNC_COMPLETED = 8
    BOOK_HEALTH_CHANGED = 9
    CANONICAL_STATE_DIVERGENCE = 10
    STORAGE_FAILURE = 11
    UPLOAD_FAILURE = 12


class CauseCode(IntEnum):
    UNKNOWN = 0
    VENUE_GAP = 1
    LOCAL_CAPACITY_DROP = 2
    PROCESS_CRASH_GAP = 3
    STORAGE_FAILURE = 4
    RAW_WRITE_FAILURE = 5
    UPLOAD_FAILURE = 6
    PARSE_FAILURE = 7
    CHECKSUM_FAILURE = 8
    STALE_DATA = 9
    CANONICAL_STATE_DIVERGENCE = 10


class RawLossStatus(IntEnum):
    UNKNOWN = 0
    NONE = 1
    POSSIBLE = 2
    CONFIRMED = 3


class RealtimeContinuity(IntEnum):
    UNKNOWN = 0
    HEALTHY = 1
    BROKEN = 2


@dataclass(frozen=True)
class QualityEvent:
    cause: CauseCode
    raw_loss_status: RawLossStatus
    realtime_continuity: RealtimeContinuity
    timestamp_ns: int
    context: ProcessingContext
    capture_id: CaptureId | None = None
    session_id: UUID | None = None

    def __post_init__(self) -> None:
        timestamp_ns(self.timestamp_ns)
        if not isinstance(self.cause, CauseCode):
            raise ContractError("INVALID_CAUSE")
        if not isinstance(self.raw_loss_status, RawLossStatus):
            raise ContractError("INVALID_RAW_LOSS_STATUS")
        if not isinstance(self.realtime_continuity, RealtimeContinuity):
            raise ContractError("INVALID_REALTIME_CONTINUITY")
        if not isinstance(self.context, ProcessingContext):
            raise ContractError("INVALID_CONTEXT")
        if self.capture_id is not None and not isinstance(self.capture_id, CaptureId):
            raise ContractError("INVALID_CAPTURE_ID")
        if self.session_id is not None and not isinstance(self.session_id, UUID):
            raise ContractError("INVALID_SESSION_ID")


@dataclass(frozen=True)
class OperationalEvent:
    kind: OperationalKind
    timestamp_ns: int
    context: ProcessingContext
    capture_id: CaptureId | None = None
    session_id: UUID | None = None
    quality: QualityEvent | None = None

    def __post_init__(self) -> None:
        timestamp_ns(self.timestamp_ns)
        if not isinstance(self.kind, OperationalKind) or self.kind == OperationalKind.UNKNOWN:
            raise ContractError("INVALID_OPERATIONAL_KIND")
        if not isinstance(self.context, ProcessingContext):
            raise ContractError("INVALID_CONTEXT")
        if self.capture_id is not None and not isinstance(self.capture_id, CaptureId):
            raise ContractError("INVALID_CAPTURE_ID")
        if self.session_id is not None and not isinstance(self.session_id, UUID):
            raise ContractError("INVALID_SESSION_ID")
        if self.quality is not None and not isinstance(self.quality, QualityEvent):
            raise ContractError("INVALID_QUALITY_EVENT")
