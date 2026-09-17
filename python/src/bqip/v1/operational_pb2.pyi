from bqip.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OperationalKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATIONAL_KIND_UNKNOWN: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_CONNECTION_OPENED: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_CONNECTION_CLOSED: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_VENUE_GAP: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_LOCAL_CAPACITY_DROP: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_PROCESS_CRASH_GAP: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_RESYNC_STARTED: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_SNAPSHOT_SELECTED: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_RESYNC_COMPLETED: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_BOOK_HEALTH_CHANGED: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_CANONICAL_STATE_DIVERGENCE: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_STORAGE_FAILURE: _ClassVar[OperationalKind]
    OPERATIONAL_KIND_UPLOAD_FAILURE: _ClassVar[OperationalKind]

class CauseCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CAUSE_CODE_UNKNOWN: _ClassVar[CauseCode]
    CAUSE_CODE_VENUE_GAP: _ClassVar[CauseCode]
    CAUSE_CODE_LOCAL_CAPACITY_DROP: _ClassVar[CauseCode]
    CAUSE_CODE_PROCESS_CRASH_GAP: _ClassVar[CauseCode]
    CAUSE_CODE_STORAGE_FAILURE: _ClassVar[CauseCode]
    CAUSE_CODE_RAW_WRITE_FAILURE: _ClassVar[CauseCode]
    CAUSE_CODE_UPLOAD_FAILURE: _ClassVar[CauseCode]
    CAUSE_CODE_PARSE_FAILURE: _ClassVar[CauseCode]
    CAUSE_CODE_CHECKSUM_FAILURE: _ClassVar[CauseCode]
    CAUSE_CODE_STALE_DATA: _ClassVar[CauseCode]
    CAUSE_CODE_CANONICAL_STATE_DIVERGENCE: _ClassVar[CauseCode]

class RawLossStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RAW_LOSS_STATUS_UNKNOWN: _ClassVar[RawLossStatus]
    RAW_LOSS_STATUS_NONE: _ClassVar[RawLossStatus]
    RAW_LOSS_STATUS_POSSIBLE: _ClassVar[RawLossStatus]
    RAW_LOSS_STATUS_CONFIRMED: _ClassVar[RawLossStatus]

class RealtimeContinuity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REALTIME_CONTINUITY_UNKNOWN: _ClassVar[RealtimeContinuity]
    REALTIME_CONTINUITY_HEALTHY: _ClassVar[RealtimeContinuity]
    REALTIME_CONTINUITY_BROKEN: _ClassVar[RealtimeContinuity]
OPERATIONAL_KIND_UNKNOWN: OperationalKind
OPERATIONAL_KIND_CONNECTION_OPENED: OperationalKind
OPERATIONAL_KIND_CONNECTION_CLOSED: OperationalKind
OPERATIONAL_KIND_VENUE_GAP: OperationalKind
OPERATIONAL_KIND_LOCAL_CAPACITY_DROP: OperationalKind
OPERATIONAL_KIND_PROCESS_CRASH_GAP: OperationalKind
OPERATIONAL_KIND_RESYNC_STARTED: OperationalKind
OPERATIONAL_KIND_SNAPSHOT_SELECTED: OperationalKind
OPERATIONAL_KIND_RESYNC_COMPLETED: OperationalKind
OPERATIONAL_KIND_BOOK_HEALTH_CHANGED: OperationalKind
OPERATIONAL_KIND_CANONICAL_STATE_DIVERGENCE: OperationalKind
OPERATIONAL_KIND_STORAGE_FAILURE: OperationalKind
OPERATIONAL_KIND_UPLOAD_FAILURE: OperationalKind
CAUSE_CODE_UNKNOWN: CauseCode
CAUSE_CODE_VENUE_GAP: CauseCode
CAUSE_CODE_LOCAL_CAPACITY_DROP: CauseCode
CAUSE_CODE_PROCESS_CRASH_GAP: CauseCode
CAUSE_CODE_STORAGE_FAILURE: CauseCode
CAUSE_CODE_RAW_WRITE_FAILURE: CauseCode
CAUSE_CODE_UPLOAD_FAILURE: CauseCode
CAUSE_CODE_PARSE_FAILURE: CauseCode
CAUSE_CODE_CHECKSUM_FAILURE: CauseCode
CAUSE_CODE_STALE_DATA: CauseCode
CAUSE_CODE_CANONICAL_STATE_DIVERGENCE: CauseCode
RAW_LOSS_STATUS_UNKNOWN: RawLossStatus
RAW_LOSS_STATUS_NONE: RawLossStatus
RAW_LOSS_STATUS_POSSIBLE: RawLossStatus
RAW_LOSS_STATUS_CONFIRMED: RawLossStatus
REALTIME_CONTINUITY_UNKNOWN: RealtimeContinuity
REALTIME_CONTINUITY_HEALTHY: RealtimeContinuity
REALTIME_CONTINUITY_BROKEN: RealtimeContinuity

class QualityEvent(_message.Message):
    __slots__ = ("cause", "raw_loss_status", "realtime_continuity", "timestamp_ns", "context", "capture_id", "session_id")
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    RAW_LOSS_STATUS_FIELD_NUMBER: _ClassVar[int]
    REALTIME_CONTINUITY_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    cause: CauseCode
    raw_loss_status: RawLossStatus
    realtime_continuity: RealtimeContinuity
    timestamp_ns: int
    context: _common_pb2.ProcessingContext
    capture_id: _common_pb2.CaptureId
    session_id: bytes
    def __init__(self, cause: _Optional[_Union[CauseCode, str]] = ..., raw_loss_status: _Optional[_Union[RawLossStatus, str]] = ..., realtime_continuity: _Optional[_Union[RealtimeContinuity, str]] = ..., timestamp_ns: _Optional[int] = ..., context: _Optional[_Union[_common_pb2.ProcessingContext, _Mapping]] = ..., capture_id: _Optional[_Union[_common_pb2.CaptureId, _Mapping]] = ..., session_id: _Optional[bytes] = ...) -> None: ...

class OperationalEvent(_message.Message):
    __slots__ = ("kind", "timestamp_ns", "context", "capture_id", "session_id", "quality")
    KIND_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    QUALITY_FIELD_NUMBER: _ClassVar[int]
    kind: OperationalKind
    timestamp_ns: int
    context: _common_pb2.ProcessingContext
    capture_id: _common_pb2.CaptureId
    session_id: bytes
    quality: QualityEvent
    def __init__(self, kind: _Optional[_Union[OperationalKind, str]] = ..., timestamp_ns: _Optional[int] = ..., context: _Optional[_Union[_common_pb2.ProcessingContext, _Mapping]] = ..., capture_id: _Optional[_Union[_common_pb2.CaptureId, _Mapping]] = ..., session_id: _Optional[bytes] = ..., quality: _Optional[_Union[QualityEvent, _Mapping]] = ...) -> None: ...
