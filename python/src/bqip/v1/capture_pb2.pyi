from bqip.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PayloadKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYLOAD_KIND_UNKNOWN: _ClassVar[PayloadKind]
    PAYLOAD_KIND_TEXT: _ClassVar[PayloadKind]
    PAYLOAD_KIND_BINARY: _ClassVar[PayloadKind]

class RawSegmentState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RAW_SEGMENT_STATE_UNKNOWN: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_OPEN: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_SEALED_LOCAL: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_UPLOADED: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_VERIFIED_REMOTE: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_MANIFEST_COMMITTED: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_GC_ELIGIBLE: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_RECOVERED_PARTIAL: _ClassVar[RawSegmentState]
    RAW_SEGMENT_STATE_FAILED: _ClassVar[RawSegmentState]
PAYLOAD_KIND_UNKNOWN: PayloadKind
PAYLOAD_KIND_TEXT: PayloadKind
PAYLOAD_KIND_BINARY: PayloadKind
RAW_SEGMENT_STATE_UNKNOWN: RawSegmentState
RAW_SEGMENT_STATE_OPEN: RawSegmentState
RAW_SEGMENT_STATE_SEALED_LOCAL: RawSegmentState
RAW_SEGMENT_STATE_UPLOADED: RawSegmentState
RAW_SEGMENT_STATE_VERIFIED_REMOTE: RawSegmentState
RAW_SEGMENT_STATE_MANIFEST_COMMITTED: RawSegmentState
RAW_SEGMENT_STATE_GC_ELIGIBLE: RawSegmentState
RAW_SEGMENT_STATE_RECOVERED_PARTIAL: RawSegmentState
RAW_SEGMENT_STATE_FAILED: RawSegmentState

class RawCaptureEvent(_message.Message):
    __slots__ = ("capture_id", "connection_id", "session_id", "transport", "source", "endpoint", "channel", "receive_timestamp_ns", "monotonic_ns_since_collector_start", "payload_kind", "payload_length", "raw_content_hash", "exchange_timestamp_ns", "venue_event_id", "processing_context")
    CAPTURE_ID_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    MONOTONIC_NS_SINCE_COLLECTOR_START_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_KIND_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_LENGTH_FIELD_NUMBER: _ClassVar[int]
    RAW_CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    EXCHANGE_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    VENUE_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSING_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    capture_id: _common_pb2.CaptureId
    connection_id: bytes
    session_id: bytes
    transport: str
    source: str
    endpoint: str
    channel: str
    receive_timestamp_ns: int
    monotonic_ns_since_collector_start: int
    payload_kind: PayloadKind
    payload_length: int
    raw_content_hash: bytes
    exchange_timestamp_ns: int
    venue_event_id: bytes
    processing_context: _common_pb2.ProcessingContext
    def __init__(self, capture_id: _Optional[_Union[_common_pb2.CaptureId, _Mapping]] = ..., connection_id: _Optional[bytes] = ..., session_id: _Optional[bytes] = ..., transport: _Optional[str] = ..., source: _Optional[str] = ..., endpoint: _Optional[str] = ..., channel: _Optional[str] = ..., receive_timestamp_ns: _Optional[int] = ..., monotonic_ns_since_collector_start: _Optional[int] = ..., payload_kind: _Optional[_Union[PayloadKind, str]] = ..., payload_length: _Optional[int] = ..., raw_content_hash: _Optional[bytes] = ..., exchange_timestamp_ns: _Optional[int] = ..., venue_event_id: _Optional[bytes] = ..., processing_context: _Optional[_Union[_common_pb2.ProcessingContext, _Mapping]] = ...) -> None: ...
