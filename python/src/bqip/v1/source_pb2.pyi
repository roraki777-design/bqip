from bqip.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SourceTransport(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SOURCE_TRANSPORT_UNSPECIFIED: _ClassVar[SourceTransport]
    SOURCE_TRANSPORT_HTTP_REST: _ClassVar[SourceTransport]
    SOURCE_TRANSPORT_WEBSOCKET: _ClassVar[SourceTransport]
SOURCE_TRANSPORT_UNSPECIFIED: SourceTransport
SOURCE_TRANSPORT_HTTP_REST: SourceTransport
SOURCE_TRANSPORT_WEBSOCKET: SourceTransport

class SourceLineage(_message.Message):
    __slots__ = ("venue_profile_id", "venue_profile_hash", "capture_id", "raw_content_hash", "receive_timestamp_ns")
    VENUE_PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    VENUE_PROFILE_HASH_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_ID_FIELD_NUMBER: _ClassVar[int]
    RAW_CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    RECEIVE_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    venue_profile_id: str
    venue_profile_hash: bytes
    capture_id: _common_pb2.CaptureId
    raw_content_hash: bytes
    receive_timestamp_ns: int
    def __init__(self, venue_profile_id: _Optional[str] = ..., venue_profile_hash: _Optional[bytes] = ..., capture_id: _Optional[_Union[_common_pb2.CaptureId, _Mapping]] = ..., raw_content_hash: _Optional[bytes] = ..., receive_timestamp_ns: _Optional[int] = ...) -> None: ...

class QueryParameter(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: str
    def __init__(self, name: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class HttpSourceContext(_message.Message):
    __slots__ = ("lineage", "transport", "method", "endpoint", "query_parameters", "http_status")
    LINEAGE_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    QUERY_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    HTTP_STATUS_FIELD_NUMBER: _ClassVar[int]
    lineage: SourceLineage
    transport: SourceTransport
    method: str
    endpoint: str
    query_parameters: _containers.RepeatedCompositeFieldContainer[QueryParameter]
    http_status: int
    def __init__(self, lineage: _Optional[_Union[SourceLineage, _Mapping]] = ..., transport: _Optional[_Union[SourceTransport, str]] = ..., method: _Optional[str] = ..., endpoint: _Optional[str] = ..., query_parameters: _Optional[_Iterable[_Union[QueryParameter, _Mapping]]] = ..., http_status: _Optional[int] = ...) -> None: ...

class StreamSourceContext(_message.Message):
    __slots__ = ("lineage", "transport", "endpoint", "channel", "original_session_id")
    LINEAGE_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    lineage: SourceLineage
    transport: SourceTransport
    endpoint: str
    channel: str
    original_session_id: bytes
    def __init__(self, lineage: _Optional[_Union[SourceLineage, _Mapping]] = ..., transport: _Optional[_Union[SourceTransport, str]] = ..., endpoint: _Optional[str] = ..., channel: _Optional[str] = ..., original_session_id: _Optional[bytes] = ...) -> None: ...
