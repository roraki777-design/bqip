from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReplayMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REPLAY_MODE_UNKNOWN: _ClassVar[ReplayMode]
    REPLAY_MODE_LIVE: _ClassVar[ReplayMode]
    REPLAY_MODE_VERSION_PINNED_RESEARCH: _ClassVar[ReplayMode]
    REPLAY_MODE_AS_LIVED_FORENSIC: _ClassVar[ReplayMode]
REPLAY_MODE_UNKNOWN: ReplayMode
REPLAY_MODE_LIVE: ReplayMode
REPLAY_MODE_VERSION_PINNED_RESEARCH: ReplayMode
REPLAY_MODE_AS_LIVED_FORENSIC: ReplayMode

class DecimalValue(_message.Message):
    __slots__ = ("coefficient", "scale")
    COEFFICIENT_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    coefficient: str
    scale: int
    def __init__(self, coefficient: _Optional[str] = ..., scale: _Optional[int] = ...) -> None: ...

class CaptureId(_message.Message):
    __slots__ = ("collector_instance_id", "capture_seq")
    COLLECTOR_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_SEQ_FIELD_NUMBER: _ClassVar[int]
    collector_instance_id: bytes
    capture_seq: int
    def __init__(self, collector_instance_id: _Optional[bytes] = ..., capture_seq: _Optional[int] = ...) -> None: ...

class ProcessingContext(_message.Message):
    __slots__ = ("processing_timestamp_ns", "code_commit", "schema_version", "run_id", "replay_mode")
    PROCESSING_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    CODE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    REPLAY_MODE_FIELD_NUMBER: _ClassVar[int]
    processing_timestamp_ns: int
    code_commit: str
    schema_version: int
    run_id: bytes
    replay_mode: ReplayMode
    def __init__(self, processing_timestamp_ns: _Optional[int] = ..., code_commit: _Optional[str] = ..., schema_version: _Optional[int] = ..., run_id: _Optional[bytes] = ..., replay_mode: _Optional[_Union[ReplayMode, str]] = ...) -> None: ...
