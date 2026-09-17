from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ManifestEnvelope(_message.Message):
    __slots__ = ("schema_version", "canonical_body_json", "manifest_hash", "comments")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    CANONICAL_BODY_JSON_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_HASH_FIELD_NUMBER: _ClassVar[int]
    COMMENTS_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    canonical_body_json: bytes
    manifest_hash: bytes
    comments: str
    def __init__(self, schema_version: _Optional[int] = ..., canonical_body_json: _Optional[bytes] = ..., manifest_hash: _Optional[bytes] = ..., comments: _Optional[str] = ...) -> None: ...
