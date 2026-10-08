"""Typed immutable HTTP evidence and presence-checked source contracts."""

from dataclasses import dataclass
from uuid import UUID

from google.protobuf.message import Message

from bqip.contracts.values import CaptureId, ContractError, timestamp_ns, validate_text
from bqip.identity import raw_hash
from bqip.v1 import common_pb2, source_pb2
from bqip.venues.profile import PROFILE_ID, profile_hash


def present(message: Message, *fields: str) -> None:
    if any(not message.HasField(field) for field in fields):
        raise ContractError("MISSING_SOURCE_FIELD")


@dataclass(frozen=True)
class HttpSource:
    venue_profile_id: str
    venue_profile_hash: bytes
    capture_id: CaptureId
    raw_content_hash: bytes
    receive_timestamp_ns: int
    method: str
    endpoint: str
    query_parameters: tuple[tuple[str, str], ...]
    http_status: int

    def __post_init__(self) -> None:
        if type(self.venue_profile_hash) is not bytes or type(self.raw_content_hash) is not bytes:
            raise ContractError("INVALID_SOURCE_LINEAGE")
        if self.venue_profile_id != PROFILE_ID or self.venue_profile_hash != profile_hash():
            raise ContractError("PROFILE_MISMATCH")
        if not isinstance(self.capture_id, CaptureId) or len(self.raw_content_hash) != 32:
            raise ContractError("INVALID_SOURCE_LINEAGE")
        timestamp_ns(self.receive_timestamp_ns)
        if self.method != "GET" or self.endpoint not in ("/fapi/v1/exchangeInfo", "/fapi/v1/depth"):
            raise ContractError("PROFILE_MISMATCH")
        if type(self.http_status) is not int or self.http_status != 200:
            raise ContractError("HTTP_STATUS_FAILURE")
        if not isinstance(self.query_parameters, tuple):
            raise ContractError("NON_CANONICAL_QUERY")
        previous = ""
        for key, value in self.query_parameters:
            if not key.isascii() or key <= previous:
                raise ContractError("NON_CANONICAL_QUERY")
            validate_text(value)
            previous = key
        expected = () if self.endpoint.endswith("exchangeInfo") else (("limit", "1000"), ("symbol", "BTCUSDT"))
        if self.query_parameters != expected:
            raise ContractError("PROFILE_MISMATCH")

    def to_proto(self) -> source_pb2.HttpSourceContext:
        return source_pb2.HttpSourceContext(
            lineage=source_pb2.SourceLineage(
                venue_profile_id=self.venue_profile_id, venue_profile_hash=self.venue_profile_hash,
                capture_id=common_pb2.CaptureId(collector_instance_id=self.capture_id.collector_instance_id.bytes,
                                              capture_seq=self.capture_id.capture_seq),
                raw_content_hash=self.raw_content_hash, receive_timestamp_ns=self.receive_timestamp_ns),
            transport=source_pb2.SOURCE_TRANSPORT_HTTP_REST, method=self.method, endpoint=self.endpoint,
            query_parameters=[source_pb2.QueryParameter(name=k, value=v) for k, v in self.query_parameters],
            http_status=self.http_status)


def validate_lineage(message: source_pb2.SourceLineage, payload: bytes | None = None) -> None:
    present(message, "venue_profile_id", "venue_profile_hash", "capture_id", "raw_content_hash", "receive_timestamp_ns")
    present(message.capture_id, "collector_instance_id", "capture_seq")
    if len(message.capture_id.collector_instance_id) != 16 or len(message.raw_content_hash) != 32:
        raise ContractError("INVALID_SOURCE_LINEAGE")
    if message.venue_profile_id != PROFILE_ID or message.venue_profile_hash != profile_hash():
        raise ContractError("PROFILE_MISMATCH")
    if payload is not None and message.raw_content_hash != raw_hash(payload):
        raise ContractError("RAW_HASH_MISMATCH")


def http_from_proto(message: source_pb2.HttpSourceContext, payload: bytes | None = None) -> HttpSource:
    present(message, "lineage", "transport", "method", "endpoint", "http_status")
    if message.transport != source_pb2.SOURCE_TRANSPORT_HTTP_REST:
        raise ContractError("PROFILE_MISMATCH")
    validate_lineage(message.lineage, payload)
    for item in message.query_parameters:
        present(item, "name", "value")
    lineage = message.lineage
    return HttpSource(lineage.venue_profile_id, lineage.venue_profile_hash,
                      CaptureId(UUID(bytes=lineage.capture_id.collector_instance_id), lineage.capture_id.capture_seq),
                      lineage.raw_content_hash, lineage.receive_timestamp_ns,
                      message.method, message.endpoint,
                      tuple((q.name, q.value) for q in message.query_parameters), message.http_status)


def validate_stream(message: source_pb2.StreamSourceContext, endpoint: str, channel: str,
                    payload: bytes | None = None) -> None:
    present(message, "lineage", "transport", "endpoint", "channel", "original_session_id")
    validate_lineage(message.lineage, payload)
    if (message.transport != source_pb2.SOURCE_TRANSPORT_WEBSOCKET
            or message.endpoint != endpoint or message.channel != channel):
        raise ContractError("PROFILE_MISMATCH")
    if len(message.original_session_id) != 16:
        raise ContractError("INVALID_ORIGINAL_SESSION")
