"""Validate official generated messages before creating immutable domain values.

Generate bqip.v1 with tools/generate_protobuf.py. Missing tooling/runtime fails
the wire gate; this module does not implement or substitute a Protobuf codec.
Keep Frame.metadata when retaining raw evidence, including unknown wire fields.
"""

from uuid import UUID

from google.protobuf.message import Message

from bqip.capture.format import Frame
from bqip.capture.model import RawCaptureEvent
from bqip.contracts.operational import (
    CauseCode,
    OperationalEvent,
    OperationalKind,
    QualityEvent,
    RawLossStatus,
    RealtimeContinuity,
)
from bqip.contracts.values import (
    U32_MAX,
    CaptureId,
    ContractError,
    DecimalValue,
    PayloadKind,
    ProcessingContext,
    ReplayMode,
    bounded_int,
)
from bqip.identity import raw_hash
from bqip.manifests import canonical_manifest, parse_manifest
from bqip.metadata import InstrumentMetadataVersion
from bqip.v1 import capture_pb2, common_pb2, manifests_pb2, metadata_pb2, operational_pb2


def _present(message: Message, *fields: str) -> None:
    for field in fields:
        if not message.HasField(field):
            raise ContractError(f"MISSING_FIELD: {field}")


def _uuid(value: bytes) -> UUID:
    if len(value) != 16:
        raise ContractError("INVALID_UUID_LENGTH")
    return UUID(bytes=value)


def decimal_from_proto(message: common_pb2.DecimalValue) -> DecimalValue:
    _present(message, "coefficient", "scale")
    return DecimalValue(message.coefficient, message.scale)


def capture_id_from_proto(message: common_pb2.CaptureId) -> CaptureId:
    _present(message, "collector_instance_id", "capture_seq")
    return CaptureId(_uuid(message.collector_instance_id), message.capture_seq)


def context_from_proto(message: common_pb2.ProcessingContext) -> ProcessingContext:
    _present(message, "processing_timestamp_ns", "code_commit", "schema_version", "run_id",
             "replay_mode")
    try:
        mode = ReplayMode(message.replay_mode)
    except ValueError as error:
        raise ContractError("INVALID_REPLAY_MODE") from error
    return ProcessingContext(message.processing_timestamp_ns, message.code_commit,
                             message.schema_version, _uuid(message.run_id), mode)


def metadata_from_proto(message: metadata_pb2.InstrumentMetadataVersion
                        ) -> InstrumentMetadataVersion:
    _present(message, "instrument_id", "metadata_version", "effective_from_ns", "known_from_ns",
             "product_type", "margin_asset", "settlement_asset")
    return InstrumentMetadataVersion(
        instrument_id=message.instrument_id,
        metadata_version=message.metadata_version,
        effective_from_ns=message.effective_from_ns,
        known_from_ns=message.known_from_ns,
        product_type=message.product_type,
        margin_asset=message.margin_asset,
        settlement_asset=message.settlement_asset,
        effective_to_ns=message.effective_to_ns if message.HasField("effective_to_ns") else None,
        known_to_ns=message.known_to_ns if message.HasField("known_to_ns") else None,
        supersedes_version=message.supersedes_version if message.HasField("supersedes_version") else None,
        contract_multiplier=(decimal_from_proto(message.contract_multiplier)
                             if message.HasField("contract_multiplier") else None),
        quantity_unit=message.quantity_unit if message.HasField("quantity_unit") else None,
        tick_size=decimal_from_proto(message.tick_size) if message.HasField("tick_size") else None,
        lot_size=decimal_from_proto(message.lot_size) if message.HasField("lot_size") else None,
    )


def raw_from_proto(message: capture_pb2.RawCaptureEvent, payload: bytes) -> RawCaptureEvent:
    _present(message, "capture_id", "connection_id", "session_id", "transport", "source",
             "endpoint", "channel", "receive_timestamp_ns", "monotonic_ns_since_collector_start",
             "payload_kind", "payload_length", "raw_content_hash")
    try:
        kind = PayloadKind(message.payload_kind)
    except ValueError as error:
        raise ContractError("INVALID_PAYLOAD_KIND") from error
    event = RawCaptureEvent(
        capture_id=capture_id_from_proto(message.capture_id),
        connection_id=_uuid(message.connection_id),
        session_id=_uuid(message.session_id),
        transport=message.transport,
        source=message.source,
        endpoint=message.endpoint,
        channel=message.channel,
        receive_timestamp_ns=message.receive_timestamp_ns,
        monotonic_ns_since_collector_start=message.monotonic_ns_since_collector_start,
        payload_kind=kind,
        payload_length=message.payload_length,
        raw_content_hash=message.raw_content_hash,
        exchange_timestamp_ns=(message.exchange_timestamp_ns
                               if message.HasField("exchange_timestamp_ns") else None),
        venue_event_id=message.venue_event_id if message.HasField("venue_event_id") else None,
        processing_context=(context_from_proto(message.processing_context)
                            if message.HasField("processing_context") else None),
    )
    event.validate_payload(payload)
    return event


def decode_raw_frame(frame: Frame) -> capture_pb2.RawCaptureEvent:
    """Return the original parsed message after presence and payload validation."""
    message = capture_pb2.RawCaptureEvent()
    message.ParseFromString(frame.metadata)
    raw_from_proto(message, frame.payload)
    return message


def quality_from_proto(message: operational_pb2.QualityEvent) -> QualityEvent:
    _present(message, "cause", "raw_loss_status", "realtime_continuity", "timestamp_ns", "context")
    try:
        cause = CauseCode(message.cause)
        loss = RawLossStatus(message.raw_loss_status)
        continuity = RealtimeContinuity(message.realtime_continuity)
    except ValueError as error:
        raise ContractError("INVALID_QUALITY_ENUM") from error
    return QualityEvent(cause, loss, continuity, message.timestamp_ns,
                        context_from_proto(message.context),
                        capture_id_from_proto(message.capture_id) if message.HasField("capture_id") else None,
                        _uuid(message.session_id) if message.HasField("session_id") else None)


def operational_from_proto(message: operational_pb2.OperationalEvent) -> OperationalEvent:
    _present(message, "kind", "timestamp_ns", "context")
    try:
        kind = OperationalKind(message.kind)
    except ValueError as error:
        raise ContractError("INVALID_OPERATIONAL_KIND") from error
    return OperationalEvent(kind, message.timestamp_ns, context_from_proto(message.context),
                            capture_id_from_proto(message.capture_id) if message.HasField("capture_id") else None,
                            _uuid(message.session_id) if message.HasField("session_id") else None,
                            quality_from_proto(message.quality) if message.HasField("quality") else None)


def validate_manifest_envelope(message: manifests_pb2.ManifestEnvelope) -> None:
    _present(message, "schema_version", "canonical_body_json", "manifest_hash")
    bounded_int(message.schema_version, 1, U32_MAX)
    try:
        body = parse_manifest(message.canonical_body_json.decode("utf-8", errors="strict"))
    except (ValueError, UnicodeError) as error:
        raise ContractError("INVALID_MANIFEST_BODY") from error
    if canonical_manifest(body) != message.canonical_body_json:
        raise ContractError("NON_CANONICAL_MANIFEST_BODY")
    if raw_hash(message.canonical_body_json) != message.manifest_hash:
        raise ContractError("MANIFEST_HASH_MISMATCH")
