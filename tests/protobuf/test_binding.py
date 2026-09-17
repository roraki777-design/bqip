"""Official-runtime checks; unavailable runtime/generated code is an error, not a skip."""

import json
import unittest
from pathlib import Path

from bqip.capture.format import Frame
from bqip.contracts.protobuf import (
    capture_id_from_proto,
    context_from_proto,
    decimal_from_proto,
    decode_raw_frame,
    metadata_from_proto,
    operational_from_proto,
    quality_from_proto,
    raw_from_proto,
    validate_manifest_envelope,
)
from bqip.contracts.values import ContractError, DecimalValue
from bqip.identity import raw_hash
from bqip.metadata import MetadataCatalog, ResolverMode
from bqip.v1 import capture_pb2, common_pb2, manifests_pb2, metadata_pb2, operational_pb2

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def context():
    return common_pb2.ProcessingContext(processing_timestamp_ns=0, code_commit="fixture",
                                        schema_version=1, run_id=bytes(range(16)),
                                        replay_mode=common_pb2.REPLAY_MODE_LIVE)


def first_raw():
    vector = fixture("capture/frames.json")[0]
    payload = bytes.fromhex(vector["payload_hex"])
    message = capture_pb2.RawCaptureEvent.FromString(bytes.fromhex(vector["metadata_hex"]))
    return message, payload


class ProtobufBindingTests(unittest.TestCase):
    def test_generated_enums_match_external_expected_values(self):
        modules = {"ReplayMode": common_pb2, "PayloadKind": capture_pb2,
                   "RawSegmentState": capture_pb2, "OperationalKind": operational_pb2,
                   "CauseCode": operational_pb2, "RawLossStatus": operational_pb2,
                   "RealtimeContinuity": operational_pb2}
        for name, vectors in fixture("contracts/enums.json").items():
            enum = getattr(modules[name], name)
            self.assertEqual(enum.items(), [(item["wire_name"], item["number"]) for item in vectors])

    def test_decimal_golden_through_generated_contract(self):
        for vector in fixture("contracts/decimals.json"):
            expected = DecimalValue(vector["coefficient"], vector["scale"])
            message = common_pb2.DecimalValue(coefficient=expected.coefficient, scale=expected.scale)
            replay = common_pb2.DecimalValue.FromString(message.SerializeToString())
            self.assertEqual(decimal_from_proto(replay), expected)

    def test_absent_scalars_are_not_explicit_zero(self):
        with self.assertRaises(ContractError):
            decimal_from_proto(common_pb2.DecimalValue(coefficient="0"))
        self.assertEqual(decimal_from_proto(common_pb2.DecimalValue(coefficient="0", scale=0)),
                         DecimalValue("0", 0))
        with self.assertRaises(ContractError):
            capture_id_from_proto(common_pb2.CaptureId(collector_instance_id=bytes(16)))
        identity = common_pb2.CaptureId(collector_instance_id=bytes(16), capture_seq=0)
        self.assertEqual(capture_id_from_proto(identity).capture_seq, 0)

    def test_metadata_golden_through_generated_contract(self):
        for vector in fixture("metadata/vectors.json"):
            versions = tuple(metadata_from_proto(metadata_pb2.InstrumentMetadataVersion(
                **{key: value for key, value in version.items() if value is not None}))
                for version in vector["versions"])
            catalog = MetadataCatalog(versions)
            arguments = ("TEST-INSTRUMENT", vector["event_time_ns"], ResolverMode(vector["mode"]),
                         vector.get("as_of_ns"))
            with self.subTest(name=vector["name"]):
                if "error" in vector:
                    with self.assertRaisesRegex(ContractError, vector["error"]):
                        catalog.resolve(*arguments)
                else:
                    self.assertEqual(catalog.resolve(*arguments).metadata_version, vector["expected"])

    def test_raw_golden_exact_frames_and_capture_identity(self):
        for vector in fixture("capture/frames.json"):
            frame = Frame(bytes.fromhex(vector["metadata_hex"]), bytes.fromhex(vector["payload_hex"]))
            message = decode_raw_frame(frame)
            event = raw_from_proto(message, frame.payload)
            self.assertEqual(event.capture_id.capture_seq, vector["capture_seq"])
            self.assertEqual(event.capture_id.collector_instance_id.bytes, bytes(range(16)))
            self.assertEqual(event.raw_content_hash.hex(), vector["raw_sha256"])
            self.assertEqual(event.receive_timestamp_ns, 1234)
            self.assertEqual(event.monotonic_ns_since_collector_start, 7)
            self.assertEqual(event.payload_kind.value, vector["payload_kind"])
            self.assertEqual(frame.encode().hex(), vector["frame_hex"])
            replay = capture_pb2.RawCaptureEvent.FromString(message.SerializeToString())
            self.assertEqual(raw_from_proto(replay, frame.payload), event)

    def test_raw_required_presence_and_unknown_enums(self):
        original, payload = first_raw()
        for field in ("capture_id", "connection_id", "session_id", "transport", "source",
                      "endpoint", "channel", "receive_timestamp_ns", "monotonic_ns_since_collector_start",
                      "payload_kind", "payload_length", "raw_content_hash"):
            message = capture_pb2.RawCaptureEvent()
            message.CopyFrom(original)
            message.ClearField(field)
            with self.subTest(field=field), self.assertRaises(ContractError):
                raw_from_proto(message, payload)
        for invalid in (0, 999):
            original.payload_kind = invalid
            with self.assertRaises(ContractError):
                raw_from_proto(original, payload)

    def test_raw_payload_integrity_is_checked(self):
        message, payload = first_raw()
        with self.assertRaisesRegex(ContractError, "LENGTH"):
            raw_from_proto(message, payload + b"x")
        with self.assertRaisesRegex(ContractError, "HASH"):
            raw_from_proto(message, b"x" * len(payload))

    def test_replay_context_does_not_change_capture_semantics(self):
        message, payload = first_raw()
        message.processing_context.CopyFrom(context())
        before = raw_from_proto(message, payload)
        message.processing_context.processing_timestamp_ns = 1
        message.processing_context.run_id = bytes(reversed(range(16)))
        message.processing_context.replay_mode = common_pb2.REPLAY_MODE_VERSION_PINNED_RESEARCH
        replay = raw_from_proto(message, payload)
        self.assertEqual(before, replay)
        self.assertEqual(before.capture_id, replay.capture_id)
        message.processing_context.replay_mode = 999
        with self.assertRaises(ContractError):
            context_from_proto(message.processing_context)

    def test_decode_keeps_unknown_wire_fields_and_original_frame(self):
        vector = fixture("capture/frames.json")[0]
        # Independently specified Protobuf field 100, varint value 7.
        metadata = bytes.fromhex(vector["metadata_hex"]) + bytes.fromhex("a00607")
        frame = Frame(metadata, bytes.fromhex(vector["payload_hex"]))
        message = decode_raw_frame(frame)
        stripped = capture_pb2.RawCaptureEvent()
        stripped.CopyFrom(message)
        stripped.DiscardUnknownFields()
        self.assertNotEqual(message.SerializeToString(), stripped.SerializeToString())
        self.assertEqual(frame.metadata, metadata)

    def test_quality_and_operational_presence(self):
        quality = operational_pb2.QualityEvent(cause=0, raw_loss_status=0, realtime_continuity=0,
                                              timestamp_ns=0, context=context())
        self.assertEqual(quality_from_proto(quality).cause.name, "UNKNOWN")
        quality.ClearField("raw_loss_status")
        with self.assertRaises(ContractError):
            quality_from_proto(quality)
        quality.raw_loss_status = 1
        quality.cause = 999
        with self.assertRaises(ContractError):
            quality_from_proto(quality)
        event = operational_pb2.OperationalEvent(kind=1, timestamp_ns=0, context=context())
        self.assertEqual(operational_from_proto(event).kind.name, "CONNECTION_OPENED")
        event.kind = 0
        with self.assertRaises(ContractError):
            operational_from_proto(event)

    def test_manifest_envelope_golden_and_integrity(self):
        for vector in fixture("manifests/vectors.json"):
            body = bytes.fromhex(vector["canonical_hex"])
            message = manifests_pb2.ManifestEnvelope(schema_version=1, canonical_body_json=body,
                                                     manifest_hash=bytes.fromhex(vector["sha256"]))
            validate_manifest_envelope(message)
            message.canonical_body_json = body + b" "
            message.manifest_hash = raw_hash(message.canonical_body_json)
            with self.assertRaisesRegex(ContractError, "NON_CANONICAL"):
                validate_manifest_envelope(message)
            message.canonical_body_json = body
            message.manifest_hash = bytes(32)
            with self.assertRaisesRegex(ContractError, "HASH"):
                validate_manifest_envelope(message)


if __name__ == "__main__":
    unittest.main()
