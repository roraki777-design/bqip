"""Independent expected fixtures and deterministic finite property checks."""

from __future__ import annotations

import hashlib
import io
import itertools
import json
import random
import re
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from bqip.capture.format import (
    CaptureFormatError,
    Frame,
    Limits,
    SegmentWriter,
    crc32c,
    iter_frames,
    read_frame,
    read_header,
    recover,
    recover_to,
    verify_segment,
)
from bqip.capture.model import RawCaptureEvent
from bqip.contracts import operational, values
from bqip.contracts.values import (
    U64_MAX,
    CaptureId,
    ContractError,
    DecimalValue,
    PayloadKind,
    ProcessingContext,
    RawSegmentState,
    ReplayMode,
    timestamp_ns,
)
from bqip.identity import (
    CaptureSequencer,
    enum_bytes,
    integer_bytes,
    logical_hash,
    preimage,
    raw_hash,
)
from bqip.manifests import canonical_bytes, canonical_manifest, manifest_hash, parse_manifest
from bqip.metadata import InstrumentMetadataVersion, MetadataCatalog, ResolverMode

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests/fixtures"


def fixture(relative: str):
    return json.loads((FIXTURES / relative).read_text(encoding="utf8"))


class DecimalTests(unittest.TestCase):
    def test_normative_vectors(self):
        for vector in fixture("contracts/decimals.json"):
            with self.subTest(vector=vector):
                result = DecimalValue.parse(vector["input"])
                self.assertEqual(result, DecimalValue(vector["coefficient"], vector["scale"]))

    def test_invalid_inputs(self):
        for text in ("", "+1", "1e3", " 1", "1 ", "NaN", "1.", ".1", "١", "--1"):
            with self.subTest(text=text), self.assertRaises(ContractError):
                DecimalValue.parse(text)

    def test_idempotent_canonicalization(self):
        for value, scale in itertools.product(range(-150, 151), range(8)):
            first = DecimalValue.normalize(str(value), scale)
            self.assertEqual(first, DecimalValue.normalize(first.coefficient, first.scale))

    def test_persisted_noncanonical_values_rejected(self):
        for coefficient, scale in (("-0", 0), ("00", 0), ("0", 1), ("120", 1), ("+1", 0)):
            with self.assertRaises(ContractError):
                DecimalValue(coefficient, scale)

    def test_very_large_coefficient_no_int_or_float_conversion(self):
        digits = "9" * 6000
        self.assertEqual(DecimalValue.parse(digits).coefficient, digits)

    def test_time_bounds(self):
        for value in (-(1 << 63), 0, (1 << 63) - 1):
            self.assertEqual(timestamp_ns(value), value)
        for value in (-(1 << 63) - 1, 1 << 63, True, 1.0):
            with self.assertRaises(ContractError):
                timestamp_ns(value)


class IdentityTests(unittest.TestCase):
    def test_normative_preimages_and_hashes(self):
        for vector in fixture("identity/vectors.json"):
            components = [(label, bytes.fromhex(value)) for label, value in vector["components"]]
            with self.subTest(name=vector["name"]):
                self.assertEqual(preimage(components).hex(), vector["preimage_hex"])
                self.assertEqual(logical_hash(components).hex(), vector["sha256"])

    def test_integer_decimal_enum_encoding(self):
        self.assertEqual(integer_bytes(-42), b"-42")
        self.assertEqual(DecimalValue.parse("123.4500").identity_bytes(), b"12345:2")
        self.assertEqual(enum_bytes("LIVE"), b"LIVE")
        with self.assertRaises(ContractError):
            enum_bytes("Live")

    def test_order_and_stability(self):
        for n in range(100):
            components = [("left", integer_bytes(n)), ("right", integer_bytes(n + 1))]
            self.assertEqual(preimage(components), preimage(components))
            self.assertNotEqual(logical_hash(components), logical_hash(components[::-1]))

    def test_capture_identity_lifecycle_and_overflow(self):
        sequencer = CaptureSequencer()
        first = sequencer.next_id()
        a, b = sequencer.new_connection_id(), sequencer.new_connection_id()
        self.assertNotEqual(a, b)
        self.assertNotEqual(sequencer.new_session_id(), sequencer.new_session_id())
        second = sequencer.next_id()
        self.assertEqual(first.collector_instance_id, second.collector_instance_id)
        self.assertEqual((first.capture_seq, second.capture_seq), (0, 1))
        sequencer._next_seq = U64_MAX
        self.assertEqual(sequencer.next_id().capture_seq, U64_MAX)
        with self.assertRaises(ContractError):
            sequencer.next_id()


class MetadataTests(unittest.TestCase):
    def test_normative_vectors_independent_of_input_order(self):
        for vector in fixture("metadata/vectors.json"):
            for ordering in itertools.permutations(vector["versions"]):
                catalog = MetadataCatalog(tuple(InstrumentMetadataVersion(**v) for v in ordering))
                arguments = ("TEST-INSTRUMENT", vector["event_time_ns"],
                             ResolverMode(vector["mode"]), vector.get("as_of_ns"))
                with self.subTest(name=vector["name"]):
                    if "error" in vector:
                        with self.assertRaisesRegex(ContractError, vector["error"]):
                            catalog.resolve(*arguments)
                    else:
                        self.assertEqual(catalog.resolve(*arguments).metadata_version, vector["expected"])

    def test_immutable_append_and_bad_catalogs(self):
        source = fixture("metadata/vectors.json")[0]["versions"]
        first, second = (InstrumentMetadataVersion(**v) for v in source)
        old = MetadataCatalog((first,))
        new = old.append(second)
        self.assertEqual(len(old.versions), 1)
        self.assertEqual(len(new.versions), 2)
        with self.assertRaisesRegex(ContractError, "DUPLICATE"):
            old.append(first)
        with self.assertRaisesRegex(ContractError, "INVALID_SUPERSESSION"):
            old.append(replace(second, supersedes_version="absent"))
        with self.assertRaisesRegex(ContractError, "AS_OF_REQUIRED"):
            old.resolve(first.instrument_id, 100, ResolverMode.AS_LIVED)
        with self.assertRaisesRegex(ContractError, "INTERVAL"):
            replace(first, effective_to_ns=first.effective_from_ns)

    def test_overlaps_never_choose_by_unrelated_known_time(self):
        source = fixture("metadata/vectors.json")[0]["versions"][0]
        first = InstrumentMetadataVersion(**source)
        for known in range(11, 50):
            second = replace(first, metadata_version="other", known_from_ns=known)
            catalog = MetadataCatalog((first, second))
            with self.assertRaisesRegex(ContractError, "METADATA_AMBIGUOUS"):
                catalog.resolve(first.instrument_id, 150, ResolverMode.CORRECTED_RESEARCH)


class ManifestTests(unittest.TestCase):
    def test_normative_bytes_and_hashes(self):
        for vector in fixture("manifests/vectors.json"):
            with self.subTest(vector=vector):
                self.assertEqual(canonical_manifest(vector["input"]).hex(), vector["canonical_hex"])
                self.assertEqual(manifest_hash(vector["input"]).hex(), vector["sha256"])

    def test_exclusion_is_root_only(self):
        base = {"schema_version": 1, "nested": {"manifest_hash": "included"}}
        for annotation in ("x", "y", "", "0" * 64):
            self.assertEqual(manifest_hash(base), manifest_hash(dict(base, manifest_hash=annotation)))
            self.assertEqual(manifest_hash(base), manifest_hash(dict(base, comments=annotation)))
        self.assertNotEqual(manifest_hash(base), manifest_hash({"schema_version": 1, "nested": {}}))

    def test_strict_rejection(self):
        for text in ('{"x":1,"x":2}', '{"x":{"a":0,"a":1}}', '{"x":1.0}',
                     '{"x":NaN}', '{"x":1e3}', '{"x":9007199254740992}',
                     '{"x":"\\ud800"}', '[]'):
            with self.subTest(text=text), self.assertRaises((ContractError, ValueError)):
                parse_manifest(text)

    def test_utf16_sort_control_escape_and_no_unicode_normalization(self):
        self.assertEqual(canonical_bytes({"\ue000": 1, "😀": 2}), '{"😀":2,"\ue000":1}'.encode())
        self.assertEqual(canonical_bytes("\x00\b\t\n\f\r"), b'"\\u0000\\b\\t\\n\\f\\r"')
        self.assertNotEqual(canonical_bytes("é"), canonical_bytes("e\u0301"))
        self.assertEqual(canonical_manifest(parse_manifest('{"x":-0}')), b'{"x":0}')


class CaptureTests(unittest.TestCase):
    def test_crc_check_value_and_normative_frames(self):
        self.assertEqual(crc32c(b"123456789"), 0xE3069283)
        for vector in fixture("capture/frames.json"):
            frame = Frame(bytes.fromhex(vector["metadata_hex"]), bytes.fromhex(vector["payload_hex"]))
            self.assertEqual(frame.encode().hex(), vector["frame_hex"])
            self.assertEqual(read_frame(io.BytesIO(bytes.fromhex(vector["frame_hex"])), 0), frame)
            self.assertEqual(raw_hash(frame.payload).hex(), vector["raw_sha256"])

    def test_streamed_segment(self):
        path = FIXTURES / "capture/complete.bqrc"
        vectors = fixture("capture/frames.json")
        expected = fixture("capture/segments.json")
        with path.open("rb") as stream:
            self.assertEqual(str(read_header(stream)), expected["segment_id"])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected["complete_sha256"])
        frames = list(iter_frames(path))
        self.assertEqual([f.payload.hex() for f in frames], [v["payload_hex"] for v in vectors])

    def test_random_binary_payload_round_trip(self):
        randomizer = random.Random(1001)
        for length in range(130):
            frame = Frame(randomizer.randbytes(length // 3), randomizer.randbytes(length))
            self.assertEqual(read_frame(io.BytesIO(frame.encode()), 0), frame)

    def test_every_final_frame_truncation_visible(self):
        frame = bytes.fromhex(fixture("capture/frames.json")[0]["frame_hex"])
        for length in range(1, len(frame)):
            with self.subTest(length=length), self.assertRaisesRegex(CaptureFormatError, "TRUNCATED"):
                read_frame(io.BytesIO(frame[:length]), 0)
        self.assertIsNone(read_frame(io.BytesIO(b""), 0))

    def test_crc_corruption_and_limits(self):
        corrupted = bytes.fromhex(fixture("capture/frames.json")[0]["frame_hex"])
        corrupted = corrupted[:-1] + bytes([corrupted[-1] ^ 1])
        with self.assertRaisesRegex(CaptureFormatError, "CRC_MISMATCH"):
            read_frame(io.BytesIO(corrupted), 0)
        with self.assertRaisesRegex(CaptureFormatError, "LIMIT"):
            read_frame(io.BytesIO(b"\xff" * 8), 0, Limits(100, 100))

    def test_header_errors(self):
        for data in (b"", b"BQRC", b"FAIL" + b"\0" * 20,
                     b"BQRC\0\0\0\2" + b"\0" * 16):
            with self.assertRaises(CaptureFormatError):
                read_header(io.BytesIO(data))

    def test_recovery_reports_and_no_corruption_skip(self):
        expected = fixture("capture/segments.json")
        truncated = recover(FIXTURES / "capture/truncated.bqrc")
        self.assertEqual(truncated.state, RawSegmentState.RECOVERED_PARTIAL)
        self.assertEqual(truncated.failure, "TRUNCATED_RECORD")
        self.assertEqual(truncated.record_count, expected["truncated_good_records"])
        self.assertEqual(truncated.valid_prefix_bytes, expected["truncated_prefix_bytes"])
        corrupted = recover(FIXTURES / "capture/corrupt.bqrc")
        self.assertEqual(corrupted.state, RawSegmentState.FAILED)
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(CaptureFormatError, "CRC_MISMATCH"):
                recover_to(FIXTURES / "capture/corrupt.bqrc", Path(temporary))

    def test_local_seal_atomic_path_and_non_destructive_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with SegmentWriter(root) as writer:
                original = writer.partial_path
                frame = Frame(b"\x50\x01", b"\xff\x00")
                writer.append(frame)
                sealed = writer.seal()
                self.assertEqual(writer.state, RawSegmentState.SEALED_LOCAL)
                self.assertFalse(original.exists())
                self.assertEqual(list(iter_frames(sealed.path)), [frame])
                self.assertEqual(sealed.sha256, hashlib.sha256(sealed.path.read_bytes()).hexdigest())
                self.assertEqual(sealed.byte_length, sealed.path.stat().st_size)
            source = FIXTURES / "capture/truncated.bqrc"
            before = source.read_bytes()
            report, saved = recover_to(source, root)
            self.assertEqual(source.read_bytes(), before)
            self.assertNotEqual(saved.segment_id, report.segment_id)
            self.assertEqual(saved.record_count, report.record_count)
            recovered_frames = list(iter_frames(saved.path))
            self.assertEqual(recovered_frames, list(iter_frames(FIXTURES / "capture/complete.bqrc"))[:-1])

    def test_failure_before_seal_does_not_publish(self):
        with tempfile.TemporaryDirectory() as temporary:
            with SegmentWriter(Path(temporary)) as writer:
                writer.append(Frame(b"metadata", b"evidence"))
                with patch("bqip.capture.format.os.fsync", side_effect=OSError("injected ENOSPC")):
                    with self.assertRaises(OSError):
                        writer.seal()
                self.assertTrue(writer.partial_path.exists())
                self.assertFalse(writer.sealed_path.exists())
                self.assertEqual(writer.state, RawSegmentState.FAILED)
                self.assertEqual(recover(writer.partial_path).record_count, 1)

    def test_external_hash_detects_whole_record_deletion(self):
        expected = fixture("capture/segments.json")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "capture.sealed"
            original = (FIXTURES / "capture/complete.bqrc").read_bytes()
            path.write_bytes(original)
            self.assertEqual(verify_segment(path, expected["complete_sha256"]).record_count, 5)
            path.write_bytes(original[:expected["truncated_prefix_bytes"]])
            self.assertIsNone(recover(path).failure)
            with self.assertRaisesRegex(CaptureFormatError, "SEGMENT_HASH_MISMATCH"):
                verify_segment(path, expected["complete_sha256"])

    def test_rename_failure_keeps_partial_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            with SegmentWriter(Path(temporary)) as writer:
                writer.append(Frame(b"", b"evidence"))
                with patch("bqip.capture.format.os.rename", side_effect=OSError("injected rename error")):
                    with self.assertRaises(OSError):
                        writer.seal()
                self.assertTrue(writer.partial_path.exists())
                self.assertFalse(writer.sealed_path.exists())
                self.assertEqual(recover(writer.partial_path).record_count, 1)

    def test_no_overwrite_and_partial_reopen(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            writer = SegmentWriter(root)
            writer.append(Frame(b"", b""))
            identity = writer.segment_id
            writer.close()
            self.assertEqual(recover(writer.partial_path).state, RawSegmentState.RECOVERED_PARTIAL)
            with self.assertRaises(FileExistsError):
                SegmentWriter(root, identity)
            with SegmentWriter(root) as second:
                second.sealed_path.write_bytes(b"existing evidence")
                with self.assertRaises(FileExistsError):
                    second.seal()
                self.assertEqual(second.sealed_path.read_bytes(), b"existing evidence")

    def test_semantic_replay_ignores_provenance_and_preserves_capture_identity(self):
        identity = CaptureId(UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"), 7)
        event = RawCaptureEvent(identity, uuid4(), uuid4(), "fixture", "fixture", "fixture",
                                "test", 1234, 7, PayloadKind.TEXT, 3, raw_hash(b"abc"))
        first = ProcessingContext(1, "commit", 1, uuid4(), ReplayMode.LIVE)
        second = ProcessingContext(2, "commit", 1, uuid4(), ReplayMode.VERSION_PINNED_RESEARCH)
        left, right = replace(event, processing_context=first), replace(event, processing_context=second)
        self.assertEqual(left, right)
        self.assertEqual(left.capture_id, right.capture_id)
        left.validate_payload(b"abc")
        with self.assertRaisesRegex(ContractError, "HASH"):
            left.validate_payload(b"abd")


class SchemaTests(unittest.TestCase):
    def test_enum_values_match_normative_idl(self):
        sources = "\n".join(p.read_text() for p in (ROOT / "proto/bqip/v1").glob("*.proto"))
        mapping = (("ReplayMode", values.ReplayMode, "REPLAY_MODE_"),
                   ("PayloadKind", values.PayloadKind, "PAYLOAD_KIND_"),
                   ("RawSegmentState", values.RawSegmentState, "RAW_SEGMENT_STATE_"),
                   ("CauseCode", operational.CauseCode, "CAUSE_CODE_"),
                   ("OperationalKind", operational.OperationalKind, "OPERATIONAL_KIND_"),
                   ("RawLossStatus", operational.RawLossStatus, "RAW_LOSS_STATUS_"),
                   ("RealtimeContinuity", operational.RealtimeContinuity, "REALTIME_CONTINUITY_"))
        for name, enum, prefix in mapping:
            body = re.search(r"enum " + name + r"\s*\{([^}]+)\}", sources).group(1)
            actual = {key.removeprefix(prefix): int(number)
                      for key, number in re.findall(r"([A-Z_]+)\s*=\s*(\d+)", body)}
            self.assertEqual(actual, {item.name: item.value for item in enum})


if __name__ == "__main__":
    unittest.main()
