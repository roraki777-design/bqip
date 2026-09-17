"""AC-001B / AR-002 and AR-003 regressions against independent external fixtures."""

import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from bqip.capture.format import (
    HEADER,
    CaptureFormatError,
    Frame,
    SegmentWriter,
    header,
    read_frame,
    read_header,
    recover,
    recover_seal,
    verify_segment,
)
from bqip.contracts.values import RawSegmentState

ROOT = Path(__file__).parent / "fixtures/capture"


def fixture(name):
    return json.loads((ROOT / name).read_text())


def canonical(body):
    # All fixture seal keys and values are ASCII strings; independent JCS bytes.
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("ascii")


class BqrcV2Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.integrity = fixture("integrity-v2.json")
        self.seals = fixture("seals-v2.json")
        self.path = self.root / "capture.sealed"

    def orphan(self):
        self.path.write_bytes((ROOT / "complete.bqrc").read_bytes())
        return self.path

    def original(self):
        self.orphan()
        (self.root / "seal.json").write_bytes(bytes.fromhex(self.seals["seals"][0]["canonical_hex"]))
        return self.path

    def check_header_error(self, name):
        vector = next(v for v in self.integrity["header_errors"] if v["name"] == name)
        with self.assertRaises(CaptureFormatError) as error:
            read_header(io.BytesIO(bytes.fromhex(vector["hex"])))
        self.assertEqual((error.exception.code, error.exception.offset),
                         (vector["error"], vector["offset"]))

    def test_header_record_bytes_and_crc_match_external_vectors(self):
        self.assertEqual(HEADER.size, 28)
        identity = UUID(self.integrity["segment_id"])
        self.assertEqual(header(identity).hex(), self.integrity["header_hex"])
        frame = Frame(b"ABC", b"DEFGH")
        self.assertEqual(frame.encode().hex(), self.integrity["record_hex"])
        self.assertEqual(read_frame(io.BytesIO(bytes.fromhex(self.integrity["record_hex"])), 0), frame)
        self.assertEqual(read_header(io.BytesIO(bytes.fromhex(self.integrity["header_hex"]))), identity)

    def test_length_prefix_partition_corruption_is_rejected(self):
        vector = self.integrity["record_errors"][0]
        data = bytes.fromhex(vector["hex"])
        valid = bytes.fromhex(self.integrity["record_hex"])
        self.assertEqual(data[8:], valid[8:])
        self.assertEqual(data[:8], bytes.fromhex("0000000400000004"))
        with self.assertRaisesRegex(CaptureFormatError, "CRC_MISMATCH"):
            read_frame(io.BytesIO(data), 0)

    def test_segment_id_header_corruption_is_rejected(self):
        self.check_header_error("segment_id_bit_flip")

    def test_header_version_corruption_is_rejected(self):
        self.check_header_error("version_bit_flip")

    def test_all_external_corruption_errors_match(self):
        for vector in self.integrity["header_errors"]:
            with self.subTest(name=vector["name"]):
                self.check_header_error(vector["name"])
        for vector in self.integrity["record_errors"]:
            with self.subTest(name=vector["name"]), self.assertRaises(CaptureFormatError) as error:
                read_frame(io.BytesIO(bytes.fromhex(vector["hex"])), 0)
            self.assertEqual(error.exception.code, vector["error"])
        data = bytes.fromhex(self.integrity["header_hex"])
        for length in range(28):
            with self.assertRaisesRegex(CaptureFormatError, "TRUNCATED_HEADER"):
                read_header(io.BytesIO(data[:length]))

    def test_sealed_hash_is_computed_from_finalized_disk_bytes(self):
        with SegmentWriter(self.root, UUID(self.integrity["segment_id"])) as writer:
            for vector in fixture("frames.json"):
                writer.append(Frame(bytes.fromhex(vector["metadata_hex"]),
                                    bytes.fromhex(vector["payload_hex"])))
            sealed = writer.seal()
            self.assertEqual(sealed.path.read_bytes(), (ROOT / "complete.bqrc").read_bytes())
            self.assertEqual(sealed.sha256, hashlib.sha256(sealed.path.read_bytes()).hexdigest())
            self.assertEqual(sealed.sha256, self.seals["seals"][0]["body"]["segment_sha256"])
            self.assertEqual(sealed.seal_bytes().hex(), self.seals["seals"][0]["canonical_hex"])
            self.assertEqual(verify_segment(sealed.path), sealed)

    def test_writer_expected_hash_mismatch_blocks_seal(self):
        # Preserve valid framing/CRCs but change actual disk bytes before seal.
        with SegmentWriter(self.root) as writer:
            writer.append(Frame(b"ABC", b"DEFGH"))
            writer._file.flush()
            with writer.partial_path.open("r+b") as disk:
                disk.seek(HEADER.size)
                disk.write(bytes.fromhex(self.integrity["alternative_record_hex"]))
                disk.flush()
            with self.assertRaisesRegex(CaptureFormatError, "WRITER_EXPECTED_HASH_MISMATCH"):
                writer.seal()
            self.assertEqual(writer.state, RawSegmentState.FAILED)
            self.assertFalse((writer.directory / "seal.json").exists())
            self.assertEqual(recover(writer.sealed_path).state, RawSegmentState.RECOVERED_UNVERIFIED)

    def test_seal_json_is_canonical_and_round_trips(self):
        self.original()
        actual = verify_segment(self.path)
        encoded = (self.root / "seal.json").read_bytes()
        self.assertEqual(actual.seal_bytes(), encoded)
        self.assertEqual(hashlib.sha256(encoded).hexdigest(), self.seals["seals"][0]["seal_sha256"])
        self.assertEqual(actual.seal_origin, "ORIGINAL")
        self.assertEqual(actual.byte_length, self.path.stat().st_size)
        self.assertEqual(actual.record_count, 5)
        self.assertEqual(recover(self.path).state.value, self.seals["verified_state"])

    def test_sealed_without_seal_json_is_not_sealed_local(self):
        self.orphan()
        self.assertEqual(recover(self.path).state.value, self.seals["orphan_state"])
        with self.assertRaisesRegex(CaptureFormatError, "SEAL_MISSING"):
            verify_segment(self.path)

    def test_orphan_sealed_segment_can_be_explicitly_recovered(self):
        self.orphan()
        before = self.path.read_bytes()
        actual = recover_seal(self.path)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(actual.seal_origin, "RECOVERED")
        seal = (self.root / "seal.json").read_bytes()
        self.assertEqual(seal.hex(), self.seals["seals"][1]["canonical_hex"])
        self.assertEqual(hashlib.sha256(seal).hexdigest(), self.seals["seals"][1]["seal_sha256"])
        self.assertEqual(recover(self.path).state.value, self.seals["verified_state"])
        self.assertEqual(recover_seal(self.path), actual)

    def bad_seal(self, field, value, code):
        self.original()
        body = dict(self.seals["seals"][0]["body"])
        body[field] = value
        encoded = canonical(body)
        (self.root / "seal.json").write_bytes(encoded)
        for operation in (verify_segment, recover, recover_seal):
            with self.assertRaisesRegex(CaptureFormatError, code):
                operation(self.path)
        self.assertEqual((self.root / "seal.json").read_bytes(), encoded)

    def test_bad_seal_hash_rejected(self):
        self.bad_seal("segment_sha256", "0" * 64, "SEGMENT_HASH_MISMATCH")

    def test_bad_seal_record_count_rejected(self):
        self.bad_seal("record_count", "4", "SEAL_RECORD_COUNT_MISMATCH")

    def test_bad_seal_byte_length_rejected(self):
        self.bad_seal("byte_length", "1", "SEAL_BYTE_LENGTH_MISMATCH")

    def test_bad_seal_segment_id_rejected(self):
        self.bad_seal("segment_id", "00000000-0000-0000-0000-000000000000", "SEAL_SEGMENT_ID_MISMATCH")

    def test_seal_partial_not_treated_as_valid_seal(self):
        self.orphan()
        partial = self.root / "seal.partial"
        partial.write_bytes(bytes.fromhex(self.seals["seals"][0]["canonical_hex"]))
        self.assertEqual(recover(self.path).state.value, self.seals["orphan_state"])
        self.assertTrue(partial.exists())
        self.assertEqual(recover_seal(self.path).seal_origin, "RECOVERED")
        self.assertFalse(partial.exists())
        partial.write_bytes(b"incomplete newer evidence")
        before = (self.root / "seal.json").read_bytes()
        self.assertEqual(recover_seal(self.path).seal_origin, "RECOVERED")
        self.assertEqual((self.root / "seal.json").read_bytes(), before)
        self.assertEqual(partial.read_bytes(), b"incomplete newer evidence")

    def test_invalid_seal_schema_or_noncanonical_json_rejected(self):
        for key, value in (("format_version", "1"), ("format_version", 2),
                           ("byte_length", "01"), ("byte_length", "18446744073709551616"),
                           ("record_count", "-1"), ("seal_origin", "UNKNOWN"),
                           ("segment_sha256", "F" * 64), ("extra", "value")):
            with self.subTest(key=key, value=value):
                self.bad_seal(key, value, "INVALID_SEAL_SCHEMA")
        self.original()
        canonical_seal = (self.root / "seal.json").read_bytes()
        (self.root / "seal.json").write_bytes(canonical_seal + b"\n")
        with self.assertRaisesRegex(CaptureFormatError, "NON_CANONICAL_SEAL"):
            verify_segment(self.path)
        (self.root / "seal.json").write_bytes(b'{"x":1,"x":1}')
        with self.assertRaisesRegex(CaptureFormatError, "INVALID_SEAL_JSON"):
            verify_segment(self.path)

    def test_seal_publication_faults_never_promote_writer(self):
        # Call order after writer creation: data fsync, directory, seal fsync, directory.
        for fail_at in (2, 3, 4):
            with self.subTest(fsync_call=fail_at), SegmentWriter(self.root) as writer:
                writer.append(Frame(b"ABC", b"DEFGH"))
                original_fsync = os.fsync
                calls = 0

                def injected(fd):
                    nonlocal calls
                    calls += 1
                    if calls == fail_at:
                        raise OSError("injected publication fault")
                    return original_fsync(fd)

                with patch("bqip.capture.format.os.fsync", side_effect=injected):
                    with self.assertRaises(OSError):
                        writer.seal()
                self.assertEqual(writer.state, RawSegmentState.FAILED)
                self.assertTrue(writer.sealed_path.exists())
                if fail_at < 4:
                    self.assertFalse((writer.directory / "seal.json").exists())
                    self.assertEqual(recover(writer.sealed_path).state, RawSegmentState.RECOVERED_UNVERIFIED)
                else:
                    self.assertEqual(verify_segment(writer.sealed_path).record_count, 1)

    def test_existing_seal_partial_blocks_normal_seal(self):
        with SegmentWriter(self.root) as writer:
            writer.append(Frame(b"ABC", b"DEFGH"))
            partial = writer.directory / "seal.partial"
            partial.write_bytes(b"crash evidence")
            with self.assertRaises(FileExistsError):
                writer.seal()
            self.assertEqual(writer.state, RawSegmentState.FAILED)
            self.assertEqual(partial.read_bytes(), b"crash evidence")
            self.assertFalse((writer.directory / "seal.json").exists())
            self.assertEqual(recover_seal(writer.sealed_path).seal_origin, "RECOVERED")


if __name__ == "__main__":
    unittest.main()
