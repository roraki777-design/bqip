"""Offline authoring only: independently derive AC-001B fixtures, never bless test output.

Inputs are the pre-existing literal metadata/payload hex in frames.json. CRC uses
forward Castagnoli with bit reversal, unlike the reflected Python implementation.
This file imports no BQIP module. It is not invoked by tests or CI.
"""

import hashlib
import json
import struct
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parent.parent
CAPTURE = ROOT / "tests/fixtures/capture"


def reference_crc(data: bytes) -> int:
    value = 0xFFFFFFFF
    for byte in data:
        value ^= int(f"{byte:08b}"[::-1], 2) << 24
        for _ in range(8):
            value = ((value << 1) ^ (0x1EDC6F41 if value & 0x80000000 else 0)) & 0xFFFFFFFF
    return int(f"{value:032b}"[::-1], 2) ^ 0xFFFFFFFF


def add_crc(data: bytes) -> bytes:
    return data + struct.pack(">I", reference_crc(data))


def dump(name: str, value: object) -> None:
    (CAPTURE / name).write_text(json.dumps(value, indent=2) + "\n")


def main() -> None:
    assert reference_crc(b"123456789") == 0xE3069283
    identity = UUID("00010203-0405-0607-0809-0a0b0c0d0e0f")
    header_prefix = b"BQRC" + struct.pack(">I", 2) + identity.bytes
    header = add_crc(header_prefix)
    vectors = json.loads((CAPTURE / "frames.json").read_text())
    frames = []
    for vector in vectors:
        metadata, payload = bytes.fromhex(vector["metadata_hex"]), bytes.fromhex(vector["payload_hex"])
        frame = add_crc(struct.pack(">II", len(metadata), len(payload)) + metadata + payload)
        vector["frame_hex"] = frame.hex()
        frames.append(frame)
    dump("frames.json", vectors)
    complete = header + b"".join(frames)
    (CAPTURE / "complete.bqrc").write_bytes(complete)
    (CAPTURE / "truncated.bqrc").write_bytes(complete[:-2])
    (CAPTURE / "corrupt.bqrc").write_bytes(complete[:-1] + bytes([complete[-1] ^ 1]))
    dump("segments.json", {"header_hex": header.hex(), "segment_id": str(identity),
                          "complete_sha256": hashlib.sha256(complete).hexdigest(),
                          "complete_bytes": len(complete), "record_count": 5,
                          "truncated_good_records": 4,
                          "truncated_prefix_bytes": len(header) + sum(map(len, frames[:-1]))})
    record = add_crc(bytes.fromhex("0000000300000005") + b"ABCDEFGH")

    def changed(data: bytes, index: int) -> bytes:
        return data[:index] + bytes([data[index] ^ 1]) + data[index + 1:]

    header_cases = [
        ("segment_id_bit_flip", changed(header, 8), "HEADER_CRC_MISMATCH", 24),
        ("version_bit_flip", changed(header, 7), "UNSUPPORTED_VERSION", 4),
        ("header_crc_bit_flip", changed(header, 27), "HEADER_CRC_MISMATCH", 24),
        ("bad_magic", b"FAIL" + header[4:], "BAD_MAGIC", 0),
        ("unsupported_valid_version", add_crc(b"BQRC" + struct.pack(">I", 3) + identity.bytes),
         "UNSUPPORTED_VERSION", 4),
        ("truncated_header", header[:-1], "TRUNCATED_HEADER", 0),
        ("rejected_v1_header", b"BQRC" + struct.pack(">I", 1) + identity.bytes + record[:4],
         "UNSUPPORTED_VERSION", 4),
    ]
    record_cases = [
        ("balanced_length_partition", bytes.fromhex("0000000400000004") + record[8:], "CRC_MISMATCH"),
        ("metadata_corruption", changed(record, 8), "CRC_MISMATCH"),
        ("payload_corruption", changed(record, 11), "CRC_MISMATCH"),
        ("record_crc_corruption", changed(record, len(record) - 1), "CRC_MISMATCH"),
        ("truncated_final_frame", record[:-1], "TRUNCATED_RECORD"),
    ]
    dump("integrity-v2.json", {
        "segment_id": str(identity), "header_hex": header.hex(),
        "header_crc32c_hex": header[-4:].hex(), "record_hex": record.hex(),
        "record_crc32c_hex": record[-4:].hex(), "metadata_hex": b"ABC".hex(),
        "payload_hex": b"DEFGH".hex(),
        "alternative_record_hex": add_crc(bytes.fromhex("0000000300000005") + b"ABCZZZZZ").hex(),
        "header_errors": [{"name": n, "hex": b.hex(), "error": e, "offset": o}
                          for n, b, e, o in header_cases],
        "record_errors": [{"name": n, "hex": b.hex(), "error": e, "offset": 0}
                          for n, b, e in record_cases],
    })
    seals = []
    for origin in ("ORIGINAL", "RECOVERED"):
        body = {"seal_schema": "BQIP-RAW-SEAL-V1", "format_version": "2",
                "segment_id": str(identity), "byte_length": str(len(complete)),
                "record_count": "5", "segment_sha256": hashlib.sha256(complete).hexdigest(),
                "seal_origin": origin}
        # All keys/values are literal ASCII strings; lexical order equals JCS UTF-16 order.
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("ascii")
        seals.append({"body": body, "canonical_hex": canonical.hex(),
                      "seal_sha256": hashlib.sha256(canonical).hexdigest()})
    dump("seals-v2.json", {"orphan_state": 9, "verified_state": 2, "seals": seals})
    enums_path = ROOT / "tests/fixtures/contracts/enums.json"
    enums = json.loads(enums_path.read_text())
    if not any(v["number"] == 9 for v in enums["RawSegmentState"]):
        enums["RawSegmentState"].append({"token": "RECOVERED_UNVERIFIED", "number": 9,
                                         "wire_name": "RAW_SEGMENT_STATE_RECOVERED_UNVERIFIED"})
    enums_path.write_text(json.dumps(enums, indent=2) + "\n")


if __name__ == "__main__":
    main()
