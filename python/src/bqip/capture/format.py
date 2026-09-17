"""Bounded, synchronous BQRC v2 reader/writer and non-destructive recovery.

Storage is single-owner: a writer exclusively creates one UUID-named directory.
Recovery never truncates, edits, or deletes the source evidence.
"""

from __future__ import annotations

import hashlib
import os
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal, Protocol
from uuid import UUID, uuid4

from bqip.contracts.values import U32_MAX, U64_MAX, ContractError, RawSegmentState, bounded_int
from bqip.manifests import JsonValue, canonical_bytes, parse_manifest

MAGIC = b"BQRC"
VERSION = 2
HEADER = struct.Struct(">4sI16sI")
LENGTHS = struct.Struct(">II")
CRC = struct.Struct(">I")


def crc32c(data: bytes) -> int:
    """CRC-32C/Castagnoli, reflected, init/xorout 0xffffffff."""
    value = 0xFFFFFFFF
    for octet in data:
        value ^= octet
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value ^ 0xFFFFFFFF


class CaptureFormatError(ValueError):
    def __init__(self, code: str, offset: int) -> None:
        self.code = code
        self.offset = offset
        super().__init__(f"{code} at byte {offset}")


@dataclass(frozen=True)
class Limits:
    metadata_bytes: int = 1 << 20
    payload_bytes: int = 64 << 20

    def __post_init__(self) -> None:
        bounded_int(self.metadata_bytes, 0, U32_MAX)
        bounded_int(self.payload_bytes, 0, U32_MAX)


@dataclass(frozen=True)
class Frame:
    metadata: bytes
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.metadata, bytes) or not isinstance(self.payload, bytes):
            raise ContractError("IMMUTABLE_FRAME_BYTES_REQUIRED")

    def encode(self, limits: Limits = Limits()) -> bytes:
        if len(self.metadata) > limits.metadata_bytes or len(self.payload) > limits.payload_bytes:
            raise CaptureFormatError("FRAME_LIMIT_EXCEEDED", 0)
        data = LENGTHS.pack(len(self.metadata), len(self.payload)) + self.metadata + self.payload
        return data + CRC.pack(crc32c(data))


class Readable(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


class _HashingReader:
    """Hash exactly the bytes consumed by structural validation, in the same pass."""
    def __init__(self, source: BinaryIO) -> None:
        self.source = source
        self.digest = hashlib.sha256()
        self.byte_length = 0

    def read(self, size: int = -1, /) -> bytes:
        data = self.source.read(size)
        self.digest.update(data)
        self.byte_length += len(data)
        return data


def header(segment_id: UUID) -> bytes:
    prefix = struct.pack(">4sI16s", MAGIC, VERSION, segment_id.bytes)
    return prefix + CRC.pack(crc32c(prefix))


def _exact(source: Readable, length: int, offset: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        part = source.read(length - len(chunks))
        if not part:
            raise CaptureFormatError("TRUNCATED_RECORD", offset)
        chunks.extend(part)
    return bytes(chunks)


def read_header(source: Readable) -> UUID:
    try:
        data = _exact(source, HEADER.size, 0)
    except CaptureFormatError as error:
        raise CaptureFormatError("TRUNCATED_HEADER", 0) from error
    magic, version, identity, expected_crc = HEADER.unpack(data)
    if magic != MAGIC:
        raise CaptureFormatError("BAD_MAGIC", 0)
    if version != VERSION:
        raise CaptureFormatError("UNSUPPORTED_VERSION", 4)
    if crc32c(data[:24]) != expected_crc:
        raise CaptureFormatError("HEADER_CRC_MISMATCH", 24)
    return UUID(bytes=identity)


def read_frame(source: Readable, offset: int, limits: Limits = Limits()) -> Frame | None:
    first = source.read(1)
    if not first:
        return None
    prefix = first + _exact(source, LENGTHS.size - 1, offset)
    metadata_length, payload_length = LENGTHS.unpack(prefix)
    if metadata_length > limits.metadata_bytes or payload_length > limits.payload_bytes:
        raise CaptureFormatError("FRAME_LIMIT_EXCEEDED", offset)
    body = _exact(source, metadata_length + payload_length, offset)
    expected = CRC.unpack(_exact(source, CRC.size, offset))[0]
    if crc32c(prefix + body) != expected:
        raise CaptureFormatError("CRC_MISMATCH", offset)
    return Frame(body[:metadata_length], body[metadata_length:])


def iter_frames(path: Path, limits: Limits = Limits()) -> Iterator[Frame]:
    with path.open("rb") as source:
        read_header(source)
        offset = HEADER.size
        while (frame := read_frame(source, offset, limits)) is not None:
            yield frame
            offset += LENGTHS.size + len(frame.metadata) + len(frame.payload) + CRC.size


@dataclass(frozen=True)
class RecoveryReport:
    segment_id: UUID
    state: RawSegmentState
    record_count: int
    valid_prefix_bytes: int
    failure: str | None
    failure_offset: int | None


def _scan(path: Path, limits: Limits) -> tuple[RecoveryReport, str, int]:
    with path.open("rb") as raw:
        source = _HashingReader(raw)
        identity = read_header(source)
        offset, count = HEADER.size, 0
        failure = None
        failed_at = None
        while True:
            try:
                frame = read_frame(source, offset, limits)
            except CaptureFormatError as error:
                failure, failed_at = error.code, error.offset
                break
            if frame is None:
                break
            count += 1
            offset += LENGTHS.size + len(frame.metadata) + len(frame.payload) + CRC.size
        # Complete the disk hash even if structural verification failed.
        while source.read(1 << 20):
            pass
        state = (RawSegmentState.FAILED if failure and failure != "TRUNCATED_RECORD"
                 else RawSegmentState.RECOVERED_PARTIAL)
        report = RecoveryReport(identity, state, count, offset, failure, failed_at)
        return report, source.digest.hexdigest(), source.byte_length


def recover(path: Path, limits: Limits = Limits()) -> RecoveryReport:
    """Read-only inspection; parsing a sealed filename never establishes a seal."""
    report, _, _ = _scan(path, limits)
    if report.failure is not None or path.name != "capture.sealed":
        return report
    state = RawSegmentState.RECOVERED_UNVERIFIED
    if (path.parent / "seal.json").exists():
        verify_segment(path, limits)
        state = RawSegmentState.SEALED_LOCAL
    return RecoveryReport(report.segment_id, state, report.record_count,
                          report.valid_prefix_bytes, None, None)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


SealOrigin = Literal["ORIGINAL", "RECOVERED"]
SEAL_FIELDS = frozenset(("seal_schema", "format_version", "segment_id", "byte_length",
                         "record_count", "segment_sha256", "seal_origin"))


@dataclass(frozen=True)
class SealedSegment:
    path: Path
    segment_id: UUID
    sha256: str
    record_count: int
    byte_length: int
    seal_origin: SealOrigin

    def seal_bytes(self) -> bytes:
        body: dict[str, JsonValue] = {
            "seal_schema": "BQIP-RAW-SEAL-V1", "format_version": "2",
            "segment_id": str(self.segment_id), "byte_length": str(self.byte_length),
            "record_count": str(self.record_count), "segment_sha256": self.sha256,
            "seal_origin": self.seal_origin,
        }
        return canonical_bytes(body)


def _seal_body(path: Path) -> dict[str, JsonValue]:
    try:
        data = path.read_bytes()
    except FileNotFoundError as error:
        raise CaptureFormatError("SEAL_MISSING", 0) from error
    try:
        body = parse_manifest(data.decode("utf-8"))
    except ValueError as error:
        raise CaptureFormatError("INVALID_SEAL_JSON", 0) from error
    if canonical_bytes(body) != data:
        raise CaptureFormatError("NON_CANONICAL_SEAL", 0)
    if (body.keys() != SEAL_FIELDS or body["seal_schema"] != "BQIP-RAW-SEAL-V1"
            or body["format_version"] != "2"
            or body["seal_origin"] not in ("ORIGINAL", "RECOVERED")):
        raise CaptureFormatError("INVALID_SEAL_SCHEMA", 0)
    for key in ("byte_length", "record_count"):
        value = body[key]
        if (not isinstance(value, str) or not value.isascii() or not value.isdecimal()
                or (len(value) > 1 and value[0] == "0") or len(value) > 20
                or int(value) > U64_MAX):
            raise CaptureFormatError("INVALID_SEAL_SCHEMA", 0)
    identity, digest = body["segment_id"], body["segment_sha256"]
    try:
        if not isinstance(identity, str) or str(UUID(identity)) != identity:
            raise ValueError("noncanonical UUID")
    except ValueError as error:
        raise CaptureFormatError("INVALID_SEAL_SCHEMA", 0) from error
    if (not isinstance(digest, str) or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)):
        raise CaptureFormatError("INVALID_SEAL_SCHEMA", 0)
    return body


def _actual_segment(path: Path, origin: SealOrigin, limits: Limits) -> SealedSegment:
    report, digest, length = _scan(path, limits)
    if report.failure is not None:
        raise CaptureFormatError(report.failure, report.failure_offset or 0)
    return SealedSegment(path, report.segment_id, digest, report.record_count, length, origin)


def verify_segment(path: Path, limits: Limits = Limits()) -> SealedSegment:
    """Verify canonical durable seal evidence against the actual finalized bytes."""
    body = _seal_body(path.parent / "seal.json")
    origin: SealOrigin = "ORIGINAL" if body["seal_origin"] == "ORIGINAL" else "RECOVERED"
    actual = _actual_segment(path, origin, limits)
    for key, value, error in (
        ("segment_id", str(actual.segment_id), "SEAL_SEGMENT_ID_MISMATCH"),
        ("byte_length", str(actual.byte_length), "SEAL_BYTE_LENGTH_MISMATCH"),
        ("record_count", str(actual.record_count), "SEAL_RECORD_COUNT_MISMATCH"),
        ("segment_sha256", actual.sha256, "SEGMENT_HASH_MISMATCH"),
    ):
        if body[key] != value:
            raise CaptureFormatError(error, 0)
    return actual


def _publish_seal(segment: SealedSegment) -> None:
    directory = segment.path.parent
    partial, final = directory / "seal.partial", directory / "seal.json"
    if final.exists():
        raise FileExistsError(final)
    with partial.open("xb") as output:
        data = segment.seal_bytes()
        if output.write(data) != len(data):
            raise OSError("SHORT_WRITE")
        output.flush()
        os.fsync(output.fileno())
    # Publication requires exclusive ownership of the quiescent directory.
    if final.exists():
        raise FileExistsError(final)
    os.rename(partial, final)
    _fsync_directory(directory)


def recover_seal(path: Path, limits: Limits = Limits()) -> SealedSegment:
    """Explicitly establish RECOVERED evidence; never modify capture.sealed."""
    if path.name != "capture.sealed":
        raise CaptureFormatError("SEALED_PATH_REQUIRED", 0)
    if (path.parent / "seal.json").exists():
        return verify_segment(path, limits)  # Never overwrite final evidence, even if invalid.
    actual = _actual_segment(path, "RECOVERED", limits)
    with path.open("rb") as source:
        os.fsync(source.fileno())
    # Only explicit recovery may discard incomplete evidence, after validating data.
    (path.parent / "seal.partial").unlink(missing_ok=True)
    _publish_seal(actual)
    return actual


class SegmentWriter:
    def __init__(self, root: Path, segment_id: UUID | None = None,
                 limits: Limits = Limits()) -> None:
        self.segment_id = segment_id if segment_id is not None else uuid4()
        self.directory = root / str(self.segment_id)
        self.directory.mkdir()  # Exclusive segment ownership; root must already exist.
        _fsync_directory(root)
        self.partial_path = self.directory / "capture.partial"
        self.sealed_path = self.directory / "capture.sealed"
        self._file: BinaryIO = self.partial_path.open("xb")
        self._state = RawSegmentState.OPEN
        self._limits = limits
        self._count = 0
        self._bytes = HEADER.size
        self._hash = hashlib.sha256()
        self._write(header(self.segment_id))

    @property
    def state(self) -> RawSegmentState:
        return self._state

    def _write(self, data: bytes) -> None:
        try:
            written = self._file.write(data)
            if written != len(data):
                raise OSError("SHORT_WRITE")
        except BaseException:
            self._state = RawSegmentState.FAILED
            self._file.close()
            raise
        self._hash.update(data)

    def append(self, frame: Frame) -> None:
        if self.state != RawSegmentState.OPEN:
            raise ValueError("SEGMENT_NOT_OPEN")
        encoded = frame.encode(self._limits)
        self._write(encoded)
        self._count += 1
        self._bytes += len(encoded)

    def seal(self) -> SealedSegment:
        if self.state != RawSegmentState.OPEN:
            raise ValueError("SEGMENT_NOT_OPEN")
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            if self.sealed_path.exists():
                raise FileExistsError(self.sealed_path)
            # Atomic within an exclusively owned directory, on one filesystem.
            os.rename(self.partial_path, self.sealed_path)
            _fsync_directory(self.directory)
            report, digest, length = _scan(self.sealed_path, self._limits)
            if digest != self._hash.hexdigest():
                raise CaptureFormatError("WRITER_EXPECTED_HASH_MISMATCH", 0)
            if report.failure is not None:
                raise CaptureFormatError(report.failure, report.failure_offset or 0)
            if report.segment_id != self.segment_id:
                raise CaptureFormatError("WRITER_SEGMENT_ID_MISMATCH", 0)
            if length != self._bytes or report.record_count != self._count:
                raise CaptureFormatError("WRITER_COUNTS_MISMATCH", 0)
            actual = SealedSegment(self.sealed_path, report.segment_id, digest,
                                   report.record_count, length, "ORIGINAL")
            _publish_seal(actual)
        except BaseException:
            self._state = RawSegmentState.FAILED
            self._file.close()
            raise
        self._state = RawSegmentState.SEALED_LOCAL
        return actual

    def close(self) -> None:
        """Close without promoting incomplete evidence to SEALED_LOCAL."""
        self._file.close()
        if self._state == RawSegmentState.OPEN:
            self._state = RawSegmentState.RECOVERED_PARTIAL

    def __enter__(self) -> SegmentWriter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def recover_to(source: Path, root: Path, new_segment_id: UUID | None = None,
               limits: Limits = Limits()) -> tuple[RecoveryReport, SealedSegment]:
    """Copy verified complete frames to a NEW segment, leaving source untouched.

    CRC corruption is not skipped; it requires investigation. A truncated tail
    may be salvaged with its report, which the caller must retain as provenance.
    """
    report = recover(source, limits)
    if report.state == RawSegmentState.FAILED:
        raise CaptureFormatError(report.failure or "RECOVERY_FAILED", report.valid_prefix_bytes)
    with source.open("rb") as stream, SegmentWriter(root, new_segment_id, limits) as writer:
        if read_header(stream) != report.segment_id:
            raise CaptureFormatError("SOURCE_CHANGED", 0)
        offset = HEADER.size
        for _ in range(report.record_count):
            frame = read_frame(stream, offset, limits)
            if frame is None:
                raise CaptureFormatError("SOURCE_CHANGED", offset)
            writer.append(frame)
            offset += LENGTHS.size + len(frame.metadata) + len(frame.payload) + CRC.size
        return report, writer.seal()
