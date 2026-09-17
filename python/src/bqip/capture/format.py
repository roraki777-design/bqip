"""Bounded, synchronous BQRC v1 reader/writer and non-destructive recovery.

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
from typing import BinaryIO
from uuid import UUID, uuid4

from bqip.contracts.values import U32_MAX, ContractError, RawSegmentState, bounded_int

MAGIC = b"BQRC"
VERSION = 1
HEADER = struct.Struct(">4sI16s")
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
        body = self.metadata + self.payload
        return LENGTHS.pack(len(self.metadata), len(self.payload)) + body + CRC.pack(crc32c(body))


def _exact(source: BinaryIO, length: int, offset: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        part = source.read(length - len(chunks))
        if not part:
            raise CaptureFormatError("TRUNCATED_RECORD", offset)
        chunks.extend(part)
    return bytes(chunks)


def read_header(source: BinaryIO) -> UUID:
    try:
        data = _exact(source, HEADER.size, 0)
    except CaptureFormatError as error:
        raise CaptureFormatError("TRUNCATED_HEADER", 0) from error
    magic, version, identity = HEADER.unpack(data)
    if magic != MAGIC:
        raise CaptureFormatError("BAD_MAGIC", 0)
    if version != VERSION:
        raise CaptureFormatError("UNSUPPORTED_VERSION", 4)
    return UUID(bytes=identity)


def read_frame(source: BinaryIO, offset: int, limits: Limits = Limits()) -> Frame | None:
    first = source.read(1)
    if not first:
        return None
    prefix = first + _exact(source, LENGTHS.size - 1, offset)
    metadata_length, payload_length = LENGTHS.unpack(prefix)
    if metadata_length > limits.metadata_bytes or payload_length > limits.payload_bytes:
        raise CaptureFormatError("FRAME_LIMIT_EXCEEDED", offset)
    body = _exact(source, metadata_length + payload_length, offset)
    expected = CRC.unpack(_exact(source, CRC.size, offset))[0]
    if crc32c(body) != expected:
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


def recover(path: Path, limits: Limits = Limits()) -> RecoveryReport:
    """Read-only scan. A valid prefix is not a claim of full source completeness."""
    with path.open("rb") as source:
        identity = read_header(source)
        offset = HEADER.size
        count = 0
        while True:
            try:
                frame = read_frame(source, offset, limits)
            except CaptureFormatError as error:
                state = (RawSegmentState.RECOVERED_PARTIAL if error.code == "TRUNCATED_RECORD"
                         else RawSegmentState.FAILED)
                return RecoveryReport(identity, state, count, offset, error.code, error.offset)
            if frame is None:
                state = (RawSegmentState.SEALED_LOCAL if path.suffix == ".sealed"
                         else RawSegmentState.RECOVERED_PARTIAL)
                return RecoveryReport(identity, state, count, offset, None, None)
            count += 1
            offset += LENGTHS.size + len(frame.metadata) + len(frame.payload) + CRC.size


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class SealedSegment:
    path: Path
    segment_id: UUID
    sha256: str
    record_count: int
    byte_length: int


def verify_segment(path: Path, expected_sha256: str, limits: Limits = Limits()) -> RecoveryReport:
    """Verify external sealed evidence, including header and record boundaries."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1 << 20):
            digest.update(chunk)
    if digest.hexdigest() != expected_sha256:
        raise CaptureFormatError("SEGMENT_HASH_MISMATCH", 0)
    report = recover(path, limits)
    if report.failure is not None:
        raise CaptureFormatError(report.failure, report.failure_offset or 0)
    return report


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
        self._write(HEADER.pack(MAGIC, VERSION, self.segment_id.bytes))

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
        except BaseException:
            self._state = RawSegmentState.FAILED
            self._file.close()
            raise
        self._state = RawSegmentState.SEALED_LOCAL
        return SealedSegment(self.sealed_path, self.segment_id, self._hash.hexdigest(),
                             self._count, self._bytes)

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
