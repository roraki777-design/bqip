# BQRC v2 — local capture and seal evidence

Normative authority: Architect Patch #001-BR / AC-001B and ADR-001.
BQRC v1 was a rejected pre-acceptance candidate, with no accepted production data.
V2 is the first acceptance candidate; no v1 read compatibility is implemented.

## Exact binary format

| Header field | Bytes | Representation |
|---|---:|---|
| magic | 4 | ASCII BQRC |
| format_version | 4 | uint32 big-endian, 2 |
| segment_id | 16 | Raw UUID bytes in RFC/network order |
| header_crc32c | 4 | uint32 big-endian CRC32C of the preceding 24 bytes |

HEADER_SIZE = 28. Header validation precedence is deterministic in both languages:
read all 28 bytes (TRUNCATED_HEADER), magic (BAD_MAGIC at 0), supported version
(UNSUPPORTED_VERSION at 4), then CRC (HEADER_CRC_MISMATCH at 24). Thus a version bit
flip from 2 to 3 reports UNSUPPORTED_VERSION even though its CRC also differs.
Segment-ID and header-CRC bit flips report HEADER_CRC_MISMATCH.

| Record field | Bytes | Representation |
|---|---:|---|
| metadata_length | 4 | uint32 big-endian |
| payload_length | 4 | uint32 big-endian |
| metadata | metadata_length | Original serialized RawCaptureEvent bytes |
| payload | payload_length | Exact application-message bytes |
| record_crc32c | 4 | uint32 big-endian CRC32C of both received length prefixes and both byte bodies |

CRC input is exactly metadata_length_be || payload_length_be || metadata || payload.
The reader hashes the received eight prefix bytes; it never derives CRC lengths
from decoded Protobuf. CRC32C is Castagnoli, init/xorout 0xffffffff. There is no
padding. Empty payload/metadata are structurally representable; required event
fields are a separate typed validation boundary. Unknown Protobuf bytes survive.

Reader limits remain 1 MiB metadata / 64 MiB payload by default, checked before
allocation. A limit violation fails visibly before body allocation/CRC checking.
Readers stop on corruption; they do not guess a resynchronization point.

## Normative seal.json

`capture.sealed` and its sibling `seal.json` together constitute local evidence.
The seal is exactly one canonical restricted-JCS object with seven fields:

| Field | Required representation |
|---|---|
| seal_schema | String BQIP-RAW-SEAL-V1 |
| format_version | String 2 |
| segment_id | Canonical lowercase, hyphenated UUID text |
| byte_length | Canonical unsigned base-10 uint64 string; no plus or leading zeros |
| record_count | Canonical unsigned base-10 uint64 string; zero permitted |
| segment_sha256 | 64 lowercase hexadecimal SHA-256 characters |
| seal_origin | ORIGINAL or RECOVERED |

All values are strings. No floats, unknown fields, duplicate keys or annotations
are accepted. Existing restricted JCS string/ordering rules apply, with no root
field exclusions: the full seal body is canonicalized. Canonical bytes contain no
trailing newline. Seal bytes/hashes for both origins are external golden fixtures.
Seal SHA-256, where tested, hashes all seal.json bytes; no self-hash is stored.

segment_sha256 covers every byte of capture.sealed, including its header CRC,
record prefixes, metadata, payloads and record CRCs.

## Publication protocol

The writer owns an exclusively created UUID directory on one POSIX filesystem.
All operations require single ownership; inspection/recovery requires a quiescent
file and directory. Concurrent external writers during verification/publication
are outside this protocol; no lock service or hostile-filesystem defense is added.

1. Finish the complete final record; flush and fsync capture.partial; close it.
2. Refuse an existing capture.sealed, rename partial to sealed atomically, fsync
   the segment directory.
3. Reopen capture.sealed read-only. In one pass, hash exactly the bytes read while
   validating the v2 header, both CRC domains, bounded records and exact EOF.
   Count actual bytes and complete records from that same stream.
4. Compare actual SHA-256 with write-time expected SHA-256, and compare actual
   identity/count/length with writer expectations. Any mismatch fails the writer
   and prevents publication of new seal evidence. A record failure drains only
   for the full-file hash; it never skips or salvages corruption during sealing.
5. Build canonical seal.json with seal_origin ORIGINAL from the actual disk
   evidence. Create seal.partial exclusively, write/flush/fsync and close it.
6. Refuse an existing seal.json; rename seal.partial to seal.json atomically;
   fsync the directory. Only then return success and transition to SEALED_LOCAL.

The read/hash/structure work shares one stream rather than separate racing passes.
The expected in-memory hash is a consistency check, never the authoritative seal
hash. WRITER_EXPECTED_HASH_MISMATCH is explicit. Header/read failures fail visibly;
no final seal is published for corrupted data. Count/identity mismatch errors are
WRITER_COUNTS_MISMATCH and WRITER_SEGMENT_ID_MISMATCH.

Any I/O failure leaves the writer FAILED. Failure after data rename leaves an
orphan. Failure after seal rename can leave a valid seal.json but still returns
FAILED; a later process must verify both files. Existence alone is never an
acknowledgment. No power-loss durability beyond the exercised POSIX operations is
claimed. Directory fsync does not certify arbitrary devices or network filesystems.

## Verification and recovery

`verify_segment(path, limits)` reads sibling seal.json, rejects invalid schema or
noncanonical bytes, then reads/hashes/validates capture.sealed. It compares segment
ID, byte length, record count and SHA-256 in that order. It returns verified
SealedSegment including seal_origin. It no longer accepts a standalone caller hash
as a substitute for durable evidence. Full-record deletion is detected by this
segment evidence even when the remaining records parse successfully.

`recover(path, limits)` is read-only inspection. Complete capture.sealed without
seal.json reports RECOVERED_UNVERIFIED (new enum tag 9), including when seal.partial
exists or even contains apparently complete JSON. A valid final seal is verified
before reporting SEALED_LOCAL. An invalid final seal raises a visible integrity
error; recovery never silently replaces it. Structural truncation still reports
the valid prefix as RECOVERED_PARTIAL; CRC failures report FAILED.

`recover_seal(path, limits)` explicitly recovers evidence for capture.sealed:

1. If seal.json exists, verify it and return its existing origin; never replace it.
2. Otherwise validate the complete sealed file, hash/count actual bytes and records,
   and fsync its read-only descriptor. Do not change any capture bytes.
3. Only after successful validation, remove a stale seal.partial if present.
4. Durably publish a new canonical seal with seal_origin RECOVERED using the same
   exclusive create, file fsync, rename and directory fsync protocol.

Repeated recovery is idempotent when final evidence verifies. seal.partial never
wins over a valid seal.json and is retained when validation fails. RECOVERED
records that the seal evidence was reconstructed; it cannot prove the original
expected record count or the absence of earlier whole-record loss. Preserve
separate historical manifests/recovery reports where those guarantees matter.

`recover_to` still copies a verified complete prefix of a truncated source to a
new UUID segment, retaining the source and its RecoveryReport. Its new segment has
an ORIGINAL seal from its writer; this is distinct from reconstructing missing
seal evidence for an unchanged orphan. CRC corruption cannot be skipped.

Remote-state enum values remain contracts only. No upload, GC or S3 behavior exists.
