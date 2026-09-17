# ADR-001 — Raw capture boundary and BQRC v1

Status: PROPOSED / IMPLEMENTED FOR REVIEW

## Context

Master Architecture v1.0 P2/P11 and Implementation Brief #001 AC-001 E/H/I
require exact application payload preservation and a recoverable local container.
This brief contains no receiver, realtime handoff or object-store networking.

## Decision

Raw payload is the complete application message supplied by the transport library
after transport framing/decompression, before any business parsing. Invalid UTF-8,
invalid JSON and empty payloads are retained unchanged. TEXT is a transport label,
not an assertion that bytes decode successfully.

BQRC v1 header is 24 bytes: ASCII BQRC (4), uint32 big-endian version 1 (4),
segment UUID in RFC byte order (16). Records follow exactly AC-001:
uint32 metadata length, uint32 payload length, metadata Protobuf bytes, payload
bytes, uint32 big-endian CRC32C over metadata concatenated with payload.
CRC32C uses Castagnoli, reflected polynomial 0x82f63b78, init/xorout 0xffffffff.
The framing layer preserves metadata bytes too, including unknown Protobuf fields.

Default read limits are 1 MiB metadata and 64 MiB payload per record, checked before
allocation. They are reader configuration, not a narrower on-disk integer type.
The reader streams frames; it does not load whole segments into memory.

Each writer exclusively creates a UUID-named directory beneath an existing spool
root. Supported durability target is Linux/POSIX with file and directory fsync
and same-filesystem atomic rename. The writer finishes a full frame, flushes,
fsyncs the file, derives SHA-256 and counts, renames capture.partial to
capture.sealed, and fsyncs the containing directory. It returns SEALED_LOCAL only
after success. Errors leave evidence and put the writer in FAILED; a failure
after rename may leave the sealed name present and must be investigated.

Recovery requires a closed/quiescent source. It scans to the first invalid frame,
reports the verified prefix and failure offset, and never overwrites the source.
A truncated tail may be copied to a new UUID segment; CRC corruption is not
skipped. The caller must retain the returned recovery report. Recovered payloads,
metadata bytes and capture IDs remain unchanged. A recovered segment is not a
claim that the original capture interval was complete.

The physical state enum includes all AC-001 states. Only local creation/seal/
recovery/failure behavior exists. No function asserts upload, remote verification,
manifest publication or GC eligibility. No automatic deletion is implemented.

## Alternatives considered

* Packet capture: outside the explicitly selected application-message boundary.
* In-place truncation: rejected because it destroys the damaged source evidence.
* Automatic seal on close: rejected because dropping a writer is not a durability
  acknowledgment.
* S3 uploader: expressly outside this brief.

## Consequences

Framing integrity and typed metadata validity are distinct. Rust exposes a typed
validated Protobuf decode. Python's validation adapter targets official generated
messages, with tests that fail visibly if its runtime/bindings are absent. Both
language suites have now executed successfully. Framing retains opaque metadata
bytes, including unknown fields, without decoding and re-encoding evidence.

CRC is not authentication. Segment SHA-256 must be retained externally to detect
changes such as deletion of an entire final frame. Scan success and a filename
alone do not establish historical completeness or object-store durability.
Filesystem/power-loss qualification remains unperformed; tests inject local
failures and do not simulate a real power cut. Live P11 is not claimed by this
format-only implementation.

## Frozen invariants affected

P1, P2, P4, P6, P10 and the preservation boundary supporting future P11.
No invariant is altered.

## What remains BENCHMARK/RESEARCH REQUIRED

Segment rotation size, operational limits, fsync cost, spool exposure/RPO, cloud
resources and capture throughput. No numeric performance claim is made.
