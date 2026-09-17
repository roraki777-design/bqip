# ADR-001 — Application-message raw capture and BQRC v2

Status: PROPOSED / IMPLEMENTED FOR REVIEW

## Context

Master Architecture v1.0 P2/P11, Brief #001 and AC-001B require immutable original
application payloads and verifiable local capture evidence. This brief contains
no receiver, realtime handoff, networking or remote storage behavior.

BQRC v1 was a pre-acceptance candidate. It was rejected during Architect Review
#001 because:

1. Record length prefixes were not integrity protected (AR-002).
2. Segment header identity lacked integrity protection (AR-002).
3. Local seal evidence was not derived durably from finalized bytes (AR-003).

No accepted production data exists in BQRC v1. BQRC v2 is the first acceptance
candidate, authorized explicitly by AC-001B. This records the rejected candidate
rather than treating it as an accepted format or silently hiding its replacement.

## Decision

Raw capture remains the complete application message after transport framing/
decompression and before business parsing. Invalid UTF-8, invalid JSON and empty
payloads remain unchanged; TEXT is a transport label, not a Unicode guarantee.
Framing preserves original metadata Protobuf bytes, including unknown fields.

V2 uses a 28-byte header: BQRC, uint32 BE version 2, 16 UUID bytes and uint32 BE
CRC32C over those first 24 bytes. Each record carries uint32 BE metadata/payload
lengths, both bodies and uint32 BE CRC32C over the exact received eight prefix
bytes plus both bodies. Header error precedence is truncated/magic/version/CRC.
Default allocation limits remain 1 MiB metadata and 64 MiB payload per record.

A writer exclusively owns a UUID directory. On Linux/POSIX, it flushes, fsyncs and
closes capture.partial, refuses overwrite, atomically renames to capture.sealed
and fsyncs the directory. It reopens the finalized file read-only; a hashing reader
validates header/record integrity and EOF while hashing exactly those same disk
bytes and counting actual lengths/records. Actual and write-time expectations must
agree. Mismatch produces FAILED and no newly published valid seal.json.

The writer uses the actual finalized-file SHA/count/length to build restricted-JCS
seal evidence. It exclusively creates seal.partial, writes/flushes/fsyncs/closes,
refuses existing seal.json, atomically renames and fsyncs the directory. Only then
may its state become SEALED_LOCAL. seal.json has the six AC-001B normative fields
plus seal_origin: ORIGINAL or RECOVERED. All fields are strings, with canonical
UUID, unsigned uint64 decimal strings and lowercase SHA-256. Exact schema and
canonical bytes are mandatory; no fields are excluded from seal hashing.

verify_segment verifies both files, all CRCs and EOF, schema/canonical JSON,
identity, byte length, record count and actual disk SHA. A suffix alone establishes
no state. Complete sealed data without final evidence is RECOVERED_UNVERIFIED,
appended at RawSegmentState tag 9; tags 0–8 are unchanged.

Explicit recover_seal validates the quiescent orphan without changing capture
bytes, syncs the data file, replaces stale seal.partial only after validation and
publishes RECOVERED evidence durably. Existing final evidence is verified and
never silently replaced. A valid seal.json always takes precedence over a partial.
A malformed final seal remains evidence requiring investigation.

Truncated-source salvage remains a distinct recover_to operation into a new UUID
segment; its new writer's seal is ORIGINAL and its source RecoveryReport must be
retained. CRC corruption is never skipped. Remote-state enums remain contracts.

## Alternatives considered

* Retain v1 for compatibility: no accepted production data exists; only its
  rejection is tested as a negative version fixture.
* Protect only body bytes: reproduced 3/5 → 4/4 partition corruption defeats it.
* Trust the incremental writer SHA: reproduced disk mutation defeats it.
* Infer sealing from a filename: would promote publication-orphan crash states.
* Rebuild evidence automatically during inspection: would hide recovered provenance.
* A separate JCS package or new I/O framework: unnecessary; existing authorized
  primitives implement this bounded local protocol.

## Consequences

The actual file hash and structural verification use one read stream. Reader and
seal protocols require a single owner and quiescent input, not concurrent external
mutation during verification. The tests explicitly mutate a flushed partial file
before sealing and require WRITER_EXPECTED_HASH_MISMATCH/FAILED with no seal.json.

An I/O failure after a rename may leave the renamed file present. The active
writer still fails; restart must verify both files. Fault injection covers the
fsync publication windows in Python and existing-evidence collisions in both
languages. No real power cut, hardware cache guarantee or remote durability is
claimed. Recovery origin cannot establish an orphan's original completeness.
CRC is not authentication; independent segment evidence detects whole-record loss.

## Frozen invariants affected

P1, P2, P4, P6, P10 and the preservation boundary supporting future P11. AC-001B
explicitly replaces the rejected format; no unrelated invariant is changed.
The live P11 guarantee awaits a future ingestion brief.

## What remains BENCHMARK/RESEARCH REQUIRED

Segment rotation, fsync/readback costs, spool exposure/RPO, bounded memory at scale,
cloud sizing and capture throughput. No performance claim or new infrastructure.
