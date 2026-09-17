# Brief #001 — implementation record for Architect review

Completion Directive #001-A is the acceptance authority. Acceptance remains
**BLOCKED** until all local gates, clean-checkout verification and a real CI run
have passed. The accompanying delivery report/logs bind actual results to a git
commit and lock hashes; this document describes the implementation and procedure.
It does not self-accept any ADR or authorize Brief #002.

## Changes made to close the partial delivery

Resolved and committed Cargo.lock and the Python development lock with real
artifact hashes. Generated official Python `.py`/`.pyi` bindings using protoc
33.6; Rust uses prost-build against the same normative IDL. Fixed a missing Rust
ManifestEnvelope import and an optional metadata loop variable's type; applied
Rust formatting and handwritten Python import lint fixes.

Added external integer-domain JCS conformance vectors (8 valid / 17 invalid),
standard CRC32C vectors, shared enum expectations, Rust whole-frame-deletion
integrity testing and a replay-identity property. No persisted IDL, existing
normative fixture, invariant or application functionality was changed.

mypy passes with development-only types-protobuf after the dependency was
reported and the user instructed continuation. Narrow exceptions apply only to
protoc-owned files: its import layout/descriptor side effects and three generated
constructors using bare Mapping. No handwritten code receives those exceptions.
The generated files are never hand-edited; byte-for-byte regeneration is a gate.

cargo-audit 0.21.2 failed to parse CVSS 4.0 in the current RustSec database.
Version 0.22.1 fixes that demonstrated tooling incompatibility and builds using
Rust 1.85.0. The compiler and project dependencies remain pinned as specified.
No advisories are removed, ignored or replaced with a stale database.

## Dependencies

| Direct dependency | Pin | Purpose / reason beyond stdlib |
|---|---|---|
| prost / prost-build | 0.13.5 | Official-IDL Rust runtime / build generation |
| serde / serde_json | 1.0.219 / 1.0.140 | JSON parsing with duplicate-key-aware visitor |
| sha2 | 0.10.8 | SHA-256 absent from Rust std |
| uuid | 1.16.0 | UUID identities and v4 generation |
| crc32c | 0.6.8 | Castagnoli absent from Rust std |
| proptest | 1.6.0 | Generated property cases and counterexample reduction; dev only |
| protobuf | 6.33.6 | Official Python runtime for protoc 33.6 output |
| types-protobuf | 6.32.1.20260221 | typeshed runtime annotations; dev only |
| Ruff / mypy | 0.11.4 / 1.15.0 | Python lint / strict typing; dev only |
| pip-audit | 2.9.0 | Audit the resolved Python lock |
| cargo-audit | 0.22.1 | Audit the resolved Cargo.lock including current CVSS 4.0 advisories |
| Gitleaks | 8.24.2 | Secret scanning |

Cargo.lock contains the resolved Rust graph. requirements-dev.lock includes the
runtime and development graph, with hashes from pip's actual resolution report.
It targets CPython 3.12/Linux x86_64. Python primitives and CRC32C use stdlib.
No additional runtime or JCS library was added. Tool installation dependencies
are distinct from the application's locked graph; scanner internals do not add
networking or async runtime code to BQIP.

Sources: [runtime](https://pypi.org/project/protobuf/6.33.6/),
[types](https://pypi.org/project/types-protobuf/6.32.1.20260221/),
[auditor](https://crates.io/crates/cargo-audit/0.22.1).

## Cross-language coverage

Each implementation checks fixed external expected outputs independently.
Agreement between implementations is not used to generate expected values.
Fixture provenance is documented in tests/fixtures/README.md.

| Fixture | Normative expectation |
|---|---|
| contracts/decimals.json | 7 coefficient/scale outcomes |
| identity/vectors.json | 5 ordered preimages and SHA-256 digests |
| metadata/vectors.json | 9 outcomes/errors, both input orders |
| manifests/vectors.json | 3 canonical byte sequences and hashes |
| manifests/conformance.json | 8 JCS byte/hash vectors and 17 rejections |
| capture/frames.json | 5 exact frame/metadata/payload byte sequences and raw hashes |
| capture/segments.json + .bqrc files | Complete segment, truncated tail, bad CRC and external integrity hashes |
| capture/crc32c.json | Empty vector and 123456789 -> e3069283; all frame footers also checked |
| contracts/enums.json | Symbolic and wire names/tags for seven shared enums |

Rust has 20 tests: 1 contract unit, 10 contract integration and 9 capture
integration tests. Six are proptest properties: decimal idempotence; stable and
order-sensitive identity; excluded manifest hash; unambiguous metadata resolution;
frame round-trip; replay identity across arbitrary collector/sequence/provenance.
Python has 40 core/conformance tests and 11 generated-Protobuf contract tests.
Tests include invalid UTF-8, invalid JSON, empty payloads and absent-vs-zero fields.

The JCS domain is precisely documented in docs/contracts/JCS-profile.md. It
rejects unsupported numeric forms, duplicate keys and malformed Unicode before
root exclusions. String ordering uses UTF-16 code units; supplementary Unicode,
control escaping, arrays, null/bool and safe integer boundaries have independent
expected fixtures. No performance claim is attached to Python CRC32C.

## Raw recovery acceptance

Tests cover a complete segment, every final-frame truncation, bad CRC, external
hash detection of complete-frame deletion, failure before seal, quiescent reopen
and recovery into a new UUID segment, no overwrite of existing sealed evidence,
normal local rename and injected fsync/rename failures. Structural recovery alone
cannot establish completeness. Recovery retains its source and reports the prefix.
No real power cut, disk-controller behavior or cross-filesystem guarantee is claimed.

## ADR text versus code

All four ADRs remain **PROPOSED / IMPLEMENTED FOR REVIEW**.

| ADR | Code behavior | Difference |
|---|---|---|
| 001 | Application-message bytes, BQRC v1, local seal/recovery and external segment hash | None identified |
| 002 | Protobuf presence, exact decimals, structured IDs, BQIP-ID-V1 and restricted JCS | None identified |
| 003 | Immutable half-open bitemporal catalog with explicit supersession | None identified |
| 008 | Raw semantic equality excludes processing provenance; shared operational contracts | None identified |

Metadata corrections are linked by supersedes_version. Unrelated overlapping
versions remain ambiguous even if one has a later knowledge timestamp; the ADR
states that interpretation for Architect review. No architecture conflict was
identified during this completion work. Tests do not imply ADR acceptance.

## Reproduction and CI

README gives exact individual and aggregate commands. tools/checks.py executes
13 gates and prints environment/git/hash evidence. CI regenerates bindings,
compares against committed bytes and invokes the same aggregate path. Third-party
actions are pinned to full commit SHAs. CI has contents:read only and checkout
disables credential persistence. No deploy/write permission is granted.

The clean-checkout acceptance run uses committed files, an empty Cargo cache and
target directory, a fresh virtual environment and --no-cache-dir --require-hashes
installation. Its logs and final commit/lock evidence are delivered separately
from the source tree. A GitHub remote and authenticated access are required for
the outstanding real CI run; a local check cannot establish that result.

## Remaining limits

No venue, network ingestion, broker, storage service, normalized book, trading or
execution functionality. Linux/POSIX is the validated filesystem target; arbitrary
other operating systems are not claimed. Raw preservation under realtime failure
P11 awaits a future ingestion brief. No cloud, performance or power-loss claim.
