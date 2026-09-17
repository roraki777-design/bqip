# ADR-002 — Encoding, exact numbers and identity

Status: PROPOSED / IMPLEMENTED FOR REVIEW

## Context

AC-001 A/B/C/D/F/J fixes Protobuf v3, exact decimals, nanosecond clocks, SHA-256,
structured observation IDs and a distinct canonical representation for manifests.

## Decision

The five files under proto/bqip/v1 are normative. Rust uses prost-build to generate
types. All scalar fields have explicit presence. Validators distinguish missing
values from an explicitly supplied zero/UNKNOWN; undefined enum numbers fail
validation. Enum wire tokens have standard type prefixes for Protobuf namespace
safety; the symbolic identity token is the unprefixed AC-001 name (for example
LIVE). Field numbers must never be reused. Incompatible changes require a new
major package; additive changes retain the major version when compatible.

DecimalValue persists an ASCII signed integer string and uint32 scale. Parsing
uses string operations, never floats, and supports coefficients beyond machine
integers. Normalization removes fractional trailing zeros only while scale is
positive, strips redundant leading zeros and maps all zero values to ("0", 0).
Persisted noncanonical objects fail validation. Input grammar is
`-?[0-9]+(\.[0-9]+)?`; exponent, plus, whitespace and non-ASCII digits are rejected.

Wall clocks are signed int64 Unix nanoseconds UTC. Monotonic offsets are uint64
nanoseconds relative to the collector process and travel with its UUID identity.

CaptureId contains collector UUID bytes and uint64 sequence. The single-owner
sequencer starts at zero, increments without wrap, and refuses exhaustion.
Reconnects create connection/session UUIDs but do not reset the collector counter.
Collector identity is generated once when constructing a process sequencer.
The public API exposes it read-only; changing it during a sequence is not allowed.

Identity preimages start with the ten literal ASCII bytes BQIP-ID-V1 and append
each component's uint32 big-endian UTF-8 label length, label, uint32 value length,
and value. Component order is significant. Integers use canonical decimal ASCII;
decimals use coefficient:scale; enum components use explicit symbolic tokens.
No trade/book identity recipe is introduced. The raw hash is SHA-256 of payload
bytes only. Protobuf serialization is never used as a canonical identity hash.

Manifest files are JSON. Hashing removes only root manifest_hash and comments,
then applies RFC 8785 and SHA-256. Nested keys with those names remain normative.
`comments` is the designated human-only annotation field. Strict parsing rejects
duplicate names and malformed Unicode. Keys sort by UTF-16 code units; strings
are not Unicode-normalized. Array order is preserved.

The no-float manifest domain uses JSON integers in [-9007199254740991,
9007199254740991]; larger exact integers, including ordinary epoch nanoseconds,
must be strings. This follows RFC 8785 section 3.1 / Appendix D. Decimal values
are explicit coefficient/scale objects. Float and exponent tokens fail. Integer
negative zero canonicalizes to zero. Normative strings/expected bytes are tested.
The narrow integer-only JCS implementation needs no extra dependency; it is not
advertised as a general floating-point JCS library.

## Alternatives considered

* Binary floats for prices: forbidden by AC-001.
* Hashing deterministic Protobuf bytes: expressly rejected by AC-001.
* Random UUID per observation: replaced by the specified structured capture ID.
* Handwritten Python Protobuf runtime: not implemented; the official runtime is
  declared after the dependency report and the user's instruction to continue.
* Full JCS package: unnecessary for this restricted numeric domain; reconsider
  only if a future ADR extends normative values.

## Consequences

Python domain objects are not generated Protobuf bindings. Agreement on their
independent golden values does not establish Python wire-level compliance.
The declared official runtime is protobuf 6.33.6, with protoc 33.6. Python 6.x is
in upstream maintenance support; runtime/generator versions are paired, and no
general codec is rebuilt. See [upstream support](https://protobuf.dev/support/version-support/)
and the [published runtime](https://pypi.org/project/protobuf/6.33.6/).
Validation adapters and eleven wire tests have executed successfully. Official
.py/.pyi output is committed, as are the resolved dependency locks. CI checks
regeneration against those files. Development-only types-protobuf stubs support
strict mypy checking; they do not replace the runtime or generated messages.

## Frozen invariants affected

P1, P3, P4, P6. No source-of-truth or identity semantics are changed.

## What remains BENCHMARK/RESEARCH REQUIRED

Serialization costs and bounded memory profiles. Compression, Parquet, databases,
venue-specific recipes and transport technology remain outside this brief.

References: https://www.rfc-editor.org/rfc/rfc8785
and https://protobuf.dev/programming-guides/serialization-not-canonical/
