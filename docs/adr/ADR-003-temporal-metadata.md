# ADR-003 — Bitemporal metadata resolution

Status: PROPOSED / IMPLEMENTED FOR REVIEW

## Context

AC-001 G specifies effective and knowledge intervals, half-open bounds, immutable
history, latest known corrected research, as-lived filtering and errors on
ambiguous overlap. Those properties must hold simultaneously.

## Decision

The catalog is immutable; append constructs a new catalog. Version keys are
(instrument_id, metadata_version), with duplicate keys rejected. Each interval is
[from, to), where absent to is unbounded; empty/reversed intervals are invalid.
Required identifiers and product/asset tokens must be explicitly supplied.

An explicit supersedes_version links a correction to an existing version of the
same instrument. Knowledge start must strictly increase across an edge; missing
parents and cycles are therefore invalid. Revision IDs are opaque strings, not
lexicographically ordered version numbers.

CORRECTED_RESEARCH considers the supplied catalog's revisions whose effective
interval contains event time. AS_LIVED additionally requires an as-of timestamp
and keeps only revisions whose knowledge interval contains that timestamp.

Within eligible candidates, explicit supersession chains remove older revisions.
One remaining candidate is returned, zero is METADATA_MISSING, and multiple are
METADATA_AMBIGUOUS. A later known_from timestamp alone is NOT permission to pick
between unrelated overlapping versions. A correction learned after the as-of
cutoff cannot suppress a version the system knew earlier. When a correction only
covers part of an effective interval, the predecessor remains applicable outside
that correction interval.

This is the narrow interpretation of "latest revision" compatible with both
supersedes_version and the mandatory ambiguous-overlap error. It is documented
for Architect review, not an implicit authorization to overwrite historical rows.
Input ordering never decides a winner.

## Alternatives considered

* Sorting arbitrary overlaps by timestamp: would hide ambiguity.
* Sorting metadata_version strings: would invent a version ordering.
* Updating old catalog entries in place: violates immutable history.
* Using corrected metadata for as-lived: violates the knowledge cutoff.

## Consequences

Catalogs used for correction resolution must include referenced ancestors.
Closed historical intervals may be supplied as immutable records; no live
metadata-acquisition or interval-closure storage protocol is implemented here.
Both languages have the same manually specified expected temporal vectors;
both implementations have executed those vectors successfully.

## Frozen invariants affected

P1, P4, P6, P12. No inference of exchange metadata history is introduced.

## What remains BENCHMARK/RESEARCH REQUIRED

Catalog indexing and memory cost at scale; venue metadata acquisition and its
evidence. The Foundation resolver deliberately uses a small in-memory catalog.

## Additive amendment — Brief 002, Directive 002-A (2026-10-03)

Status remains PROPOSED / IMPLEMENTED FOR REVIEW. This amendment preserves the
Foundation rationale above and implements the Architecture Lead's B2-01 decision.

Economic time and knowledge time are distinct. `effective_time_basis` uses tags
0 UNSPECIFIED, 1 VENUE_DECLARED, 2 BQIP_CONFIRMED, 3 UNKNOWN. UNKNOWN requires an
absent effective start and a present knowledge start. Known bases require an
effective start. Explicit UNSPECIFIED and unrecognized values are invalid.
Receive time, onboardDate, zero, file time and process start are never inferred
to be economic transition times. The exchangeInfo parser creates UNKNOWN
observations, with known_from equal to the independently supplied receive time.

`resolve_with_basis` returns the selected version, resolution basis, and quality.
AS_LIVED's explicit `as_of` argument is the original event receive time; UNKNOWN
observations use only their half-open knowledge interval, regardless of event
economic time. Their result is KNOWLEDGE_ONLY with
METADATA_EFFECTIVE_TIME_UNKNOWN. Unrelated eligible observations remain an
ambiguity error. Known intervals retain Foundation resolution semantics.

CORRECTED_RESEARCH considers only known-effective intervals. If none covers the
event and UNKNOWN evidence exists for that instrument, resolution fails with
METADATA_EFFECTIVE_TIME_UNKNOWN. It never falls back to knowledge-only or projects
an observation backward. Other no-match cases remain METADATA_MISSING.

Later confirmation appends a new version with a known basis and explicit
supersedes_version. Eligible descendants suppress ancestors as before; later
knowledge cannot change forensic resolution at an earlier receive time.
The original observation is immutable, including its source evidence.

Fields 1–14 and their tags are unchanged. Fields 15–26 add basis, instrument
semantics, typed HTTP source evidence, and quantity bounds. The source includes
profile ID/hash, capture ID, exact raw hash, receive time, method, endpoint,
canonical query parameters, status and transport; ProcessingContext is not used
for observed facts. Full filters remain in immutable raw evidence. LOT_SIZE
supplies lot_size; PRICE_FILTER supplies tick_size. No precision-based default.

Compatibility: Foundation records with absent basis, no extension fields, and
an explicit effective interval remain readable under their original contract.
Absence is distinct from explicit UNSPECIFIED. New Binance records require the
extension and its evidence. The legacy `resolve` API refuses knowledge-only
results with LABELED_METADATA_RESOLUTION_REQUIRED, so it cannot silently discard
their quality label. No accepted Foundation history or existing enum tags change.
