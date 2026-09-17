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
