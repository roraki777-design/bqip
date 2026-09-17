# ADR-008 — Replay and operational provenance

Status: PROPOSED / IMPLEMENTED FOR REVIEW

## Context

AC-001 K/L distinguishes replay semantics from Protobuf byte equality and defines
ProcessingContext plus operational/error-quality contracts.

## Decision

ReplayMode defines LIVE, VERSION_PINNED_RESEARCH and AS_LIVED_FORENSIC, with an
explicit UNKNOWN wire value that cannot validate as an active processing mode.
ProcessingContext contains processing time, code commit, schema version, run UUID
and replay mode. It is provenance separate from raw observation facts.

RawCaptureEvent semantic equality compares its observation fields, native IDs,
raw hash and source timestamps, excluding ProcessingContext. Thus new processing
time/run identity does not change the original observation. This is a raw-event
comparison only: no future normalized-event or feature comparator is invented.
Consumers must separately check pinned pipeline identities when claiming a
specific replay guarantee.

Capture identity is conserved through reading/reprocessing; replay does not call
the live CaptureSequencer. Raw protobuf metadata bytes can be preserved exactly,
but semantic replay does not require Protobuf bytes to be canonical.

OperationalEvent and QualityEvent encode all named AC-001 kinds, causes, raw loss
statuses and realtime continuity states. Presence is distinct from UNKNOWN and
from explicit zero/non-loss values. They carry timestamp/context and optional
capture/session references. These are data contracts, not Book Service behavior,
an event bus, or a claim that historical decision logging is already deployed.

## Alternatives considered

* Compare encoded Protobuf bytes: not the selected replay guarantee.
* Refresh capture IDs during replay: would turn reprocessing into new observations.
* Infer realtime decisions from raw alone: insufficient for as-lived reconstruction.

## Consequences

The full forensic guarantee starts only when a future ingestion component records
the required operational history and exact deployed artifacts. Brief #001 does
not promise forensic reconstruction for absent history. It supplies contracts and
local fixtures for that future boundary. Operational reason does not itself
assert raw loss: UPLOAD_FAILURE, for example, can coexist with intact local data.

## Frozen invariants affected

P3, P4, P6 and future P11 observability. P1-P14 remain unchanged.

## What remains BENCHMARK/RESEARCH REQUIRED

Artifact retention cost, operational-event volume and full replay throughput.
Scheduler/timer capture, venue reconstruction and runtime replay ordering beyond
stored segment order belong to their later briefs.
