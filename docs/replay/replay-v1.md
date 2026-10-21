# Brief #005 — deterministic replay and normalization

Authority: [Brief-005](../briefs/Brief-005.txt). Integrated baseline:
`c67481d339b5779e1a18fb95bf3d2b74ae8dcd10`. One offline crate,
`bqip-normalizer`, provides a library and `bqip-replay` CLI. This remains Phase 2.
Collector and archive behavior is unchanged; the collector never depends on replay.
The crate calls the accepted collector manifest validator, without starting its
network runtime. No new third-party dependency or Protobuf change is introduced.

## Reproduce

Use the root README's pinned development tools and hash-locked Python dependencies:
Rust 1.85.0, protoc 33.6, Python 3.12.x, protobuf 6.33.6. Keep build products outside
the source and evidence secret scans.

```sh
export CARGO_TARGET_DIR=/tmp/bqip-target
cargo build --release --locked -p bqip-normalizer --bin bqip-replay
/tmp/bqip-target/release/bqip-replay version
/tmp/bqip-target/release/bqip-replay plan \
  --source-root /data/accepted-copy --state-root /data/plan-001 \
  --segment binance/usdm/btcusdt/rest/SEGMENT_UUID \
  --segment binance/usdm/btcusdt/depth/SEGMENT_UUID \
  --segment binance/usdm/btcusdt/aggtrade/SEGMENT_UUID
/tmp/bqip-target/release/bqip-replay run \
  --source-root /data/accepted-copy --state-root /data/run-a \
  --plan /data/plan-001/plan.json
/tmp/bqip-target/release/bqip-replay verify-transcript \
  --transcript /data/run-a/transcript.json
```

Replace SEGMENT_UUID with each actual directory ID. State directories must be new,
have an existing parent and be outside the source tree; outputs are create-only.
`--config FILE` selects the versioned runtime config. Repeated `--metadata FILE.pb`
arguments supply a complete explicit catalog of accepted InstrumentMetadataVersion
messages instead of automatic observation discovery. The sources are not implicitly
merged. Such messages require independently justified effective intervals for research.
The CLI never manufactures confirmation. Optional `--run-id UUID` and
`--processing-time-ns N` affect operational provenance only.

The binary embeds Git HEAD and Cargo.lock. A source ZIP without Git requires a
build-time `BQIP_BUILD_COMMIT` with the exact candidate's 40 lowercase hexadecimal
characters. Invalid/missing identity becomes UNVERIFIED and cannot run accepted
replay. An explicit value is a provenance assertion, not a source attestation.
Acceptance uses the exact clean candidate, never a dirty development build.

## Input, plan and memory

Each input directory contains capture.sealed, seal.json and manifest.json. Its UUID
must match the header, seal and manifest. The shared accepted byte validators check
magic/version, framing, header/record CRC, counts, length, SHA-256, seal origin,
canonical manifest, typed transport evidence, capture sequence and session lineage.
Invalid evidence fails with SEGMENT_SOURCE_INVALID and no output for that segment.

BQIP-REPLAY-PLAN-V1 pins replay mode, profile ID/hash, full normalizer commit,
config hash and explicit ordered segment paths, UUIDs, three file hashes and record
counts. Counts are canonical unsigned decimal strings. Duplicate keys, unknown
fields, duplicate segment IDs and unsupported pins fail. The hash is
SHA256(restricted JCS of root excluding plan_hash and comments). Only root fields
are excluded. Config hashing includes every accepted config field and no local path.

Before parsing any record, execution verifies all segments and their pinned hashes.
Every pass uses a validated immutable byte snapshot; source hashes are checked
again before and after transcript finalization. Changed bytes fail with
REPLAY_INPUT_CHANGED. Invalid/missing/unsafe source paths may instead give a source
or path error; both prevent successful completion. No source is written or recovered.
Replacing or mutating a previously consumed segment is tested explicitly.

Plan order is an operational traversal order, not economic causality or an exchange,
wire or market total order. BQRC record order is preserved, with zero-based indices.
Absolute, empty, dot, parent, backslash and drive-style paths fail. All symlinks,
including ancestors, fail. Reads check regular-file type, size and modification
time, and on POSIX device/inode, before/after reading. This assumes cooperative
filesystem permissions, not a hostile kernel or an atomic filesystem snapshot.

One capture snapshot is retained at a time: default 64 MiB, configurable up to a
hard 256 MiB cap. Frames are processed incrementally using accepted frame limits.
Control files are capped at 1 MiB, plans at 8 MiB, segment count at 4096 and metadata
at 4096 versions / 16 MiB encoded bytes. Transcript writes stream; CLI verification
has a 64 MiB document cap. Over-limit inputs fail loudly. No unbounded segment set
or normalized event collection is retained. Resource limits participate in config
hashing; the benchmark does not redefine them as architecture performance targets.

## Outcomes and accepted mappings

Every record produces exactly one NORMALIZED_MARKET_EVENT, METADATA_OBSERVATION or
REJECTED outcome. Their sum must equal the plan count. Distinct captures are never
deduplicated by logical identity. Original capture ID, payload SHA-256, receive
time and monotonic time survive; RAW hashing never uses reformatted JSON.

| Accepted typed source | Payload | Economic metadata time |
| --- | --- | --- |
| MARKET btcusdt@aggTrade | TradeEvent | trade timestamp T |
| PUBLIC btcusdt@depth@100ms | BinanceDepthUpdate | transaction timestamp T |
| GET /fapi/v1/depth, limit=1000, symbol=BTCUSDT | BinanceDepthSnapshot | transaction timestamp T |
| GET /fapi/v1/exchangeInfo, no query | InstrumentMetadataVersion | UNKNOWN observation |

Exact endpoints come from the committed Binance profile. Venue, profile/hash,
transport, endpoint, channel, instrument and typed/original lineage must agree.
Payload substrings never select a parser. HTTP status must be 200; declared content
encoding must be exactly identity, or absent, matching accepted collector behavior.
Encoded bodies remain unchanged and are rejected. Existing parser error codes,
exact DecimalValue semantics and additive unknown-field drift are preserved.

The internal event envelope carries segment/record/capture/RAW lineage, original
clocks, venue/instrument, resolved metadata/basis, replay/schema/code pins, new
ProcessingContext, sorted quality/drift and an accepted typed payload. It is not a
new canonical wire schema or dataset. All trade quantities, RPI semantics, IDs,
aggressor, granularity, liquidation observations and clocks remain accepted values.
Depth U/u/pu, levels and original-session logical identity remain unchanged. Snapshots
are observations, not BookState. Receive minus exchange time is not network latency.

## Metadata and forensic guard

Catalog wraps the existing immutable MetadataCatalog and uses its resolver.
Automatic discovery makes a first pass over accepted exchangeInfo observations;
each version ID is original collector UUID + colon + decimal capture sequence.
They remain independent UNKNOWN-effective observations. Traversal order supplies
neither economic dates nor implicit supersession. Each observation still receives
its own outcome. Supplied catalogs may contain independently confirmed intervals
and explicit supersession. Duplicate (instrument, metadata_version) keys fail
construction, never pick a winner. History is not mutated.

VERSION_PINNED_RESEARCH always calls CorrectedResearch at Binance T. Missing,
ambiguous or UNKNOWN-effective metadata rejects the market event deterministically.
Later known corrections can affect research only under accepted economic intervals
and supersession. No fallback to as-lived selection occurs. Synthetic confirmed
metadata proves success; archived UNKNOWN evidence is never promoted to confirmed.

The metadata-only AsLived API uses original receive time as as_of, not execution or
event time. KnowledgeOnly preserves METADATA_EFFECTIVE_TIME_UNKNOWN quality.
Future-known corrections cannot affect earlier AsLived resolution. This does not
grant permission to label a whole replay forensic. LIVE is rejected by the offline
CLI. AS_LIVED_FORENSIC always returns FORENSIC_PROVENANCE_INSUFFICIENT in #005: no
complete approved deployment/artifact decision-provenance input exists here. There
is no silent research fallback. Historical #003 RAW cannot prove #005's as-lived
beliefs before its deployment.

The catalog digest sorts versions by instrument ID then metadata version, and uses
restricted JCS over every accepted semantic field, including null, temporal fields,
decimals and explicit source lineage. Large timestamps/sequences are strings. This
is provenance, not the metadata identity. Catalog vector order cannot alter it.
Shared contract additions are only read-only catalog enumeration and the unchanged
metadata observation identity recipe; golden tests prove the exact ID result.

## Transcript and operational report

BQIP-REPLAY-TRANSCRIPT-V1 has ordered entries plus a pinned header. Entries contain
segment ordinal/UUID/index, original capture UUID/sequence, RAW hash, source class,
lane, clocks, explicit NONE/POSSIBLE RAW loss status, outcome, parsed kind,
applicable venue/logical IDs, resolved metadata/basis, sorted quality/drift,
payload digest and stable rejection code. RAW_LOSS_RECORDED and SCHEMA_DRIFT_OBSERVED
remain visible. Unknown-effective rejection retains that quality code. No missing
market events are synthesized.

The header pins plan hash/mode, normalizer commit/config, Cargo.lock SHA-256,
profile ID/hash, schema, semantic catalog digest, count and ordered input file hashes.
Hash: SHA256(restricted JCS of root excluding transcript_hash and comments).
Only fixed object framing is streamed directly; every variable value uses the
accepted JCS implementation. Tests compare it with full-document canonical hashing.
Runtime IDs, processing/report times, absolute paths, host, CPU and temporary names
never enter the deterministic core. Saved transcript corruption fails verification.
The hash is repeatability evidence, not a signature or independent attestation.

Payload digests use SHA256(prost encoding). They are implementation-pinned
repeatability digests, **not logical_event_id**, not universal canonical Protobuf
hashes and not guaranteed cross-language byte identity. Tests independently compare
decoded typed values, including original lineage. No analytical dataset is persisted.

BQIP-REPLAY-RUN-V1 separately records run ID, processing time, elapsed/transcript
timings, counts and parser/rejection summaries. Parser success remains visible even
when metadata prevents normalization. Runtime fields may differ between executions.
The CLI writes transcript.partial, revalidates input, flushes/syncs and only then
renames successful output and writes a create-only report. Failures may retain
partial operational artifacts but never publish them as accepted runs. Library
callback outputs and writer bytes are tentative until execute returns Ok; discard
them on failure. No power-loss or atomic two-file publication guarantee is claimed.

## Errors and gates

Plan/segment errors abort: REPLAY_PLAN_INVALID, REPLAY_INPUT_CHANGED,
DUPLICATE_REPLAY_SEGMENT, SEGMENT_SOURCE_INVALID, REPLAY_PROFILE_MISMATCH,
NORMALIZER_COMMIT_MISMATCH, NORMALIZER_CONFIG_MISMATCH, UNSUPPORTED_SCHEMA_VERSION,
FORENSIC_PROVENANCE_INSUFFICIENT. Additional operational errors cover capacity,
output and transcript integrity. Record rejections include UNSUPPORTED_RAW_SOURCE,
UNSUPPORTED_CONTENT_ENCODING, PARSE_FAILURE, PROFILE_MISMATCH, METADATA_MISSING,
METADATA_AMBIGUOUS, METADATA_EFFECTIVE_TIME_UNKNOWN, METADATA_EVENT_TIME_MISSING,
plus unchanged accepted contract codes such as HTTP_STATUS_FAILURE and
INVALID_VENUE_DECIMAL. Codes contain no raw body, path, wall clock or random ID.
Source defects already forbidden by accepted manifest validation fail at segment
level; direct record tests separately verify record-level rejection behavior.

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
python3 tools/generate_protobuf.py
python3 tools/generate_protobuf.py --check
python3 tools/checks.py
sha256sum --check FILES.sha256
python3 tools/run_replay_benchmark.py --output /tmp/bqip-replay-benchmark.json
```

The determinism super-gate runs two fresh processes with different runtime contexts.
Both compare decoded typed fields against independent expectations and produce
identical transcripts. The negative forensic super-gate fails historical RAW
without provenance. Fault tests cover mutation/replacement, CRC/seal/manifest,
pins/paths, source context, metadata history/ambiguity and output isolation.
The benchmark excludes fixture preparation: 1x is 100 cycles of exchangeInfo,
snapshot, depth, two trade observations and malformed RAW; 5x/10x are 500/1000
cycles. It measures release replay throughput, RAW MiB/s, success/rejection rates,
transcript cost and per-process peak RSS/CPU. It does not close B2 or B4.

## Known limitations

One Binance BTCUSDT USD-M perpetual profile. No Book Service, LOB, BBO, bridging,
canonical HEALTHY/DEGRADED state, realtime normalizer deployment, remote retrieval,
Parquet, analytical dataset, NATS/JetStream, Deribit, features, private APIs or
trading. No cross-lane economic total order, universal Protobuf byte canonicalization
or full forensic reconstruction without historical provenance. UNKNOWN metadata
limits corrected research intentionally. No Brief #006 work is included.
