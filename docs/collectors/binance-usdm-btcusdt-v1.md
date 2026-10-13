# Brief 003: public Binance collector

Scope: one Rust process for BTCUSDT USDⓈ-M perpetual public depth, aggTrade,
exchangeInfo and REST depth. Base: `2b216ee6d6c86cb1d5aa742fd3cb314947ec8c5b`.
No credentials, private API, trading, uploader, broker or production Book Service.
The normative brief is `docs/briefs/Brief-003.txt`. Accepted venue semantics and
BQRC v2 framing remain unchanged. Existing ADR status is not promoted by tests.

## Build and validation

Use Rust 1.85.0 with clippy/rustfmt, Python 3.12.14, protoc 33.6 and the hashed
Python lock (protobuf 6.33.6). Use cargo-audit 0.22.1 and gitleaks 8.24.2 as in
the existing CI workflow. No runtime Python dependency was added.

```sh
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --require-hashes -r python/requirements-dev.lock
python3 tools/generate_protobuf.py
python3 tools/generate_protobuf.py --check
python3 tools/checks.py
sha256sum --check FILES.sha256
cargo build --locked --release -p bqip-binance-collector --examples --bins
```

Build provenance comes from Git HEAD (build script watches HEAD and its ref).
From a Source ZIP without Git, set `BQIP_BUILD_COMMIT` to the full reviewed
candidate SHA. Unknown provenance fails startup. A dirty build is not acceptance
evidence: validate and capture from a clean candidate checkout.

The existing CI runs the same semantic gates and all workspace tests, including
the collector. Tests use numeric loopback WS/HTTP peers; none need Binance.
A workflow definition is not evidence of an executed remote CI run.

## Finite public capture

```sh
mkdir -p artifacts/live
cargo run --locked --release -p bqip-binance-collector -- run \
  --config config/runtime/binance-btcusdt-collector-v1.json \
  --seconds 90 --log artifacts/live/events.jsonl --report artifacts/live/report.json
```

The command uses exactly:

- `wss://fstream.binance.com/public/ws/btcusdt@depth@100ms`
- `wss://fstream.binance.com/market/ws/btcusdt@aggTrade`
- `https://fapi.binance.com/fapi/v1/exchangeInfo`
- `https://fapi.binance.com/fapi/v1/depth?limit=1000&symbol=BTCUSDT`

TLS certificate verification stays enabled. The binary has no endpoint override;
only the library harness accepts numeric loopback test transports. No HTTP
redirects, credentials or automatic body decompression. Existing output log or
report files are rejected. SIGINT, SIGTERM or duration expiry stop capture, finish
in-flight HTTP, drain writers, fence, seal and verify. A finite capture duration
is not a hard shutdown deadline for a wedged kernel filesystem operation.

An exit code of zero means shutdown completed; it does not prove live acceptance.
Check both streams received payloads, valid metadata and snapshot counters are
positive, writer failures zero, all segments/manifests verified and shutdown true.
Store actual live artifacts outside the reviewed Git commit.

## Preservation and health

The complete application message is timestamped using wall nanoseconds and
monotonic nanoseconds since process start, assigned a global sequence, and sent
to its bounded raw actor. The actor owns synchronous BQRC writes on
`spawn_blocking`. The source awaits append acknowledgement before `try_send` to
a bounded realtime queue. JSON, UTF-8 interpretation and accepted venue parsing
cannot decide whether application payload bytes are preserved. Disk backpressure
can delay reception; realtime processing cannot backpressure raw through a queue.
This does not promise preservation of bytes never received during a disconnect.

Append acknowledgement (`RAW_APPEND_ACK`, accepted=true) is distinct from
`RAW_DURABILITY_FENCED` and `RAW_SEGMENT_SEALED`. A periodic fence flushes,
`sync_data`s the OPEN segment and syncs its directory; records the last fenced
sequence. Rotation/shutdown fence before seal. Faults revoke realtime trust and
stop the affected source, with STORAGE_FAILURE/RAW_WRITE_FAILURE and POSSIBLE
raw loss. Other lanes remain independent. No tested power-loss guarantee is
claimed beyond the specified filesystem operations.

Realtime saturation emits LOCAL_CAPACITY_DROP with raw loss NONE; parser errors
also have raw loss NONE. Depth becomes BROKEN and requests resync. Same-session
resync retains the original depth identity session. A physical reconnect creates
new connection/session UUIDs and new WS segments; aggTrade identity still excludes
session. `capture_seq` is process ingress order, not economic/global venue order.
Raw health and realtime health have distinct fields. Intentional shutdown leaves
closed realtime lanes BROKEN; resync_completed counters show earlier health.

There are three raw queues, two realtime queues, one latest desired-snapshot watch
value and a one-result snapshot queue. Bridge buffering and total parsed book
session memory/event retention are bounded. On session budget exhaustion, depth
realtime stays BROKEN until the next physical session while RAW continues. This
bounds the accepted BookSync implementation's retained identities and book levels;
it is a health demonstration, not a production Book Service. Config validation
limits queue payload envelope to 512 MiB, with separate bounded bridge/book and
transport overhead. This is not an exact RSS cap.

## Network byte boundaries

HTTP raw is the complete body after HTTP transfer framing, before content decoding
or JSON parsing. Request `Accept-Encoding: identity`; encoded responses are retained
byte-exact with their opaque Content-Encoding bytes and rejected for parsing.
Non-200 complete bodies also survive raw capture. An incomplete/oversize response
has no complete application body and produces HTTP_FAILURE/POSSIBLE; it is not
falsely persisted as complete. Status and canonical query remain typed evidence.

The only IDL change is additive RawCaptureEvent tags 16 (HttpSourceContext),
17 (StreamSourceContext), 18 (optional opaque Content-Encoding bytes). No prior tag,
enum or BQRC framing byte changes. Official bindings are generated, never edited.
Independent wire-key fixtures cover the new tags in both languages.

WebSocket uses reqwest's rustls HTTP/1 upgrade and tokio-tungstenite framing. The
handshake validates status, Upgrade, Connection and Sec-WebSocket-Accept, rejecting
unsolicited extensions/subprotocols. Compression is not negotiated. A bounded
14-byte frame-header adapter presents TEXT internally as BINARY to prevent a
framing-library UTF-8 check discarding the original bytes; payload bytes are never
changed and original PayloadKind is retained. After raw ACK invalid TEXT UTF-8 is
reported and closed with 1007. Fragmentation/interleaved control frames, large
length forms and coalesced/bytewise delivery are tested. Ping is flushed through
tungstenite's single automatic pong; PING/PONG/CLOSE are telemetry, not market RAW.

Two independent supervisors reconnect with bounded exponential delay, resetting
backoff only after 30 seconds connected. Each attempt has a fresh connection UUID;
each established continuity epoch has a fresh session UUID. Quiet sources poll
raw health every 100 ms. The server's 24-hour close is handled by reconnection.

## REST, metadata and resync

One REST worker serializes all HTTP requests, so at most one snapshot is in flight.
A deterministic guard spaces attempts (5 s, 10 s, then 60 s cooldown by default).
Neither reconnect, new resync generation nor success resets the request budget.
This bounds repeated weight-20 snapshots. Metadata refresh is separate from the
snapshot budget but shares the serialized worker. An initial standalone snapshot
may establish HTTP availability before any depth session; it is not reused to
bridge a later session. Responses for stale generations are not trusted.

Accepted #002 bridge rules stay `u < S` discard, `U <= S <= u` bridge, then
`current.pu == previous.u`. Identity conflict detection survives same-session
resync. Old-session snapshots cannot restore a new session's health.

exchangeInfo is raw-preserved then parsed with #002. Observations have UNKNOWN
effective basis, absent effective_from, knowledge timestamp from receive time,
and unique collector/sequence version. Semantically changed metadata appends a new
protobuf observation with supersedes_version within this process; repeated equal
observations do not mutate prior files. Restart appends a new initial observation,
without claiming a cross-process supersession chain. No tick/step size is hardcoded.

## Local spool, manifests and recovery

Layout: `<spool_root>/binance/usdm/btcusdt/{depth,aggtrade,rest}/<segment-uuid>/`.
A root `collector.owner` marker is created exclusively and contains process/token
identity; release checks ownership. This portable filesystem guard deliberately
fails closed after a crash. Before restart, an operator must establish the former
process is dead and remove only its stale marker. Never remove capture evidence to
bypass recovery. There is no automatic stale-owner stealing.

Recovery completes before new connections. It verifies existing seal+manifest,
recovers orphan sealed files with RECOVERED origin, or copies a partial's valid
prefix into a new sealed recovery segment with PROCESS_CRASH_GAP/POSSIBLE and
explicit origin linkage. Original partial bytes are retained. Completed recovery
is idempotent; corrupt CRC/empty/corrupt evidence fails visibly. A further crash
mid-recovery can leave additional orphan evidence; nothing is silently removed.
Structural recovery is not proof that no complete frame disappeared: accepted
seal/hash/count evidence is required for that stronger check.

Manifest schema is BQIP-RAW-SEGMENT-MANIFEST-V1. It is recomputed by streaming every
frame and checking typed lineage, IDs, sequence/timestamp ranges, profile, code
commit, count, bytes and segment/seal hashes. Restricted existing JCS hashes exclude
only root manifest_hash; wide integers use decimal strings. Manifest publication
requires a valid seal, then create-new temp, fsync, atomic rename and directory
fsync. Existing destinations, symlinks and ownership collisions fail. Atomic rename
assumes a supported local POSIX filesystem and cooperating exclusive spool owner;
concurrent external mutation and NFS/Windows semantics are not supported claims.

## Operational defaults

| Parameter | Default |
|---|---:|
| spool_root | spool |
| raw_queues (depth, aggTrade, REST) | 16, 16, 4 |
| realtime_queues (depth, aggTrade) | 64, 128 |
| snapshot_buffer | 128 |
| book_session_events | 10000 |
| book_session_bytes | 16777216 |
| log_capacity | 1024 |
| ws_max_bytes | 1048576 |
| http_max_bytes | 16777216 |
| segment_max_bytes | 67108864 |
| segment_max_age_ms | 60000 |
| fence_ms | 1000 |
| connect_timeout_ms / http_timeout_ms | 10000 / 10000 |
| close_timeout_ms | 2000 |
| reconnect_min_ms / reconnect_max_ms | 1000 / 30000 |
| snapshot_retry_min_ms / snapshot_retry_max_ms | 5000 / 20000 |
| snapshot_attempt_budget / snapshot_cooldown_ms | 3 / 60000 |
| metadata_refresh_ms | 300000 |

Config is strict JSON (no duplicate/unknown fields). Queues 1..65536; durations
1 ms..24 h; WS up to 4 MiB, HTTP up to 32 MiB; book up to 100000 events/128 MiB;
segment up to 4 GiB and at least max payload + 2 MiB. Live minimums: reconnect and
snapshot retry 1 s, cooldown 30 s, metadata refresh 60 s. HTTP status/queries and
profile are fixed. The binary uses four Tokio workers. Structured log queue is
bounded and can drop records; the report counts dropped_log_records explicitly.
Counters remain available independently. Logs are flushed/synced on shutdown.

## Offline falsification and benchmark

Integration binaries cover stalled realtime preserving all payloads, malformed
JSON, append/fence/seal failures, byte/age/session rotation, global sequence,
handshake rejection, exact ping/pong, invalid UTF-8 and binary payloads, reconnect,
HTTP non-200/encoding/timeout, delayed snapshot while depth proceeds, semantic
metadata change, stale snapshots, gaps, identity conflicts across resync, bridge
and session budgets, storm guard and original-partial recovery/manifest attacks.

```sh
cargo build --locked --release -p bqip-binance-collector --example collector_benchmark
python3 tools/run_collector_benchmark.py \
  --binary target/release/examples/collector_benchmark \
  --output artifacts/benchmark --count 1000
```

Output directory must be absent; parent must exist. Four bounded offered rates:
100, 1000, 10000 messages/s, then unpaced; one stalled realtime slot. All raw bytes
are independently reread and compared to offered payloads. Report actual offered
rate, bytes/s, ACK p50/p95/p99, drops, loss, fence/seal samples, process CPU/RSS and
logical file sizes (not storage-device write amplification). Results are measurements
on the recorded environment, not hardware promises. A comparison to observed live
traffic is possible only when live connectivity succeeds.

## Authorized dependency delta and sources

New direct families, exact pins, defaults disabled:

| Dependency | Version | Features | Purpose |
|---|---|---|---|
| tokio | 1.48.0 | rt-multi-thread, macros, net, io-util, sync, time, signal | bounded runtime/actors; test-util only in dev |
| tokio-tungstenite | 0.30.0 | handshake | framing, fragmentation, control protocol |
| futures-util | 0.3.31 | std, sink | stream/sink operations |
| reqwest | 0.12.28 | rustls-tls-webpki-roots | verified TLS, HTTP/1, WS upgrade |

Cargo.lock resolves transitive versions; no Git dependencies, native-tls/OpenSSL,
Binance SDK, or separate direct TLS/channel dependency. Supply-chain and parser
surface increase with networking; locked audit and offline framing attacks are
acceptance gates. Existing crates are reused for UUID, hashing, CRC, protobuf,
serde and contract semantics. Lock delta and feature tree accompany validation.

`binance-runtime-sources-v1.json` records official URLs and every operational
assertion used: routed split, lower-case streams, 24-hour lifetime, ping/pong,
message/stream limits, depth bridge semantics, aggTrade semantics, REST parameters,
exchangeInfo weight 1 and depth-1000 weight 20. Architect revalidation: 2026-10-09;
implementation revalidation: 2026-10-13. The accepted #002 source manifest stays
unchanged. No Binance HTML hashes or runtime documentation scraping.
