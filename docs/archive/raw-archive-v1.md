# Brief #004: sealed RAW archive

`bqip-raw-archive` is an independent downstream process. The collector owns
socket reception, RAW preservation, fences and sealing. The archive process
reads completed segments and owns only its separate derivative state directory.
No collector ACK, lock, queue or shutdown depends on remote storage. Brief #004
does not implement normalization, Book Service, replay or retention.

## Input and byte contract

Only complete `capture.sealed`, `seal.json`, `manifest.json` sets under the
accepted collector lane/UUID directories are eligible. Active or incomplete
sets are skipped and discovered on a subsequent scan. Symlinks, path escape,
noncanonical UUIDs, repeated segment IDs across lanes and invalid evidence fail
visibly. No unrelated directories are recursively scanned.

Before network I/O, the uploader reads a bounded snapshot, verifies file identity
and length, then uses the accepted BQRC scanner/seal and collector manifest
validators. Those validators now accept byte slices as well as paths; persisted
BQRC v2, seal and manifest semantics are unchanged. CRC, segment SHA-256, frame
count, canonical manifest hash, lane/session/segment lineage all remain checked.
The accepted `raw_loss_status=POSSIBLE` remains archivable. This does not assert
that market data is complete. Unknown loss statuses remain invalid under the
accepted manifest contract.

The original three byte buffers are uploaded without reserialization,
compression or normalization. The local set is revalidated before receipt
publication; a changed source prevents success. The uploader never writes,
renames or deletes source files. Read-only access may update filesystem atime;
content, names, length and mtime are unchanged. The source/state directories
must be operator-controlled: this is not a sandbox against a privileged local
attacker racing filesystem replacement. Existing symlinks are rejected in every
path component before/after reads, and opened Unix device/inode identity is
checked.

## Keys and namespace

Keys are deterministic:

```text
{prefix}/raw/v1/binance/usd_m_futures/btcusdt/{lane}/{segment_uuid}/capture.bqrc
{prefix}/raw/v1/binance/usd_m_futures/btcusdt/{lane}/{segment_uuid}/seal.json
{prefix}/raw/v1/binance/usd_m_futures/btcusdt/{lane}/{segment_uuid}/manifest.json
```

`lane` is exactly `depth_ws`, `aggtrade_ws` or `rest`. Prefix components are
nonempty ASCII letters/digits/dash/underscore/dot, excluding `.` and `..`.
Leading/trailing/repeated separators, backslashes and control characters fail.
The segment UUID is the archive unit; one session may span several segments.

The archive namespace is lowercase SHA-256 of restricted JCS over exactly
`backend`, `endpoint`, `region`, `bucket`, `prefix`. The HTTPS endpoint is
canonicalized by URL parsing (including host case/default port), with its
trailing slash removed. Credentials, timeouts and local paths are excluded.
Endpoint/bucket/prefix/region changes yield a separate receipt namespace.
Independent static hash vectors live in `tests/fixtures/archive/vectors.json`;
ASCII keys make Python stdlib sorted JSON an independent canonical-byte oracle.

## Create and verify

The adapter uses `object_store = 0.14.2`, default features disabled, `aws`
enabled, and explicit `S3ConditionalPut::ETagMatch` plus `PutMode::Create` on
every PUT. Offline adapter tests observe `If-None-Match: *` on the actual
generated request. There is no delete, list, overwrite or multipart operation
in the archive interface. Provider-side enforcement still requires a finite
real-provider capability smoke; mock evidence cannot certify a provider.

New sets publish capture, then seal, then manifest. The manifest is the commit
marker. A complete set becomes `REMOTE_DURABLE` only after independent full GET
of all three objects, exact length/SHA-256/byte comparison, remote-returned
control-file semantic validation, final local validation and durable receipt
publication. HEAD and ETag never establish correctness. ETag/version are
informational receipt fields only.

An AlreadyExists response requires exact GET comparison. Different bytes cause
`REMOTE_OBJECT_CONFLICT`, without overwrite. An uncertain PUT outcome triggers
GET: equal bytes resolve success; absent/unavailable data remain retryable;
different bytes are a conflict. A manifest found at the start is checked first,
then all three objects are verified. Missing, different or unreadable committed
evidence becomes `REMOTE_ARCHIVE_DIVERGENCE`; no automatic repair is attempted.
Partial pre-manifest uploads can be completed idempotently at the same keys.

## Receipts and restart

Receipts are derivative `BQIP-RAW-ARCHIVE-RECEIPT-V1` canonical JSON at
`archive_state_root/v1/{archive_namespace_id}/{segment_id}.json`. Required fields
are defined in Brief #004 §§35–37 and implemented in `receipt.rs`. Sizes and
nanosecond time are canonical unsigned decimal strings. The receipt contains
all remote keys, hashes and lengths, local lineage hashes, uploader commit,
verification method `FULL_GET_SHA256`, verification time, and nullable ETags
and versions.

`receipt_hash` is SHA-256 of the repository's restricted JCS with only the
root-level `receipt_hash` removed. Nested names are not excluded. Duplicate
keys, noncanonical encoding, mismatching fields, invalid time/commit, corrupt
hashes, extra fields and oversized receipts do not count as valid. The existing
restricted JCS domain applies; floating-point values are not accepted.

Publication uses create-new temporary file, write, flush, file sync, atomic
rename and the existing directory-sync portability helper. No success is
emitted before publication finishes. Temporary and invalidated receipt evidence
is retained. A lost or malformed receipt causes full remote re-verification;
the source spool is never repaired or modified by this process.

At startup, valid receipts for the current namespace classify local segments
as remote durable. They prove a point-in-time verification, not continuous
remote integrity. Use `--once --reverify` for a new full GET verification. This
invalidates an old receipt before the new attempt; a detected divergence cannot
leave that old receipt counting as current success.

## Configuration and operation

Copy `config/runtime/raw-archive-v1.json` and set the real HTTPS endpoint,
region, bucket, prefix and separate spool/state roots. The example endpoint is
deliberately non-routable. Unknown JSON fields fail. Relative roots resolve
against the process working directory. Neither root may contain the other.
There are no secrets in the JSON configuration.

Set only these archive-specific process environment credentials through the
operator's secret mechanism: `BQIP_ARCHIVE_ACCESS_KEY_ID`,
`BQIP_ARCHIVE_SECRET_ACCESS_KEY`, optionally `BQIP_ARCHIVE_SESSION_TOKEN`.
Empty/invalid values fail. There is no ambient AWS profile, credentials file,
IMDS or exchange credential fallback. Do not put secrets in command arguments,
repository files or logs. TLS and certificate verification are mandatory;
HTTP endpoints, userinfo, query parameters and endpoint paths are rejected.

Minimum provider permissions are exact-prefix object GET and conditional PUT.
Do not grant DeleteObject, multipart or broad administrative permissions. Some
providers obscure missing-object GET as AccessDenied without scoped list
permission; this implementation fails visibly in that case. The operator must
choose/document a provider permission model that supplies safe exact-key
missing-object behavior. The uploader itself never lists objects. Configure a
dedicated nonproduction smoke prefix. External cleanup is outside this brief.

From a committed Git checkout:

```bash
export CARGO_TARGET_DIR="/tmp/bqip-target"
cargo build --locked --release -p bqip-raw-archive
"$CARGO_TARGET_DIR/release/bqip-raw-archive" --config /absolute/path/archive.json --once
"$CARGO_TARGET_DIR/release/bqip-raw-archive" --config /absolute/path/archive.json --once --reverify
"$CARGO_TARGET_DIR/release/bqip-raw-archive" --config /absolute/path/archive.json --run-for-ms 60000
"$CARGO_TARGET_DIR/release/bqip-raw-archive" --config /absolute/path/archive.json
```

With a source ZIP lacking Git metadata, set `BQIP_BUILD_COMMIT` to the exact
candidate SHA in the delivery report before compiling. Invalid/missing build
identity prevents archival. The finite duration stops discovery at its deadline
and drains active bounded operations; it is not a hard process-kill deadline.
SIGINT/SIGTERM use the same drain path, including `--once`: remaining segments
stay pending and only active work drains. An uninterrupted `--once` exits
nonzero for unresolved pending or invalid sources. Daemon mode reports failed segments and continues
processing other eligible segments; a clean daemon exit alone is not evidence
that its backlog drained.

## Bounds and retry policy

Defaults: one inflight segment, 128 MiB per RAW object, 1 MiB per control file,
one-second scan interval, 1–60 second exponential retry delay, 30-second request
and five-second connection deadlines, 100,000 inspected segment entries.
Configured object bounds are 4 KiB–256 MiB and concurrency is 1–8, further
restricted by `(N+1)*(3*max_single_put_bytes+64 MiB) <= 2 GiB`. This conservative
buffer budget includes scanner/validation and adapter copies; it is not an RSS
guarantee for the whole workstation. Inputs exceeding the per-object cap remain
pending with `ARCHIVE_OBJECT_TOO_LARGE`; they are never partially uploaded.
Per-process scanned metadata and task counts are bounded. Full local validation
on every scan trades CPU/disk cost for correctness; choose a scan interval and
cardinality cap appropriate to retained spool size. A scan-capacity failure is
visible and requires operator configuration; it never silently truncates a scan.

Only `UPLOAD_RETRYABLE` failures retry automatically. The runner retains
deterministic exponential retry state across scans, capped at `retry_max_ms`.
Each GET may make at most three attempts under the same delay/deadline bounds;
the SDK's own retry count is zero. Auth, capability, conflict, corruption and
state-I/O failures are blocked for that segment until operator action/restart.
Retry history is process-local; restart reconstructs from source plus receipts.
No unbounded pending task queue is created. Shutdown stops discovery, drains
the current bounded worker set and leaves incomplete remote sets for restart.

## Observability and recovery

JSONL contains bounded state transitions, typed/static error diagnostics and
periodic summaries. No raw payload, credentials, URLs with secrets, authorization
headers or provider response body is logged. Status and provider code are
allowlisted; unrecognized provider codes are omitted. Logging uses a bounded
1,024-record queue; overflow increments `dropped_log_records`. A separate stdout
writer isolates archive work from slow log consumption; an indefinitely blocked
stdout sink can delay process termination and should be managed by the operator.

States: `DISCOVERED`, `LOCAL_VERIFYING`, `LOCAL_VERIFIED`, `UPLOAD_PENDING`,
`UPLOADING_CAPTURE`, `UPLOADING_SEAL`, `UPLOADING_MANIFEST`, `REMOTE_VERIFYING`,
`REMOTE_DURABLE`. Typed errors are in `error.rs` and retain all required failure
semantics from Brief #004 §34.

Summaries expose pending/durable segment counts and total three-object bytes,
oldest pending first-receive age, source validation errors, upload/verification
attempts/successes/failures/retries, auth/capability/conflict/divergence counts,
upload/download bytes, current/peak inflight segments and latency distributions.
`bytes_uploaded_total` counts attempted PUT body bytes (including conflicts or
failed attempts), not proven network egress. Download bytes count full objects
returned to verification. Upload latency is per PUT; verification latency covers
the full three-object GET/hash/semantic phase. Percentiles use nearest rank over
the last 1,024 samples; lifetime count and total time are retained. Invalid local
evidence is counted as pending, but its untrusted timestamp cannot contribute a
trusted oldest-age value. This is explicit off-host exposure, not an RPO promise.

On failure: inspect typed errors and backlog; resolve credentials/capability or
local state problems; restart and reverify. Do not overwrite conflicted objects,
repair a committed divergent set, delete partial objects or evict local evidence.
Receipts never authorize deletion. Provider/operator mutation outside BQIP's
permissions remains possible. Bucket versioning/Object Lock may be external
hardening; this implementation does not configure them or claim absolute cloud
immutability.

## Verification and limitations

```bash
export CARGO_TARGET_DIR="/tmp/bqip-target"
python3 tools/generate_protobuf.py
python3 tools/generate_protobuf.py --check
python3 tools/checks.py
sha256sum --check FILES.sha256
python3 tools/run_archive_benchmark.py --output /tmp/archive-benchmark.json --samples 3
```

The existing CI runs workspace tests, all-target Clippy, Python/Protobuf and
security gates. Archive tests require no cloud secrets or network. Offline
fixtures use real accepted segment writers and deterministic fault stores;
adapter tests exercise actual object_store request generation. The benchmark
uses 1/8/32/64 MiB BQRC objects and records real measured in-memory rates, latency,
CPU/RSS, concurrency, retry counts, state bytes and observed backlog/age. This
does not measure real S3 throughput or close B2/B4.

Use an external Cargo target directory for validation, as CI does. Upstream
`object_store` documentation contains public example AWS identifiers that rustc
embeds in compiled metadata. Keeping dependency build output outside the source
tree prevents those examples from polluting `gitleaks dir .`; no detector rule
or source/evidence scan is disabled. Move any old in-tree build directory out
before validation. The delivery contains source and sanitized evidence only.

Single PUT only; no multipart, automatic local/remote deletion or eviction.
One configured namespace per process. Full GET and repeated local validation
consume bandwidth, CPU and disk. Retained source/receipt evidence grows until a
separately authorized retention policy exists. Receipts inherit the accepted
Windows directory-sync limitation: no POSIX directory-entry power-loss durability
claim is made for Windows. No new Windows execution or universal RPO is claimed
without corresponding evidence. Real-provider smoke is a separate finite gate;
absent explicit credentials/provider configuration it is externally blocked.
