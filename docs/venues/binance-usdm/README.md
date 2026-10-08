# BinanceUsdMVenueProfile v1 — offline contract proof

Authority: `docs/briefs/Brief-002.txt`, `Directive-002-A.txt`, and `Recovery-002.txt`.
Foundation is c7deed5b17c9bb0dbc50622e3c3ef565907c92d7. This is a new implementation
candidate. The lost bae55a689085fadac0f0ca98420b4e8088e3eec4 candidate has not been
recovered, replicated as a Git object, or accepted by the Architect.

## Profile and evidence

The normative profile is `config/venues/binance/usdm/btcusdt-perpetual-v1.json`.
Its hash uses Foundation JCS/manifest semantics, with no HTML hash. Rust includes
this committed profile at build time; the Python repository package reads the
same file. Independent profile vectors pin every field and canonical byte.
Use from a repository checkout; wheel/distribution packaging is outside this brief.

The instrument is BINANCE:USD_M_FUTURES:BTCUSDT, linear perpetual, base BTC,
quote and margin USDT. The profile contains no mutable tick or quantity filters.
`parse_exchange_info` validates the exact symbol/pair/assets/contract type and
preserves venue status, including a non-TRADING status. PRICE_FILTER.tickSize and
LOT_SIZE.stepSize/minQty/maxQty supply normalized values. MARKET_LOT_SIZE applies
to market orders and remains in raw evidence. Precision fields are not increments.
The contract does not invent a contract_multiplier. Quantity unit is BTC.

Metadata extensions and compatibility are documented in ADR-003. An observed
snapshot has UNKNOWN effective time. Later confirmation is explicit appended
evidence from the caller; parsers never infer it. Metadata versions are opaque
caller-supplied identifiers. `as_of` in AS_LIVED is the event's original receive
time, not current replay/processing time.

HTTP evidence is typed, transport=HTTP_REST, method=GET, endpoint a path, status
200 for these successful-response parsers. Decoded query keys are unique ASCII
and ascending; exchangeInfo has no query, depth has limit=1000 and symbol=BTCUSDT.
SourceLineage preserves profile ID/hash, capture ID, exact body hash and receive
timestamp. Complete native filters are recoverable from the raw reference/hash.
Parsers compare the hash against the unmodified input bytes before interpretation.
An HTTP error can still be raw evidence; it is not a valid successful snapshot.

## Future transport constraints (documentation only)

Depth: `wss://fstream.binance.com/public/ws/btcusdt@depth@100ms`.
AggTrade: `wss://fstream.binance.com/market/ws/btcusdt@aggTrade`.
These are separate connections/continuity epochs. Current documented constraints:
24-hour connection lifetime, server ping every 3 minutes, pong required within
10 minutes, at most 10 incoming messages/second and 1024 streams/connection.
No reconnect, socket, HTTP client, live market access or credentials exist here.

Official titles, URLs and exact BQIP assertions are in `sources-v1.json`; the
accepted retrieval date remains 2026-10-03, rechecked on 2026-10-08. Future source
changes require profile review, not automatic runtime scraping or mutation.

## Native semantics and identity

AggTrade is always AGGREGATED, including f=l. q is total, nq excludes RPI; require
0<=nq<=q and f<=l. m=true is SELL, false BUY. No child fills, liquidation inference
or RPI member inference. See ADR-015. All required fields must exist; st may be
absent, but if present must be integer 1. st=2 is PROFILE_MISMATCH.

Depth requires e/E/T/s/U/u/pu/b/a; optional ps must equal BTCUSDT and st must be 1.
U<=u. E and T are preserved as unsigned native milliseconds and converted by
checked multiplication to signed 64-bit nanoseconds. IDs use unsigned 64-bit
integers; booleans and floating-point semantic numbers are invalid. Decimal
values are strings, finite exact base-10, with nonnegative quantities and
positive prices/increments. Timestamp zero is legitimate input, never a fabricated
metadata effective start. Receive time remains independently observed and is not
treated as pure network latency relative to E.

Ordered depth identity labels: venue, product_family, symbol, event_class,
session_id, U, u; values BINANCE, USD_M_FUTURES, BTCUSDT, DEPTH_UPDATE, original
16 UUID bytes, and decimal ASCII U/u. pu/times/levels/quantities/raw hash are
excluded. Same U/u across sessions has distinct IDs; cross-session deduplication
is not promised. Replay accepts RawCaptureEvent and payload, and obtains the
original session from lineage; it cannot take a replacement session argument.

Duplicate detection compares known normalized content: U/u/pu, E/T, and level
replacements. It excludes capture/hash/receive data and unknown wire fields.
Optional migration fields are equivalent after profile validation. Levels are
compared after final replacement per price, sorted by their normalized key.
Equivalent decimal spellings compare equally. Same session/U/u with incompatible
content raises VENUE_IDENTITY_CONFLICT, distinct from VENUE_GAP. No arbitrary
winner or second valid logical identity is manufactured.

Unknown additive JSON fields are accepted and returned as sorted drift paths;
exchangeInfo reports target-symbol, root and filter drift. Required missing or
invalid fields fail. Duplicate JSON keys and non-finite JSON fail before mapping.
No unknown field is silently assigned semantics. Raw bodies are never rewritten.

## Offline book synchronization

BookSync is a finite in-memory contract proof for one original session. It is
not a collector, Book Service, publisher or unbounded production buffering policy.
The caller supplies capture-ordered events and explicit snapshot actions.

Transitions: EMPTY -> BUFFERING -> SNAPSHOT_REQUIRED -> BRIDGING -> HEALTHY.
At S=snapshot.lastUpdateId, discard u<S, never u<=S. The first remaining event
must satisfy U<=S<=u; do not skip a non-stale non-bridging event to cherry-pick a
later bridge. If absent, remain BRIDGING without trusted output; an explicit
replacement snapshot is allowed. No recovery scheduler is implemented.

Initialize from the snapshot and apply the bridge. Subsequently require
current.pu=previous.u. Quantities replace absolute quantities; zero deletes even
when a price does not exist. Repeated prices within a side use last listed
replacement. Trusted views sort bids numerically descending, asks ascending.
Duplicates do not reapply or advance the chain. Owned copies protect buffered
data and output views from caller mutation.

pu mismatch -> VENUE_GAP; identity conflict -> VENUE_IDENTITY_CONFLICT. Both record
DEGRADED then RESYNC_REQUIRED. Invalid events or another original session also
invalidate trust. A failed instance is terminal; a future collector must create
a new synchronization attempt. trusted_book fails unless HEALTHY. The visible
book excludes RPI and may be limited by snapshot depth; it is not complete
exchange liquidity. No power-loss or production performance claim is made.

## Tests, reproduction and compatibility

Use the Foundation pinned toolchain and hash-locked dependencies. Run:

```sh
python3 -m pip install --require-hashes -r python/requirements-dev.lock
python3 tools/generate_protobuf.py
python3 tools/generate_protobuf.py --check
sha256sum --check FILES.sha256
python3 tools/readiness.py
python3 tools/checks.py
git status --porcelain
```

`tools/checks.py` executes Rust fmt/clippy/tests, Python unittest/bindings/Ruff/mypy,
drift and security gates. The unchanged CI workflow calls the same gates.
Clean-checkout evidence is delivered separately; external CI is not claimed.
Fixture authoring is manual/stdlib-only and never imported into test execution.
See `tests/fixtures/binance-usdm/README.md`. No observed live capture is claimed.

Foundation source and tests are retained. CauseCode fixtures append tags 11–14
without changing entries 0–10; existing exhaustive enum tests still execute.
The existing generated-stub-only mypy exception covers new protoc modules with
the same bare Mapping issue; handwritten source remains strict, without Any or
type-ignore escapes. Cargo/Python dependencies and locks are unchanged.
