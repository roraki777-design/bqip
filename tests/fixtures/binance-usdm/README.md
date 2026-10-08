# Normative synthetic Brief 002 vectors

All messages here are manually designed contract examples, not observed live
Binance responses. Tiny prices, times and increments are test values. They are
never defaults in the profile. Source authority: docs/briefs and the official
documentation assertions in docs/venues/binance-usdm/sources-v1.json.

53 parser vectors cover ExchangeInfo, AggTrade, diff depth and REST snapshots.
16 temporal vectors cover validation, half-open knowledge intervals, uncertainty,
confirmation and ambiguity. Six bridge vectors pin strict u<S and U<=S<=u.
The state sequence includes stale discard, bridge, absolute update, zero/absent
deletion and a pu gap with trusted-output invalidation.

Identity vectors independently frame BQIP-ID-V1 and SHA-256. Four Protobuf vectors
are encoded from explicit field tags using the Protobuf wire specification in
the stdlib-only authoring script, without BQIP or generated binding imports.
Rust and Python independently parse the original bytes and match these external
wire bytes and hashes. Tests never create expectations from implementation output.
The profile vector independently specifies ASCII-key JCS bytes; Foundation's
full restricted-JCS conformance suite remains mandatory.

`tools/author_brief002_fixtures.py` records reproducible authorship. It is not a
normal build step. Changing a normative fixture requires review; do not regenerate
it to bless an implementation failure. Additional property, missing-field,
source/hash, identity conflict, mutation-isolation, wire-presence and BQRC replay
tests exercise invariants beyond the compact matrix. All market tests are offline.
