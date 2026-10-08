# ADR-015 — Canonical trade source granularity

Status: PROPOSED / IMPLEMENTED FOR ARCHITECT REVIEW

## Context and authority

Brief 002 authorizes the minimal additive TradeEvent contract. Foundation has no
TradeEvent IDL. Binance aggTrade supplies aggregates, not individual fills. Its
member ID range cannot establish individual quantities or timestamps. Treating
each aggregate as a fill would corrupt research and any later derived data.

## Decision

`market.proto` defines TradeGranularity: UNKNOWN=0, INDIVIDUAL=1, AGGREGATED=2.
The Binance parser always emits one AGGREGATED TradeEvent, including when f=l.
No child fills or child identities are generated. INDIVIDUAL is a vocabulary
entry, not an implemented individual-trade source.

The canonical event carries instrument ID, decimal ASCII venue_event_id=a,
logical ID, exact price=p, quantity=q, native E/T milliseconds and checked
canonical nanoseconds, buyer_is_maker, aggressor, granularity, optional aggregate
ID and first/last member IDs, quantity_ex_rpi, liquidation observation, and typed
original stream evidence. The aggTrade validator requires all of these fields.
Prices and quantities use Foundation DecimalValue, never floating point.

q includes RPI contribution, nq excludes it, and 0 <= nq <= q. This brief does
not emit a derived RPI field or claim which members were RPI. A consumer may
later derive q-nq with explicit derivation provenance. f must be <= l.

m=true means SELL aggressor; false means BUY. Liquidation is always
NOT_OBSERVED_FROM_THIS_SOURCE. It is not inferred from price, quantity, timing
or aggregate membership. Liquidation collection is deferred.

Logical identity is Foundation BQIP-ID-V1 with ordered labels/values:
venue/BINANCE, product_family/USD_M_FUTURES, symbol/BTCUSDT,
event_class/AGG_TRADE, a/decimal-ASCII-ID. Session and capture provenance do not
enter trade logical identity. st, when supplied, must be integer 1.

SourceLineage binds the event to the frozen profile ID/hash and original raw
capture ID/hash/receive time. StreamSourceContext preserves the original session,
MARKET endpoint and channel separately from logical identity. No networking,
processing-time substitution, future execution fields or dependencies are added.

## Verification and limits

Committed synthetic semantic, identity and independent Protobuf wire vectors
are consumed separately by Rust and Python. Existing enum tags are preserved.
The documentation source manifest records the official sources and the frozen
2026-10-03 retrieval date. Passing tests does not accept this ADR.
