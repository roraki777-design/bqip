"""Offline Binance USD-M parsing. Each parser binds untouched bytes to observed lineage."""

import json
import math

from bqip.contracts.protobuf import metadata_from_proto
from bqip.contracts.source import http_from_proto, validate_stream
from bqip.contracts.values import I64_MAX, U64_MAX, ContractError, DecimalValue
from bqip.identity import logical_hash
from bqip.v1 import common_pb2 as c
from bqip.v1 import market_pb2 as m
from bqip.v1 import metadata_pb2 as md
from bqip.v1 import source_pb2 as s
from bqip.venues.profile import (
    DEPTH_ENDPOINT,
    DEPTH_STREAM,
    INSTRUMENT_ID,
    TRADE_ENDPOINT,
    TRADE_STREAM,
)


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def document(payload: bytes) -> dict[str, object]:
    def integer(token: str) -> int | float:
        # Match serde_json: preserve negative zero as a non-integer. uint()
        # rejects it in semantic fields; unknown finite fields remain drift.
        return -0.0 if token == "-0" else int(token)
    try:
        value: object = json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs, parse_int=integer)
        def check(item: object) -> None:
            if isinstance(item, str):
                item.encode("utf-8", errors="strict")
            elif isinstance(item, float) and not math.isfinite(item):
                raise ContractError("PARSE_FAILURE")
            elif isinstance(item, list):
                for child in item:
                    check(child)
            elif isinstance(item, dict):
                for key, child in item.items():
                    check(key)
                    check(child)
        check(value)
    except (ValueError, UnicodeError) as error:
        raise ContractError("PARSE_FAILURE") from error
    return obj(value)


def obj(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(k, str) for k in value):
        raise ContractError("PARSE_FAILURE")
    return value


def array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ContractError("PARSE_FAILURE")
    return value


def text(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ContractError("PARSE_FAILURE")
    return value


def uint(value: object) -> int:
    if type(value) is not int or not 0 <= value <= U64_MAX:
        raise ContractError("PARSE_FAILURE")
    return value


def ms_to_ns(value: int) -> int:
    value = uint(value)
    if value > I64_MAX // 1_000_000:
        raise ContractError("TIMESTAMP_OVERFLOW")
    return value * 1_000_000


def decimal(value: object, *, positive: bool = False) -> c.DecimalValue:
    try:
        parsed = DecimalValue.parse(text(value))
    except ValueError as error:
        raise ContractError("INVALID_VENUE_DECIMAL") from error
    if parsed.coefficient.startswith("-") or (positive and parsed.coefficient == "0"):
        raise ContractError("INVALID_VENUE_DECIMAL")
    return c.DecimalValue(coefficient=parsed.coefficient, scale=parsed.scale)


def compare(left: c.DecimalValue, right: c.DecimalValue) -> int:
    # No float, fixed-width coefficient, or enormous power-of-ten expansion.
    if left.coefficient == "0" or right.coefficient == "0":
        return (left.coefficient != "0") - (right.coefficient != "0")
    a, b = len(left.coefficient) - left.scale, len(right.coefficient) - right.scale
    if a != b:
        return (a > b) - (a < b)
    width = max(len(left.coefficient), len(right.coefficient))
    x, y = left.coefficient.ljust(width, "0"), right.coefficient.ljust(width, "0")
    return (x > y) - (x < y)


def levels(value: object) -> list[m.PriceLevel]:
    result = []
    for row in array(value):
        pair = array(row)
        if len(pair) != 2:
            raise ContractError("INVALID_LEVEL")
        result.append(m.PriceLevel(price=decimal(pair[0], positive=True), quantity=decimal(pair[1])))
    return result


def _migration(value: dict[str, object], symbol_key: str = "s") -> None:
    if value.get(symbol_key) != "BTCUSDT":
        raise ContractError("PROFILE_MISMATCH")
    if "st" in value and uint(value["st"]) != 1:
        raise ContractError("PROFILE_MISMATCH")
    if "ps" in value and value["ps"] != "BTCUSDT":
        raise ContractError("PROFILE_MISMATCH")


def identity_components(event_class: str, ids: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    return [("venue", b"BINANCE"), ("product_family", b"USD_M_FUTURES"),
            ("symbol", b"BTCUSDT"), ("event_class", event_class.encode("ascii")), *ids]


def trade_id(aggregate_id: int) -> bytes:
    return logical_hash(identity_components("AGG_TRADE", [("a", str(uint(aggregate_id)).encode())]))


def depth_id(session: bytes, first: int, final: int) -> bytes:
    if len(session) != 16:
        raise ContractError("INVALID_ORIGINAL_SESSION")
    return logical_hash(identity_components("DEPTH_UPDATE", [("session_id", session),
                        ("U", str(uint(first)).encode()), ("u", str(uint(final)).encode())]))


def parse_trade(payload: bytes, source: s.StreamSourceContext) -> tuple[m.TradeEvent, tuple[str, ...]]:
    validate_stream(source, TRADE_ENDPOINT, TRADE_STREAM, payload)
    v = document(payload)
    _migration(v)
    maker = v.get("m")
    if v.get("e") != "aggTrade" or not isinstance(maker, bool):
        raise ContractError("PARSE_FAILURE")
    a, f, last = uint(v.get("a")), uint(v.get("f")), uint(v.get("l"))
    if f > last:
        raise ContractError("INVALID_TRADE_RANGE")
    event, trade = uint(v.get("E")), uint(v.get("T"))
    q, nq = decimal(v.get("q"), positive=True), decimal(v.get("nq"))
    if compare(nq, q) > 0:
        raise ContractError("INVALID_RPI_QUANTITY")
    result = m.TradeEvent(
        instrument_id=INSTRUMENT_ID, venue_event_id=str(a), logical_event_id=trade_id(a),
        price=decimal(v.get("p"), positive=True), quantity=q, quantity_ex_rpi=nq,
        event_timestamp_ns=ms_to_ns(event), trade_timestamp_ns=ms_to_ns(trade),
        native_event_time_ms=event, native_trade_time_ms=trade,
        aggressor=m.AGGRESSOR_SIDE_SELL if maker else m.AGGRESSOR_SIDE_BUY,
        granularity=m.TRADE_GRANULARITY_AGGREGATED, aggregate_id=a,
        first_venue_trade_id=f, last_venue_trade_id=last, buyer_is_maker=maker,
        liquidation=m.LIQUIDATION_OBSERVATION_NOT_OBSERVED_FROM_THIS_SOURCE, source=source)
    return result, tuple(sorted(v.keys() - {"e", "E", "s", "a", "p", "q", "nq", "f", "l", "T", "m", "st", "ps"}))


def parse_depth(payload: bytes, source: s.StreamSourceContext) -> tuple[m.BinanceDepthUpdate, tuple[str, ...]]:
    validate_stream(source, DEPTH_ENDPOINT, DEPTH_STREAM, payload)
    v = document(payload)
    _migration(v)
    if v.get("e") != "depthUpdate":
        raise ContractError("PARSE_FAILURE")
    first, final, previous = uint(v.get("U")), uint(v.get("u")), uint(v.get("pu"))
    if first > final:
        raise ContractError("INVALID_DEPTH_RANGE")
    event, transaction = uint(v.get("E")), uint(v.get("T"))
    result = m.BinanceDepthUpdate(
        first_update_id=first, final_update_id=final, previous_final_update_id=previous,
        native_event_time_ms=event, native_transaction_time_ms=transaction,
        event_timestamp_ns=ms_to_ns(event), transaction_timestamp_ns=ms_to_ns(transaction),
        bids=levels(v.get("b")), asks=levels(v.get("a")),
        logical_event_id=depth_id(source.original_session_id, first, final), source=source)
    return result, tuple(sorted(v.keys() - {"e", "E", "T", "s", "U", "u", "pu", "b", "a", "ps", "st"}))


def parse_snapshot(payload: bytes, source: s.HttpSourceContext) -> tuple[m.BinanceDepthSnapshot, tuple[str, ...]]:
    http = http_from_proto(source, payload)
    if http.endpoint != "/fapi/v1/depth":
        raise ContractError("PROFILE_MISMATCH")
    v = document(payload)
    event, transaction = uint(v.get("E")), uint(v.get("T"))
    result = m.BinanceDepthSnapshot(
        last_update_id=uint(v.get("lastUpdateId")), native_event_time_ms=event,
        native_transaction_time_ms=transaction, event_timestamp_ns=ms_to_ns(event),
        transaction_timestamp_ns=ms_to_ns(transaction), bids=levels(v.get("bids")),
        asks=levels(v.get("asks")), source=source)
    return result, tuple(sorted(v.keys() - {"lastUpdateId", "E", "T", "bids", "asks"}))


def parse_exchange_info(payload: bytes, source: s.HttpSourceContext, version: str
                        ) -> tuple[md.InstrumentMetadataVersion, tuple[str, ...]]:
    http = http_from_proto(source, payload)
    if http.endpoint != "/fapi/v1/exchangeInfo" or not version:
        raise ContractError("PROFILE_MISMATCH")
    root = document(payload)
    symbols = [obj(record) for record in array(root.get("symbols"))]
    matches = [record for record in symbols if record.get("symbol") == "BTCUSDT"]
    if len(matches) != 1:
        raise ContractError("METADATA_INCOMPLETE")
    v = matches[0]
    for key, expected in {"pair": "BTCUSDT", "contractType": "PERPETUAL", "baseAsset": "BTC",
                          "quoteAsset": "USDT", "marginAsset": "USDT"}.items():
        if v.get(key) != expected:
            raise ContractError("PROFILE_MISMATCH")
    _migration(v, "symbol")
    filters: dict[str, dict[str, object]] = {}
    for item in array(v.get("filters")):
        record = obj(item)
        name = text(record.get("filterType"))
        if name in filters:
            raise ContractError("METADATA_INCOMPLETE")
        filters[name] = record
    if "PRICE_FILTER" not in filters or "LOT_SIZE" not in filters:
        raise ContractError("METADATA_INCOMPLETE")
    price, lot = filters["PRICE_FILTER"], filters["LOT_SIZE"]
    if "tickSize" not in price or any(k not in lot for k in ("stepSize", "minQty", "maxQty")):
        raise ContractError("METADATA_INCOMPLETE")
    minimum, maximum = decimal(lot["minQty"]), decimal(lot["maxQty"], positive=True)
    if compare(minimum, maximum) > 0:
        raise ContractError("METADATA_INCOMPLETE")
    result = md.InstrumentMetadataVersion(
        instrument_id=INSTRUMENT_ID, metadata_version=version,
        known_from_ns=http.receive_timestamp_ns,
        effective_time_basis=md.EFFECTIVE_TIME_BASIS_UNKNOWN,
        product_type="LINEAR", margin_asset="USDT", settlement_asset="USDT", quantity_unit="BTC",
        tick_size=decimal(price["tickSize"], positive=True), lot_size=decimal(lot["stepSize"], positive=True),
        minimum_quantity=minimum, maximum_quantity=maximum,
        venue="BINANCE", product_family="USD_M_FUTURES", symbol="BTCUSDT", pair="BTCUSDT",
        contract_type="PERPETUAL", base_asset="BTC", quote_asset="USDT", venue_status=text(v.get("status")), source=source)
    metadata_from_proto(result)
    known = {"symbol", "pair", "contractType", "baseAsset", "quoteAsset", "marginAsset", "status",
             "pricePrecision", "quantityPrecision", "onboardDate", "filters", "st", "ps"}
    drift = {"symbols.BTCUSDT." + k for k in v.keys() - known}
    drift.update("root." + k for k in root.keys() - {"symbols", "serverTime", "timezone", "rateLimits", "exchangeFilters", "assets"})
    for name, fields in filters.items():
        allowed = {"filterType", "tickSize", "minPrice", "maxPrice"} if name == "PRICE_FILTER" else {"filterType", "stepSize", "minQty", "maxQty"}
        if name not in ("PRICE_FILTER", "LOT_SIZE"):
            drift.add("filter." + name)
        else:
            drift.update("filter." + name + "." + key for key in fields.keys() - allowed)
    return result, tuple(sorted(drift))
