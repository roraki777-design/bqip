"""Semantic checks at the generated Protobuf boundary."""

from google.protobuf.message import Message

from bqip.contracts.protobuf import decimal_from_proto
from bqip.contracts.source import http_from_proto, validate_stream
from bqip.contracts.values import ContractError
from bqip.v1 import market_pb2 as m
from bqip.venues.binance import compare, depth_id, ms_to_ns, trade_id
from bqip.venues.profile import (
    DEPTH_ENDPOINT,
    DEPTH_STREAM,
    INSTRUMENT_ID,
    TRADE_ENDPOINT,
    TRADE_STREAM,
)


def required(message: Message, *fields: str) -> None:
    if any(not message.HasField(f) for f in fields):
        raise ContractError("MISSING_MARKET_FIELD")


def validate_level(level: m.PriceLevel) -> None:
    required(level, "price", "quantity")
    price, quantity = decimal_from_proto(level.price), decimal_from_proto(level.quantity)
    if price.coefficient.startswith("-") or price.coefficient == "0" or quantity.coefficient.startswith("-"):
        raise ContractError("INVALID_VENUE_DECIMAL")


def validate_trade(v: m.TradeEvent) -> None:
    required(v, "instrument_id", "venue_event_id", "logical_event_id", "price", "quantity",
             "event_timestamp_ns", "trade_timestamp_ns", "native_event_time_ms", "native_trade_time_ms",
             "aggressor", "granularity", "aggregate_id", "first_venue_trade_id", "last_venue_trade_id",
             "quantity_ex_rpi", "buyer_is_maker", "liquidation", "source")
    validate_stream(v.source, TRADE_ENDPOINT, TRADE_STREAM)
    for value in (v.price, v.quantity, v.quantity_ex_rpi):
        decimal_from_proto(value)
        if value.coefficient.startswith("-"):
            raise ContractError("INVALID_VENUE_DECIMAL")
    if v.price.coefficient == "0" or v.quantity.coefficient == "0":
        raise ContractError("INVALID_VENUE_DECIMAL")
    if compare(v.quantity_ex_rpi, v.quantity) > 0:
        raise ContractError("INVALID_RPI_QUANTITY")
    if v.first_venue_trade_id > v.last_venue_trade_id:
        raise ContractError("INVALID_TRADE_RANGE")
    if (v.instrument_id != INSTRUMENT_ID or v.venue_event_id != str(v.aggregate_id)
            or v.logical_event_id != trade_id(v.aggregate_id)):
        raise ContractError("INVALID_MARKET_IDENTITY")
    if (v.event_timestamp_ns != ms_to_ns(v.native_event_time_ms)
            or v.trade_timestamp_ns != ms_to_ns(v.native_trade_time_ms)):
        raise ContractError("TIMESTAMP_MISMATCH")
    if (v.granularity != m.TRADE_GRANULARITY_AGGREGATED
            or v.aggressor != (m.AGGRESSOR_SIDE_SELL if v.buyer_is_maker else m.AGGRESSOR_SIDE_BUY)
            or v.liquidation != m.LIQUIDATION_OBSERVATION_NOT_OBSERVED_FROM_THIS_SOURCE):
        raise ContractError("INVALID_TRADE_SEMANTICS")


def validate_depth(v: m.BinanceDepthUpdate) -> None:
    required(v, "first_update_id", "final_update_id", "previous_final_update_id",
             "native_event_time_ms", "native_transaction_time_ms", "event_timestamp_ns",
             "transaction_timestamp_ns", "logical_event_id", "source")
    validate_stream(v.source, DEPTH_ENDPOINT, DEPTH_STREAM)
    if v.first_update_id > v.final_update_id:
        raise ContractError("INVALID_DEPTH_RANGE")
    if v.logical_event_id != depth_id(v.source.original_session_id, v.first_update_id, v.final_update_id):
        raise ContractError("INVALID_MARKET_IDENTITY")
    if (v.event_timestamp_ns != ms_to_ns(v.native_event_time_ms)
            or v.transaction_timestamp_ns != ms_to_ns(v.native_transaction_time_ms)):
        raise ContractError("TIMESTAMP_MISMATCH")
    for level in (*v.bids, *v.asks):
        validate_level(level)


def validate_snapshot(v: m.BinanceDepthSnapshot) -> None:
    required(v, "last_update_id", "native_event_time_ms", "native_transaction_time_ms",
             "event_timestamp_ns", "transaction_timestamp_ns", "source")
    source = http_from_proto(v.source)
    if source.endpoint != "/fapi/v1/depth":
        raise ContractError("PROFILE_MISMATCH")
    if (v.event_timestamp_ns != ms_to_ns(v.native_event_time_ms)
            or v.transaction_timestamp_ns != ms_to_ns(v.native_transaction_time_ms)):
        raise ContractError("TIMESTAMP_MISMATCH")
    for level in (*v.bids, *v.asks):
        validate_level(level)
