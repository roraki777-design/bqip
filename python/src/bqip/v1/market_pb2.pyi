from bqip.v1 import common_pb2 as _common_pb2
from bqip.v1 import source_pb2 as _source_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TradeGranularity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TRADE_GRANULARITY_UNKNOWN: _ClassVar[TradeGranularity]
    TRADE_GRANULARITY_INDIVIDUAL: _ClassVar[TradeGranularity]
    TRADE_GRANULARITY_AGGREGATED: _ClassVar[TradeGranularity]

class AggressorSide(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    AGGRESSOR_SIDE_UNKNOWN: _ClassVar[AggressorSide]
    AGGRESSOR_SIDE_BUY: _ClassVar[AggressorSide]
    AGGRESSOR_SIDE_SELL: _ClassVar[AggressorSide]

class LiquidationObservation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LIQUIDATION_OBSERVATION_UNKNOWN: _ClassVar[LiquidationObservation]
    LIQUIDATION_OBSERVATION_NOT_OBSERVED_FROM_THIS_SOURCE: _ClassVar[LiquidationObservation]
TRADE_GRANULARITY_UNKNOWN: TradeGranularity
TRADE_GRANULARITY_INDIVIDUAL: TradeGranularity
TRADE_GRANULARITY_AGGREGATED: TradeGranularity
AGGRESSOR_SIDE_UNKNOWN: AggressorSide
AGGRESSOR_SIDE_BUY: AggressorSide
AGGRESSOR_SIDE_SELL: AggressorSide
LIQUIDATION_OBSERVATION_UNKNOWN: LiquidationObservation
LIQUIDATION_OBSERVATION_NOT_OBSERVED_FROM_THIS_SOURCE: LiquidationObservation

class TradeEvent(_message.Message):
    __slots__ = ("instrument_id", "venue_event_id", "logical_event_id", "price", "quantity", "event_timestamp_ns", "trade_timestamp_ns", "native_event_time_ms", "native_trade_time_ms", "aggressor", "granularity", "aggregate_id", "first_venue_trade_id", "last_venue_trade_id", "quantity_ex_rpi", "buyer_is_maker", "liquidation", "source")
    INSTRUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    VENUE_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    TRADE_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    NATIVE_EVENT_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    NATIVE_TRADE_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    AGGRESSOR_FIELD_NUMBER: _ClassVar[int]
    GRANULARITY_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_ID_FIELD_NUMBER: _ClassVar[int]
    FIRST_VENUE_TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_VENUE_TRADE_ID_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_EX_RPI_FIELD_NUMBER: _ClassVar[int]
    BUYER_IS_MAKER_FIELD_NUMBER: _ClassVar[int]
    LIQUIDATION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    instrument_id: str
    venue_event_id: str
    logical_event_id: bytes
    price: _common_pb2.DecimalValue
    quantity: _common_pb2.DecimalValue
    event_timestamp_ns: int
    trade_timestamp_ns: int
    native_event_time_ms: int
    native_trade_time_ms: int
    aggressor: AggressorSide
    granularity: TradeGranularity
    aggregate_id: int
    first_venue_trade_id: int
    last_venue_trade_id: int
    quantity_ex_rpi: _common_pb2.DecimalValue
    buyer_is_maker: bool
    liquidation: LiquidationObservation
    source: _source_pb2.StreamSourceContext
    def __init__(self, instrument_id: _Optional[str] = ..., venue_event_id: _Optional[str] = ..., logical_event_id: _Optional[bytes] = ..., price: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., quantity: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., event_timestamp_ns: _Optional[int] = ..., trade_timestamp_ns: _Optional[int] = ..., native_event_time_ms: _Optional[int] = ..., native_trade_time_ms: _Optional[int] = ..., aggressor: _Optional[_Union[AggressorSide, str]] = ..., granularity: _Optional[_Union[TradeGranularity, str]] = ..., aggregate_id: _Optional[int] = ..., first_venue_trade_id: _Optional[int] = ..., last_venue_trade_id: _Optional[int] = ..., quantity_ex_rpi: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., buyer_is_maker: _Optional[bool] = ..., liquidation: _Optional[_Union[LiquidationObservation, str]] = ..., source: _Optional[_Union[_source_pb2.StreamSourceContext, _Mapping]] = ...) -> None: ...

class PriceLevel(_message.Message):
    __slots__ = ("price", "quantity")
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    price: _common_pb2.DecimalValue
    quantity: _common_pb2.DecimalValue
    def __init__(self, price: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., quantity: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ...) -> None: ...

class BinanceDepthUpdate(_message.Message):
    __slots__ = ("first_update_id", "final_update_id", "previous_final_update_id", "native_event_time_ms", "native_transaction_time_ms", "event_timestamp_ns", "transaction_timestamp_ns", "bids", "asks", "logical_event_id", "source")
    FIRST_UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    FINAL_UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_FINAL_UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    NATIVE_EVENT_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    NATIVE_TRANSACTION_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    BIDS_FIELD_NUMBER: _ClassVar[int]
    ASKS_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    native_event_time_ms: int
    native_transaction_time_ms: int
    event_timestamp_ns: int
    transaction_timestamp_ns: int
    bids: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    asks: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    logical_event_id: bytes
    source: _source_pb2.StreamSourceContext
    def __init__(self, first_update_id: _Optional[int] = ..., final_update_id: _Optional[int] = ..., previous_final_update_id: _Optional[int] = ..., native_event_time_ms: _Optional[int] = ..., native_transaction_time_ms: _Optional[int] = ..., event_timestamp_ns: _Optional[int] = ..., transaction_timestamp_ns: _Optional[int] = ..., bids: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., asks: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., logical_event_id: _Optional[bytes] = ..., source: _Optional[_Union[_source_pb2.StreamSourceContext, _Mapping]] = ...) -> None: ...

class BinanceDepthSnapshot(_message.Message):
    __slots__ = ("last_update_id", "native_event_time_ms", "native_transaction_time_ms", "event_timestamp_ns", "transaction_timestamp_ns", "bids", "asks", "source")
    LAST_UPDATE_ID_FIELD_NUMBER: _ClassVar[int]
    NATIVE_EVENT_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    NATIVE_TRANSACTION_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_TIMESTAMP_NS_FIELD_NUMBER: _ClassVar[int]
    BIDS_FIELD_NUMBER: _ClassVar[int]
    ASKS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    last_update_id: int
    native_event_time_ms: int
    native_transaction_time_ms: int
    event_timestamp_ns: int
    transaction_timestamp_ns: int
    bids: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    asks: _containers.RepeatedCompositeFieldContainer[PriceLevel]
    source: _source_pb2.HttpSourceContext
    def __init__(self, last_update_id: _Optional[int] = ..., native_event_time_ms: _Optional[int] = ..., native_transaction_time_ms: _Optional[int] = ..., event_timestamp_ns: _Optional[int] = ..., transaction_timestamp_ns: _Optional[int] = ..., bids: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., asks: _Optional[_Iterable[_Union[PriceLevel, _Mapping]]] = ..., source: _Optional[_Union[_source_pb2.HttpSourceContext, _Mapping]] = ...) -> None: ...
