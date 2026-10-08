from bqip.v1 import common_pb2 as _common_pb2
from bqip.v1 import source_pb2 as _source_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EffectiveTimeBasis(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EFFECTIVE_TIME_BASIS_UNSPECIFIED: _ClassVar[EffectiveTimeBasis]
    EFFECTIVE_TIME_BASIS_VENUE_DECLARED: _ClassVar[EffectiveTimeBasis]
    EFFECTIVE_TIME_BASIS_BQIP_CONFIRMED: _ClassVar[EffectiveTimeBasis]
    EFFECTIVE_TIME_BASIS_UNKNOWN: _ClassVar[EffectiveTimeBasis]

class MetadataResolutionBasis(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    METADATA_RESOLUTION_BASIS_UNSPECIFIED: _ClassVar[MetadataResolutionBasis]
    METADATA_RESOLUTION_BASIS_EFFECTIVE_INTERVAL: _ClassVar[MetadataResolutionBasis]
    METADATA_RESOLUTION_BASIS_KNOWLEDGE_ONLY: _ClassVar[MetadataResolutionBasis]
EFFECTIVE_TIME_BASIS_UNSPECIFIED: EffectiveTimeBasis
EFFECTIVE_TIME_BASIS_VENUE_DECLARED: EffectiveTimeBasis
EFFECTIVE_TIME_BASIS_BQIP_CONFIRMED: EffectiveTimeBasis
EFFECTIVE_TIME_BASIS_UNKNOWN: EffectiveTimeBasis
METADATA_RESOLUTION_BASIS_UNSPECIFIED: MetadataResolutionBasis
METADATA_RESOLUTION_BASIS_EFFECTIVE_INTERVAL: MetadataResolutionBasis
METADATA_RESOLUTION_BASIS_KNOWLEDGE_ONLY: MetadataResolutionBasis

class InstrumentMetadataVersion(_message.Message):
    __slots__ = ("instrument_id", "metadata_version", "effective_from_ns", "effective_to_ns", "known_from_ns", "known_to_ns", "supersedes_version", "contract_multiplier", "quantity_unit", "tick_size", "lot_size", "product_type", "margin_asset", "settlement_asset", "effective_time_basis", "venue", "product_family", "symbol", "pair", "contract_type", "base_asset", "quote_asset", "venue_status", "source", "minimum_quantity", "maximum_quantity")
    INSTRUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_VERSION_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_FROM_NS_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_TO_NS_FIELD_NUMBER: _ClassVar[int]
    KNOWN_FROM_NS_FIELD_NUMBER: _ClassVar[int]
    KNOWN_TO_NS_FIELD_NUMBER: _ClassVar[int]
    SUPERSEDES_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_UNIT_FIELD_NUMBER: _ClassVar[int]
    TICK_SIZE_FIELD_NUMBER: _ClassVar[int]
    LOT_SIZE_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_TYPE_FIELD_NUMBER: _ClassVar[int]
    MARGIN_ASSET_FIELD_NUMBER: _ClassVar[int]
    SETTLEMENT_ASSET_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_TIME_BASIS_FIELD_NUMBER: _ClassVar[int]
    VENUE_FIELD_NUMBER: _ClassVar[int]
    PRODUCT_FAMILY_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    PAIR_FIELD_NUMBER: _ClassVar[int]
    CONTRACT_TYPE_FIELD_NUMBER: _ClassVar[int]
    BASE_ASSET_FIELD_NUMBER: _ClassVar[int]
    QUOTE_ASSET_FIELD_NUMBER: _ClassVar[int]
    VENUE_STATUS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    MINIMUM_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    MAXIMUM_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    instrument_id: str
    metadata_version: str
    effective_from_ns: int
    effective_to_ns: int
    known_from_ns: int
    known_to_ns: int
    supersedes_version: str
    contract_multiplier: _common_pb2.DecimalValue
    quantity_unit: str
    tick_size: _common_pb2.DecimalValue
    lot_size: _common_pb2.DecimalValue
    product_type: str
    margin_asset: str
    settlement_asset: str
    effective_time_basis: EffectiveTimeBasis
    venue: str
    product_family: str
    symbol: str
    pair: str
    contract_type: str
    base_asset: str
    quote_asset: str
    venue_status: str
    source: _source_pb2.HttpSourceContext
    minimum_quantity: _common_pb2.DecimalValue
    maximum_quantity: _common_pb2.DecimalValue
    def __init__(self, instrument_id: _Optional[str] = ..., metadata_version: _Optional[str] = ..., effective_from_ns: _Optional[int] = ..., effective_to_ns: _Optional[int] = ..., known_from_ns: _Optional[int] = ..., known_to_ns: _Optional[int] = ..., supersedes_version: _Optional[str] = ..., contract_multiplier: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., quantity_unit: _Optional[str] = ..., tick_size: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., lot_size: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., product_type: _Optional[str] = ..., margin_asset: _Optional[str] = ..., settlement_asset: _Optional[str] = ..., effective_time_basis: _Optional[_Union[EffectiveTimeBasis, str]] = ..., venue: _Optional[str] = ..., product_family: _Optional[str] = ..., symbol: _Optional[str] = ..., pair: _Optional[str] = ..., contract_type: _Optional[str] = ..., base_asset: _Optional[str] = ..., quote_asset: _Optional[str] = ..., venue_status: _Optional[str] = ..., source: _Optional[_Union[_source_pb2.HttpSourceContext, _Mapping]] = ..., minimum_quantity: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., maximum_quantity: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ...) -> None: ...
