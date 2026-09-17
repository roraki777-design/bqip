from bqip.v1 import common_pb2 as _common_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InstrumentMetadataVersion(_message.Message):
    __slots__ = ("instrument_id", "metadata_version", "effective_from_ns", "effective_to_ns", "known_from_ns", "known_to_ns", "supersedes_version", "contract_multiplier", "quantity_unit", "tick_size", "lot_size", "product_type", "margin_asset", "settlement_asset")
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
    def __init__(self, instrument_id: _Optional[str] = ..., metadata_version: _Optional[str] = ..., effective_from_ns: _Optional[int] = ..., effective_to_ns: _Optional[int] = ..., known_from_ns: _Optional[int] = ..., known_to_ns: _Optional[int] = ..., supersedes_version: _Optional[str] = ..., contract_multiplier: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., quantity_unit: _Optional[str] = ..., tick_size: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., lot_size: _Optional[_Union[_common_pb2.DecimalValue, _Mapping]] = ..., product_type: _Optional[str] = ..., margin_asset: _Optional[str] = ..., settlement_asset: _Optional[str] = ...) -> None: ...
