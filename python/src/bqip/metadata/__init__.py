"""Immutable bitemporal catalog. Explicit supersession resolves revisions only."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntEnum

from bqip.contracts.source import HttpSource
from bqip.contracts.values import ContractError, DecimalValue, timestamp_ns, validate_text


class EffectiveTimeBasis(IntEnum):
    UNSPECIFIED = 0
    VENUE_DECLARED = 1
    BQIP_CONFIRMED = 2
    UNKNOWN = 3


class MetadataResolutionBasis(IntEnum):
    UNSPECIFIED = 0
    EFFECTIVE_INTERVAL = 1
    KNOWLEDGE_ONLY = 2


class ResolverMode(Enum):
    CORRECTED_RESEARCH = "CORRECTED_RESEARCH"
    AS_LIVED = "AS_LIVED"


@dataclass(frozen=True)
class InstrumentMetadataVersion:
    instrument_id: str
    metadata_version: str
    effective_from_ns: int | None
    known_from_ns: int
    product_type: str
    margin_asset: str
    settlement_asset: str
    effective_to_ns: int | None = None
    known_to_ns: int | None = None
    supersedes_version: str | None = None
    contract_multiplier: DecimalValue | None = None
    quantity_unit: str | None = None
    tick_size: DecimalValue | None = None
    lot_size: DecimalValue | None = None

    effective_time_basis: EffectiveTimeBasis | None = None
    venue: str | None = None
    product_family: str | None = None
    symbol: str | None = None
    pair: str | None = None
    contract_type: str | None = None
    base_asset: str | None = None
    quote_asset: str | None = None
    venue_status: str | None = None
    source: HttpSource | None = None
    minimum_quantity: DecimalValue | None = None
    maximum_quantity: DecimalValue | None = None

    def __post_init__(self) -> None:
        for value in (self.instrument_id, self.metadata_version, self.product_type,
                      self.margin_asset, self.settlement_asset):
            validate_text(value, allow_empty=False)
        for optional_text in (self.quantity_unit, self.supersedes_version):
            if optional_text is not None:
                validate_text(optional_text)
        for decimal in (self.contract_multiplier, self.tick_size, self.lot_size):
            if decimal is not None and not isinstance(decimal, DecimalValue):
                raise ContractError("DECIMAL_VALUE_REQUIRED")
        timestamp_ns(self.known_from_ns)
        basis = self.effective_time_basis
        if basis is not None and (not isinstance(basis, EffectiveTimeBasis) or basis == EffectiveTimeBasis.UNSPECIFIED):
            raise ContractError("INVALID_EFFECTIVE_TIME_BASIS")
        if basis == EffectiveTimeBasis.UNKNOWN:
            if self.effective_from_ns is not None:
                raise ContractError("UNKNOWN_EFFECTIVE_START_MUST_BE_ABSENT")
        elif self.effective_from_ns is None:
            raise ContractError("MISSING_METADATA_TIME")
        extended = (self.venue, self.product_family, self.symbol, self.pair, self.contract_type,
                    self.base_asset, self.quote_asset, self.venue_status, self.source,
                    self.minimum_quantity, self.maximum_quantity)
        if basis is None and any(v is not None for v in extended):
            raise ContractError("MISSING_EFFECTIVE_TIME_BASIS")
        if self.instrument_id == "BINANCE:USD_M_FUTURES:BTCUSDT" or any(v is not None for v in extended):
            expected = ("BINANCE", "USD_M_FUTURES", "BTCUSDT", "BTCUSDT", "PERPETUAL", "BTC", "USDT")
            if self.instrument_id != "BINANCE:USD_M_FUTURES:BTCUSDT" or extended[:7] != expected or self.product_type != "LINEAR" or self.margin_asset != "USDT" or self.settlement_asset != "USDT":
                raise ContractError("PROFILE_MISMATCH")
            if not self.venue_status or self.source is None or self.tick_size is None or self.lot_size is None:
                raise ContractError("METADATA_INCOMPLETE")
            if self.source.endpoint != "/fapi/v1/exchangeInfo" or self.known_from_ns < self.source.receive_timestamp_ns:
                raise ContractError("INVALID_METADATA_SOURCE")
            if basis == EffectiveTimeBasis.UNKNOWN and self.known_from_ns != self.source.receive_timestamp_ns:
                raise ContractError("INVALID_METADATA_SOURCE")
        for bound in (self.minimum_quantity, self.maximum_quantity):
            if bound is not None and not isinstance(bound, DecimalValue):
                raise ContractError("DECIMAL_VALUE_REQUIRED")
        if self.source is not None:
            for increment in (self.tick_size, self.lot_size, self.maximum_quantity):
                if increment is not None and (increment.coefficient.startswith("-") or increment.coefficient == "0"):
                    raise ContractError("INVALID_VENUE_DECIMAL")
            if self.minimum_quantity is not None and self.minimum_quantity.coefficient.startswith("-"):
                raise ContractError("INVALID_VENUE_DECIMAL")
            if self.minimum_quantity is not None and self.maximum_quantity is not None:
                a, b = self.minimum_quantity, self.maximum_quantity
                width = max(len(a.coefficient), len(b.coefficient))
                left = (len(a.coefficient)-a.scale, a.coefficient.ljust(width, "0"))
                right = (len(b.coefficient)-b.scale, b.coefficient.ljust(width, "0"))
                if a.coefficient != "0" and left > right:
                    raise ContractError("METADATA_INCOMPLETE")
        for lower, upper in ((self.effective_from_ns, self.effective_to_ns),
                             (self.known_from_ns, self.known_to_ns)):
            if lower is not None:
                timestamp_ns(lower)
            if upper is not None:
                timestamp_ns(upper)
                if lower is not None and upper <= lower:
                    raise ContractError("INVALID_METADATA_INTERVAL")


@dataclass(frozen=True)
class MetadataResolution:
    version: InstrumentMetadataVersion
    resolution_basis: MetadataResolutionBasis
    quality: str | None


@dataclass(frozen=True)
class MetadataCatalog:
    versions: tuple[InstrumentMetadataVersion, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.versions, tuple):
            raise ContractError("IMMUTABLE_VERSION_TUPLE_REQUIRED")
        indexed = {(v.instrument_id, v.metadata_version): v for v in self.versions}
        if len(indexed) != len(self.versions):
            raise ContractError("DUPLICATE_METADATA_VERSION")
        for version in self.versions:
            if version.supersedes_version is not None:
                older = indexed.get((version.instrument_id, version.supersedes_version))
                if older is None or older.known_from_ns >= version.known_from_ns:
                    raise ContractError("INVALID_SUPERSESSION")

    def append(self, version: InstrumentMetadataVersion) -> MetadataCatalog:
        return MetadataCatalog((*self.versions, version))

    def resolve_with_basis(self, instrument_id: str, event_time_ns: int, mode: ResolverMode,
                as_of_ns: int | None = None) -> MetadataResolution:
        timestamp_ns(event_time_ns)
        if mode not in (ResolverMode.AS_LIVED, ResolverMode.CORRECTED_RESEARCH):
            raise ContractError("INVALID_RESOLVER_MODE")
        if mode == ResolverMode.AS_LIVED and as_of_ns is None:
            raise ContractError("AS_OF_REQUIRED")
        if as_of_ns is not None:
            timestamp_ns(as_of_ns)
        applicable = []
        for version in self.versions:
            if version.instrument_id != instrument_id:
                continue
            if version.effective_time_basis == EffectiveTimeBasis.UNKNOWN:
                if mode == ResolverMode.CORRECTED_RESEARCH:
                    continue
            else:
                assert version.effective_from_ns is not None
                if not version.effective_from_ns <= event_time_ns:
                    continue
                if version.effective_to_ns is not None and event_time_ns >= version.effective_to_ns:
                    continue
            if mode == ResolverMode.AS_LIVED:
                assert as_of_ns is not None
                if as_of_ns < version.known_from_ns:
                    continue
                if version.known_to_ns is not None and as_of_ns >= version.known_to_ns:
                    continue
            applicable.append(version)
        indexed = {(v.instrument_id, v.metadata_version): v for v in self.versions}
        superseded: set[str] = set()
        for version in applicable:
            parent = version.supersedes_version
            while parent is not None:
                superseded.add(parent)
                parent = indexed[(instrument_id, parent)].supersedes_version
        winners = [v for v in applicable if v.metadata_version not in superseded]
        if not winners:
            if mode == ResolverMode.CORRECTED_RESEARCH and any(
                v.instrument_id == instrument_id and v.effective_time_basis == EffectiveTimeBasis.UNKNOWN
                for v in self.versions
            ):
                raise ContractError("METADATA_EFFECTIVE_TIME_UNKNOWN")
            raise ContractError("METADATA_MISSING")
        if len(winners) != 1:
            raise ContractError("METADATA_AMBIGUOUS")
        winner = winners[0]
        unknown = winner.effective_time_basis == EffectiveTimeBasis.UNKNOWN
        return MetadataResolution(winner, MetadataResolutionBasis.KNOWLEDGE_ONLY if unknown
                                  else MetadataResolutionBasis.EFFECTIVE_INTERVAL,
                                  "METADATA_EFFECTIVE_TIME_UNKNOWN" if unknown else None)

    def resolve(self, instrument_id: str, event_time_ns: int, mode: ResolverMode,
                as_of_ns: int | None = None) -> InstrumentMetadataVersion:
        result = self.resolve_with_basis(instrument_id, event_time_ns, mode, as_of_ns)
        if result.resolution_basis == MetadataResolutionBasis.KNOWLEDGE_ONLY:
            raise ContractError("LABELED_METADATA_RESOLUTION_REQUIRED")
        return result.version
