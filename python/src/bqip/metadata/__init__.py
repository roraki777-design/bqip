"""Immutable bitemporal catalog. Explicit supersession resolves revisions only."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from bqip.contracts.values import ContractError, DecimalValue, timestamp_ns, validate_text


class ResolverMode(Enum):
    CORRECTED_RESEARCH = "CORRECTED_RESEARCH"
    AS_LIVED = "AS_LIVED"


@dataclass(frozen=True)
class InstrumentMetadataVersion:
    instrument_id: str
    metadata_version: str
    effective_from_ns: int
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
        for lower, upper in ((self.effective_from_ns, self.effective_to_ns),
                             (self.known_from_ns, self.known_to_ns)):
            timestamp_ns(lower)
            if upper is not None:
                timestamp_ns(upper)
                if upper <= lower:
                    raise ContractError("INVALID_METADATA_INTERVAL")


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

    def resolve(self, instrument_id: str, event_time_ns: int, mode: ResolverMode,
                as_of_ns: int | None = None) -> InstrumentMetadataVersion:
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
            raise ContractError("METADATA_MISSING")
        if len(winners) != 1:
            raise ContractError("METADATA_AMBIGUOUS")
        return winners[0]
