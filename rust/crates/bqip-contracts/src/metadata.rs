use crate::{
    proto::{EffectiveTimeBasis, InstrumentMetadataVersion, MetadataResolutionBasis},
    ContractError, Result,
};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolverMode {
    CorrectedResearch,
    AsLived,
}

pub struct MetadataCatalog {
    versions: Vec<InstrumentMetadataVersion>,
}

fn required(value: &Option<String>) -> Result<&str> {
    value
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or(ContractError("EMPTY_METADATA_IDENTITY"))
}

impl MetadataCatalog {
    pub fn new(versions: Vec<InstrumentMetadataVersion>) -> Result<Self> {
        let mut index = HashMap::new();
        for version in &versions {
            let key = (
                required(&version.instrument_id)?,
                required(&version.metadata_version)?,
            );
            if index.insert(key, version).is_some() {
                return Err(ContractError("DUPLICATE_METADATA_VERSION"));
            }
            version.validate_metadata()?;
        }
        for version in &versions {
            if let Some(parent) = &version.supersedes_version {
                let older = index
                    .get(&(required(&version.instrument_id)?, parent.as_str()))
                    .ok_or(ContractError("INVALID_SUPERSESSION"))?;
                if older.known_from_ns >= version.known_from_ns {
                    return Err(ContractError("INVALID_SUPERSESSION"));
                }
            }
        }
        Ok(Self { versions })
    }

    pub fn append(&self, version: InstrumentMetadataVersion) -> Result<Self> {
        let mut versions = self.versions.clone();
        versions.push(version);
        Self::new(versions)
    }

    pub fn resolve_with_basis(
        &self,
        instrument: &str,
        event_time: i64,
        mode: ResolverMode,
        as_of: Option<i64>,
    ) -> Result<MetadataResolution<'_>> {
        if mode == ResolverMode::AsLived && as_of.is_none() {
            return Err(ContractError("AS_OF_REQUIRED"));
        }
        let candidates: Vec<_> = self
            .versions
            .iter()
            .filter(|v| {
                v.instrument_id.as_deref() == Some(instrument)
                    && (if v.effective_time_basis == Some(EffectiveTimeBasis::Unknown as i32) {
                        mode == ResolverMode::AsLived
                    } else {
                        v.effective_from_ns.is_some_and(|from| from <= event_time)
                            && v.effective_to_ns.is_none_or(|to| event_time < to)
                    })
                    && (mode == ResolverMode::CorrectedResearch
                        || as_of.is_some_and(|time| {
                            v.known_from_ns.is_some_and(|from| from <= time)
                                && v.known_to_ns.is_none_or(|to| time < to)
                        }))
            })
            .collect();
        let index: HashMap<_, _> = self
            .versions
            .iter()
            .filter(|v| v.instrument_id.as_deref() == Some(instrument))
            .map(|v| (v.metadata_version.as_deref().unwrap_or_default(), v))
            .collect();
        let mut superseded = HashSet::new();
        for version in &candidates {
            let mut parent = version.supersedes_version.as_deref();
            while let Some(id) = parent {
                superseded.insert(id);
                parent = index
                    .get(id)
                    .ok_or(ContractError("INVALID_SUPERSESSION"))?
                    .supersedes_version
                    .as_deref();
            }
        }
        let mut winners = candidates
            .into_iter()
            .filter(|v| !superseded.contains(v.metadata_version.as_deref().unwrap_or_default()));
        let missing = if mode == ResolverMode::CorrectedResearch
            && self.versions.iter().any(|v| {
                v.instrument_id.as_deref() == Some(instrument)
                    && v.effective_time_basis == Some(EffectiveTimeBasis::Unknown as i32)
            }) {
            "METADATA_EFFECTIVE_TIME_UNKNOWN"
        } else {
            "METADATA_MISSING"
        };
        let winner = winners.next().ok_or(ContractError(missing))?;
        if winners.next().is_some() {
            return Err(ContractError("METADATA_AMBIGUOUS"));
        }
        let unknown = winner.effective_time_basis == Some(EffectiveTimeBasis::Unknown as i32);
        Ok(MetadataResolution {
            version: winner,
            resolution_basis: if unknown {
                MetadataResolutionBasis::KnowledgeOnly
            } else {
                MetadataResolutionBasis::EffectiveInterval
            },
            quality: unknown.then_some("METADATA_EFFECTIVE_TIME_UNKNOWN"),
        })
    }

    pub fn resolve(
        &self,
        instrument: &str,
        event_time: i64,
        mode: ResolverMode,
        as_of: Option<i64>,
    ) -> Result<&InstrumentMetadataVersion> {
        let resolved = self.resolve_with_basis(instrument, event_time, mode, as_of)?;
        if resolved.resolution_basis == MetadataResolutionBasis::KnowledgeOnly {
            return Err(ContractError("LABELED_METADATA_RESOLUTION_REQUIRED"));
        }
        Ok(resolved.version)
    }
}

#[derive(Debug)]
pub struct MetadataResolution<'a> {
    pub version: &'a InstrumentMetadataVersion,
    pub resolution_basis: MetadataResolutionBasis,
    pub quality: Option<&'static str>,
}

impl InstrumentMetadataVersion {
    pub fn validate_metadata(&self) -> Result<()> {
        for field in [
            &self.instrument_id,
            &self.metadata_version,
            &self.product_type,
            &self.margin_asset,
            &self.settlement_asset,
        ] {
            required(field)?;
        }
        let basis = self
            .effective_time_basis
            .map(EffectiveTimeBasis::try_from)
            .transpose()
            .map_err(|_| ContractError("INVALID_EFFECTIVE_TIME_BASIS"))?;
        if basis == Some(EffectiveTimeBasis::Unspecified) {
            return Err(ContractError("INVALID_EFFECTIVE_TIME_BASIS"));
        }
        if basis == Some(EffectiveTimeBasis::Unknown) {
            if self.effective_from_ns.is_some() {
                return Err(ContractError("UNKNOWN_EFFECTIVE_START_MUST_BE_ABSENT"));
            }
        } else if self.effective_from_ns.is_none() {
            return Err(ContractError("MISSING_METADATA_TIME"));
        }
        self.known_from_ns
            .ok_or(ContractError("MISSING_METADATA_TIME"))?;
        for (from, to) in [
            (self.effective_from_ns, self.effective_to_ns),
            (self.known_from_ns, self.known_to_ns),
        ] {
            if let (Some(from), Some(to)) = (from, to) {
                if to <= from {
                    return Err(ContractError("INVALID_METADATA_INTERVAL"));
                }
            }
        }
        let extended = [
            &self.venue,
            &self.product_family,
            &self.symbol,
            &self.pair,
            &self.contract_type,
            &self.base_asset,
            &self.quote_asset,
            &self.venue_status,
        ]
        .iter()
        .any(|s| s.is_some())
            || self.source.is_some()
            || self.minimum_quantity.is_some()
            || self.maximum_quantity.is_some();
        if extended && basis.is_none() {
            return Err(ContractError("MISSING_EFFECTIVE_TIME_BASIS"));
        }
        for decimal in [
            &self.contract_multiplier,
            &self.tick_size,
            &self.lot_size,
            &self.minimum_quantity,
            &self.maximum_quantity,
        ]
        .into_iter()
        .flatten()
        {
            decimal.validate()?;
        }
        if extended || self.instrument_id.as_deref() == Some(crate::binance::INSTRUMENT_ID) {
            for (actual, expected) in [
                (&self.instrument_id, crate::binance::INSTRUMENT_ID),
                (&self.venue, "BINANCE"),
                (&self.product_family, "USD_M_FUTURES"),
                (&self.symbol, "BTCUSDT"),
                (&self.pair, "BTCUSDT"),
                (&self.contract_type, "PERPETUAL"),
                (&self.base_asset, "BTC"),
                (&self.quote_asset, "USDT"),
                (&self.product_type, "LINEAR"),
                (&self.margin_asset, "USDT"),
                (&self.settlement_asset, "USDT"),
            ] {
                if actual.as_deref() != Some(expected) {
                    return Err(ContractError("PROFILE_MISMATCH"));
                }
            }
            required(&self.venue_status)?;
            for value in [&self.tick_size, &self.lot_size] {
                crate::binance::validate_decimal(
                    value.as_ref().ok_or(ContractError("METADATA_INCOMPLETE"))?,
                    true,
                )?;
            }
            let source = self
                .source
                .as_ref()
                .ok_or(ContractError("METADATA_INCOMPLETE"))?;
            crate::binance::source::http(source, None)?;
            let receive = source.lineage.as_ref().and_then(|l| l.receive_timestamp_ns);
            if source.endpoint.as_deref() != Some("/fapi/v1/exchangeInfo")
                || self.known_from_ns < receive
                || (basis == Some(EffectiveTimeBasis::Unknown) && self.known_from_ns != receive)
            {
                return Err(ContractError("INVALID_METADATA_SOURCE"));
            }
            if let Some(minimum) = &self.minimum_quantity {
                crate::binance::validate_decimal(minimum, false)?;
            }
            if let Some(maximum) = &self.maximum_quantity {
                crate::binance::validate_decimal(maximum, true)?;
            }
            if let (Some(a), Some(b)) = (&self.minimum_quantity, &self.maximum_quantity) {
                if crate::binance::compare(a, b).is_gt() {
                    return Err(ContractError("METADATA_INCOMPLETE"));
                }
            }
        }
        Ok(())
    }
}
