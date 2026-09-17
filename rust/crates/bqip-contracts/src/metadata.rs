use crate::{proto::InstrumentMetadataVersion, ContractError, Result};
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
            required(&version.product_type)?;
            required(&version.margin_asset)?;
            required(&version.settlement_asset)?;
            for (from, to) in [
                (version.effective_from_ns, version.effective_to_ns),
                (version.known_from_ns, version.known_to_ns),
            ] {
                let from = from.ok_or(ContractError("MISSING_METADATA_TIME"))?;
                if to.is_some_and(|to| to <= from) {
                    return Err(ContractError("INVALID_METADATA_INTERVAL"));
                }
            }
            for decimal in [
                &version.contract_multiplier,
                &version.tick_size,
                &version.lot_size,
            ]
            .into_iter()
            .flatten()
            {
                decimal.validate()?;
            }
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

    pub fn resolve(
        &self,
        instrument: &str,
        event_time: i64,
        mode: ResolverMode,
        as_of: Option<i64>,
    ) -> Result<&InstrumentMetadataVersion> {
        if mode == ResolverMode::AsLived && as_of.is_none() {
            return Err(ContractError("AS_OF_REQUIRED"));
        }
        let candidates: Vec<_> = self
            .versions
            .iter()
            .filter(|v| {
                v.instrument_id.as_deref() == Some(instrument)
                    && v.effective_from_ns.is_some_and(|from| from <= event_time)
                    && v.effective_to_ns.is_none_or(|to| event_time < to)
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
        let winner = winners.next().ok_or(ContractError("METADATA_MISSING"))?;
        if winners.next().is_some() {
            return Err(ContractError("METADATA_AMBIGUOUS"));
        }
        Ok(winner)
    }
}
