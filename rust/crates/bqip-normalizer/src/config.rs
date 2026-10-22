use crate::{hash, Error, Result};
use bqip_contracts::{binance, manifests};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CONFIG: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../config/runtime/binance-btcusdt-normalizer-v1.json"
));
pub const BUILD_COMMIT: &str = env!("BQIP_CODE_COMMIT");
pub const LOCK_BYTES: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../../Cargo.lock"));

pub fn full_hash(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: String,
    pub venue_profile_id: String,
    pub venue: String,
    pub product_family: String,
    pub instrument_id: String,
    pub schema_version: u32,
    pub metadata_policy: String,
    pub normalizer_policy: String,
    pub max_segment_bytes: u64,
    pub max_segments: usize,
    pub max_metadata_versions: usize,
    pub max_metadata_bytes: usize,
}
impl Config {
    pub fn parse(text: &str) -> Result<Self> {
        let v = manifests::parse_manifest(text).map_err(|_| Error::NormalizerConfigMismatch)?;
        let result: Self =
            serde_json::from_value(v).map_err(|_| Error::NormalizerConfigMismatch)?;
        result.validate()?;
        Ok(result)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 {
            return Err(Error::UnsupportedSchemaVersion);
        }
        if self.venue_profile_id != binance::PROFILE_ID
            || self.venue != "BINANCE"
            || self.product_family != "USD_M_FUTURES"
            || self.instrument_id != binance::INSTRUMENT_ID
        {
            return Err(Error::ReplayProfileMismatch);
        }
        if self.schema != "BQIP-NORMALIZER-CONFIG-V1"
            || self.metadata_policy != "CORRECTED_RESEARCH_T_V1"
            || self.normalizer_policy != "BINANCE_TYPED_SOURCE_V1"
        {
            return Err(Error::NormalizerConfigMismatch);
        }
        if !(1..=256 * 1024 * 1024).contains(&self.max_segment_bytes)
            || !(1..=4096).contains(&self.max_segments)
            || !(1..=4096).contains(&self.max_metadata_versions)
            || !(1..=16 * 1024 * 1024).contains(&self.max_metadata_bytes)
        {
            return Err(Error::CapacityExceeded);
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<String> {
        self.validate()?;
        Ok(hash(&manifests::canonical_bytes(
            &serde_json::to_value(self).map_err(|_| Error::NormalizerConfigMismatch)?,
        )?))
    }
}
