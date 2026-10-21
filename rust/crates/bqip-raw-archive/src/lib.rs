//! Read-only local evidence, conditional creation and full remote verification.
pub mod config;
pub mod error;
pub mod key;
pub mod local;
pub mod receipt;
pub mod s3;
pub mod store;
pub mod telemetry;
pub mod uploader;
pub use error::{Error, Kind, Result};
use sha2::{Digest, Sha256};
pub fn sha256(bytes: &[u8]) -> String {
    bqip_contracts::hex(&Sha256::digest(bytes))
}
pub fn now_ns() -> Result<u64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|v| u64::try_from(v.as_nanos()).ok())
        .ok_or(Error::new(
            Kind::Configuration,
            "wall clock outside supported range",
        ))
}
