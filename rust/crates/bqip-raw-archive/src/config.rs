use crate::{sha256, Error, Kind, Result};
use bqip_contracts::manifests;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs,
    path::{Component, Path, PathBuf},
    time::Duration,
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: String,
    pub backend: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub spool_root: PathBuf,
    pub archive_state_root: PathBuf,
    pub scan_interval_ms: u64,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
    pub max_inflight_segments: usize,
    pub max_single_put_bytes: u64,
    pub request_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    pub max_scanned_segments: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            schema: "BQIP-RAW-ARCHIVE-CONFIG-V1".into(),
            backend: "S3_COMPATIBLE".into(),
            endpoint: "https://s3.example.invalid".into(),
            region: "us-east-1".into(),
            bucket: "bqip-archive".into(),
            prefix: "bqip".into(),
            spool_root: "spool".into(),
            archive_state_root: "archive-state".into(),
            scan_interval_ms: 1000,
            retry_min_ms: 1000,
            retry_max_ms: 60000,
            max_inflight_segments: 1,
            max_single_put_bytes: 134217728,
            request_timeout_ms: 30000,
            connect_timeout_ms: 5000,
            max_scanned_segments: 100000,
        }
    }
}
fn invalid() -> Error {
    Error::new(Kind::Configuration, "invalid archive configuration")
}
pub fn component(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 255
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}
pub fn safe_absolute(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(invalid());
    }
    let full = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir().map_err(|_| invalid())?.join(path)
    };
    let mut current = PathBuf::new();
    for c in full.components() {
        current.push(c.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(m) if m.file_type().is_symlink() => return Err(invalid()),
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(_) => return Err(invalid()),
        }
    }
    Ok(full)
}
impl Config {
    pub fn validate(mut self) -> Result<Self> {
        let u = reqwest::Url::parse(&self.endpoint).map_err(|_| invalid())?;
        let tail = self
            .endpoint
            .split_once("://")
            .ok_or_else(invalid)?
            .1
            .trim_end_matches('/');
        if self.schema != "BQIP-RAW-ARCHIVE-CONFIG-V1"
            || self.backend != "S3_COMPATIBLE"
            || u.scheme() != "https"
            || u.host_str().is_none()
            || !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
            || u.path() != "/"
            || tail.contains('/')
            || !component(&self.region)
            || !component(&self.bucket)
            || self.prefix.len() > 1024
            || !self.prefix.split('/').all(component)
            || !(1..=8).contains(&self.max_inflight_segments)
            || !(4096..=268435456).contains(&self.max_single_put_bytes)
            || !(1..=1000000).contains(&self.max_scanned_segments)
            || !(1..=3600000).contains(&self.scan_interval_ms)
            || self.retry_min_ms == 0
            || self.retry_min_ms > self.retry_max_ms
            || self.retry_max_ms > 3600000
            || self.connect_timeout_ms == 0
            || self.connect_timeout_ms > self.request_timeout_ms
            || self.request_timeout_ms > 300000
        {
            return Err(invalid());
        }
        // Source snapshot, adapter copy, GET buffer, frame validation, plus scanner.
        if (self.max_inflight_segments as u64 + 1) * (3 * self.max_single_put_bytes + 67108864)
            > 2147483648
        {
            return Err(invalid());
        }
        self.endpoint = u.to_string().trim_end_matches('/').to_owned();
        self.spool_root = safe_absolute(&self.spool_root)?;
        self.archive_state_root = safe_absolute(&self.archive_state_root)?;
        if self.spool_root.starts_with(&self.archive_state_root)
            || self.archive_state_root.starts_with(&self.spool_root)
        {
            return Err(invalid());
        }
        Ok(self)
    }
    pub fn namespace_bytes(&self) -> Result<Vec<u8>> {
        manifests::canonical_bytes(&json!({"backend":self.backend,"endpoint":self.endpoint,"region":self.region,"bucket":self.bucket,"prefix":self.prefix})).map_err(|_|invalid())
    }
    pub fn namespace_id(&self) -> Result<String> {
        Ok(sha256(&self.namespace_bytes()?))
    }
    pub fn backoff(&self, n: u32) -> Duration {
        Duration::from_millis(
            self.retry_min_ms
                .saturating_mul(1u64.checked_shl(n.saturating_sub(1)).unwrap_or(u64::MAX))
                .min(self.retry_max_ms),
        )
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}
