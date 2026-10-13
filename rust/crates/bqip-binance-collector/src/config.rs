use crate::{Error, Kind, Result};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub spool_root: PathBuf,
    pub raw_queues: [usize; 3],
    pub realtime_queues: [usize; 2],
    pub snapshot_buffer: usize,
    pub book_session_events: usize,
    pub book_session_bytes: usize,
    pub log_capacity: usize,
    pub ws_max_bytes: usize,
    pub http_max_bytes: usize,
    pub segment_max_bytes: u64,
    pub segment_max_age_ms: u64,
    pub fence_ms: u64,
    pub connect_timeout_ms: u64,
    pub http_timeout_ms: u64,
    pub close_timeout_ms: u64,
    pub reconnect_min_ms: u64,
    pub reconnect_max_ms: u64,
    pub snapshot_retry_min_ms: u64,
    pub snapshot_retry_max_ms: u64,
    pub snapshot_attempt_budget: u32,
    pub snapshot_cooldown_ms: u64,
    pub metadata_refresh_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            spool_root: "spool".into(),
            raw_queues: [16, 16, 4],
            realtime_queues: [64, 128],
            snapshot_buffer: 128,
            book_session_events: 10000,
            book_session_bytes: 16 * 1024 * 1024,
            log_capacity: 1024,
            ws_max_bytes: 1024 * 1024,
            http_max_bytes: 16 * 1024 * 1024,
            segment_max_bytes: 64 * 1024 * 1024,
            segment_max_age_ms: 60000,
            fence_ms: 1000,
            connect_timeout_ms: 10000,
            http_timeout_ms: 10000,
            close_timeout_ms: 2000,
            reconnect_min_ms: 1000,
            reconnect_max_ms: 30000,
            snapshot_retry_min_ms: 5000,
            snapshot_retry_max_ms: 20000,
            snapshot_attempt_budget: 3,
            snapshot_cooldown_ms: 60000,
            metadata_refresh_ms: 300000,
        }
    }
}
impl Config {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let text = std::str::from_utf8(bytes).map_err(|e| Error::new(Kind::Configuration, e))?;
        let value = bqip_contracts::manifests::parse_manifest(text)?;
        let c: Self =
            serde_json::from_value(value).map_err(|e| Error::new(Kind::Configuration, e))?;
        c.validate(false)?;
        Ok(c)
    }
    pub fn validate(&self, live: bool) -> Result<()> {
        let fail = || Error::new(Kind::Configuration, "invalid or excessive runtime bound");
        if self.spool_root.as_os_str().is_empty() {
            return Err(fail());
        }
        for n in self
            .raw_queues
            .into_iter()
            .chain(self.realtime_queues)
            .chain([self.snapshot_buffer, self.log_capacity])
        {
            if !(1..=65536).contains(&n) {
                return Err(fail());
            }
        }
        for n in [
            self.segment_max_age_ms,
            self.fence_ms,
            self.connect_timeout_ms,
            self.http_timeout_ms,
            self.close_timeout_ms,
            self.reconnect_min_ms,
            self.reconnect_max_ms,
            self.snapshot_retry_min_ms,
            self.snapshot_retry_max_ms,
            self.snapshot_cooldown_ms,
            self.metadata_refresh_ms,
        ] {
            if !(1..=86_400_000).contains(&n) {
                return Err(fail());
            }
        }
        if !(1..=4 * 1024 * 1024).contains(&self.ws_max_bytes)
            || !(1..=32 * 1024 * 1024).contains(&self.http_max_bytes)
            || !(1..=100000).contains(&self.book_session_events)
            || !(1..=128 * 1024 * 1024).contains(&self.book_session_bytes)
            || !(1..=10).contains(&self.snapshot_attempt_budget)
            || self.reconnect_min_ms > self.reconnect_max_ms
            || self.snapshot_retry_min_ms > self.snapshot_retry_max_ms
            || self.segment_max_bytes > 4 * 1024 * 1024 * 1024
            || self.segment_max_bytes
                < self.http_max_bytes.max(self.ws_max_bytes) as u64 + 2 * 1024 * 1024
        {
            return Err(fail());
        }
        let bytes = (self.raw_queues[0] as u64
            + self.raw_queues[1] as u64
            + self.realtime_queues[0] as u64
            + self.realtime_queues[1] as u64)
            * self.ws_max_bytes as u64
            + self.raw_queues[2] as u64 * self.http_max_bytes as u64;
        if bytes > 512 * 1024 * 1024
            || (live
                && (self.snapshot_retry_min_ms < 1000
                    || self.snapshot_cooldown_ms < 30000
                    || self.metadata_refresh_ms < 60000
                    || self.reconnect_min_ms < 1000))
        {
            return Err(fail());
        }
        Ok(())
    }
}
pub fn delay(ms: u64) -> Duration {
    Duration::from_millis(ms)
}
pub fn backoff(min: u64, max: u64, attempt: u32) -> Duration {
    delay(
        min.saturating_mul(1u64.checked_shl(attempt.min(63)).unwrap_or(u64::MAX))
            .min(max),
    )
}
