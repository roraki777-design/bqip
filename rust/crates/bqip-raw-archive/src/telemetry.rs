use crate::{Error, Kind};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{mpsc::SyncSender, Mutex},
    time::Duration,
};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum State {
    Discovered,
    LocalVerifying,
    LocalVerified,
    UploadPending,
    UploadingCapture,
    UploadingSeal,
    UploadingManifest,
    RemoteVerifying,
    RemoteDurable,
}
#[derive(Clone, Default, serde::Serialize)]
pub struct Backlog {
    pub archive_pending_segments: u64,
    pub archive_pending_bytes: u64,
    pub archive_remote_durable_segments: u64,
    pub archive_remote_durable_bytes: u64,
    pub oldest_unuploaded_age_seconds: f64,
    pub invalid_source_segments: u64,
    pub source_errors: BTreeMap<Kind, u64>,
}
#[derive(Default)]
struct Latency {
    count: u64,
    total_us: u64,
    samples: VecDeque<u64>,
}
impl Latency {
    fn add(&mut self, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        self.count = self.count.saturating_add(1);
        self.total_us = self.total_us.saturating_add(us);
        if self.samples.len() == 1024 {
            self.samples.pop_front();
        }
        self.samples.push_back(us);
    }
    fn value(&self) -> Value {
        let mut a: Vec<_> = self.samples.iter().copied().collect();
        a.sort_unstable();
        let q = |p: usize| {
            a.get((a.len() * p).div_ceil(100).saturating_sub(1))
                .copied()
                .unwrap_or(0) as f64
                / 1000.0
        };
        json!({"count":self.count,"total_ms":self.total_us as f64/1000.0,"p50":q(50),"p95":q(95),"p99":q(99),"window_samples":a.len()})
    }
}
struct Inner {
    counters: BTreeMap<&'static str, u64>,
    backlog: Backlog,
    upload: Latency,
    verify: Latency,
}
pub struct Meter {
    inner: Mutex<Inner>,
    logs: Option<SyncSender<Value>>,
}
impl Meter {
    pub fn new(logs: Option<SyncSender<Value>>) -> Self {
        let counters = [
            "upload_attempts_total",
            "upload_successes_total",
            "upload_retries_total",
            "upload_failures_total",
            "remote_verify_attempts_total",
            "remote_verify_successes_total",
            "remote_verify_failures_total",
            "remote_conflicts_total",
            "archive_divergence_total",
            "auth_failures_total",
            "capability_failures_total",
            "bytes_uploaded_total",
            "bytes_verified_download_total",
            "active_uploads",
            "peak_active_uploads",
            "dropped_log_records",
            "invalid_receipts_total",
        ]
        .into_iter()
        .map(|k| (k, 0))
        .collect();
        Self {
            inner: Mutex::new(Inner {
                counters,
                backlog: Backlog::default(),
                upload: Latency::default(),
                verify: Latency::default(),
            }),
            logs,
        }
    }
    pub fn count(&self, k: &'static str, n: u64) {
        let mut s = self.inner.lock().expect("meter mutex");
        let v = s.counters.get_mut(k).expect("declared bounded counter");
        *v = v.saturating_add(n);
    }
    pub fn value(&self, k: &str) -> u64 {
        self.inner
            .lock()
            .expect("meter mutex")
            .counters
            .get(k)
            .copied()
            .unwrap_or(0)
    }
    pub fn active(&self, start: bool) {
        let mut s = self.inner.lock().expect("meter mutex");
        let n = s.counters["active_uploads"];
        let n = if start { n + 1 } else { n.saturating_sub(1) };
        s.counters.insert("active_uploads", n);
        let peak = n.max(s.counters["peak_active_uploads"]);
        s.counters.insert("peak_active_uploads", peak);
    }
    pub fn event(&self, v: Value) {
        if self.logs.as_ref().is_some_and(|tx| tx.try_send(v).is_err()) {
            self.count("dropped_log_records", 1);
        }
    }
    pub fn state(&self, id: Uuid, state: State) {
        self.event(json!({"segment_id":id.to_string(),"state":state}));
    }
    pub fn failure(&self, id: Uuid, e: &Error) {
        self.count("upload_failures_total", 1);
        match e.kind {
            Kind::RemoteObjectConflict => self.count("remote_conflicts_total", 1),
            Kind::RemoteArchiveDivergence => self.count("archive_divergence_total", 1),
            Kind::ArchiveAuthFailure => self.count("auth_failures_total", 1),
            Kind::ArchiveCapabilityUnsupported => self.count("capability_failures_total", 1),
            _ => (),
        }
        self.event(json!({"segment_id":id.to_string(),"error":e}));
    }
    pub fn latency(&self, upload: bool, d: Duration) {
        let mut s = self.inner.lock().expect("meter mutex");
        if upload {
            s.upload.add(d)
        } else {
            s.verify.add(d)
        }
    }
    pub fn backlog(&self, b: Backlog) {
        self.inner.lock().expect("meter mutex").backlog = b;
    }
    pub fn summary(&self) -> Value {
        let s = self.inner.lock().expect("meter mutex");
        json!({"backlog":s.backlog,"counters":s.counters,"upload_latency_ms":s.upload.value(),"verification_latency_ms":s.verify.value()})
    }
}
