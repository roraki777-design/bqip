// Integration binaries and benchmark intentionally use different harness subsets.
#![allow(dead_code)]
use bqip_binance_collector::{
    capture::{Ingress, Lane, Record},
    recovery::{self, LIMITS},
};
use bqip_capture_format::{Frame, SegmentWriter};
use bqip_contracts::{binance, proto::*};
use bqip_raw_archive::{
    config::Config,
    local::{Descriptor, Segment},
    sha256,
    store::{ArchiveStore, Created, RemoteMetadata, RemoteObject},
    telemetry::Meter,
    uploader::Archive,
    Error, Kind, Result,
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use uuid::Uuid;
pub const BASE: &str = "f6bffe6f603122a5170e91449e6847efdfe2f10c";
pub const DEPTH: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/depth-ordinary.json"
));
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Create,
    Get,
}
#[derive(Clone, Debug)]
pub enum Action {
    Fail(Kind),
    LostAck,
    Stall,
    Corrupt,
    CreateDirectory(PathBuf),
    ChangeLocal(PathBuf, Vec<u8>),
}
struct Fault {
    op: Op,
    key: String,
    skip: usize,
    action: Action,
}
#[derive(Default)]
pub struct Memory {
    inner: Mutex<MemoryState>,
}
#[derive(Default)]
struct MemoryState {
    objects: BTreeMap<String, Vec<u8>>,
    calls: Vec<(Op, String)>,
    creates: Vec<String>,
    faults: Vec<Fault>,
}
impl Memory {
    pub fn fault(&self, op: Op, key: &str, skip: usize, action: Action) {
        self.inner.lock().unwrap().faults.push(Fault {
            op,
            key: key.into(),
            skip,
            action,
        });
    }
    fn action(&self, op: Op, key: &str) -> Option<Action> {
        let mut s = self.inner.lock().unwrap();
        s.calls.push((op, key.into()));
        let i = s.faults.iter().position(|f| f.op == op && f.key == key)?;
        if s.faults[i].skip > 0 {
            s.faults[i].skip -= 1;
            None
        } else {
            Some(s.faults.remove(i).action)
        }
    }
    pub fn objects(&self) -> BTreeMap<String, Vec<u8>> {
        self.inner.lock().unwrap().objects.clone()
    }
    pub fn calls(&self) -> Vec<(Op, String)> {
        self.inner.lock().unwrap().calls.clone()
    }
    pub fn creations(&self) -> Vec<String> {
        self.inner.lock().unwrap().creates.clone()
    }
    // External administrator fault injection only, never uploader capabilities.
    pub fn set(&self, key: &str, bytes: Vec<u8>) {
        self.inner.lock().unwrap().objects.insert(key.into(), bytes);
    }
    pub fn remove(&self, key: &str) {
        self.inner.lock().unwrap().objects.remove(key);
    }
}
impl ArchiveStore for Memory {
    async fn create_if_absent(&self, key: &str, bytes: &[u8]) -> Result<Created> {
        let action = self.action(Op::Create, key);
        match action {
            Some(Action::Fail(k)) => return Err(Error::new(k, "injected create failure")),
            Some(Action::Stall) => std::future::pending::<()>().await,
            _ => (),
        }
        let created = {
            let mut s = self.inner.lock().unwrap();
            if s.objects.contains_key(key) {
                Created::AlreadyExists
            } else {
                s.objects.insert(key.into(), bytes.to_vec());
                s.creates.push(key.into());
                Created::New
            }
        };
        if matches!(action, Some(Action::LostAck)) {
            return Err(Error::new(Kind::UploadRetryable, "injected lost ACK"));
        }
        Ok(created)
    }
    async fn get_exact(&self, key: &str, max_bytes: u64) -> Result<Option<RemoteObject>> {
        let action = self.action(Op::Get, key);
        match action {
            Some(Action::Fail(k)) => return Err(Error::new(k, "injected GET failure")),
            Some(Action::Stall) => std::future::pending::<()>().await,
            Some(Action::CreateDirectory(ref p)) => fs::create_dir_all(p).unwrap(),
            Some(Action::ChangeLocal(ref p, ref bytes)) => fs::write(p, bytes).unwrap(),
            _ => (),
        }
        let Some(mut bytes) = self.inner.lock().unwrap().objects.get(key).cloned() else {
            return Ok(None);
        };
        if bytes.len() as u64 > max_bytes {
            return Err(Error::new(
                Kind::RemoteLengthMismatch,
                "remote object too long",
            ));
        }
        if matches!(action, Some(Action::Corrupt)) {
            bytes[0] ^= 1;
        }
        Ok(Some(RemoteObject {
            bytes,
            metadata: RemoteMetadata {
                etag: Some("unchanged-even-if-corrupted".into()),
                version: Some("test-version".into()),
            },
        }))
    }
}
pub struct Env {
    pub temp: TempDir,
    pub config: Config,
    pub store: Arc<Memory>,
    pub meter: Arc<Meter>,
    pub archive: Arc<Archive<Memory>>,
}
impl Env {
    pub fn new() -> Self {
        Self::with(|_| ())
    }
    pub fn with(edit: impl FnOnce(&mut Config)) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let mut c = Config {
            spool_root: temp.path().join("spool"),
            archive_state_root: temp.path().join("state"),
            request_timeout_ms: 100,
            connect_timeout_ms: 50,
            retry_min_ms: 1,
            retry_max_ms: 4,
            ..Default::default()
        };
        edit(&mut c);
        let config = c.validate().unwrap();
        let store = Arc::new(Memory::default());
        let meter = Arc::new(Meter::new(None));
        let archive =
            Arc::new(Archive::new(config.clone(), store.clone(), meter.clone(), BASE).unwrap());
        Self {
            temp,
            config,
            store,
            meter,
            archive,
        }
    }
    pub fn segment(&self, lane: Lane) -> Descriptor {
        write_segment(&self.config.spool_root, lane, None)
    }
    pub fn load(&self, d: &Descriptor) -> Segment {
        bqip_raw_archive::local::load(&self.config, d).unwrap()
    }
    pub fn keys(&self, d: &Descriptor) -> [String; 3] {
        bqip_raw_archive::key::keys(&self.config.prefix, d.lane, d.id).unwrap()
    }
    pub fn receipt(&self, d: &Descriptor) -> PathBuf {
        bqip_raw_archive::receipt::path(&self.config, d.id).unwrap()
    }
    pub fn restart(&self) -> Arc<Archive<Memory>> {
        Arc::new(
            Archive::new(
                self.config.clone(),
                self.store.clone(),
                self.meter.clone(),
                BASE,
            )
            .unwrap(),
        )
    }
}
pub fn record(
    ingress: &Ingress,
    lane: Lane,
    payload: Vec<u8>,
    stamp: (CaptureId, i64, u64),
) -> Record {
    let mut r = ingress
        .record(
            lane,
            Uuid::from_u128(222),
            Uuid::from_u128(333),
            PayloadKind::Text,
            payload,
            stamp,
        )
        .unwrap();
    if lane == Lane::Rest {
        r.event.endpoint = Some("/fapi/v1/exchangeInfo".into());
        r.event.http_source = Some(HttpSourceContext {
            lineage: Some(SourceLineage {
                venue_profile_id: Some(binance::PROFILE_ID.into()),
                venue_profile_hash: Some(binance::profile_hash().unwrap().to_vec()),
                capture_id: r.event.capture_id.clone(),
                raw_content_hash: r.event.raw_content_hash.clone(),
                receive_timestamp_ns: r.event.receive_timestamp_ns,
            }),
            transport: Some(SourceTransport::HttpRest as i32),
            endpoint: Some("/fapi/v1/exchangeInfo".into()),
            method: Some("GET".into()),
            http_status: Some(200),
            ..Default::default()
        });
    }
    r
}
pub fn write_segment(root: &Path, lane: Lane, target: Option<u64>) -> Descriptor {
    let lane_root = recovery::lane_root(root, lane);
    fs::create_dir_all(&lane_root).unwrap();
    let id = Uuid::new_v4();
    let mut w = SegmentWriter::new(&lane_root, id, LIMITS).unwrap();
    let ingress = Ingress::new(BASE).unwrap();
    let frames = target.map_or(1, |n| n.div_ceil(16 * 1024 * 1024));
    for i in 0..frames {
        let stamp = ingress.stamp().unwrap();
        let goal = target.map(|n| (n - 28) / frames + u64::from(i < (n - 28) % frames));
        let mut payload = DEPTH.to_vec();
        let frame = loop {
            let r = record(&ingress, lane, payload.clone(), stamp.clone());
            let frame = Frame::from_event(&r.event, r.payload).unwrap();
            let actual = 12 + frame.metadata.len() + frame.payload.len();
            if let Some(goal) = goal {
                if actual as u64 != goal {
                    let next = (payload.len() as i64 + goal as i64 - actual as i64) as usize;
                    payload.resize(next, b' ');
                    continue;
                }
            }
            break frame;
        };
        w.append(&frame).unwrap();
    }
    let sealed = w.seal().unwrap();
    if let Some(n) = target {
        assert_eq!(sealed.byte_length, n);
    }
    recovery::publish_manifest(&sealed.path, lane, "SHUTDOWN", "NONE", None).unwrap();
    recovery::verify_manifest(&sealed.path, lane).unwrap();
    Descriptor {
        id,
        lane,
        directory: lane_root.join(id.to_string()),
    }
}
pub fn source_snapshot(root: &Path) -> BTreeMap<PathBuf, (String, u64, std::time::SystemTime)> {
    fn walk(
        root: &Path,
        p: &Path,
        out: &mut BTreeMap<PathBuf, (String, u64, std::time::SystemTime)>,
    ) {
        for e in fs::read_dir(p).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                walk(root, &p, out)
            } else {
                let m = fs::metadata(&p).unwrap();
                out.insert(
                    p.strip_prefix(root).unwrap().to_path_buf(),
                    (
                        sha256(&fs::read(&p).unwrap()),
                        m.len(),
                        m.modified().unwrap(),
                    ),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
