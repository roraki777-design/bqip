// Each integration binary uses a subset of this shared transport/evidence harness.
#![allow(dead_code)]
use bqip_binance_collector::{
    capture::{Ingress, Lane, Record},
    config::Config,
    recovery::{self, SpoolGuard, LIMITS},
    telemetry::{Epoch, Logger, Meter},
};
use bqip_capture_format as bqrc;
use bqip_contracts::proto::PayloadKind;
use std::{
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;
pub const BASE: &str = "2b216ee6d6c86cb1d5aa742fd3cb314947ec8c5b";
pub const DEPTH: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/depth-ordinary.json"
));
pub const TRADE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/trade-buy.json"
));
pub const SNAPSHOT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/snapshot-valid.json"
));
pub const EXCHANGE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/exchange-valid.json"
));
pub struct Env {
    pub base: PathBuf,
    pub config: Config,
    pub ingress: Arc<Ingress>,
    pub meter: Arc<Meter>,
    logger: Logger,
    guard: SpoolGuard,
}
impl Env {
    pub fn new() -> Self {
        let base = std::env::temp_dir().join(format!("bqip003-test-{}", Uuid::new_v4()));
        fs::create_dir(&base).unwrap();
        let config = Config {
            spool_root: base.join("spool"),
            ..Config::default()
        };
        let guard = SpoolGuard::acquire(&config.spool_root).unwrap();
        let (meter, logger) = Meter::new(&base.join("events.jsonl"), 8192).unwrap();
        recovery::recover_spool(&config.spool_root, &meter).unwrap();
        Self {
            base,
            config,
            ingress: Arc::new(Ingress::new(BASE).unwrap()),
            meter,
            logger,
            guard,
        }
    }
    pub fn record(
        &self,
        lane: Lane,
        connection: Uuid,
        session: Uuid,
        payload: &[u8],
    ) -> Arc<Record> {
        Arc::new(
            self.ingress
                .record(
                    lane,
                    connection,
                    session,
                    PayloadKind::Text,
                    payload.to_vec(),
                    self.ingress.stamp().unwrap(),
                )
                .unwrap(),
        )
    }
    pub fn open(&self, lane: Lane) -> (Uuid, Uuid, Epoch) {
        let c = Uuid::new_v4();
        let s = Uuid::new_v4();
        self.meter.open(lane, c, s);
        (c, s, self.meter.health(lane).epoch.unwrap())
    }
    pub fn paths(&self, lane: Lane) -> Vec<PathBuf> {
        let mut paths: Vec<_> = fs::read_dir(recovery::lane_root(&self.config.spool_root, lane))
            .unwrap()
            .map(|e| e.unwrap().path().join("capture.sealed"))
            .filter(|p| p.exists())
            .collect();
        paths.sort();
        paths
    }
    pub fn records(&self, lane: Lane) -> Vec<bqrc::Frame> {
        let mut records = Vec::new();
        for path in self.paths(lane) {
            recovery::verify_manifest(&path, lane).unwrap();
            let mut f = File::open(path).unwrap();
            bqrc::read_header(&mut f).unwrap();
            let mut offset = bqrc::HEADER_SIZE;
            while let Some(frame) = bqrc::read_frame(&mut f, offset, LIMITS).unwrap() {
                offset += 12 + frame.metadata.len() as u64 + frame.payload.len() as u64;
                records.push(frame);
            }
        }
        records.sort_by_key(|f| f.decode_event().unwrap().capture_id.unwrap().capture_seq);
        records
    }
    pub fn finish(self) {
        self.logger.finish().unwrap();
        self.guard.release().unwrap();
        fs::remove_dir_all(self.base).unwrap();
    }
}
pub async fn request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        stream.read_exact(&mut b).await.unwrap();
        bytes.push(b[0]);
        assert!(bytes.len() < 16384);
    }
    bytes
}
pub async fn upgraded(stream: &mut tokio::net::TcpStream) {
    let request = request(stream).await;
    let text = std::str::from_utf8(&request).unwrap();
    let key = text
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, v)| v.trim())
        })
        .unwrap();
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    stream.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
}
pub fn frame(opcode: u8, final_frame: bool, payload: &[u8]) -> Vec<u8> {
    let mut b = vec![opcode | if final_frame { 128 } else { 0 }];
    match payload.len() {
        0..=125 => b.push(payload.len() as u8),
        126..=65535 => {
            b.push(126);
            b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            b.push(127);
            b.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    b.extend_from_slice(payload);
    b
}
