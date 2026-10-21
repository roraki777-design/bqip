use bqip_binance_collector::{capture::Lane, recovery};
use bqip_capture_format::{Frame, SegmentWriter};
use bqip_contracts::{binance, proto::*};
use bqip_normalizer::{
    config::{Config, BUILD_COMMIT, DEFAULT_CONFIG},
    hash,
    metadata::Catalog,
    normalize::metadata_observation,
    plan::{Mode, ReplayPlan},
};
use prost::Message;
use std::{
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub const COLLECTOR: &str = "00010203-0405-0607-0809-0a0b0c0d0e0f";
pub const SESSION: &str = "10111213-1415-1617-1819-1a1b1c1d1e1f";
pub const HISTORICAL_COMMIT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const TRADE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/trade-buy.json"
));
pub const DEPTH: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/depth-ordinary.json"
));
pub const SNAPSHOT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/snapshot-valid.json"
));
pub const EXCHANGE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../tests/fixtures/binance-usdm/raw/exchange-valid.json"
));

pub fn config() -> Config {
    Config::parse(DEFAULT_CONFIG).unwrap()
}
pub fn context(run: u128, time: i64) -> ProcessingContext {
    ProcessingContext {
        processing_timestamp_ns: Some(time),
        code_commit: Some(BUILD_COMMIT.into()),
        schema_version: Some(1),
        run_id: Some(Uuid::from_u128(run).as_bytes().to_vec()),
        replay_mode: Some(ReplayMode::VersionPinnedResearch as i32),
    }
}
pub fn raw(lane: Lane, bytes: &[u8], seq: u64, endpoint: Option<&str>) -> RawCaptureEvent {
    let capture = CaptureId {
        collector_instance_id: Some(Uuid::parse_str(COLLECTOR).unwrap().as_bytes().to_vec()),
        capture_seq: Some(seq),
    };
    let hash = bqip_contracts::identity::raw_hash(bytes).to_vec();
    let lineage = SourceLineage {
        venue_profile_id: Some(binance::PROFILE_ID.into()),
        venue_profile_hash: Some(binance::profile_hash().unwrap().to_vec()),
        capture_id: Some(capture.clone()),
        raw_content_hash: Some(hash.clone()),
        receive_timestamp_ns: Some(5_000_000),
    };
    let session = Uuid::parse_str(SESSION).unwrap().as_bytes().to_vec();
    let rest = lane == Lane::Rest;
    let endpoint = endpoint.unwrap_or(lane.endpoint());
    RawCaptureEvent {
        capture_id: Some(capture),
        connection_id: Some(vec![0x40; 16]),
        session_id: Some(session.clone()),
        transport: Some(if rest { "HTTP_REST" } else { "WEBSOCKET" }.into()),
        source: Some("BINANCE".into()),
        endpoint: Some(endpoint.into()),
        channel: Some(lane.channel().into()),
        receive_timestamp_ns: Some(5_000_000),
        monotonic_ns_since_collector_start: Some(seq * 10),
        payload_kind: Some(PayloadKind::Text as i32),
        payload_length: Some(bytes.len() as u32),
        raw_content_hash: Some(hash),
        processing_context: Some(ProcessingContext {
            code_commit: Some(HISTORICAL_COMMIT.into()),
            replay_mode: Some(ReplayMode::Live as i32),
            ..context(42, 5_000_000)
        }),
        stream_source: if rest {
            None
        } else {
            Some(StreamSourceContext {
                lineage: Some(lineage.clone()),
                transport: Some(SourceTransport::Websocket as i32),
                endpoint: Some(endpoint.into()),
                channel: Some(lane.channel().into()),
                original_session_id: Some(session),
            })
        },
        http_source: if rest {
            Some(HttpSourceContext {
                lineage: Some(lineage),
                transport: Some(SourceTransport::HttpRest as i32),
                method: Some("GET".into()),
                endpoint: Some(endpoint.into()),
                http_status: Some(200),
                query_parameters: if endpoint == "/fapi/v1/depth" {
                    vec![
                        QueryParameter {
                            name: Some("limit".into()),
                            value: Some("1000".into()),
                        },
                        QueryParameter {
                            name: Some("symbol".into()),
                            value: Some("BTCUSDT".into()),
                        },
                    ]
                } else {
                    vec![]
                },
            })
        } else {
            None
        },
        ..Default::default()
    }
}
pub fn confirmed(version: &str, known: i64) -> InstrumentMetadataVersion {
    let r = raw(Lane::Rest, EXCHANGE, 1, Some("/fapi/v1/exchangeInfo"));
    let mut m = metadata_observation(&r, EXCHANGE).unwrap().0;
    m.metadata_version = Some(version.into());
    m.known_from_ns = Some(known);
    m.effective_from_ns = Some(0);
    m.effective_time_basis = Some(EffectiveTimeBasis::BqipConfirmed as i32);
    m.validate_metadata().unwrap();
    m
}
pub fn segment(
    root: &Path,
    id: u128,
    lane: Lane,
    records: &[(RawCaptureEvent, Vec<u8>)],
    loss: bool,
) -> String {
    let id = Uuid::from_u128(id);
    let mut w = SegmentWriter::new(root, id, recovery::LIMITS).unwrap();
    for (raw, bytes) in records {
        w.append(&Frame::from_event(raw, bytes.clone()).unwrap())
            .unwrap();
    }
    let s = w.seal().unwrap();
    recovery::publish_manifest(
        &s.path,
        lane,
        "SHUTDOWN",
        if loss { "POSSIBLE" } else { "NONE" },
        None,
    )
    .unwrap();
    id.to_string()
}
pub struct Fixture {
    pub base: PathBuf,
    pub source: PathBuf,
    pub plan: ReplayPlan,
    pub catalog: Catalog,
    pub config: Config,
}
impl Fixture {
    pub fn new() -> Self {
        let base = std::env::temp_dir().join(format!("bqip-replay-test-{}", Uuid::new_v4()));
        fs::create_dir(&base).unwrap();
        let source = base.join("source");
        fs::create_dir(&source).unwrap();
        let paths = vec![
            segment(
                &source,
                101,
                Lane::Rest,
                &[
                    (
                        raw(Lane::Rest, EXCHANGE, 1, Some("/fapi/v1/exchangeInfo")),
                        EXCHANGE.to_vec(),
                    ),
                    (
                        raw(Lane::Rest, SNAPSHOT, 2, Some("/fapi/v1/depth")),
                        SNAPSHOT.to_vec(),
                    ),
                ],
                false,
            ),
            segment(
                &source,
                102,
                Lane::Depth,
                &[(raw(Lane::Depth, DEPTH, 3, None), DEPTH.to_vec())],
                true,
            ),
            segment(
                &source,
                103,
                Lane::Aggtrade,
                &[
                    (raw(Lane::Aggtrade, TRADE, 4, None), TRADE.to_vec()),
                    (raw(Lane::Aggtrade, TRADE, 5, None), TRADE.to_vec()),
                    (raw(Lane::Aggtrade, b"{", 6, None), b"{".to_vec()),
                ],
                false,
            ),
        ];
        let config = config();
        let catalog = Catalog::new(vec![confirmed("confirmed-v1", 5_000_000)], &config).unwrap();
        let plan =
            ReplayPlan::create(&source, &paths, &config, Mode::VersionPinnedResearch).unwrap();
        Self {
            base,
            source,
            plan,
            catalog,
            config,
        }
    }
    pub fn metadata_file(&self) -> PathBuf {
        let path = self.base.join("confirmed.pb");
        fs::write(&path, self.catalog.versions()[0].encode_to_vec()).unwrap();
        path
    }
    pub fn hashes(&self) -> Vec<(String, String)> {
        self.plan
            .ordered_segments
            .iter()
            .flat_map(|p| {
                bqip_normalizer::input::NAMES.map(|name| {
                    let relative = format!("{}/{name}", p.relative_path);
                    let digest = hash(&fs::read(self.source.join(&relative)).unwrap());
                    (relative, digest)
                })
            })
            .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.base).unwrap();
    }
}
