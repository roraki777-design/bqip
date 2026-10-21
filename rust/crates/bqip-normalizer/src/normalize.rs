use crate::{config::BUILD_COMMIT, hash, metadata::Catalog, Error, Result};
use bqip_binance_collector::capture::Lane;
use bqip_contracts::{binance, metadata::ResolverMode, proto::*};
use prost::Message;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceClass {
    AggTrade,
    DepthUpdate,
    DepthSnapshot,
    ExchangeInfo,
    Unsupported,
}
impl SourceClass {
    pub fn name(self) -> &'static str {
        match self {
            Self::AggTrade => "WS_AGGTRADE",
            Self::DepthUpdate => "WS_DEPTH_UPDATE",
            Self::DepthSnapshot => "REST_DEPTH_SNAPSHOT",
            Self::ExchangeInfo => "REST_EXCHANGE_INFO",
            Self::Unsupported => "UNSUPPORTED",
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum Payload {
    Trade(TradeEvent),
    Depth(BinanceDepthUpdate),
    Snapshot(BinanceDepthSnapshot),
}
impl Payload {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Trade(_) => "TradeEvent",
            Self::Depth(_) => "BinanceDepthUpdate",
            Self::Snapshot(_) => "BinanceDepthSnapshot",
        }
    }
    pub fn digest(&self) -> String {
        hash(&match self {
            Self::Trade(v) => v.encode_to_vec(),
            Self::Depth(v) => v.encode_to_vec(),
            Self::Snapshot(v) => v.encode_to_vec(),
        })
    }
    pub fn event_time(&self) -> Option<i64> {
        match self {
            Self::Trade(v) => v.trade_timestamp_ns,
            Self::Depth(v) => v.transaction_timestamp_ns,
            Self::Snapshot(v) => v.transaction_timestamp_ns,
        }
    }
    pub fn venue_id(&self) -> Option<String> {
        match self {
            Self::Trade(v) => v.venue_event_id.clone(),
            _ => None,
        }
    }
    pub fn logical_id(&self) -> Option<Vec<u8>> {
        match self {
            Self::Trade(v) => v.logical_event_id.clone(),
            Self::Depth(v) => v.logical_event_id.clone(),
            Self::Snapshot(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordLineage {
    pub source_segment_id: Uuid,
    pub input_segment_ordinal: usize,
    pub source_record_index: u64,
    pub lane: &'static str,
    pub capture_id: CaptureId,
    pub raw_content_hash: Vec<u8>,
    pub original_receive_timestamp_ns: i64,
    pub original_monotonic_ns: u64,
    pub raw_loss_status: &'static str,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CanonicalNormalizedEvent {
    pub lineage: RecordLineage,
    pub venue: &'static str,
    pub instrument_id: &'static str,
    pub metadata_version: String,
    pub metadata_resolution_basis: MetadataResolutionBasis,
    pub replay_mode: ReplayMode,
    pub normalizer_commit: String,
    pub schema_version: u32,
    pub processing_context: ProcessingContext,
    pub quality_codes: Vec<String>,
    pub schema_drift_paths: Vec<String>,
    pub payload: Payload,
}
#[derive(Clone, Debug, PartialEq)]
pub enum OutcomeValue {
    Market(Box<CanonicalNormalizedEvent>),
    Metadata(Box<InstrumentMetadataVersion>),
    Rejected(Error),
}
#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    pub lineage: RecordLineage,
    pub source_class: SourceClass,
    pub parsed_kind: Option<&'static str>,
    pub venue_event_id: Option<String>,
    pub logical_event_id: Option<Vec<u8>>,
    pub quality_codes: Vec<String>,
    pub schema_drift_paths: Vec<String>,
    pub value: OutcomeValue,
}
impl Outcome {
    pub fn name(&self) -> &'static str {
        match self.value {
            OutcomeValue::Market(_) => "NORMALIZED_MARKET_EVENT",
            OutcomeValue::Metadata(_) => "METADATA_OBSERVATION",
            OutcomeValue::Rejected(_) => "REJECTED",
        }
    }
}

pub fn classify(raw: &RawCaptureEvent, lane: Lane) -> Result<SourceClass> {
    if raw.source.as_deref() != Some("BINANCE") {
        return Err(Error::ProfileMismatch);
    }
    match raw.transport.as_deref() {
        Some("WEBSOCKET") => {
            let class = match raw.channel.as_deref() {
                Some(binance::TRADE_STREAM) => SourceClass::AggTrade,
                Some(binance::DEPTH_STREAM) => SourceClass::DepthUpdate,
                _ => return Err(Error::UnsupportedRawSource),
            };
            let expected = if class == SourceClass::AggTrade {
                Lane::Aggtrade
            } else {
                Lane::Depth
            };
            let s = raw.stream_source.as_ref().ok_or(Error::ProfileMismatch)?;
            if expected != lane
                || raw.http_source.is_some()
                || raw.http_content_encoding.is_some()
                || raw.endpoint.as_deref() != Some(expected.endpoint())
                || raw.endpoint != s.endpoint
                || raw.channel != s.channel
                || raw.session_id != s.original_session_id
            {
                return Err(Error::ProfileMismatch);
            }
            check_lineage(raw, s.lineage.as_ref())?;
            binance::source::stream(s, expected.endpoint(), expected.channel(), None)?;
            Ok(class)
        }
        Some("HTTP_REST") => {
            let class = match raw.endpoint.as_deref() {
                Some("/fapi/v1/depth") => SourceClass::DepthSnapshot,
                Some("/fapi/v1/exchangeInfo") => SourceClass::ExchangeInfo,
                _ => return Err(Error::UnsupportedRawSource),
            };
            let s = raw.http_source.as_ref().ok_or(Error::ProfileMismatch)?;
            if lane != Lane::Rest
                || raw.stream_source.is_some()
                || raw.channel.as_deref() != Some("REST")
                || raw.endpoint != s.endpoint
            {
                return Err(Error::ProfileMismatch);
            }
            check_lineage(raw, s.lineage.as_ref())?;
            binance::source::http(s, None)?;
            if raw
                .http_content_encoding
                .as_deref()
                .is_some_and(|b| b != b"identity")
            {
                return Err(Error::UnsupportedContentEncoding);
            }
            Ok(class)
        }
        _ => Err(Error::UnsupportedRawSource),
    }
}
fn check_lineage(raw: &RawCaptureEvent, s: Option<&SourceLineage>) -> Result<()> {
    let s = s.ok_or(Error::ProfileMismatch)?;
    if s.capture_id != raw.capture_id
        || s.raw_content_hash != raw.raw_content_hash
        || s.receive_timestamp_ns != raw.receive_timestamp_ns
    {
        return Err(Error::ProfileMismatch);
    }
    binance::source::lineage(s, None)?;
    Ok(())
}

pub fn metadata_observation(
    raw: &RawCaptureEvent,
    bytes: &[u8],
) -> Result<(InstrumentMetadataVersion, Vec<String>)> {
    raw.validate(bytes)?;
    if classify(raw, Lane::Rest)? != SourceClass::ExchangeInfo {
        return Err(Error::ProfileMismatch);
    }
    let id =
        binance::metadata_observation_id(raw.capture_id.as_ref().ok_or(Error::ProfileMismatch)?)?;
    binance::parse_exchange_info(
        bytes,
        raw.http_source.as_ref().ok_or(Error::ProfileMismatch)?,
        &id,
    )
    .map_err(Error::from)
}

pub struct RecordPosition {
    pub segment_id: Uuid,
    pub segment_ordinal: usize,
    pub record_index: u64,
    pub lane: Lane,
    pub loss_recorded: bool,
}

/// Valid RAW contract required. Payload/source/metadata failures yield one REJECTED outcome.
pub fn normalize_record(
    raw: &RawCaptureEvent,
    bytes: &[u8],
    position: &RecordPosition,
    catalog: &Catalog,
    processing: &ProcessingContext,
) -> Result<Outcome> {
    raw.validate(bytes)?;
    processing.validate()?;
    if processing.replay_mode != Some(ReplayMode::VersionPinnedResearch as i32) {
        return Err(Error::ForensicProvenanceInsufficient);
    }
    if processing.code_commit.as_deref() != Some(BUILD_COMMIT) {
        return Err(Error::NormalizerCommitMismatch);
    }
    let lineage = RecordLineage {
        raw_loss_status: if position.loss_recorded {
            "POSSIBLE"
        } else {
            "NONE"
        },
        source_segment_id: position.segment_id,
        input_segment_ordinal: position.segment_ordinal,
        source_record_index: position.record_index,
        lane: position.lane.name(),
        capture_id: raw.capture_id.clone().ok_or(Error::SegmentSourceInvalid)?,
        raw_content_hash: raw
            .raw_content_hash
            .clone()
            .ok_or(Error::SegmentSourceInvalid)?,
        original_receive_timestamp_ns: raw
            .receive_timestamp_ns
            .ok_or(Error::SegmentSourceInvalid)?,
        original_monotonic_ns: raw
            .monotonic_ns_since_collector_start
            .ok_or(Error::SegmentSourceInvalid)?,
    };
    let mut o = Outcome {
        lineage,
        source_class: SourceClass::Unsupported,
        parsed_kind: None,
        venue_event_id: None,
        logical_event_id: None,
        quality_codes: Vec::new(),
        schema_drift_paths: Vec::new(),
        value: OutcomeValue::Rejected(Error::ParseFailure),
    };
    if position.loss_recorded {
        o.quality_codes.push("RAW_LOSS_RECORDED".into());
    }
    let result = normalize_inner(&mut o, raw, bytes, catalog, processing);
    if let Err(e) = result {
        if e == Error::MetadataEffectiveTimeUnknown {
            o.quality_codes.push(e.code().into());
        }
        o.value = OutcomeValue::Rejected(e);
    }
    o.quality_codes.sort();
    o.quality_codes.dedup();
    o.schema_drift_paths.sort();
    o.schema_drift_paths.dedup();
    if let OutcomeValue::Market(e) = &mut o.value {
        e.quality_codes.clone_from(&o.quality_codes);
        e.schema_drift_paths.clone_from(&o.schema_drift_paths);
    }
    Ok(o)
}
fn normalize_inner(
    o: &mut Outcome,
    raw: &RawCaptureEvent,
    bytes: &[u8],
    catalog: &Catalog,
    processing: &ProcessingContext,
) -> Result<()> {
    if processing.schema_version != Some(1)
        || raw
            .processing_context
            .as_ref()
            .and_then(|p| p.schema_version)
            != Some(1)
    {
        return Err(Error::UnsupportedSchemaVersion);
    }
    let lane = match o.lineage.lane {
        "depth" => Lane::Depth,
        "aggtrade" => Lane::Aggtrade,
        _ => Lane::Rest,
    };
    o.source_class = classify(raw, lane)?;
    if o.source_class == SourceClass::ExchangeInfo {
        let (value, drift) = metadata_observation(raw, bytes)?;
        o.parsed_kind = Some("InstrumentMetadataVersion");
        o.schema_drift_paths = drift;
        o.quality_codes
            .push("METADATA_EFFECTIVE_TIME_UNKNOWN".into());
        if !o.schema_drift_paths.is_empty() {
            o.quality_codes.push("SCHEMA_DRIFT_OBSERVED".into());
        }
        o.value = OutcomeValue::Metadata(Box::new(value));
        return Ok(());
    }
    let (payload, drift) = match o.source_class {
        SourceClass::AggTrade => {
            let (v, d) = binance::parse_trade(
                bytes,
                raw.stream_source.as_ref().ok_or(Error::ProfileMismatch)?,
            )?;
            (Payload::Trade(v), d)
        }
        SourceClass::DepthUpdate => {
            let (v, d) = binance::parse_depth(
                bytes,
                raw.stream_source.as_ref().ok_or(Error::ProfileMismatch)?,
            )?;
            (Payload::Depth(v), d)
        }
        SourceClass::DepthSnapshot => {
            let (v, d) = binance::parse_snapshot(
                bytes,
                raw.http_source.as_ref().ok_or(Error::ProfileMismatch)?,
            )?;
            (Payload::Snapshot(v), d)
        }
        _ => return Err(Error::UnsupportedRawSource),
    };
    o.parsed_kind = Some(payload.kind());
    o.venue_event_id = payload.venue_id();
    o.logical_event_id = payload.logical_id();
    o.schema_drift_paths = drift;
    if !o.schema_drift_paths.is_empty() {
        o.quality_codes.push("SCHEMA_DRIFT_OBSERVED".into());
    }
    let resolved = catalog.resolve(
        binance::INSTRUMENT_ID,
        payload.event_time(),
        o.lineage.original_receive_timestamp_ns,
        ResolverMode::CorrectedResearch,
    )?;
    if let Some(q) = resolved.quality {
        o.quality_codes.push(q.into());
    }
    o.value = OutcomeValue::Market(Box::new(CanonicalNormalizedEvent {
        lineage: o.lineage.clone(),
        venue: "BINANCE",
        instrument_id: binance::INSTRUMENT_ID,
        metadata_version: resolved
            .version
            .metadata_version
            .clone()
            .ok_or(Error::CatalogInvalid)?,
        metadata_resolution_basis: resolved.resolution_basis,
        replay_mode: ReplayMode::VersionPinnedResearch,
        normalizer_commit: BUILD_COMMIT.into(),
        schema_version: 1,
        processing_context: processing.clone(),
        quality_codes: Vec::new(),
        schema_drift_paths: Vec::new(),
        payload,
    }));
    Ok(())
}
