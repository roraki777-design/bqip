use crate::{Error, Kind, Result};
use bqip_contracts::{binance::*, proto::*};
use sha2::{Digest, Sha256};
use std::{
    sync::Mutex,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Lane {
    Depth,
    Aggtrade,
    Rest,
}
impl Lane {
    pub const ALL: [Self; 3] = [Self::Depth, Self::Aggtrade, Self::Rest];
    pub fn index(self) -> usize {
        match self {
            Self::Depth => 0,
            Self::Aggtrade => 1,
            Self::Rest => 2,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Depth => "depth",
            Self::Aggtrade => "aggtrade",
            Self::Rest => "rest",
        }
    }
    pub fn endpoint(self) -> &'static str {
        match self {
            Self::Depth => DEPTH_ENDPOINT,
            Self::Aggtrade => TRADE_ENDPOINT,
            Self::Rest => "https://fapi.binance.com",
        }
    }
    pub fn channel(self) -> &'static str {
        match self {
            Self::Depth => DEPTH_STREAM,
            Self::Aggtrade => TRADE_STREAM,
            Self::Rest => "REST",
        }
    }
}
#[derive(Clone)]
pub struct Record {
    pub lane: Lane,
    pub event: RawCaptureEvent,
    pub payload: Vec<u8>,
}
impl Record {
    pub fn seq(&self) -> u64 {
        self.event
            .capture_id
            .as_ref()
            .and_then(|v| v.capture_seq)
            .unwrap_or(0)
    }
}
pub struct Ingress {
    pub id: Uuid,
    pub commit: String,
    start: Instant,
    sequence: Mutex<u64>,
}
impl Ingress {
    pub fn new(commit: &str) -> Result<Self> {
        if commit.len() != 40 || !commit.bytes().all(|x| x.is_ascii_hexdigit()) {
            return Err(Error::new(Kind::Configuration, "full code commit required"));
        }
        Ok(Self {
            id: Uuid::new_v4(),
            commit: commit.into(),
            start: Instant::now(),
            sequence: Mutex::new(0),
        })
    }
    pub fn stamp(&self) -> Result<(CaptureId, i64, u64)> {
        let mut seq = self
            .sequence
            .lock()
            .map_err(|_| Error::new(Kind::RawWrite, "ingress lock poisoned"))?;
        let wall = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::new(Kind::RawWrite, e))?
                .as_nanos(),
        )
        .map_err(|e| Error::new(Kind::RawWrite, e))?;
        let mono = u64::try_from(self.start.elapsed().as_nanos())
            .map_err(|e| Error::new(Kind::RawWrite, e))?;
        *seq = seq
            .checked_add(1)
            .ok_or_else(|| Error::new(Kind::RawWrite, "capture sequence exhausted"))?;
        Ok((
            CaptureId {
                collector_instance_id: Some(self.id.as_bytes().to_vec()),
                capture_seq: Some(*seq),
            },
            wall,
            mono,
        ))
    }
    pub fn record(
        &self,
        lane: Lane,
        connection: Uuid,
        session: Uuid,
        kind: PayloadKind,
        payload: Vec<u8>,
        stamp: (CaptureId, i64, u64),
    ) -> Result<Record> {
        let (id, wall, mono) = stamp;
        let hash = Sha256::digest(&payload).to_vec();
        let lineage = SourceLineage {
            venue_profile_id: Some(PROFILE_ID.into()),
            venue_profile_hash: Some(profile_hash()?.to_vec()),
            capture_id: Some(id.clone()),
            raw_content_hash: Some(hash.clone()),
            receive_timestamp_ns: Some(wall),
        };
        let event = RawCaptureEvent {
            capture_id: Some(id),
            connection_id: Some(connection.as_bytes().to_vec()),
            session_id: Some(session.as_bytes().to_vec()),
            transport: Some(
                if lane == Lane::Rest {
                    "HTTP_REST"
                } else {
                    "WEBSOCKET"
                }
                .into(),
            ),
            source: Some("BINANCE".into()),
            endpoint: Some(lane.endpoint().into()),
            channel: Some(lane.channel().into()),
            receive_timestamp_ns: Some(wall),
            monotonic_ns_since_collector_start: Some(mono),
            payload_kind: Some(kind as i32),
            payload_length: Some(
                u32::try_from(payload.len()).map_err(|e| Error::new(Kind::RawWrite, e))?,
            ),
            raw_content_hash: Some(hash),
            processing_context: Some(ProcessingContext {
                processing_timestamp_ns: Some(wall),
                code_commit: Some(self.commit.clone()),
                schema_version: Some(1),
                run_id: Some(self.id.as_bytes().to_vec()),
                replay_mode: Some(ReplayMode::Live as i32),
            }),
            stream_source: if lane != Lane::Rest {
                Some(StreamSourceContext {
                    lineage: Some(lineage),
                    transport: Some(SourceTransport::Websocket as i32),
                    endpoint: Some(lane.endpoint().into()),
                    channel: Some(lane.channel().into()),
                    original_session_id: Some(session.as_bytes().to_vec()),
                })
            } else {
                None
            },
            ..Default::default()
        };
        Ok(Record {
            lane,
            event,
            payload,
        })
    }
}
