use crate::{
    capture::{Lane, Record},
    config::{delay, Config},
    recovery::{self, LIMITS},
    telemetry::Meter,
    Error, Kind, Result,
};
use bqip_capture_format::{Frame, SegmentWriter};
use serde_json::json;
use std::{sync::Arc, time::Instant};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

#[derive(Default, Clone, Copy)]
pub struct Faults {
    pub append_at: Option<u64>,
    pub fence: bool,
    pub seal: bool,
}
enum Command {
    Append(Arc<Record>, oneshot::Sender<Result<()>>),
    Boundary(oneshot::Sender<Result<()>>),
    Tick,
    Shutdown(oneshot::Sender<Result<()>>),
}
#[derive(Clone)]
pub struct RawSink {
    tx: mpsc::Sender<Command>,
    pub lane: Lane,
    pub meter: Arc<Meter>,
}
pub struct RawWriter {
    pub sink: RawSink,
    tick: JoinHandle<()>,
    worker: JoinHandle<Result<()>>,
}
impl RawSink {
    pub async fn append(&self, record: Arc<Record>) -> Result<()> {
        let start = Instant::now();
        let evidence = json!({"capture_seq":record.seq().to_string(),"collector_instance_id":record.event.capture_id.as_ref().and_then(|v|v.collector_instance_id.as_deref()).map(recovery::hex),"payload_bytes":record.payload.len(),"connection_id":record.event.connection_id.as_deref().map(recovery::hex),"session_id":record.event.session_id.as_deref().map(recovery::hex)});
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Command::Append(record, tx)).await.is_err() {
            let e = Error::new(Kind::RawWrite, "raw queue closed");
            self.meter.raw_failure(self.lane, &e);
            return Err(e);
        }
        let result = rx
            .await
            .unwrap_or_else(|_| Err(Error::new(Kind::RawWrite, "raw actor acknowledgement lost")));
        if let Err(e) = &result {
            self.meter.raw_failure(self.lane, e);
        }
        self.meter.event(
            self.lane,
            "RAW_APPEND_ACK",
            if result.is_ok() { "NONE" } else { "POSSIBLE" },
            json!({"accepted":result.is_ok(),"capture":evidence,"elapsed_ns":start.elapsed().as_nanos().to_string()}),
        );
        result
    }
    pub async fn boundary(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Boundary(tx))
            .await
            .map_err(|e| Error::new(Kind::RawWrite, e))?;
        rx.await.map_err(|e| Error::new(Kind::RawWrite, e))?
    }
}
impl RawWriter {
    pub fn start(config: Config, lane: Lane, meter: Arc<Meter>, faults: Faults) -> Self {
        let (tx, mut rx) = mpsc::channel(config.raw_queues[lane.index()]);
        let sink = RawSink {
            tx: tx.clone(),
            lane,
            meter: meter.clone(),
        };
        let interval = delay(config.fence_ms.min(config.segment_max_age_ms));
        let tick = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if tx.send(Command::Tick).await.is_err() {
                    break;
                }
            }
        });
        let worker = tokio::task::spawn_blocking(move || {
            let mut actor = Actor {
                config,
                lane,
                meter: meter.clone(),
                faults,
                writer: None,
                session: None,
                created: Instant::now(),
                last_fence: Instant::now(),
                last_seq: 0,
                accepted: 0,
            };
            while let Some(command) = rx.blocking_recv() {
                let (result, ack, stop) = match command {
                    Command::Append(record, ack) => (actor.append(&record), Some(ack), false),
                    Command::Boundary(ack) => (actor.close("SESSION"), Some(ack), false),
                    Command::Tick => (actor.tick(), None, false),
                    Command::Shutdown(ack) => (actor.close("SHUTDOWN"), Some(ack), true),
                };
                if let Some(ack) = ack {
                    let _ = ack.send(result.clone());
                }
                if let Err(e) = &result {
                    meter.raw_failure(lane, e);
                }
                result?;
                if stop {
                    return Ok(());
                }
            }
            let error = Error::new(
                Kind::RawWrite,
                "writer command senders disappeared before shutdown",
            );
            meter.raw_failure(lane, &error);
            Err(error)
        });
        Self { sink, tick, worker }
    }
    pub async fn shutdown(self) -> Result<()> {
        self.tick.abort();
        let (tx, rx) = oneshot::channel();
        let queued = self.sink.tx.send(Command::Shutdown(tx)).await.is_ok();
        let ack = if queued {
            rx.await.map_err(|e| Error::new(Kind::Shutdown, e))?
        } else {
            Err(Error::new(Kind::Shutdown, "raw actor already stopped"))
        };
        let joined = self
            .worker
            .await
            .map_err(|e| Error::new(Kind::Shutdown, e))?;
        ack?;
        joined
    }
}
struct Actor {
    config: Config,
    lane: Lane,
    meter: Arc<Meter>,
    faults: Faults,
    writer: Option<SegmentWriter>,
    session: Option<Vec<u8>>,
    created: Instant,
    last_fence: Instant,
    last_seq: u64,
    accepted: u64,
}
impl Actor {
    fn append(&mut self, record: &Record) -> Result<()> {
        if record.lane != self.lane || record.seq() <= self.last_seq {
            return Err(Error::new(
                Kind::RawWrite,
                "wrong lane or nonmonotonic capture",
            ));
        }
        if self.faults.append_at == Some(self.accepted) {
            return Err(Error::new(Kind::RawWrite, "injected append failure"));
        }
        let frame = Frame::from_event(&record.event, record.payload.clone())
            .map_err(|e| Error::new(Kind::RawWrite, e))?;
        let size = frame
            .encode(LIMITS)
            .map_err(|e| Error::new(Kind::RawWrite, e))?
            .len() as u64;
        if self.writer.is_some()
            && self.lane != Lane::Rest
            && self.session != record.event.session_id
        {
            self.close("SESSION")?;
        }
        if self
            .writer
            .as_ref()
            .is_some_and(|w| w.byte_length() + size > self.config.segment_max_bytes)
        {
            self.close("BYTES")?;
        }
        if self.writer.is_some() && self.created.elapsed() >= delay(self.config.segment_max_age_ms)
        {
            self.close("AGE")?;
        }
        if self.writer.is_none() {
            self.writer = Some(
                SegmentWriter::new(
                    &recovery::lane_root(&self.config.spool_root, self.lane),
                    Uuid::new_v4(),
                    LIMITS,
                )
                .map_err(|e| Error::new(Kind::RawWrite, e))?,
            );
            self.session = record.event.session_id.clone();
            self.created = Instant::now();
            self.last_fence = self.created;
            self.meter
                .count(self.lane, "spool_bytes", bqip_capture_format::HEADER_SIZE);
        }
        self.writer
            .as_mut()
            .ok_or_else(|| Error::new(Kind::RawWrite, "writer not open"))?
            .append(&frame)
            .map_err(|e| Error::new(Kind::RawWrite, e))?;
        self.last_seq = record.seq();
        self.accepted += 1;
        self.meter.count(self.lane, "raw_append_accepted_total", 1);
        self.meter
            .count(self.lane, "raw_bytes_total", record.payload.len() as u64);
        self.meter.count(self.lane, "spool_bytes", size);
        Ok(())
    }
    fn fence(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            let start = Instant::now();
            if self.faults.fence {
                return Err(Error::new(Kind::DurabilityFence, "injected fence failure"));
            }
            writer
                .durability_fence()
                .map_err(|e| Error::new(Kind::DurabilityFence, e))?;
            self.last_fence = Instant::now();
            self.meter.fenced(self.lane, self.last_seq);
            self.meter
                .count(self.lane, "raw_durability_fenced_total", 1);
            self.meter.event(self.lane,"RAW_DURABILITY_FENCED","NONE",json!({"capture_seq":self.last_seq.to_string(),"elapsed_ns":start.elapsed().as_nanos().to_string()}));
        }
        Ok(())
    }
    fn close(&mut self, reason: &str) -> Result<()> {
        self.fence()?;
        if let Some(mut writer) = self.writer.take() {
            let start = Instant::now();
            if self.faults.seal {
                return Err(Error::new(Kind::SealManifest, "injected seal failure"));
            }
            let segment = writer
                .seal()
                .map_err(|e| Error::new(Kind::SealManifest, e))?;
            recovery::publish_manifest(&segment.path, self.lane, reason, "NONE", None)?;
            recovery::verify_manifest(&segment.path, self.lane)?;
            let sidecars = segment
                .path
                .with_file_name("seal.json")
                .metadata()
                .map_err(|e| Error::new(Kind::SealManifest, e))?
                .len()
                + segment
                    .path
                    .with_file_name("manifest.json")
                    .metadata()
                    .map_err(|e| Error::new(Kind::SealManifest, e))?
                    .len();
            self.meter.count(self.lane, "spool_bytes", sidecars);
            self.meter.count(self.lane, "raw_segments_sealed_total", 1);
            self.meter.event(self.lane,"RAW_SEGMENT_SEALED","NONE",json!({"segment_id":segment.segment_id.to_string(),"close_reason":reason,"elapsed_ns":start.elapsed().as_nanos().to_string()}));
        }
        Ok(())
    }
    fn tick(&mut self) -> Result<()> {
        if self.created.elapsed() >= delay(self.config.segment_max_age_ms) {
            self.close("AGE")?;
        } else if self.last_fence.elapsed() >= delay(self.config.fence_ms) {
            self.fence()?;
        }
        Ok(())
    }
}
