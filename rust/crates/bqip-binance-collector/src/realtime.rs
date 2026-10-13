use crate::{
    capture::{Lane, Record},
    config::{delay, Config},
    http::Snapshot,
    telemetry::{Epoch, Meter},
    Error, Result,
};
use bqip_contracts::binance::{
    self,
    book::{depth_semantic_bytes, BookSync, BookSyncState},
};
use prost::Message;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, watch};
pub struct Item {
    pub record: Arc<Record>,
    pub epoch: Epoch,
}
pub fn handoff(tx: &mpsc::Sender<Item>, record: Arc<Record>, epoch: Epoch, meter: &Meter) {
    let lane = record.lane;
    if !meter.trust(lane, epoch) {
        meter.count(lane, "stale_realtime_copies_total", 1);
        meter.event(
            lane,
            "STALE_REALTIME_COPY",
            "NONE",
            json!({"capture_seq":record.seq().to_string()}),
        );
        return;
    }
    if tx.try_send(Item { record, epoch }).is_err() {
        meter.count(lane, "local_capacity_drops_total", 1);
        meter.invalidate(lane, "LOCAL_CAPACITY_DROP", "NONE");
    }
}
fn failure(meter: &Meter, lane: Lane, error: Error) {
    let (counter, cause) = match error.kind {
        crate::Kind::VenueGap => ("venue_gaps_total", "VENUE_GAP"),
        crate::Kind::IdentityConflict => ("identity_conflicts_total", "VENUE_IDENTITY_CONFLICT"),
        crate::Kind::ProfileMismatch => ("parse_failures_total", "PROFILE_MISMATCH"),
        _ => ("parse_failures_total", "PARSE_FAILURE"),
    };
    meter.count(lane, counter, 1);
    meter.invalidate(lane, cause, "NONE");
    meter.event(lane, cause, "NONE", json!({"error":error.detail}));
}
pub async fn trades(mut input: mpsc::Receiver<Item>, meter: Arc<Meter>) -> Result<()> {
    while let Some(item) = input.recv().await {
        let source = item
            .record
            .event
            .stream_source
            .as_ref()
            .ok_or_else(|| crate::Error::new(crate::Kind::Parse, "stream context missing"))?;
        match binance::parse_trade(&item.record.payload, source) {
            Ok(_) => {
                meter.count(Lane::Aggtrade, "parsed_total", 1);
                if item.epoch.generation == 0 {
                    meter.healthy(Lane::Aggtrade, item.epoch);
                }
            }
            Err(e) => failure(&meter, Lane::Aggtrade, e.into()),
        }
    }
    Ok(())
}
pub async fn depth(
    mut input: mpsc::Receiver<Item>,
    mut snapshots: mpsc::Receiver<Snapshot>,
    desired: watch::Sender<Option<Epoch>>,
    meter: Arc<Meter>,
    config: Config,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut epoch = None;
    let mut book = None;
    let mut seen: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut session = None;
    let mut event_count = 0usize;
    let mut memory = 0usize;
    let mut buffered = 0usize;
    let mut exhausted = false;
    let mut snapshot_open = true;
    let mut tick = tokio::time::interval(delay(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    enum Wake {
        Stop,
        Tick,
        Item(Option<Item>),
        Snapshot(Option<Box<Snapshot>>),
    }
    loop {
        let wake = tokio::select! {
            _=stop.changed()=>Wake::Stop,
            _=tick.tick()=>Wake::Tick,
            item=input.recv()=>Wake::Item(item),
            result=snapshots.recv(),if snapshot_open=>Wake::Snapshot(result.map(Box::new)),
        };
        if matches!(wake, Wake::Stop) && *stop.borrow() {
            break;
        }
        // A handshake may establish the first epoch while recv is pending.
        let health = meter.health(Lane::Depth);
        if health.raw_failed {
            desired.send_replace(None);
            book = None;
        } else if health.connected && health.epoch != epoch {
            if let Some(next) = health.epoch {
                if session != Some(next.session) {
                    seen.clear();
                    event_count = 0;
                    memory = 0;
                    exhausted = false;
                    session = Some(next.session);
                }
                epoch = Some(next);
                buffered = 0;
                if !exhausted {
                    let mut new = BookSync::new(next.session.as_bytes())?;
                    new.begin()?;
                    new.request_snapshot()?;
                    book = Some(new);
                    desired.send_replace(Some(next));
                    meter.count(Lane::Depth, "resync_started_total", 1);
                }
            }
        }
        match wake {
            Wake::Stop | Wake::Tick => {}
            Wake::Item(item) => {
                let Some(item) = item else {
                    break;
                };
                // Track identity for every parsed event in the original epoch, even after a realtime generation reset.
                let source = item.record.event.stream_source.as_ref().ok_or_else(|| {
                    crate::Error::new(crate::Kind::Parse, "stream context missing")
                })?;
                let value = match binance::parse_depth(&item.record.payload, source) {
                    Ok((v, _)) => v,
                    Err(e) => {
                        failure(&meter, Lane::Depth, e.into());
                        continue;
                    }
                };
                meter.count(Lane::Depth, "parsed_total", 1);
                if session != Some(item.epoch.session) || exhausted {
                    continue;
                }
                event_count = event_count.saturating_add(1);
                memory = memory.saturating_add(value.encoded_len().saturating_mul(4));
                if event_count > config.book_session_events || memory > config.book_session_bytes {
                    exhausted = true;
                    book = None;
                    desired.send_replace(None);
                    meter.invalidate(Lane::Depth, "LOCAL_CAPACITY_DROP", "NONE");
                    meter.count(Lane::Depth, "local_capacity_drops_total", 1);
                    continue;
                }
                let content = depth_semantic_bytes(&value);
                let id = value.logical_event_id.clone().unwrap_or_default();
                if let Some(old) = seen.get(&id) {
                    if old != &content {
                        failure(
                            &meter,
                            Lane::Depth,
                            bqip_contracts::ContractError("VENUE_IDENTITY_CONFLICT").into(),
                        );
                        continue;
                    }
                } else {
                    seen.insert(id, content);
                }
                if epoch != Some(item.epoch) || !meter.trust(Lane::Depth, item.epoch) {
                    continue;
                }
                if let Some(book) = &mut book {
                    if book.state() != BookSyncState::Healthy {
                        buffered += 1;
                        if buffered > config.snapshot_buffer {
                            meter.count(Lane::Depth, "local_capacity_drops_total", 1);
                            meter.invalidate(Lane::Depth, "LOCAL_CAPACITY_DROP", "NONE");
                            continue;
                        }
                    }
                    match book.ingest(&value) {
                        Ok(_) => update_health(book, &meter, item.epoch),
                        Err(e) => failure(&meter, Lane::Depth, e.into()),
                    }
                }
            }
            Wake::Snapshot(result) => {
                if let Some(result) = result {
                    if epoch == Some(result.epoch) && meter.trust(Lane::Depth, result.epoch) {
                        if let Some(book) = &mut book {
                            match book.snapshot(&result.value) {
                                Ok(()) => update_health(book, &meter, result.epoch),
                                Err(e) => failure(&meter, Lane::Depth, e.into()),
                            }
                        }
                    }
                } else {
                    snapshot_open = false;
                }
            }
        }
    }
    desired.send_replace(None);
    Ok(())
}
fn update_health(book: &BookSync, meter: &Meter, epoch: Epoch) {
    if book.state() == BookSyncState::Healthy {
        if meter.health(Lane::Depth).realtime != "HEALTHY" {
            meter.count(Lane::Depth, "resync_completed_total", 1);
        }
        meter.healthy(Lane::Depth, epoch);
    }
}
