//! Offline state-machine proof, scoped to one original session. No service/runtime.
use super::{compare, validation};
use crate::{proto::*, ContractError, Result};
use prost::Message;
use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookSyncState {
    Empty,
    Buffering,
    SnapshotRequired,
    Bridging,
    Healthy,
    Degraded,
    ResyncRequired,
}
impl BookSyncState {
    pub fn token(self) -> &'static str {
        match self {
            Self::Empty => "EMPTY",
            Self::Buffering => "BUFFERING",
            Self::SnapshotRequired => "SNAPSHOT_REQUIRED",
            Self::Bridging => "BRIDGING",
            Self::Healthy => "HEALTHY",
            Self::Degraded => "DEGRADED",
            Self::ResyncRequired => "RESYNC_REQUIRED",
        }
    }
}
fn key(v: &PriceLevel) -> String {
    let p = v.price.as_ref().expect("validated level");
    format!(
        "{}:{}",
        p.coefficient.as_deref().unwrap_or_default(),
        p.scale.unwrap_or_default()
    )
}
fn semantic(v: &BinanceDepthUpdate) -> Vec<u8> {
    let side = |levels: &[PriceLevel]| -> Vec<PriceLevel> {
        levels
            .iter()
            .map(|v| (key(v), v.clone()))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect()
    };
    BinanceDepthUpdate {
        source: None,
        logical_event_id: None,
        bids: side(&v.bids),
        asks: side(&v.asks),
        ..v.clone()
    }
    .encode_to_vec()
}

pub struct BookSync {
    session: Vec<u8>,
    state: BookSyncState,
    transitions: Vec<BookSyncState>,
    cause: Option<&'static str>,
    buffer: VecDeque<BinanceDepthUpdate>,
    seen: HashMap<Vec<u8>, Vec<u8>>,
    snapshot: Option<BinanceDepthSnapshot>,
    previous: Option<u64>,
    bids: BTreeMap<String, PriceLevel>,
    asks: BTreeMap<String, PriceLevel>,
}
impl BookSync {
    pub fn new(original_session_id: &[u8]) -> Result<Self> {
        if original_session_id.len() != 16 {
            return Err(ContractError("INVALID_ORIGINAL_SESSION"));
        }
        Ok(Self {
            session: original_session_id.into(),
            state: BookSyncState::Empty,
            transitions: vec![BookSyncState::Empty],
            cause: None,
            buffer: VecDeque::new(),
            seen: HashMap::new(),
            snapshot: None,
            previous: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        })
    }
    pub fn state(&self) -> BookSyncState {
        self.state
    }
    pub fn transitions(&self) -> &[BookSyncState] {
        &self.transitions
    }
    pub fn cause(&self) -> Option<&'static str> {
        self.cause
    }
    fn transition(&mut self, state: BookSyncState) {
        self.state = state;
        self.transitions.push(state);
    }
    fn fail(&mut self, cause: &'static str) -> ContractError {
        self.cause = Some(cause);
        self.transition(BookSyncState::Degraded);
        self.transition(BookSyncState::ResyncRequired);
        ContractError(cause)
    }
    pub fn begin(&mut self) -> Result<()> {
        if self.state != BookSyncState::Empty {
            return Err(ContractError("INVALID_SYNC_STATE"));
        }
        self.transition(BookSyncState::Buffering);
        Ok(())
    }
    pub fn request_snapshot(&mut self) -> Result<()> {
        if self.state != BookSyncState::Buffering {
            return Err(ContractError("INVALID_SYNC_STATE"));
        }
        self.transition(BookSyncState::SnapshotRequired);
        Ok(())
    }
    pub fn ingest(&mut self, event: &BinanceDepthUpdate) -> Result<&'static str> {
        if !matches!(
            self.state,
            BookSyncState::Buffering
                | BookSyncState::SnapshotRequired
                | BookSyncState::Bridging
                | BookSyncState::Healthy
        ) {
            return Err(ContractError("INVALID_SYNC_STATE"));
        }
        if let Err(error) = validation::depth(event) {
            return Err(self.fail(error.0));
        }
        if event
            .source
            .as_ref()
            .and_then(|s| s.original_session_id.as_deref())
            != Some(self.session.as_slice())
        {
            return Err(self.fail("ORIGINAL_SESSION_MISMATCH"));
        }
        let id = event.logical_event_id.as_ref().expect("validated identity");
        let content = semantic(event);
        if let Some(previous) = self.seen.get(id) {
            if *previous != content {
                return Err(self.fail("VENUE_IDENTITY_CONFLICT"));
            }
            return Ok("DUPLICATE");
        }
        self.seen.insert(id.clone(), content);
        if self.state == BookSyncState::Healthy {
            self.apply(event, true)?;
            return Ok("APPLIED");
        }
        self.buffer.push_back(event.clone());
        self.bridge()?;
        Ok(if self.state == BookSyncState::Healthy {
            "APPLIED"
        } else {
            "BUFFERED"
        })
    }
    pub fn snapshot(&mut self, snapshot: &BinanceDepthSnapshot) -> Result<()> {
        if !matches!(
            self.state,
            BookSyncState::SnapshotRequired | BookSyncState::Bridging
        ) {
            return Err(ContractError("INVALID_SYNC_STATE"));
        }
        validation::snapshot(snapshot)?;
        self.snapshot = Some(snapshot.clone());
        self.transition(BookSyncState::Bridging);
        self.bridge()
    }
    fn bridge(&mut self) -> Result<()> {
        if self.state != BookSyncState::Bridging {
            return Ok(());
        }
        let snapshot = self
            .snapshot
            .as_ref()
            .expect("bridging requires snapshot")
            .clone();
        let sequence = snapshot.last_update_id.expect("validated snapshot");
        self.buffer
            .retain(|v| v.final_update_id.is_some_and(|n| n >= sequence));
        if self
            .buffer
            .front()
            .is_none_or(|v| v.first_update_id.is_some_and(|n| n > sequence))
        {
            return Ok(());
        }
        self.bids.clear();
        self.asks.clear();
        Self::replace(&snapshot.bids, &mut self.bids);
        Self::replace(&snapshot.asks, &mut self.asks);
        let first = self.buffer.pop_front().expect("bridge exists");
        self.apply(&first, false)?;
        self.transition(BookSyncState::Healthy);
        while let Some(event) = self.buffer.pop_front() {
            self.apply(&event, true)?;
        }
        Ok(())
    }
    fn replace(levels: &[PriceLevel], target: &mut BTreeMap<String, PriceLevel>) {
        for level in levels {
            if level
                .quantity
                .as_ref()
                .and_then(|q| q.coefficient.as_deref())
                == Some("0")
            {
                target.remove(&key(level));
            } else {
                target.insert(key(level), level.clone());
            }
        }
    }
    fn apply(&mut self, event: &BinanceDepthUpdate, continuation: bool) -> Result<()> {
        if continuation && event.previous_final_update_id != self.previous {
            return Err(self.fail("VENUE_GAP"));
        }
        Self::replace(&event.bids, &mut self.bids);
        Self::replace(&event.asks, &mut self.asks);
        self.previous = event.final_update_id;
        Ok(())
    }
    pub fn trusted_book(&self) -> Result<(Vec<PriceLevel>, Vec<PriceLevel>)> {
        if self.state != BookSyncState::Healthy {
            return Err(ContractError("BOOK_NOT_HEALTHY"));
        }
        let order = |side: &BTreeMap<String, PriceLevel>, reverse: bool| {
            let mut values: Vec<_> = side.values().cloned().collect();
            values.sort_by(|a, b| {
                let order = compare(
                    a.price.as_ref().expect("validated"),
                    b.price.as_ref().expect("validated"),
                );
                if reverse {
                    order.reverse()
                } else {
                    order
                }
            });
            values
        };
        Ok((order(&self.bids, true), order(&self.asks, false)))
    }
}
