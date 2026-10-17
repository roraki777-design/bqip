use crate::{capture::Lane, Error, Kind, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    sync::{
        mpsc::{sync_channel, SyncSender},
        Arc, Mutex, MutexGuard,
    },
    thread::JoinHandle,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch {
    pub session: Uuid,
    pub generation: u64,
}
#[derive(Clone, Debug)]
pub struct Health {
    pub raw_failed: bool,
    pub realtime: &'static str,
    pub epoch: Option<Epoch>,
    pub connection: Option<Uuid>,
    pub connected: bool,
    pub fenced_seq: u64,
}
impl Default for Health {
    fn default() -> Self {
        Self {
            raw_failed: false,
            realtime: "EMPTY",
            epoch: None,
            connection: None,
            connected: false,
            fenced_seq: 0,
        }
    }
}
struct State {
    counters: [BTreeMap<&'static str, u64>; 3],
    health: [Health; 3],
    dropped_logs: u64,
}
enum Log {
    Event(Value),
    Finish,
}
pub struct Meter {
    state: Mutex<State>,
    logs: SyncSender<Log>,
}
pub struct Logger {
    logs: SyncSender<Log>,
    thread: JoinHandle<Result<()>>,
}
impl Logger {
    pub fn finish(self) -> Result<()> {
        self.logs
            .send(Log::Finish)
            .map_err(|e| Error::new(Kind::Shutdown, e))?;
        self.thread
            .join()
            .map_err(|_| Error::new(Kind::Shutdown, "log thread panic"))?
    }
}
impl Meter {
    pub fn new(path: &Path, capacity: usize) -> Result<(Arc<Self>, Logger)> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| Error::new(Kind::Configuration, e))?;
        let (tx, rx) = sync_channel(capacity);
        let thread = std::thread::spawn(move || {
            let mut out = BufWriter::new(file);
            while let Ok(Log::Event(value)) = rx.recv() {
                serde_json::to_writer(&mut out, &value)
                    .map_err(|e| Error::new(Kind::Shutdown, e))?;
                out.write_all(b"\n")
                    .map_err(|e| Error::new(Kind::Shutdown, e))?;
            }
            out.flush()
                .and_then(|()| out.get_ref().sync_all())
                .map_err(|e| Error::new(Kind::Shutdown, e))
        });
        Ok((
            Arc::new(Self {
                state: Mutex::new(State {
                    counters: std::array::from_fn(|_| {
                        [
                            "events_received_total",
                            "raw_append_accepted_total",
                            "raw_durability_fenced_total",
                            "raw_segments_sealed_total",
                            "raw_bytes_total",
                            "raw_writer_failures_total",
                            "local_capacity_drops_total",
                            "parse_failures_total",
                            "venue_gaps_total",
                            "identity_conflicts_total",
                            "resync_started_total",
                            "resync_completed_total",
                            "snapshot_requests_total",
                            "snapshot_failures_total",
                            "websocket_connects_total",
                            "websocket_disconnects_total",
                            "reconnects_total",
                            "ping_received_total",
                            "spool_bytes",
                            "parsed_total",
                        ]
                        .into_iter()
                        .map(|name| (name, 0))
                        .collect()
                    }),
                    health: std::array::from_fn(|_| Health::default()),
                    dropped_logs: 0,
                }),
                logs: tx.clone(),
            }),
            Logger { logs: tx, thread },
        ))
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poison| {
            let mut state = poison.into_inner();
            for health in &mut state.health {
                health.raw_failed = true;
                health.realtime = "BROKEN";
            }
            state
        })
    }
    pub fn count(&self, lane: Lane, name: &'static str, n: u64) {
        let mut s = self.state();
        let v = s.counters[lane.index()].entry(name).or_default();
        *v = v.saturating_add(n);
    }
    pub fn value(&self, lane: Lane, name: &str) -> u64 {
        self.state().counters[lane.index()]
            .get(name)
            .copied()
            .unwrap_or(0)
    }
    pub fn event(&self, lane: Lane, cause: &str, loss: &str, detail: Value) {
        let h = self.health(lane);
        if self.logs.try_send(Log::Event(json!({"lane":lane,"cause":cause,"raw_loss_status":loss,"raw_failed":h.raw_failed,"realtime":h.realtime,"connection_id":h.connection.map(|x|x.to_string()),"session_id":h.epoch.map(|x|x.session.to_string()),"detail":detail}))).is_err() {let mut s=self.state();s.dropped_logs=s.dropped_logs.saturating_add(1);}
    }
    pub fn health(&self, lane: Lane) -> Health {
        self.state().health[lane.index()].clone()
    }
    pub fn open(&self, lane: Lane, connection: Uuid, session: Uuid) {
        let mut s = self.state();
        let h = &mut s.health[lane.index()];
        h.epoch = Some(Epoch {
            session,
            generation: 0,
        });
        h.connection = Some(connection);
        h.connected = true;
        h.realtime = "BUFFERING";
    }
    pub fn invalidate(&self, lane: Lane, cause: &str, loss: &str) {
        {
            let mut s = self.state();
            let h = &mut s.health[lane.index()];
            h.realtime = "BROKEN";
            if let Some(e) = &mut h.epoch {
                match e.generation.checked_add(1) {
                    Some(n) => e.generation = n,
                    None => h.raw_failed = true,
                }
            }
        }
        self.event(lane, cause, loss, Value::Null);
    }
    pub fn raw_failure(&self, lane: Lane, error: &Error) {
        let first = {
            let mut s = self.state();
            let h = &mut s.health[lane.index()];
            let first = !h.raw_failed;
            h.raw_failed = true;
            h.realtime = "BROKEN";
            first
        };
        if first {
            self.count(lane, "raw_writer_failures_total", 1);
        }
        self.event(
            lane,
            "RAW_WRITE_FAILURE",
            "POSSIBLE",
            json!({"kind":error.kind,"error":error.detail,"state":"STORAGE_FAILURE"}),
        );
    }
    pub fn disconnect(&self, lane: Lane, loss: &str) {
        self.state().health[lane.index()].connected = false;
        self.invalidate(lane, "CONNECTION_CLOSED", loss);
    }
    pub fn trust(&self, lane: Lane, epoch: Epoch) -> bool {
        let h = self.health(lane);
        !h.raw_failed && h.connected && h.epoch == Some(epoch)
    }
    pub fn healthy(&self, lane: Lane, epoch: Epoch) {
        let mut s = self.state();
        let h = &mut s.health[lane.index()];
        if !h.raw_failed && h.connected && h.epoch == Some(epoch) {
            h.realtime = "HEALTHY";
        }
    }
    pub fn fenced(&self, lane: Lane, seq: u64) {
        self.state().health[lane.index()].fenced_seq = seq;
    }
    pub fn summary(&self) -> Value {
        let s = self.state();
        json!({"dropped_log_records":s.dropped_logs,"lanes":Lane::ALL.map(|lane| {
            let h=&s.health[lane.index()];json!({"lane":lane,"counters":s.counters[lane.index()],"raw_failed":h.raw_failed,"realtime":h.realtime,"connected":h.connected,"active_session_id":h.epoch.map(|e|e.session.to_string()),"connection_id":h.connection.map(|x|x.to_string()),"last_fenced_capture_seq":h.fenced_seq.to_string()})
        })})
    }
}
// Flush directory entries on POSIX; validate the directory on Windows.
// Windows does not provide the POSIX directory-entry durability guarantee here.
pub fn sync_dir(path: &Path) -> Result<()> {
    bqip_capture_format::sync_dir(path).map_err(|e| Error::new(Kind::SealManifest, e))
}
