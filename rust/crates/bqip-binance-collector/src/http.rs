use crate::{
    capture::{Ingress, Lane, Record},
    config::{backoff, delay, Config},
    raw_writer::RawSink,
    recovery,
    telemetry::{Epoch, Meter},
    Error, Kind, Result,
};
use bqip_contracts::{binance, proto::*};
use prost::Message;
use serde_json::json;
use std::sync::Arc;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use uuid::Uuid;

pub fn client(config: &Config) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .connect_timeout(delay(config.connect_timeout_ms))
        .build()
        .map_err(|e| Error::new(Kind::Configuration, e))
}
pub struct Rest {
    pub client: reqwest::Client,
    pub base: String,
    pub config: Config,
    pub ingress: Arc<Ingress>,
    pub raw: RawSink,
    pub meter: Arc<Meter>,
}
#[derive(Clone)]
pub struct Snapshot {
    pub epoch: Epoch,
    pub value: BinanceDepthSnapshot,
}
pub struct RateGuard {
    next: Instant,
    attempts: u32,
}
impl Default for RateGuard {
    fn default() -> Self {
        Self {
            next: Instant::now(),
            attempts: 0,
        }
    }
}
impl RateGuard {
    pub fn ready(&self) -> bool {
        Instant::now() >= self.next
    }
    pub fn consume(&mut self, c: &Config) -> Result<()> {
        if !self.ready() {
            return Err(Error::new(Kind::ResyncExhausted, "snapshot cooldown"));
        }
        self.attempts += 1;
        let wait = if self.attempts >= c.snapshot_attempt_budget {
            self.attempts = 0;
            delay(c.snapshot_cooldown_ms)
        } else {
            backoff(
                c.snapshot_retry_min_ms,
                c.snapshot_retry_max_ms,
                self.attempts - 1,
            )
        };
        self.next = Instant::now() + wait;
        Ok(())
    }
}
impl Rest {
    pub async fn capture(&self, snapshot: bool) -> Result<Arc<Record>> {
        let endpoint = if snapshot {
            "/fapi/v1/depth"
        } else {
            "/fapi/v1/exchangeInfo"
        };
        let connection = Uuid::new_v4();
        let session = Uuid::new_v4();
        let query = if snapshot {
            vec![("limit", "1000"), ("symbol", "BTCUSDT")]
        } else {
            vec![]
        };
        let mut response = self
            .client
            .get(format!("{}{endpoint}", self.base))
            .query(&query)
            .header("Accept-Encoding", "identity")
            .header("Connection", "close")
            .timeout(delay(self.config.http_timeout_ms))
            .send()
            .await
            .map_err(|e| crate::error::network_error(Kind::HttpRequest, &e))?;
        let status = response.status().as_u16();
        let encoding = response
            .headers()
            .get("content-encoding")
            .map(|v| v.as_bytes().to_vec());
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| crate::error::network_error(Kind::HttpRequest, &e))?
        {
            if body.len().saturating_add(chunk.len()) > self.config.http_max_bytes {
                return Err(Error::new(
                    Kind::HttpRequest,
                    "body exceeds bound; incomplete application response",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let stamp = self.ingress.stamp()?;
        self.meter.count(Lane::Rest, "events_received_total", 1);
        let mut record = self.ingress.record(
            Lane::Rest,
            connection,
            session,
            PayloadKind::Binary,
            body,
            stamp,
        )?;
        record.event.endpoint = Some(endpoint.into());
        record.event.http_content_encoding = encoding;
        record.event.http_source = Some(HttpSourceContext {
            lineage: Some(SourceLineage {
                venue_profile_id: Some(binance::PROFILE_ID.into()),
                venue_profile_hash: Some(binance::profile_hash()?.to_vec()),
                capture_id: record.event.capture_id.clone(),
                raw_content_hash: record.event.raw_content_hash.clone(),
                receive_timestamp_ns: record.event.receive_timestamp_ns,
            }),
            transport: Some(SourceTransport::HttpRest as i32),
            method: Some("GET".into()),
            endpoint: Some(endpoint.into()),
            query_parameters: query
                .into_iter()
                .map(|(k, v)| QueryParameter {
                    name: Some(k.into()),
                    value: Some(v.into()),
                })
                .collect(),
            http_status: Some(u32::from(status)),
        });
        let record = Arc::new(record);
        self.raw.append(record.clone()).await?;
        Ok(record)
    }
    pub fn successful(record: &Record) -> Result<&HttpSourceContext> {
        let source = record
            .event
            .http_source
            .as_ref()
            .ok_or_else(|| Error::new(Kind::HttpStatus, "missing HTTP evidence"))?;
        if source.http_status != Some(200) {
            return Err(Error::new(
                Kind::HttpStatus,
                format!("status {:?}", source.http_status),
            ));
        }
        if record
            .event
            .http_content_encoding
            .as_deref()
            .is_some_and(|s| s != b"identity")
        {
            return Err(Error::new(
                Kind::Parse,
                "encoded HTTP body preserved unchanged; parsing forbidden",
            ));
        }
        Ok(source)
    }
    pub async fn run(
        self,
        desired: watch::Receiver<Option<Epoch>>,
        results: mpsc::Sender<Snapshot>,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut rate = RateGuard::default();
        let mut metadata_due = Instant::now();
        let mut semantic = None;
        let mut previous_version = None;
        let mut completed: Option<Epoch> = None;
        let mut initial_snapshot = true;
        while !*stop.borrow() && !self.meter.health(Lane::Rest).raw_failed {
            if Instant::now() >= metadata_due {
                // A complete response is never cancelled between stamp and RAW ACK.
                let result = self.capture(false).await;
                match result {
                    Ok(record) => {
                        let parsed = Self::successful(&record).and_then(|source| {
                            let version = format!("{}:{}", self.ingress.id, record.seq());
                            binance::parse_exchange_info(&record.payload, source, &version)
                                .map(|p| p.0)
                                .map_err(Error::from)
                        });
                        match parsed {
                            Ok(mut value) => {
                                let mut content = value.clone();
                                content.metadata_version = None;
                                content.known_from_ns = None;
                                content.source = None;
                                let bytes = content.encode_to_vec();
                                if semantic.as_ref() != Some(&bytes) {
                                    value.supersedes_version = previous_version.clone();
                                    let path = self.config.spool_root.join("metadata");
                                    let file = path.join(format!(
                                        "{}-{}.pb",
                                        self.ingress.id,
                                        record.seq()
                                    ));
                                    let output = value.encode_to_vec();
                                    tokio::task::spawn_blocking(move || {
                                        recovery::create_dirs(&path)?;
                                        recovery::atomic_bytes(&file, &output)
                                    })
                                    .await
                                    .map_err(|e| Error::new(Kind::RawWrite, e))??;
                                    previous_version = value.metadata_version;
                                    semantic = Some(bytes);
                                    self.meter
                                        .count(Lane::Rest, "metadata_observations_total", 1);
                                }
                                self.meter.count(Lane::Rest, "valid_exchange_info_total", 1);
                            }
                            Err(e) => self.failure(&e, false),
                        }
                    }
                    Err(e) => {
                        if self.meter.health(Lane::Rest).raw_failed {
                            return Err(e);
                        }
                        self.failure(&e, false);
                    }
                }
                metadata_due = Instant::now() + delay(self.config.metadata_refresh_ms);
            }
            if *stop.borrow() {
                break;
            }
            let target = *desired.borrow();
            if (initial_snapshot || target.is_some() && target != completed) && rate.ready() {
                rate.consume(&self.config)?;
                self.meter.count(Lane::Rest, "snapshot_requests_total", 1);
                let result = self.capture(true).await;
                match result.and_then(|record| {
                    binance::parse_snapshot(&record.payload, Self::successful(&record)?)
                        .map(|v| v.0)
                        .map_err(Error::from)
                }) {
                    Ok(value) => {
                        self.meter.count(Lane::Rest, "valid_snapshots_total", 1);
                        initial_snapshot = false;
                        completed = target;
                        if let Some(epoch) = target {
                            if results.try_send(Snapshot { epoch, value }).is_err() {
                                self.meter
                                    .invalidate(Lane::Depth, "LOCAL_CAPACITY_DROP", "NONE");
                                self.meter
                                    .count(Lane::Depth, "local_capacity_drops_total", 1);
                            }
                        }
                    }
                    Err(e) => {
                        initial_snapshot = false;
                        if self.meter.health(Lane::Rest).raw_failed {
                            return Err(e);
                        }
                        self.failure(&e, true);
                    }
                }
            }
            tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(delay(10))=>{}}
        }
        Ok(())
    }
    fn failure(&self, error: &Error, snapshot: bool) {
        self.meter.count(
            Lane::Rest,
            if snapshot {
                "snapshot_failures_total"
            } else {
                "metadata_failures_total"
            },
            1,
        );
        let cause = match error.kind {
            Kind::Parse | Kind::ProfileMismatch => {
                self.meter.count(Lane::Rest, "parse_failures_total", 1);
                if error.kind == Kind::ProfileMismatch {
                    "PROFILE_MISMATCH"
                } else {
                    "PARSE_FAILURE"
                }
            }
            _ => "HTTP_FAILURE",
        };
        self.meter.event(
            Lane::Rest,
            cause,
            if matches!(error.kind, Kind::HttpRequest) {
                "POSSIBLE"
            } else {
                "NONE"
            },
            json!({"kind":error.kind,"error":error.detail,"snapshot":snapshot}),
        );
    }
}
