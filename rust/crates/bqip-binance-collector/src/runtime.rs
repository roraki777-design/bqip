use crate::{
    capture::{Ingress, Lane},
    config::Config,
    http::{self, Rest},
    raw_writer::{Faults, RawWriter},
    realtime,
    recovery::{self, SpoolGuard},
    telemetry::Meter,
    ws::Source,
    Error, Kind, Result,
};
use serde_json::{json, Value};
use std::{
    future::Future,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};

pub const CODE_COMMIT: &str = env!("BQIP_CODE_COMMIT");
#[derive(Clone)]
pub struct Endpoints {
    pub depth: String,
    pub aggtrade: String,
    pub rest: String,
}
impl Endpoints {
    pub fn live() -> Self {
        Self {
            depth: Lane::Depth.endpoint().into(),
            aggtrade: Lane::Aggtrade.endpoint().into(),
            rest: Lane::Rest.endpoint().into(),
        }
    }
    fn validate(&self) -> Result<bool> {
        let live = Self::live();
        if self.depth == live.depth && self.aggtrade == live.aggtrade && self.rest == live.rest {
            return Ok(true);
        }
        for (text, scheme) in [
            (&self.depth, "ws"),
            (&self.aggtrade, "ws"),
            (&self.rest, "http"),
        ] {
            let url = reqwest::Url::parse(text).map_err(|e| Error::new(Kind::Configuration, e))?;
            let ip = url
                .host_str()
                .and_then(|s| s.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok());
            if url.scheme() != scheme
                || !ip.is_some_and(|ip| ip.is_loopback())
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::new(
                    Kind::Configuration,
                    "only fixed Binance URLs or numeric loopback mocks are allowed",
                ));
            }
        }
        Ok(false)
    }
}
async fn tracked(
    future: impl Future<Output = Result<()>>,
    meter: Arc<Meter>,
    lane: Lane,
) -> Result<()> {
    let result = future.await;
    if let Err(e) = &result {
        if matches!(
            e.kind,
            Kind::RawWrite | Kind::DurabilityFence | Kind::SealManifest
        ) {
            meter.raw_failure(lane, e);
        } else {
            meter.invalidate(lane, "TASK_FAILED", "POSSIBLE");
        }
        meter.event(
            lane,
            "TASK_FAILED",
            "POSSIBLE",
            json!({"error":e.to_string()}),
        );
    }
    result
}
fn utc_ns() -> Result<String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::new(Kind::Configuration, e))?
        .as_nanos()
        .to_string())
}
pub async fn run(
    config: Config,
    endpoints: Endpoints,
    duration: Duration,
    log: PathBuf,
    mut external_stop: watch::Receiver<bool>,
) -> Result<Value> {
    let live = endpoints.validate()?;
    config.validate(live)?;
    if duration.is_zero() || duration > Duration::from_secs(86400) {
        return Err(Error::new(
            Kind::Configuration,
            "finite duration must be 1ns..24h",
        ));
    }
    let ingress = Arc::new(Ingress::new(CODE_COMMIT)?);
    let start = utc_ns()?;
    let cfg = config.clone();
    let (guard, meter, logger) = tokio::task::spawn_blocking(move || {
        let guard = SpoolGuard::acquire(&cfg.spool_root)?;
        let (meter, logger) = Meter::new(&log, cfg.log_capacity)?;
        recovery::recover_spool(&cfg.spool_root, &meter)?;
        Ok::<_, Error>((guard, meter, logger))
    })
    .await
    .map_err(|e| Error::new(Kind::Configuration, e))??;
    meter.event(Lane::Rest,"STARTUP","NONE",json!({"config":config,"collector_instance_id":ingress.id.to_string(),"code_commit":CODE_COMMIT,"live":live}));
    let writers: Vec<_> = Lane::ALL
        .into_iter()
        .map(|lane| RawWriter::start(config.clone(), lane, meter.clone(), Faults::default()))
        .collect();
    let (depth_tx, depth_rx) = mpsc::channel(config.realtime_queues[0]);
    let (trade_tx, trade_rx) = mpsc::channel(config.realtime_queues[1]);
    let (request_tx, request_rx) = watch::channel(None);
    let (snapshot_tx, snapshot_rx) = mpsc::channel(1);
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = Vec::new();
    for (lane, url, tx) in [
        (Lane::Depth, endpoints.depth, depth_tx),
        (Lane::Aggtrade, endpoints.aggtrade, trade_tx),
    ] {
        let source = Source {
            lane,
            url,
            config: config.clone(),
            ingress: ingress.clone(),
            raw: writers[lane.index()].sink.clone(),
            realtime: tx,
            meter: meter.clone(),
        };
        tasks.push(tokio::spawn(tracked(
            source.run(stop_rx.clone()),
            meter.clone(),
            lane,
        )));
    }
    let rest = Rest {
        client: http::client(&config)?,
        base: endpoints.rest,
        config: config.clone(),
        ingress: ingress.clone(),
        raw: writers[2].sink.clone(),
        meter: meter.clone(),
    };
    tasks.push(tokio::spawn(tracked(
        rest.run(request_rx, snapshot_tx, stop_rx.clone()),
        meter.clone(),
        Lane::Rest,
    )));
    tasks.push(tokio::spawn(tracked(
        realtime::depth(
            depth_rx,
            snapshot_rx,
            request_tx,
            meter.clone(),
            config.clone(),
            stop_rx.clone(),
        ),
        meter.clone(),
        Lane::Depth,
    )));
    tasks.push(tokio::spawn(tracked(
        realtime::trades(trade_rx, meter.clone()),
        meter.clone(),
        Lane::Aggtrade,
    )));
    if !*external_stop.borrow() {
        tokio::select! {_=tokio::time::sleep(duration)=>{},_=external_stop.changed()=>{}}
    }
    stop_tx.send_replace(true);
    let mut errors = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errors.push(e.to_string()),
            Err(e) => errors.push(e.to_string()),
        }
    }
    for writer in writers {
        if let Err(e) = writer.shutdown().await {
            errors.push(e.to_string());
        }
    }
    let root = config.spool_root.clone();
    let verification =
        match tokio::task::spawn_blocking(move || recovery::verify_spool(&root)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                errors.push(e.to_string());
                Value::Null
            }
            Err(e) => {
                errors.push(e.to_string());
                Value::Null
            }
        };
    let summary = meter.summary();
    let finish = tokio::task::spawn_blocking(move || {
        let logs = logger.finish();
        let ownership = guard.release();
        logs?;
        ownership
    })
    .await;
    match finish {
        Ok(Ok(())) => {}
        Ok(Err(e)) => errors.push(e.to_string()),
        Err(e) => errors.push(e.to_string()),
    }
    Ok(
        json!({"schema":"BQIP-COLLECTOR-RUN-V1","start_utc_unix_ns":start,"end_utc_unix_ns":utc_ns()?,"collector_commit":CODE_COMMIT,"collector_instance_id":ingress.id.to_string(),"profile_hash":recovery::hex(&bqip_contracts::binance::profile_hash()?),"public_live_endpoints":live,"config":config,"summary":summary,"verification":verification,"shutdown_ok":errors.is_empty(),"errors":errors}),
    )
}
