use bqip_raw_archive::{
    config::Config,
    local,
    s3::{Credentials, S3Store},
    telemetry::Meter,
    uploader::{Archive, Runner},
    Error, Kind, Result,
};
use serde_json::json;
use std::{
    io::Write,
    path::PathBuf,
    sync::{mpsc::sync_channel, Arc},
    time::Duration,
};
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {_=tokio::signal::ctrl_c()=>(),_=s.recv()=>()}
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut path = None;
    let mut once = false;
    let mut reverify = false;
    let mut duration = None;
    let usage = || {
        Error::new(
            Kind::Configuration,
            "usage: --config FILE [--once [--reverify] | --run-for-ms N]",
        )
    };
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" if path.is_none() => {
                path = Some(PathBuf::from(args.next().ok_or_else(usage)?))
            }
            "--once" if !once => once = true,
            "--reverify" if !reverify => reverify = true,
            "--run-for-ms" if duration.is_none() => {
                duration = Some(
                    args.next()
                        .ok_or_else(usage)?
                        .parse::<u64>()
                        .map_err(|_| usage())?,
                )
            }
            _ => return Err(usage()),
        }
    }
    if reverify && !once
        || once && duration.is_some()
        || duration.is_some_and(|d| d == 0 || d > 86400000)
    {
        return Err(usage());
    }
    let bytes = local::read_bounded(&path.ok_or_else(usage)?, local::CONTROL_LIMIT)?;
    let c: Config = serde_json::from_slice(&bytes).map_err(|_| usage())?;
    let c = c.validate()?;
    let store = Arc::new(S3Store::new(&c, Credentials::from_env()?)?);
    let (tx, rx) = sync_channel::<serde_json::Value>(1024);
    let logger = std::thread::spawn(move || -> std::io::Result<()> {
        let out = std::io::stdout();
        let mut out = out.lock();
        while let Ok(v) = rx.recv() {
            serde_json::to_writer(&mut out, &v)?;
            out.write_all(b"\n")?;
        }
        out.flush()
    });
    let meter = Arc::new(Meter::new(Some(tx)));
    let archive = Arc::new(Archive::new(
        c.clone(),
        store,
        meter.clone(),
        env!("BQIP_CODE_COMMIT"),
    )?);
    let result = if once {
        archive.once_until(reverify, shutdown_signal()).await
    } else {
        let mut runner = Runner::new(archive.clone());
        let mut interval = tokio::time::interval(Duration::from_millis(c.scan_interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let deadline = async {
            match duration {
                Some(n) => tokio::time::sleep(Duration::from_millis(n)).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline);
        let signal = shutdown_signal();
        tokio::pin!(signal);
        let r = loop {
            tokio::select! {_=&mut deadline=>break Ok(()),_=&mut signal=>break Ok(()),_=interval.tick()=>{if let Err(e)=runner.tick().await{break Err(e);}meter.event(json!({"summary":meter.summary()}));}}
        };
        let drained = runner.drain().await;
        r.and(drained)
    };
    meter.event(json!({"summary":meter.summary()}));
    drop(archive);
    drop(meter);
    logger
        .join()
        .map_err(|_| Error::new(Kind::StateIo, "log worker failed"))?
        .map_err(|_| Error::new(Kind::StateIo, "log output failed"))?;
    result
}
#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{}", json!({"error":e}));
        std::process::exit(1);
    }
}
