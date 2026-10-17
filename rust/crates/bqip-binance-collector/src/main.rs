use bqip_binance_collector::{
    config::Config,
    recovery,
    runtime::{self, Endpoints},
    Error, Kind, Result,
};
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;

#[tokio::main(worker_threads = 4)]
async fn main() {
    if let Err(e) = entry().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
async fn entry() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["default-config"] {
        println!(
            "{}",
            serde_json::to_string_pretty(&Config::default())
                .map_err(|e| Error::new(Kind::Configuration, e))?
        );
        return Ok(());
    }
    if args.len() != 9
        || args[0] != "run"
        || args[1] != "--config"
        || args[3] != "--seconds"
        || args[5] != "--log"
        || args[7] != "--report"
    {
        return Err(Error::new(
            Kind::Configuration,
            "usage: run --config FILE --seconds N --log FILE --report FILE; or default-config",
        ));
    }
    let config =
        Config::parse(&std::fs::read(&args[2]).map_err(|e| Error::new(Kind::Configuration, e))?)?;
    let seconds = args[4]
        .parse::<u64>()
        .map_err(|e| Error::new(Kind::Configuration, e))?;
    let report = PathBuf::from(&args[8]);
    if report.exists() {
        return Err(Error::new(Kind::Configuration, "report already exists"));
    }
    let (tx, rx) = watch::channel(false);
    #[cfg(unix)]
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| Error::new(Kind::Configuration, e))?;
    let signal = tokio::spawn(async move {
        #[cfg(unix)]
        tokio::select! {r=tokio::signal::ctrl_c()=>r.map_err(|e|Error::new(Kind::Shutdown,e))?,_=term.recv()=>{}}
        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| Error::new(Kind::Shutdown, e))?;
        tx.send_replace(true);
        Ok::<(), Error>(())
    });
    let result = runtime::run(
        config,
        Endpoints::live(),
        Duration::from_secs(seconds),
        PathBuf::from(&args[6]),
        rx,
    )
    .await?;
    if signal.is_finished() {
        signal.await.map_err(|e| Error::new(Kind::Shutdown, e))??;
    } else {
        signal.abort();
    }
    let success = result["shutdown_ok"] == true;
    let bytes = serde_json::to_vec_pretty(&result).map_err(|e| Error::new(Kind::Shutdown, e))?;
    tokio::task::spawn_blocking(move || recovery::atomic_bytes(&report, &bytes))
        .await
        .map_err(|e| Error::new(Kind::Shutdown, e))??;
    if !success {
        return Err(Error::new(
            Kind::Shutdown,
            "collector failed; inspect report",
        ));
    }
    Ok(())
}
