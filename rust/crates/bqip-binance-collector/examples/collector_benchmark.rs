//! Finite synthetic preservation measurement; no network and no performance threshold.
use bqip_binance_collector::{
    capture::{Ingress, Lane},
    config::Config,
    raw_writer::{Faults, RawWriter},
    realtime,
    recovery::{self, SpoolGuard, LIMITS},
    runtime::CODE_COMMIT,
    telemetry::Meter,
};
use bqip_capture_format as bqrc;
use bqip_contracts::proto::PayloadKind;
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use uuid::Uuid;
const PAYLOAD:&[u8]=br#"{"e":"depthUpdate","E":2,"T":1,"s":"BTCUSDT","U":98,"u":102,"pu":97,"b":[["100","2"]],"a":[["101","3"]],"ps":"BTCUSDT","st":1}"#;
fn percentiles(mut values: Vec<u128>) -> Value {
    values.sort();
    if values.is_empty() {
        return json!({"samples":0});
    }
    let p = |n: usize| values[(values.len() * n).div_ceil(100) - 1].to_string();
    json!({"samples":values.len(),"p50_ns":p(50),"p95_ns":p(95),"p99_ns":p(99)})
}
fn logical_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        total += if path.is_dir() {
            logical_bytes(&path)?
        } else {
            path.metadata()?.len()
        };
    }
    Ok(total)
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: collector_benchmark OUTPUT_DIRECTORY COUNT".into());
    }
    let out = Path::new(&args[1]);
    let count: usize = args[2].parse()?;
    if !(1..=100000).contains(&count) {
        return Err("COUNT must be 1..100000".into());
    }
    fs::create_dir(out)?;
    let ingress = Arc::new(Ingress::new(CODE_COMMIT)?);
    let mut cases = Vec::new();
    for rate in [100u64, 1000, 10000, 0] {
        let dir = out.join(format!("rate-{rate}"));
        fs::create_dir(&dir)?;
        let config = Config {
            spool_root: dir.join("spool"),
            raw_queues: [4, 4, 1],
            realtime_queues: [1, 1],
            fence_ms: 100,
            segment_max_age_ms: 1000,
            ..Config::default()
        };
        let guard = SpoolGuard::acquire(&config.spool_root)?;
        let (meter, logger) = Meter::new(&dir.join("events.jsonl"), 8192)?;
        recovery::recover_spool(&config.spool_root, &meter)?;
        let c = Uuid::new_v4();
        let s = Uuid::new_v4();
        meter.open(Lane::Depth, c, s);
        let writer = RawWriter::start(
            config.clone(),
            Lane::Depth,
            meter.clone(),
            Faults::default(),
        );
        let (tx, _stalled) = mpsc::channel(1);
        let mut acks = Vec::with_capacity(count);
        let start = Instant::now();
        for index in 0..count {
            if rate > 0 {
                let due = start + Duration::from_nanos(index as u64 * 1_000_000_000 / rate);
                tokio::time::sleep_until(due.into()).await;
            }
            let record = Arc::new(ingress.record(
                Lane::Depth,
                c,
                s,
                PayloadKind::Text,
                PAYLOAD.to_vec(),
                ingress.stamp()?,
            )?);
            let ack = Instant::now();
            writer.sink.append(record.clone()).await?;
            acks.push(ack.elapsed().as_nanos());
            realtime::handoff(
                &tx,
                record,
                meter.health(Lane::Depth).epoch.ok_or("missing epoch")?,
                &meter,
            );
        }
        let ingest_seconds = start.elapsed().as_secs_f64();
        writer.shutdown().await?;
        let verified = recovery::verify_spool(&config.spool_root)?;
        let mut payloads = 0usize;
        for entry in fs::read_dir(recovery::lane_root(&config.spool_root, Lane::Depth))? {
            let mut f = File::open(entry?.path().join("capture.sealed"))?;
            bqrc::read_header(&mut f)?;
            let mut offset = bqrc::HEADER_SIZE;
            while let Some(frame) = bqrc::read_frame(&mut f, offset, LIMITS)? {
                if frame.payload != PAYLOAD {
                    return Err("benchmark payload mismatch".into());
                }
                offset += 12 + frame.metadata.len() as u64 + frame.payload.len() as u64;
                payloads += 1;
            }
        }
        if payloads != count || meter.value(Lane::Depth, "raw_writer_failures_total") != 0 {
            return Err("benchmark raw loss".into());
        }
        let summary = meter.summary();
        logger.finish()?;
        guard.release()?;
        let mut fences = Vec::new();
        let mut seals = Vec::new();
        for line in fs::read_to_string(dir.join("events.jsonl"))?.lines() {
            let event: Value = serde_json::from_str(line)?;
            let target = match event["cause"].as_str() {
                Some("RAW_DURABILITY_FENCED") => &mut fences,
                Some("RAW_SEGMENT_SEALED") => &mut seals,
                _ => continue,
            };
            target.push(
                event["detail"]["elapsed_ns"]
                    .as_str()
                    .ok_or("latency missing")?
                    .parse()?,
            );
        }
        cases.push(json!({"offered_target_messages_s":rate,"unpaced":rate==0,"messages":count,"actual_messages_s":count as f64/ingest_seconds,"payload_bytes_s":(count*PAYLOAD.len()) as f64/ingest_seconds,"ingest_seconds":ingest_seconds,"append_ack":percentiles(acks),"realtime_drops":meter.value(Lane::Depth,"local_capacity_drops_total"),"raw_loss_count":count-payloads,"disk_logical_file_bytes":logical_bytes(&dir)?,"durability_fence":percentiles(fences),"segment_seal":percentiles(seals),"verification":verified,"summary":summary}));
    }
    let report = json!({"schema":"BQIP-COLLECTOR-BENCHMARK-V1","collector_commit":CODE_COMMIT,"cases":cases});
    fs::write(
        out.join("benchmark.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{}", out.join("benchmark.json").display());
    Ok(())
}
