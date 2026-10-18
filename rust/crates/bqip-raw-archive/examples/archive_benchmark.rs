//! In-memory synthetic archive timings; NOT S3/network performance claims.
#[path = "../tests/support/mod.rs"]
mod support;
use bqip_binance_collector::capture::Lane;
use serde_json::{json, Value};
use std::{fs, path::Path, time::Instant};
use support::*;
fn state_bytes(p: &Path) -> u64 {
    fs::read_dir(p)
        .map(|entries| {
            entries
                .map(|e| {
                    let p = e.unwrap().path();
                    if p.is_dir() {
                        state_bytes(&p)
                    } else {
                        fs::metadata(p).unwrap().len()
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 3, "arguments: BQRC_BYTES SAMPLES");
    let size = args[1].parse::<u64>().unwrap();
    let samples = args[2].parse::<u32>().unwrap();
    assert!((1048576..=134217728).contains(&size));
    assert!((1..=100).contains(&samples));
    let mut output: Vec<Value> = Vec::new();
    for _ in 0..samples {
        let e = Env::new();
        let d = write_segment(&e.config.spool_root, Lane::Depth, Some(size));
        let before = source_snapshot(&e.config.spool_root);
        e.archive.scan(false).unwrap();
        let backlog = e.meter.summary()["backlog"].clone();
        let start = Instant::now();
        e.archive.archive(d.clone()).await.unwrap();
        let elapsed = start.elapsed().as_secs_f64();
        e.archive.scan(false).unwrap();
        assert_eq!(source_snapshot(&e.config.spool_root), before);
        let metrics = e.meter.summary();
        let c = &metrics["counters"];
        output.push(json!({"source_bqrc_bytes":size,"source_set_bytes":e.load(&d).bytes(),"end_to_end_seconds":elapsed,
            "upload_bytes":c["bytes_uploaded_total"],"verified_download_bytes":c["bytes_verified_download_total"],
            "upload_latency_ms":metrics["upload_latency_ms"],"verification_latency_ms":metrics["verification_latency_ms"],
            "upload_retries":c["upload_retries_total"],"peak_active_uploads":c["peak_active_uploads"],
            "archive_state_bytes":state_bytes(&e.config.archive_state_root),"maximum_pending_bytes_observed":backlog["archive_pending_bytes"],
            "maximum_oldest_unuploaded_age_seconds_observed":backlog["oldest_unuploaded_age_seconds"],"source_unchanged":true,"final_backlog":metrics["backlog"]}));
    }
    println!(
        "{}",
        json!({"backend":"DETERMINISTIC_IN_MEMORY","profile":"release","samples":output})
    );
}
