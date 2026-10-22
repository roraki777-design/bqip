//! Bounded synthetic workload preparation, outside benchmark timing.
// The shared offline fixture module also serves integration tests; this example
// uses only raw builders and confirmed metadata, never test-directory helpers.
#[allow(dead_code)]
#[path = "../tests/support/mod.rs"]
mod support;
use bqip_binance_collector::{capture::Lane, recovery};
use bqip_capture_format::{Frame, SegmentWriter};
use bqip_contracts::manifests;
use bqip_normalizer::plan::{Mode, ReplayPlan};
use prost::Message;
use std::{fs, path::PathBuf};
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().ok_or("workload directory required")?);
    let cycles: u64 = args.next().ok_or("cycles required")?.parse()?;
    if !(1..=1000).contains(&cycles) || args.next().is_some() {
        return Err("cycles must be 1..1000".into());
    }
    fs::create_dir(&root)?;
    let source = root.join("source");
    fs::create_dir(&source)?;
    let mut paths = Vec::new();
    for (ordinal, lane) in [Lane::Rest, Lane::Depth, Lane::Aggtrade]
        .into_iter()
        .enumerate()
    {
        let id = Uuid::from_u128(100 + ordinal as u128);
        let mut writer = SegmentWriter::new(&source, id, recovery::LIMITS)?;
        for i in 0..cycles {
            let rows: Vec<(&[u8], Option<&str>, u64)> = match lane {
                Lane::Rest => vec![
                    (support::EXCHANGE, Some("/fapi/v1/exchangeInfo"), 6 * i + 1),
                    (support::SNAPSHOT, Some("/fapi/v1/depth"), 6 * i + 2),
                ],
                Lane::Depth => vec![(support::DEPTH, None, 6 * i + 3)],
                Lane::Aggtrade => vec![
                    (support::TRADE, None, 6 * i + 4),
                    (support::TRADE, None, 6 * i + 5),
                    (b"{", None, 6 * i + 6),
                ],
            };
            for (payload, endpoint, seq) in rows {
                writer.append(&Frame::from_event(
                    &support::raw(lane, payload, seq, endpoint),
                    payload.to_vec(),
                )?)?;
            }
        }
        let sealed = writer.seal()?;
        recovery::publish_manifest(&sealed.path, lane, "SHUTDOWN", "NONE", None)?;
        paths.push(id.to_string());
    }
    let plan = ReplayPlan::create(
        &source,
        &paths,
        &support::config(),
        Mode::VersionPinnedResearch,
    )?;
    fs::write(
        root.join("plan.json"),
        manifests::canonical_bytes(&plan.value()?)?,
    )?;
    fs::write(
        root.join("confirmed.pb"),
        support::confirmed("confirmed-v1", 5_000_000).encode_to_vec(),
    )?;
    println!("{}", plan.total_records()?);
    Ok(())
}
