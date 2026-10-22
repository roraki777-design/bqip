use bqip_contracts::{manifests, proto::*};
use bqip_normalizer::{
    config::{Config, BUILD_COMMIT, DEFAULT_CONFIG},
    input::{read_bounded, safe_path},
    metadata::Catalog,
    plan::{Mode, ReplayPlan},
    transcript, Error, Replay, Result,
};
use prost::Message;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

fn usage() -> Error {
    eprintln!("bqip-replay plan --source-root DIR --state-root NEW_DIR [--config FILE] [--mode VERSION_PINNED_RESEARCH] --segment RELATIVE_DIR ...\n\
bqip-replay run --source-root DIR --state-root NEW_DIR --plan FILE [--config FILE] [--metadata FILE.pb ...] [--run-id UUID] [--processing-time-ns N]\n\
bqip-replay verify-transcript --transcript FILE\n\
bqip-replay version");
    Error::ReplayPlanInvalid
}
fn required<'a>(args: &'a BTreeMap<String, Vec<String>>, key: &str) -> Result<&'a str> {
    args.get(key)
        .and_then(|v| v.first())
        .map(String::as_str)
        .ok_or_else(usage)
}
fn text_file(path: &Path, cap: u64) -> Result<String> {
    String::from_utf8(read_bounded(path, cap)?).map_err(|_| Error::ReplayPlanInvalid)
}
fn new_state(root: &Path, source: &Path) -> Result<PathBuf> {
    let state = safe_path(root)?;
    let source = safe_path(source)?;
    if state.starts_with(&source) || source.starts_with(&state) {
        return Err(Error::OutputInvalid);
    }
    // Reserve a fresh sibling directory. Existing paths are never overwritten.
    fs::create_dir(&state).map_err(|_| Error::OutputInvalid)?;
    safe_path(&state).map_err(|_| Error::OutputInvalid)
}
fn write_new(path: &Path, v: &Value) -> Result<()> {
    let mut f = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|_| Error::OutputInvalid)?;
    f.write_all(&manifests::canonical_bytes(v)?)
        .and_then(|()| f.sync_all())
        .map_err(|_| Error::OutputInvalid)
}
fn main_result() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let cmd = arguments.next().ok_or_else(usage)?;
    let mut args: BTreeMap<String, Vec<String>> = BTreeMap::new();
    while let Some(k) = arguments.next() {
        let value = arguments.next().ok_or_else(usage)?;
        if ![
            "--source-root",
            "--state-root",
            "--config",
            "--mode",
            "--segment",
            "--plan",
            "--metadata",
            "--run-id",
            "--processing-time-ns",
            "--transcript",
        ]
        .contains(&k.as_str())
            || (args.contains_key(&k) && k != "--segment" && k != "--metadata")
        {
            return Err(usage());
        }
        args.entry(k).or_default().push(value);
    }
    if cmd == "version" {
        if !args.is_empty() {
            return Err(usage());
        }
        println!("{BUILD_COMMIT}");
        return Ok(());
    }
    if cmd == "verify-transcript" {
        if args.len() != 1 {
            return Err(usage());
        }
        let v = transcript::verify(&text_file(
            Path::new(required(&args, "--transcript")?),
            64 * 1024 * 1024,
        )?)?;
        println!(
            "{}",
            v["transcript_hash"]
                .as_str()
                .ok_or(Error::TranscriptInvalid)?
        );
        return Ok(());
    }
    if !["plan", "run"].contains(&cmd.as_str()) {
        return Err(usage());
    }
    let allowed: &[&str] = if cmd == "plan" {
        &[
            "--source-root",
            "--state-root",
            "--config",
            "--mode",
            "--segment",
        ]
    } else {
        &[
            "--source-root",
            "--state-root",
            "--config",
            "--plan",
            "--metadata",
            "--run-id",
            "--processing-time-ns",
        ]
    };
    if args.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(usage());
    }
    let config = match args.get("--config") {
        Some(v) => Config::parse(&text_file(Path::new(&v[0]), 1024 * 1024)?)?,
        None => Config::parse(DEFAULT_CONFIG)?,
    };
    let root = Path::new(required(&args, "--source-root")?);
    let state_path = Path::new(required(&args, "--state-root")?);
    if cmd == "plan" {
        let mode = match args
            .get("--mode")
            .map(|v| v[0].as_str())
            .unwrap_or("VERSION_PINNED_RESEARCH")
        {
            "VERSION_PINNED_RESEARCH" => Mode::VersionPinnedResearch,
            "AS_LIVED_FORENSIC" => Mode::AsLivedForensic,
            "LIVE" => Mode::Live,
            _ => return Err(usage()),
        };
        let paths = args.get("--segment").ok_or_else(usage)?;
        let plan = ReplayPlan::create(root, paths, &config, mode)?;
        let state = new_state(state_path, root)?;
        write_new(&state.join("plan.json"), &plan.value()?)?;
        println!("{}", plan.plan_hash);
        return Ok(());
    }
    let plan = ReplayPlan::parse(&text_file(
        Path::new(required(&args, "--plan")?),
        8 * 1024 * 1024,
    )?)?;
    plan.verify_inputs(root, &config)?;
    let mut versions = Vec::new();
    if let Some(paths) = args.get("--metadata") {
        if paths.len() > config.max_metadata_versions {
            return Err(Error::CapacityExceeded);
        }
        let mut bytes = 0usize;
        for p in paths {
            let data = read_bounded(Path::new(p), config.max_metadata_bytes as u64)?;
            bytes += data.len();
            if bytes > config.max_metadata_bytes {
                return Err(Error::CapacityExceeded);
            }
            versions.push(
                InstrumentMetadataVersion::decode(data.as_slice())
                    .map_err(|_| Error::CatalogInvalid)?,
            );
        }
    }
    let catalog = if args.contains_key("--metadata") {
        Some(Catalog::new(versions, &config)?)
    } else {
        None
    };
    let run = match args.get("--run-id") {
        Some(v) => Uuid::parse_str(&v[0]).map_err(|_| Error::ReplayPlanInvalid)?,
        None => Uuid::new_v4(),
    };
    let time = match args.get("--processing-time-ns") {
        Some(v) => v[0].parse::<i64>().map_err(|_| Error::ReplayPlanInvalid)?,
        None => i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::OutputInvalid)?
                .as_nanos(),
        )
        .map_err(|_| Error::OutputInvalid)?,
    };
    let context = ProcessingContext {
        processing_timestamp_ns: Some(time),
        code_commit: Some(BUILD_COMMIT.into()),
        schema_version: Some(1),
        run_id: Some(run.as_bytes().to_vec()),
        replay_mode: Some(ReplayMode::VersionPinnedResearch as i32),
    };
    let state = new_state(state_path, root)?;
    let temporary = state.join("transcript.partial");
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|_| Error::OutputInvalid)?;
    let mut output = BufWriter::new(file);
    let replay = Replay {
        source_root: root,
        plan: &plan,
        config: &config,
        catalog: catalog.as_ref(),
    };
    let summary = replay.execute(&context, &mut output, |_| Ok(()))?;
    output
        .flush()
        .and_then(|()| output.get_ref().sync_all())
        .map_err(|_| Error::OutputInvalid)?;
    drop(output);
    fs::rename(&temporary, state.join("transcript.json")).map_err(|_| Error::OutputInvalid)?;
    write_new(&state.join("report.json"), &summary.report(&plan, &context))?;
    println!("{}", summary.transcript_hash);
    Ok(())
}
fn main() {
    if let Err(e) = main_result() {
        eprintln!("{}", e.code());
        std::process::exit(1);
    }
}
