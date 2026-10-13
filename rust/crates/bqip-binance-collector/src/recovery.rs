//! Single-owner POSIX spool. Original partial evidence is never edited in recovery.
use crate::{
    capture::Lane,
    telemetry::{sync_dir, Meter},
    Error, Kind, Result,
};
use bqip_capture_format::{self as bqrc, Limits};
use bqip_contracts::{binance, manifests, proto::*};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub const LIMITS: Limits = Limits {
    metadata_bytes: 1024 * 1024,
    payload_bytes: 32 * 1024 * 1024,
};
fn err(e: impl std::fmt::Display) -> Error {
    Error::new(Kind::SealManifest, e)
}
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        text.push(char::from(DIGITS[usize::from(b >> 4)]));
        text.push(char::from(DIGITS[usize::from(b & 15)]));
    }
    text
}
pub fn file_hash(path: &Path) -> Result<String> {
    let mut file = File::open(path).map_err(err)?;
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf).map_err(err)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(hex(&hash.finalize()))
}
pub fn lane_root(root: &Path, lane: Lane) -> PathBuf {
    root.join("binance/usdm/btcusdt").join(lane.name())
}
pub fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temporary = parent.join(format!(".publish-{}", Uuid::new_v4()));
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(err)?;
    f.write_all(bytes)
        .and_then(|()| f.flush())
        .and_then(|()| f.sync_all())
        .map_err(err)?;
    drop(f);
    if path.exists() {
        return Err(err("destination exists; evidence retained"));
    }
    fs::rename(&temporary, path).map_err(err)?;
    sync_dir(parent)
}
pub fn atomic_json(path: &Path, v: &Value) -> Result<()> {
    atomic_bytes(path, &manifests::canonical_bytes(v)?)
}
pub fn spool_bytes(root: &Path) -> Result<u64> {
    if !root.exists() {
        return Ok(0);
    }
    let metadata = fs::symlink_metadata(root).map_err(err)?;
    if metadata.file_type().is_symlink() {
        return Err(err("symlink in spool"));
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    let mut sum = 0u64;
    for e in fs::read_dir(root).map_err(err)? {
        sum = sum
            .checked_add(spool_bytes(&e.map_err(err)?.path())?)
            .ok_or_else(|| err("spool size overflow"))?;
    }
    Ok(sum)
}
pub struct SpoolGuard {
    path: PathBuf,
    token: Vec<u8>,
    released: bool,
}
impl SpoolGuard {
    pub fn acquire(root: &Path) -> Result<Self> {
        spool_bytes(root)?;
        create_dirs(root)?;
        let path = root.join("collector.owner");
        let token = format!("{} {}\n", std::process::id(), Uuid::new_v4()).into_bytes();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| {
                err(format!(
                    "exclusive spool owner (stale marker requires operator verification): {e}"
                ))
            })?;
        file.write_all(&token)
            .and_then(|()| file.sync_all())
            .map_err(err)?;
        sync_dir(root)?;
        Ok(Self {
            path,
            token,
            released: false,
        })
    }
    pub fn release(mut self) -> Result<()> {
        self.remove()?;
        self.released = true;
        Ok(())
    }
    fn remove(&self) -> Result<()> {
        if fs::read(&self.path).map_err(err)? != self.token {
            return Err(err("spool ownership token changed"));
        }
        fs::remove_file(&self.path).map_err(err)?;
        sync_dir(self.path.parent().ok_or_else(|| err("owner parent"))?)
    }
}
impl Drop for SpoolGuard {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.remove();
        }
    }
}

fn validate_source(event: &RawCaptureEvent, payload: &[u8], lane: Lane) -> Result<()> {
    event.validate(payload)?;
    let lineage = if lane == Lane::Rest {
        if event.stream_source.is_some() {
            return Err(err("mixed source context"));
        }
        let source = event
            .http_source
            .as_ref()
            .ok_or_else(|| err("missing typed HTTP context"))?;
        if !matches!(source.http_status, Some(100..=599)) {
            return Err(err("invalid HTTP status"));
        }
        let mut shape = source.clone();
        shape.http_status = Some(200);
        binance::source::http(&shape, Some(payload))?;
        if source.endpoint != event.endpoint {
            return Err(err("HTTP endpoint mismatch"));
        }
        source.lineage.as_ref()
    } else {
        if event.http_source.is_some() || event.http_content_encoding.is_some() {
            return Err(err("mixed source context"));
        }
        let source = event
            .stream_source
            .as_ref()
            .ok_or_else(|| err("missing typed stream context"))?;
        binance::source::stream(source, lane.endpoint(), lane.channel(), Some(payload))?;
        if source.original_session_id != event.session_id
            || source.endpoint != event.endpoint
            || source.channel != event.channel
        {
            return Err(err("stream lineage mismatch"));
        }
        source.lineage.as_ref()
    }
    .ok_or_else(|| err("missing lineage"))?;
    if lineage.capture_id != event.capture_id
        || lineage.raw_content_hash != event.raw_content_hash
        || lineage.receive_timestamp_ns != event.receive_timestamp_ns
    {
        return Err(err("capture lineage mismatch"));
    }
    Ok(())
}
pub fn manifest(
    path: &Path,
    lane: Lane,
    reason: &str,
    loss: &str,
    recovered_from: Option<&str>,
) -> Result<Value> {
    if !["AGE", "BYTES", "SESSION", "SHUTDOWN", "RECOVERED"].contains(&reason)
        || !["NONE", "POSSIBLE"].contains(&loss)
    {
        return Err(err("unknown manifest close reason/loss"));
    }
    if reason == "RECOVERED" && (loss != "POSSIBLE" || recovered_from.is_none()) {
        return Err(err("recovery provenance required"));
    }
    if reason != "RECOVERED" && recovered_from.is_some() {
        return Err(err("unexpected recovery source"));
    }
    if let Some(id) = recovered_from {
        Uuid::parse_str(id).map_err(err)?;
    }
    let sealed = bqrc::verify_segment(path, LIMITS).map_err(err)?;
    let directory = path
        .parent()
        .ok_or_else(|| err("segment directory missing"))?;
    if directory.file_name().and_then(|v| v.to_str())
        != Some(sealed.segment_id.to_string().as_str())
    {
        return Err(err("directory/segment identity mismatch"));
    }
    let mut file = File::open(path).map_err(err)?;
    bqrc::read_header(&mut file).map_err(err)?;
    let mut first: Option<RawCaptureEvent> = None;
    let mut last: Option<RawCaptureEvent> = None;
    let mut offset = bqrc::HEADER_SIZE;
    let mut count = 0u64;
    while let Some(frame) = bqrc::read_frame(&mut file, offset, LIMITS).map_err(err)? {
        let event = frame.decode_event().map_err(err)?;
        validate_source(&event, &frame.payload, lane)?;
        if let Some(old) = &last {
            if old.capture_id.as_ref().and_then(|v| v.capture_seq)
                >= event.capture_id.as_ref().and_then(|v| v.capture_seq)
                || old
                    .capture_id
                    .as_ref()
                    .and_then(|v| v.collector_instance_id.as_ref())
                    != event
                        .capture_id
                        .as_ref()
                        .and_then(|v| v.collector_instance_id.as_ref())
                || old
                    .processing_context
                    .as_ref()
                    .and_then(|v| v.code_commit.as_ref())
                    != event
                        .processing_context
                        .as_ref()
                        .and_then(|v| v.code_commit.as_ref())
                || (lane != Lane::Rest
                    && (old.session_id != event.session_id
                        || old.connection_id != event.connection_id))
            {
                return Err(err("mixed or nonmonotonic segment lineage"));
            }
        }
        if first.is_none() {
            first = Some(event.clone());
        }
        last = Some(event);
        count += 1;
        offset += 12 + frame.metadata.len() as u64 + frame.payload.len() as u64;
    }
    let first = first.ok_or_else(|| err("empty segment cannot claim capture provenance"))?;
    let last = last.ok_or_else(|| err("missing final record"))?;
    let id = first
        .capture_id
        .as_ref()
        .ok_or_else(|| err("capture id missing"))?;
    let code = first
        .processing_context
        .as_ref()
        .and_then(|v| v.code_commit.as_ref())
        .ok_or_else(|| err("code commit missing"))?;
    if code.len() != 40
        || !code.bytes().all(|x| x.is_ascii_hexdigit())
        || count != sealed.record_count
    {
        return Err(err("invalid code/count"));
    }
    let uuid_text = |b: Option<&[u8]>| -> Result<String> {
        Ok(Uuid::from_slice(b.ok_or_else(|| err("UUID missing"))?)
            .map_err(err)?
            .to_string())
    };
    let mut value = json!({"schema":"BQIP-RAW-SEGMENT-MANIFEST-V1","segment_id":sealed.segment_id.to_string(),"bqrc_format_version":2,
        "lane":lane,"venue_profile_id":binance::PROFILE_ID,"venue_profile_hash":hex(&binance::profile_hash()?),
        "collector_instance_id":uuid_text(id.collector_instance_id.as_deref())?,"collector_code_commit":code,
        "connection_id":if lane==Lane::Rest {None}else{Some(uuid_text(first.connection_id.as_deref())?)},
        "session_id":if lane==Lane::Rest {None}else{Some(uuid_text(first.session_id.as_deref())?)},
        "first_capture_seq":id.capture_seq.unwrap_or(0).to_string(),"last_capture_seq":last.capture_id.and_then(|v|v.capture_seq).unwrap_or(0).to_string(),
        "record_count":count.to_string(),"first_receive_timestamp_ns":first.receive_timestamp_ns.map(|n|n.to_string()),"last_receive_timestamp_ns":last.receive_timestamp_ns.map(|n|n.to_string()),
        "byte_length":sealed.byte_length.to_string(),"segment_sha256":hex(&sealed.sha256),"seal_json_sha256":file_hash(&directory.join("seal.json"))?,
        "close_reason":reason,"raw_loss_status":loss,"seal_origin":sealed.seal_origin.as_str(),"recovered_from_segment_id":recovered_from});
    value["manifest_hash"] = hex(&manifests::manifest_hash(&value)?).into();
    Ok(value)
}
pub fn publish_manifest(
    path: &Path,
    lane: Lane,
    reason: &str,
    loss: &str,
    source: Option<&str>,
) -> Result<()> {
    let value = manifest(path, lane, reason, loss, source)?;
    atomic_json(&path.with_file_name("manifest.json"), &value)
}
pub fn verify_manifest(path: &Path, lane: Lane) -> Result<Value> {
    let text = fs::read_to_string(path.with_file_name("manifest.json")).map_err(err)?;
    let value = manifests::parse_manifest(&text)?;
    if manifests::canonical_bytes(&value)? != text.as_bytes() {
        return Err(err("noncanonical manifest"));
    }
    let expected = manifest(
        path,
        lane,
        value["close_reason"].as_str().unwrap_or(""),
        value["raw_loss_status"].as_str().unwrap_or(""),
        value["recovered_from_segment_id"].as_str(),
    )?;
    if value != expected {
        return Err(err("manifest evidence mismatch"));
    }
    Ok(value)
}
pub fn recover_spool(root: &Path, meter: &Meter) -> Result<()> {
    spool_bytes(root)?;
    for lane in Lane::ALL {
        let dir = lane_root(root, lane);
        create_dirs(&dir)?;
        let mut paths = fs::read_dir(&dir)
            .map_err(err)?
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(err)?;
        paths.sort();
        for segment in paths {
            if !segment.is_dir() {
                return Err(err("unexpected lane entry"));
            }
            let sealed = segment.join("capture.sealed");
            let partial = segment.join("capture.partial");
            if sealed.exists() && partial.exists() {
                return Err(err("sealed/partial collision"));
            }
            if sealed.exists() {
                let source = segment
                    .file_name()
                    .and_then(|v| v.to_str())
                    .ok_or_else(|| err("segment id"))?;
                if !segment.join("seal.json").exists() {
                    bqrc::recover_seal(&sealed, LIMITS).map_err(err)?;
                }
                if !segment.join("manifest.json").exists() {
                    publish_manifest(&sealed, lane, "RECOVERED", "POSSIBLE", Some(source))?;
                    meter.event(
                        lane,
                        "PROCESS_CRASH_GAP",
                        "POSSIBLE",
                        json!({"segment_id":source}),
                    );
                }
                verify_manifest(&sealed, lane)?;
            } else if partial.exists() {
                let report = bqrc::recover(&partial, LIMITS).map_err(err)?;
                if report.state == RawSegmentState::Failed || report.record_count == 0 {
                    return Err(err(
                        "corrupt or empty partial: retain evidence, operator review required",
                    ));
                }
                if segment.file_name().and_then(|v| v.to_str())
                    != Some(report.segment_id.to_string().as_str())
                {
                    return Err(err("partial directory id mismatch"));
                }
                let hash = file_hash(&partial)?;
                let note = segment.join("recovery.json");
                if note.exists() {
                    let v = manifests::parse_manifest(&fs::read_to_string(&note).map_err(err)?)?;
                    let target =
                        Uuid::parse_str(v["target"].as_str().unwrap_or("")).map_err(err)?;
                    let m = verify_manifest(
                        &dir.join(target.to_string()).join("capture.sealed"),
                        lane,
                    )?;
                    if v != json!({"source_sha256":hash,"target":target.to_string(),"valid_prefix_bytes":report.valid_prefix_bytes.to_string(),"records":report.record_count.to_string()})
                        || m["recovered_from_segment_id"] != report.segment_id.to_string()
                        || m["record_count"].as_str()
                            != Some(report.record_count.to_string().as_str())
                    {
                        return Err(err("recovery evidence changed"));
                    }
                } else {
                    let target = Uuid::new_v4();
                    let (_, s) = bqrc::recover_to(&partial, &dir, target, LIMITS).map_err(err)?;
                    if file_hash(&partial)? != hash {
                        return Err(err("partial changed during recovery"));
                    }
                    publish_manifest(
                        &s.path,
                        lane,
                        "RECOVERED",
                        "POSSIBLE",
                        Some(&report.segment_id.to_string()),
                    )?;
                    atomic_json(
                        &note,
                        &json!({"source_sha256":hash,"target":target.to_string(),"valid_prefix_bytes":report.valid_prefix_bytes.to_string(),"records":report.record_count.to_string()}),
                    )?;
                    meter.count(lane, "recovered_segments_total", 1);
                    meter.event(lane,"PROCESS_CRASH_GAP","POSSIBLE",json!({"source":report.segment_id.to_string(),"target":target.to_string(),"structural_failure":report.failure}));
                }
            } else {
                return Err(err("segment has no capture evidence"));
            }
        }
    }
    Ok(())
}
pub fn verify_spool(root: &Path) -> Result<Value> {
    let mut count = 0u64;
    let mut records = 0u64;
    for lane in Lane::ALL {
        for entry in fs::read_dir(lane_root(root, lane)).map_err(err)? {
            let path = entry.map_err(err)?.path().join("capture.sealed");
            if path.exists() {
                let m = verify_manifest(&path, lane)?;
                records += m["record_count"]
                    .as_str()
                    .unwrap_or("")
                    .parse::<u64>()
                    .map_err(err)?;
                count += 1;
            }
        }
    }
    Ok(
        json!({"segments_verified":count,"records_verified":records,"spool_bytes":spool_bytes(root)?}),
    )
}

/// Fence directory entries up to the first existing ancestor.
pub fn create_dirs(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::metadata(current) {
            Ok(m) if m.is_dir() => break,
            Ok(_) => return Err(err("directory path is a file")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(err(e)),
        }
        missing.push(current.to_path_buf());
        let parent = current
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if parent == current {
            return Err(err("no existing directory ancestor"));
        }
        current = parent;
    }
    fs::create_dir_all(path).map_err(err)?;
    for dir in missing {
        sync_dir(&dir)?;
        sync_dir(
            dir.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
    }
    Ok(())
}
