use crate::{
    config::{safe_absolute, Config},
    key,
    local::{self, Segment},
    now_ns, sha256,
    store::RemoteMetadata,
    Error, Kind, Result,
};
use bqip_contracts::manifests;
use serde_json::{json, Value};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
};
use uuid::Uuid;
fn err() -> Error {
    Error::new(Kind::StateIo, "receipt state I/O or schema failure")
}
pub fn path(c: &Config, id: Uuid) -> Result<PathBuf> {
    Ok(c.archive_state_root
        .join("v1")
        .join(c.namespace_id()?)
        .join(format!("{id}.json")))
}
fn identity(c: &Config, s: &Segment) -> Result<Value> {
    let k = key::keys(&c.prefix, s.descriptor.lane, s.descriptor.id)?;
    Ok(
        json!({"schema":"BQIP-RAW-ARCHIVE-RECEIPT-V1","segment_id":s.descriptor.id.to_string(),"archive_namespace_id":c.namespace_id()?,"backend_kind":c.backend,
        "venue":"BINANCE","market":"USD_M_FUTURES","instrument":"BTCUSDT","lane":key::lane_slug(s.descriptor.lane),
        "local_manifest_hash":s.manifest["manifest_hash"],"local_segment_sha256":s.hashes[0],"local_seal_sha256":s.hashes[1],
        "remote_capture_key":k[0],"remote_capture_sha256":s.hashes[0],"remote_capture_bytes":s.objects[0].len().to_string(),
        "remote_seal_key":k[1],"remote_seal_sha256":s.hashes[1],"remote_seal_bytes":s.objects[1].len().to_string(),
        "remote_manifest_key":k[2],"remote_manifest_sha256":s.hashes[2],"remote_manifest_bytes":s.objects[2].len().to_string(),"verification_method":"FULL_GET_SHA256"}),
    )
}
pub fn hash(v: &Value) -> Result<String> {
    let mut body = v.as_object().ok_or_else(err)?.clone();
    body.remove("receipt_hash");
    Ok(sha256(
        &manifests::canonical_bytes(&Value::Object(body)).map_err(|_| err())?,
    ))
}
fn metadata_valid(v: &Value) -> bool {
    v.is_null()
        || v.as_str()
            .is_some_and(|s| s.len() <= 1024 && !s.chars().any(char::is_control))
}
pub fn commit_valid(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
pub fn build(
    c: &Config,
    s: &Segment,
    metadata: &[RemoteMetadata; 3],
    commit: &str,
) -> Result<Value> {
    if !commit_valid(commit) {
        return Err(err());
    }
    let mut v = identity(c, s)?;
    v["verified_at_unix_ns"] = now_ns()?.to_string().into();
    v["uploader_commit"] = commit.into();
    for (i, name) in ["capture", "seal", "manifest"].iter().enumerate() {
        for (kind, item) in [
            ("etag", &metadata[i].etag),
            ("version", &metadata[i].version),
        ] {
            let item = json!(item);
            if !metadata_valid(&item) {
                return Err(err());
            }
            v[format!("remote_{name}_{kind}")] = item;
        }
    }
    v["receipt_hash"] = hash(&v)?.into();
    Ok(v)
}
pub fn valid(c: &Config, s: &Segment) -> bool {
    let check = || -> Result<bool> {
        let bytes = local::read_bounded(&path(c, s.descriptor.id)?, local::CONTROL_LIMIT)?;
        let v = manifests::parse_manifest(std::str::from_utf8(&bytes).map_err(|_| err())?)
            .map_err(|_| err())?;
        let expected = identity(c, s)?;
        if manifests::canonical_bytes(&v).map_err(|_| err())? != bytes
            || v["receipt_hash"] != hash(&v)?
            || v.as_object().ok_or_else(err)?.len()
                != expected.as_object().ok_or_else(err)?.len() + 9
            || expected
                .as_object()
                .ok_or_else(err)?
                .iter()
                .any(|(k, value)| v[k] != *value)
            || !v["uploader_commit"].as_str().is_some_and(commit_valid)
            || !v["verified_at_unix_ns"]
                .as_str()
                .is_some_and(|s| s.parse::<u64>().is_ok_and(|n| n > 0 && n.to_string() == s))
        {
            return Ok(false);
        }
        for name in ["capture", "seal", "manifest"] {
            for kind in ["etag", "version"] {
                if v.get(format!("remote_{name}_{kind}"))
                    .is_none_or(|v| !metadata_valid(v))
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    };
    check().unwrap_or(false)
}
pub fn invalidate(c: &Config, id: Uuid) -> Result<()> {
    let p = path(c, id)?;
    safe_absolute(&p).map_err(|_| err())?;
    match fs::symlink_metadata(&p) {
        Ok(m) if m.is_file() => {
            let target = p.with_file_name(format!(".invalid-{id}-{}.json", Uuid::new_v4()));
            match fs::rename(&p, target) {
                Ok(()) => {
                    bqip_capture_format::sync_dir(p.parent().ok_or_else(err)?).map_err(|_| err())
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(_) => Err(err()),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(err()),
    }
}
pub fn publish(c: &Config, s: &Segment, v: &Value) -> Result<()> {
    let p = path(c, s.descriptor.id)?;
    safe_absolute(&p).map_err(|_| err())?;
    let parent = p.parent().ok_or_else(err)?;
    bqip_binance_collector::recovery::create_dirs(parent).map_err(|_| err())?;
    safe_absolute(parent).map_err(|_| err())?;
    let tmp = parent.join(format!(".receipt-{}.tmp", Uuid::new_v4()));
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(|_| err())?;
    f.write_all(&manifests::canonical_bytes(v).map_err(|_| err())?)
        .and_then(|()| f.flush())
        .and_then(|()| f.sync_all())
        .map_err(|_| err())?;
    drop(f);
    safe_absolute(&p).map_err(|_| err())?;
    // A concurrent Windows publisher can have won; accept only a valid receipt
    // for the same namespace and exact local evidence. Temporary evidence stays.
    if fs::rename(&tmp, &p).is_err() && !valid(c, s) {
        return Err(err());
    }
    bqip_capture_format::sync_dir(parent).map_err(|_| err())
}
