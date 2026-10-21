//! One bounded immutable segment snapshot at a time; accepted validators are authoritative.
use crate::{config::Config, hash, Error, Result};
use bqip_binance_collector::{capture::Lane, recovery};
use bqip_capture_format::{self as bqrc, Frame};
use serde_json::Value;
use std::{
    fs::{self, File},
    io::{Cursor, Read},
    path::{Component, Path, PathBuf},
};
use uuid::Uuid;

pub const NAMES: [&str; 3] = ["capture.sealed", "seal.json", "manifest.json"];
pub const CONTROL_CAP: u64 = 1024 * 1024;

pub fn relative_path(s: &str) -> Result<()> {
    if s.is_empty()
        || s.contains('\\')
        || s.contains(':')
        || s.split('/').any(|c| c.is_empty() || c == "." || c == "..")
        || Path::new(s)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(Error::ReplayPlanInvalid);
    }
    Ok(())
}

/// Reject all symlinks, including ancestor symlinks, before opening evidence/output.
pub fn safe_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()
            .map_err(|_| Error::ReplayPlanInvalid)?
            .join(path)
    };
    let mut prefix = PathBuf::new();
    for c in absolute.components() {
        if matches!(c, Component::ParentDir | Component::CurDir) {
            return Err(Error::ReplayPlanInvalid);
        }
        prefix.push(c.as_os_str());
        match fs::symlink_metadata(&prefix) {
            Ok(m) if m.file_type().is_symlink() => return Err(Error::ReplayPlanInvalid),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Error::ReplayPlanInvalid),
        }
    }
    Ok(absolute)
}

pub fn read_bounded(path: &Path, cap: u64) -> Result<Vec<u8>> {
    safe_path(path)?;
    let before = fs::symlink_metadata(path).map_err(|_| Error::SegmentSourceInvalid)?;
    if !before.is_file() {
        return Err(Error::SegmentSourceInvalid);
    }
    if before.len() > cap {
        return Err(Error::CapacityExceeded);
    }
    let f = File::open(path).map_err(|_| Error::SegmentSourceInvalid)?;
    let opened = f.metadata().map_err(|_| Error::SegmentSourceInvalid)?;
    let mut data = Vec::with_capacity(before.len() as usize);
    f.take(cap + 1)
        .read_to_end(&mut data)
        .map_err(|_| Error::SegmentSourceInvalid)?;
    safe_path(path)?;
    let after = fs::symlink_metadata(path).map_err(|_| Error::ReplayInputChanged)?;
    if data.len() as u64 > cap
        || before.len() != data.len() as u64
        || opened.len() != before.len()
        || after.len() != before.len()
        || before.modified().ok() != after.modified().ok()
    {
        return Err(Error::ReplayInputChanged);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (before.dev(), before.ino()) != (opened.dev(), opened.ino())
            || (before.dev(), before.ino()) != (after.dev(), after.ino())
        {
            return Err(Error::ReplayInputChanged);
        }
    }
    Ok(data)
}

pub struct Segment {
    pub id: Uuid,
    pub capture: Vec<u8>,
    pub hashes: [String; 3],
    pub manifest: Value,
    pub record_count: u64,
    pub lane: Lane,
}
impl Segment {
    pub fn load(root: &Path, relative: &str, config: &Config) -> Result<Self> {
        Self::load_pinned(root, relative, config, None)
    }
    pub fn load_pinned(
        root: &Path,
        relative: &str,
        config: &Config,
        expected: Option<&[String; 3]>,
    ) -> Result<Self> {
        relative_path(relative)?;
        let root = safe_path(root)?;
        let directory = safe_path(&root.join(relative))?;
        if !directory.starts_with(&root) {
            return Err(Error::ReplayPlanInvalid);
        }
        let capture = read_bounded(&directory.join(NAMES[0]), config.max_segment_bytes)?;
        let seal = read_bounded(&directory.join(NAMES[1]), CONTROL_CAP)?;
        let manifest_bytes = read_bounded(&directory.join(NAMES[2]), CONTROL_CAP)?;
        let hashes = [hash(&capture), hash(&seal), hash(&manifest_bytes)];
        if expected.is_some_and(|h| *h != hashes) {
            return Err(Error::ReplayInputChanged);
        }
        let id = bqrc::read_header(&mut Cursor::new(&capture))
            .map_err(|_| Error::SegmentSourceInvalid)?;
        if directory.file_name().and_then(|s| s.to_str()) != Some(&id.to_string()) {
            return Err(Error::SegmentSourceInvalid);
        }
        let v = bqip_contracts::manifests::parse_manifest(
            std::str::from_utf8(&manifest_bytes).map_err(|_| Error::SegmentSourceInvalid)?,
        )
        .map_err(|_| Error::SegmentSourceInvalid)?;
        let lane = match v["lane"].as_str() {
            Some("DEPTH") => Lane::Depth,
            Some("AGGTRADE") => Lane::Aggtrade,
            Some("REST") => Lane::Rest,
            _ => return Err(Error::SegmentSourceInvalid),
        };
        let manifest = recovery::verify_manifest_bytes(&capture, &seal, &manifest_bytes, lane, id)
            .map_err(|_| Error::SegmentSourceInvalid)?;
        let record_count = manifest["record_count"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(Error::SegmentSourceInvalid)?;
        Ok(Self {
            id,
            capture,
            hashes,
            manifest,
            record_count,
            lane,
        })
    }
    pub fn records(&self) -> Records<'_> {
        Records {
            cursor: Cursor::new(&self.capture[bqrc::HEADER_SIZE as usize..]),
            offset: bqrc::HEADER_SIZE,
        }
    }
    pub fn loss_recorded(&self) -> bool {
        self.manifest["raw_loss_status"] != "NONE"
    }
}
pub struct Records<'a> {
    cursor: Cursor<&'a [u8]>,
    offset: u64,
}
impl Iterator for Records<'_> {
    type Item = Result<Frame>;
    fn next(&mut self) -> Option<Self::Item> {
        match bqrc::read_frame(&mut self.cursor, self.offset, recovery::LIMITS) {
            Ok(Some(f)) => {
                self.offset += 12 + f.metadata.len() as u64 + f.payload.len() as u64;
                Some(Ok(f))
            }
            Ok(None) => None,
            Err(_) => Some(Err(Error::SegmentSourceInvalid)),
        }
    }
}
