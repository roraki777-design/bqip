use crate::{
    config::{safe_absolute, Config},
    sha256, Error, Kind, Result,
};
use bqip_binance_collector::{capture::Lane, recovery};
use serde_json::Value;
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use uuid::Uuid;
pub const CONTROL_LIMIT: u64 = 1048576;
pub const NAMES: [&str; 3] = ["capture.sealed", "seal.json", "manifest.json"];
#[derive(Clone, Debug)]
pub struct Descriptor {
    pub id: Uuid,
    pub lane: Lane,
    pub directory: PathBuf,
}
pub struct Segment {
    pub descriptor: Descriptor,
    pub objects: [Vec<u8>; 3],
    pub hashes: [String; 3],
    pub manifest: Value,
    pub first_receive_ns: u64,
}
impl Segment {
    pub fn bytes(&self) -> u64 {
        self.objects.iter().map(|b| b.len() as u64).sum()
    }
}
fn invalid() -> Error {
    Error::new(Kind::LocalSourceInvalid, "local evidence validation failed")
}
pub fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    safe_absolute(path).map_err(|_| invalid())?;
    let before = fs::symlink_metadata(path).map_err(|_| invalid())?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err(invalid());
    }
    if before.len() > limit {
        return Err(Error::new(
            Kind::ArchiveObjectTooLarge,
            "object exceeds configured cap",
        ));
    }
    let f = File::open(path).map_err(|_| invalid())?;
    let opened = f.metadata().map_err(|_| invalid())?;
    let mut bytes = Vec::with_capacity(before.len() as usize);
    f.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid())?;
    safe_absolute(path).map_err(|_| invalid())?;
    let after = fs::symlink_metadata(path).map_err(|_| invalid())?;
    if bytes.len() as u64 > limit {
        return Err(Error::new(
            Kind::ArchiveObjectTooLarge,
            "object grew beyond cap",
        ));
    }
    if !after.is_file()
        || before.len() != bytes.len() as u64
        || opened.len() != before.len()
        || after.len() != before.len()
        || before.modified().ok() != after.modified().ok()
    {
        return Err(invalid());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if (before.dev(), before.ino()) != (opened.dev(), opened.ino())
            || (before.dev(), before.ino()) != (after.dev(), after.ino())
        {
            return Err(invalid());
        }
    }
    Ok(bytes)
}
pub fn discover(c: &Config) -> Result<Vec<Descriptor>> {
    safe_absolute(&c.spool_root).map_err(|_| invalid())?;
    let mut found = Vec::new();
    let mut inspected = 0usize;
    for lane in Lane::ALL {
        let root = recovery::lane_root(&c.spool_root, lane);
        safe_absolute(&root).map_err(|_| invalid())?;
        let entries = match fs::read_dir(&root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(invalid()),
        };
        for e in entries {
            inspected += 1;
            if inspected > c.max_scanned_segments {
                return Err(Error::new(
                    Kind::ScanCapacity,
                    "scan cardinality limit reached",
                ));
            }
            let e = e.map_err(|_| invalid())?;
            if !e.file_type().map_err(|_| invalid())?.is_dir() {
                return Err(invalid());
            }
            let name = e.file_name();
            let text = name.to_str().ok_or_else(invalid)?;
            let id = Uuid::parse_str(text).map_err(|_| invalid())?;
            if id.to_string() != text {
                return Err(invalid());
            }
            let directory = e.path();
            if directory.join("capture.partial").exists()
                || directory.join("seal.partial").exists()
                || NAMES.iter().any(|n| !directory.join(n).exists())
            {
                continue;
            }
            found.push(Descriptor {
                id,
                lane,
                directory,
            });
        }
    }
    found.sort_by_key(|d| (d.lane.index(), d.id));
    Ok(found)
}
pub fn load(c: &Config, d: &Descriptor) -> Result<Segment> {
    let expected = recovery::lane_root(&c.spool_root, d.lane).join(d.id.to_string());
    if safe_absolute(&d.directory).map_err(|_| invalid())? != expected
        || d.directory.join("capture.partial").exists()
        || d.directory.join("seal.partial").exists()
    {
        return Err(invalid());
    }
    let mut objects: [Vec<u8>; 3] = Default::default();
    for i in 0..3 {
        let cap = if i == 0 {
            c.max_single_put_bytes
        } else {
            c.max_single_put_bytes.min(CONTROL_LIMIT)
        };
        objects[i] = read_bounded(&d.directory.join(NAMES[i]), cap)?;
    }
    let manifest =
        recovery::verify_manifest_bytes(&objects[0], &objects[1], &objects[2], d.lane, d.id)
            .map_err(|_| invalid())?;
    let first_receive_ns = manifest["first_receive_timestamp_ns"]
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(invalid)?;
    let hashes = std::array::from_fn(|i| sha256(&objects[i]));
    Ok(Segment {
        descriptor: d.clone(),
        objects,
        hashes,
        manifest,
        first_receive_ns,
    })
}
