//! BQRC v2: bounded synchronous framing, local seal and non-destructive recovery.

use bqip_contracts::{
    hex, manifests,
    proto::{RawCaptureEvent, RawSegmentState},
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub const HEADER_SIZE: u64 = 28;
pub const VERSION: u32 = 2;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Format { code: &'static str, offset: u64 },
    Contract(bqip_contracts::ContractError),
    Protobuf(prost::DecodeError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Format { code, offset } => write!(f, "{code} at byte {offset}"),
            Self::Contract(error) => error.fmt(f),
            Self::Protobuf(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<bqip_contracts::ContractError> for Error {
    fn from(e: bqip_contracts::ContractError) -> Self {
        Self::Contract(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn format_error(code: &'static str, offset: u64) -> Error {
    Error::Format { code, offset }
}

#[derive(Clone, Copy)]
pub struct Limits {
    pub metadata_bytes: u32,
    pub payload_bytes: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            metadata_bytes: 1 << 20,
            payload_bytes: 64 << 20,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn from_event(event: &RawCaptureEvent, payload: Vec<u8>) -> Result<Self> {
        event.validate(&payload)?;
        Ok(Self {
            metadata: event.encode_to_vec(),
            payload,
        })
    }

    pub fn decode_event(&self) -> Result<RawCaptureEvent> {
        let event = RawCaptureEvent::decode(self.metadata.as_slice()).map_err(Error::Protobuf)?;
        event.validate(&self.payload)?;
        Ok(event)
    }

    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        if self.metadata.len() > limits.metadata_bytes as usize
            || self.payload.len() > limits.payload_bytes as usize
        {
            return Err(format_error("FRAME_LIMIT_EXCEEDED", 0));
        }
        let mut data = Vec::with_capacity(12 + self.metadata.len() + self.payload.len());
        data.extend_from_slice(&(self.metadata.len() as u32).to_be_bytes());
        data.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        data.extend_from_slice(&self.metadata);
        data.extend_from_slice(&self.payload);
        let checksum = crc32c::crc32c(&data);
        data.extend_from_slice(&checksum.to_be_bytes());
        Ok(data)
    }
    fn encoded_len(&self) -> u64 {
        12 + self.metadata.len() as u64 + self.payload.len() as u64
    }
}

pub fn header(segment_id: Uuid) -> Vec<u8> {
    let mut data = b"BQRC".to_vec();
    data.extend_from_slice(&VERSION.to_be_bytes());
    data.extend_from_slice(segment_id.as_bytes());
    data.extend_from_slice(&crc32c::crc32c(&data).to_be_bytes());
    data
}

fn exact<R: Read>(source: &mut R, bytes: &mut [u8], offset: u64) -> Result<()> {
    match source.read_exact(bytes) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            Err(format_error("TRUNCATED_RECORD", offset))
        }
        Err(e) => Err(Error::Io(e)),
    }
}

pub fn read_header<R: Read>(source: &mut R) -> Result<Uuid> {
    let mut data = [0; HEADER_SIZE as usize];
    exact(source, &mut data, 0).map_err(|e| match e {
        Error::Format { .. } => format_error("TRUNCATED_HEADER", 0),
        other => other,
    })?;
    if &data[..4] != b"BQRC" {
        return Err(format_error("BAD_MAGIC", 0));
    }
    if data[4..8] != VERSION.to_be_bytes() {
        return Err(format_error("UNSUPPORTED_VERSION", 4));
    }
    if crc32c::crc32c(&data[..24]).to_be_bytes() != data[24..28] {
        return Err(format_error("HEADER_CRC_MISMATCH", 24));
    }
    Uuid::from_slice(&data[8..24]).map_err(|_| format_error("INVALID_SEGMENT_UUID", 8))
}

pub fn read_frame<R: Read>(source: &mut R, offset: u64, limits: Limits) -> Result<Option<Frame>> {
    let mut prefix = [0; 8];
    // read_exact retries Interrupted and distinguishes boundary EOF from a partial prefix.
    match source.read_exact(&mut prefix[..1]) {
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
        Ok(()) => (),
    }
    exact(source, &mut prefix[1..], offset)?;
    let metadata_length = u32::from_be_bytes(
        prefix[..4]
            .try_into()
            .map_err(|_| format_error("BAD_LENGTH", offset))?,
    );
    let payload_length = u32::from_be_bytes(
        prefix[4..]
            .try_into()
            .map_err(|_| format_error("BAD_LENGTH", offset))?,
    );
    if metadata_length > limits.metadata_bytes || payload_length > limits.payload_bytes {
        return Err(format_error("FRAME_LIMIT_EXCEEDED", offset));
    }
    let length = (metadata_length as usize)
        .checked_add(payload_length as usize)
        .ok_or_else(|| format_error("FRAME_LIMIT_EXCEEDED", offset))?;
    let mut data = vec![0; length];
    exact(source, &mut data, offset)?;
    let mut checksum = [0; 4];
    exact(source, &mut checksum, offset)?;
    if crc32c::crc32c_append(crc32c::crc32c(&prefix), &data) != u32::from_be_bytes(checksum) {
        return Err(format_error("CRC_MISMATCH", offset));
    }
    let payload = data.split_off(metadata_length as usize);
    Ok(Some(Frame {
        metadata: data,
        payload,
    }))
}

#[derive(Debug)]
pub struct RecoveryReport {
    pub segment_id: Uuid,
    pub state: RawSegmentState,
    pub record_count: u64,
    pub valid_prefix_bytes: u64,
    pub failure: Option<&'static str>,
    pub failure_offset: Option<u64>,
}

// Hash the exact bytes consumed by validation, without re-encoding any record.
struct HashingReader {
    file: File,
    hash: Sha256,
    bytes: u64,
}
impl Read for HashingReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let count = self.file.read(buffer)?;
        self.hash.update(&buffer[..count]);
        self.bytes += count as u64;
        Ok(count)
    }
}

fn scan(path: &Path, limits: Limits) -> Result<(RecoveryReport, [u8; 32], u64)> {
    let mut source = HashingReader {
        file: File::open(path)?,
        hash: Sha256::new(),
        bytes: 0,
    };
    let segment_id = read_header(&mut source)?;
    let mut offset = HEADER_SIZE;
    let mut count = 0;
    let report = loop {
        match read_frame(&mut source, offset, limits) {
            Ok(Some(frame)) => {
                count += 1;
                offset += frame.encoded_len();
            }
            Ok(None) => {
                break RecoveryReport {
                    segment_id,
                    record_count: count,
                    valid_prefix_bytes: offset,
                    state: RawSegmentState::RecoveredPartial,
                    failure: None,
                    failure_offset: None,
                }
            }
            Err(Error::Format {
                code,
                offset: failed_at,
            }) => {
                break RecoveryReport {
                    segment_id,
                    record_count: count,
                    valid_prefix_bytes: offset,
                    state: if code == "TRUNCATED_RECORD" {
                        RawSegmentState::RecoveredPartial
                    } else {
                        RawSegmentState::Failed
                    },
                    failure: Some(code),
                    failure_offset: Some(failed_at),
                }
            }
            Err(error) => return Err(error),
        }
    };
    // Complete the disk hash even after a structural failure; no salvage is skipped.
    io::copy(&mut source, &mut io::sink())?;
    Ok((report, source.hash.finalize().into(), source.bytes))
}

pub fn recover(path: &Path, limits: Limits) -> Result<RecoveryReport> {
    let (mut report, _, _) = scan(path, limits)?;
    if report.failure.is_none() && path.file_name().is_some_and(|n| n == "capture.sealed") {
        report.state = RawSegmentState::RecoveredUnverified;
        if path.with_file_name("seal.json").exists() {
            verify_segment(path, limits)?;
            report.state = RawSegmentState::SealedLocal;
        }
    }
    Ok(report)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealOrigin {
    Original,
    Recovered,
}
impl SealOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Original => "ORIGINAL",
            Self::Recovered => "RECOVERED",
        }
    }
}

#[derive(Debug)]
pub struct SealedSegment {
    pub path: PathBuf,
    pub segment_id: Uuid,
    pub sha256: [u8; 32],
    pub record_count: u64,
    pub byte_length: u64,
    pub seal_origin: SealOrigin,
}
impl SealedSegment {
    pub fn seal_bytes(&self) -> Result<Vec<u8>> {
        Ok(manifests::canonical_bytes(&serde_json::json!({
            "seal_schema": "BQIP-RAW-SEAL-V1", "format_version": "2",
            "segment_id": self.segment_id.to_string(), "byte_length": self.byte_length.to_string(),
            "record_count": self.record_count.to_string(), "segment_sha256": hex(&self.sha256),
            "seal_origin": self.seal_origin.as_str(),
        }))?)
    }
}

fn read_seal(path: &Path) -> Result<SealedSegment> {
    let data = fs::read(path.with_file_name("seal.json")).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            format_error("SEAL_MISSING", 0)
        } else {
            Error::Io(error)
        }
    })?;
    let text = std::str::from_utf8(&data).map_err(|_| format_error("INVALID_SEAL_JSON", 0))?;
    let body = manifests::parse_manifest(text).map_err(|_| format_error("INVALID_SEAL_JSON", 0))?;
    if manifests::canonical_bytes(&body)? != data {
        return Err(format_error("NON_CANONICAL_SEAL", 0));
    }
    let invalid = || format_error("INVALID_SEAL_SCHEMA", 0);
    let fields = [
        "seal_schema",
        "format_version",
        "segment_id",
        "byte_length",
        "record_count",
        "segment_sha256",
        "seal_origin",
    ];
    let object = body.as_object().ok_or_else(invalid)?;
    if object.len() != fields.len() || fields.iter().any(|key| !object.contains_key(*key)) {
        return Err(invalid());
    }
    let string = |key: &str| body[key].as_str().ok_or_else(invalid);
    if string("seal_schema")? != "BQIP-RAW-SEAL-V1" || string("format_version")? != "2" {
        return Err(invalid());
    }
    let integer = |key: &str| -> Result<u64> {
        let text = string(key)?;
        let n = text.parse::<u64>().map_err(|_| invalid())?;
        if n.to_string() != text {
            return Err(invalid());
        }
        Ok(n)
    };
    let segment_id = Uuid::parse_str(string("segment_id")?).map_err(|_| invalid())?;
    if segment_id.to_string() != string("segment_id")? {
        return Err(invalid());
    }
    let digest = string("segment_sha256")?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut sha256 = [0u8; 32];
    for (index, value) in sha256.iter_mut().enumerate() {
        *value =
            u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16).map_err(|_| invalid())?;
    }
    let seal_origin = match string("seal_origin")? {
        "ORIGINAL" => SealOrigin::Original,
        "RECOVERED" => SealOrigin::Recovered,
        _ => return Err(invalid()),
    };
    Ok(SealedSegment {
        path: path.to_owned(),
        segment_id,
        sha256,
        record_count: integer("record_count")?,
        byte_length: integer("byte_length")?,
        seal_origin,
    })
}

fn actual_segment(path: &Path, origin: SealOrigin, limits: Limits) -> Result<SealedSegment> {
    let (report, sha256, byte_length) = scan(path, limits)?;
    if let Some(code) = report.failure {
        return Err(format_error(code, report.failure_offset.unwrap_or(0)));
    }
    Ok(SealedSegment {
        path: path.to_owned(),
        segment_id: report.segment_id,
        sha256,
        record_count: report.record_count,
        byte_length,
        seal_origin: origin,
    })
}

pub fn verify_segment(path: &Path, limits: Limits) -> Result<SealedSegment> {
    let expected = read_seal(path)?;
    let actual = actual_segment(path, expected.seal_origin, limits)?;
    for (different, code) in [
        (
            actual.segment_id != expected.segment_id,
            "SEAL_SEGMENT_ID_MISMATCH",
        ),
        (
            actual.byte_length != expected.byte_length,
            "SEAL_BYTE_LENGTH_MISMATCH",
        ),
        (
            actual.record_count != expected.record_count,
            "SEAL_RECORD_COUNT_MISMATCH",
        ),
        (actual.sha256 != expected.sha256, "SEGMENT_HASH_MISMATCH"),
    ] {
        if different {
            return Err(format_error(code, 0));
        }
    }
    Ok(actual)
}

fn publish_seal(segment: &SealedSegment) -> Result<()> {
    let directory = segment
        .path
        .parent()
        .ok_or_else(|| format_error("INVALID_SEGMENT_PATH", 0))?;
    let partial = directory.join("seal.partial");
    let final_path = directory.join("seal.json");
    if final_path.exists() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "seal exists").into());
    }
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial)?;
    output.write_all(&segment.seal_bytes()?)?;
    output.flush()?;
    output.sync_all()?;
    drop(output);
    // Publication requires exclusive ownership of this quiescent directory.
    if final_path.exists() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "seal exists").into());
    }
    fs::rename(partial, final_path)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

pub fn recover_seal(path: &Path, limits: Limits) -> Result<SealedSegment> {
    if path.file_name().is_none_or(|name| name != "capture.sealed") {
        return Err(format_error("SEALED_PATH_REQUIRED", 0));
    }
    if path.with_file_name("seal.json").exists() {
        return verify_segment(path, limits);
    }
    let actual = actual_segment(path, SealOrigin::Recovered, limits)?;
    File::open(path)?.sync_all()?;
    // Only explicit recovery can remove incomplete evidence, after data validation.
    match fs::remove_file(path.with_file_name("seal.partial")) {
        Ok(()) => (),
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    publish_seal(&actual)?;
    Ok(actual)
}

pub struct SegmentWriter {
    pub segment_id: Uuid,
    pub partial_path: PathBuf,
    state: RawSegmentState,
    directory: PathBuf,
    file: Option<File>,
    limits: Limits,
    count: u64,
    bytes: u64,
    hash: Sha256,
}

impl SegmentWriter {
    pub fn state(&self) -> RawSegmentState {
        self.state
    }

    pub fn new(root: &Path, segment_id: Uuid, limits: Limits) -> Result<Self> {
        let directory = root.join(segment_id.to_string());
        fs::create_dir(&directory)?;
        File::open(root)?.sync_all()?;
        let partial_path = directory.join("capture.partial");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_path)?;
        let mut writer = Self {
            segment_id,
            partial_path,
            state: RawSegmentState::Open,
            directory,
            file: Some(file),
            limits,
            count: 0,
            bytes: HEADER_SIZE,
            hash: Sha256::new(),
        };
        writer.write(&header(segment_id))?;
        Ok(writer)
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| format_error("SEGMENT_NOT_OPEN", self.bytes))?;
        if let Err(error) = file.write_all(data) {
            self.state = RawSegmentState::Failed;
            return Err(error.into());
        }
        self.hash.update(data);
        Ok(())
    }

    pub fn append(&mut self, frame: &Frame) -> Result<()> {
        if self.state != RawSegmentState::Open {
            return Err(format_error("SEGMENT_NOT_OPEN", self.bytes));
        }
        let encoded = frame.encode(self.limits)?;
        self.write(&encoded)?;
        self.count += 1;
        self.bytes += encoded.len() as u64;
        Ok(())
    }

    pub fn seal(&mut self) -> Result<SealedSegment> {
        if self.state != RawSegmentState::Open {
            return Err(format_error("SEGMENT_NOT_OPEN", self.bytes));
        }
        let path = self.directory.join("capture.sealed");
        let result = (|| -> Result<SealedSegment> {
            let mut file = self
                .file
                .take()
                .ok_or_else(|| format_error("SEGMENT_NOT_OPEN", self.bytes))?;
            file.flush()?;
            file.sync_all()?;
            drop(file);
            if path.exists() {
                return Err(
                    io::Error::new(io::ErrorKind::AlreadyExists, "sealed file exists").into(),
                );
            }
            fs::rename(&self.partial_path, &path)?;
            File::open(&self.directory)?.sync_all()?;
            let (report, actual_hash, actual_bytes) = scan(&path, self.limits)?;
            let expected_hash: [u8; 32] = self.hash.clone().finalize().into();
            if actual_hash != expected_hash {
                return Err(format_error("WRITER_EXPECTED_HASH_MISMATCH", 0));
            }
            if let Some(code) = report.failure {
                return Err(format_error(code, report.failure_offset.unwrap_or(0)));
            }
            if report.segment_id != self.segment_id {
                return Err(format_error("WRITER_SEGMENT_ID_MISMATCH", 0));
            }
            if report.record_count != self.count || actual_bytes != self.bytes {
                return Err(format_error("WRITER_COUNTS_MISMATCH", 0));
            }
            let actual = SealedSegment {
                path,
                segment_id: report.segment_id,
                sha256: actual_hash,
                record_count: report.record_count,
                byte_length: actual_bytes,
                seal_origin: SealOrigin::Original,
            };
            publish_seal(&actual)?;
            Ok(actual)
        })();
        self.state = if result.is_ok() {
            RawSegmentState::SealedLocal
        } else {
            RawSegmentState::Failed
        };
        result
    }
}

pub fn recover_to(
    source: &Path,
    root: &Path,
    new_id: Uuid,
    limits: Limits,
) -> Result<(RecoveryReport, SealedSegment)> {
    let report = recover(source, limits)?;
    if report.state == RawSegmentState::Failed {
        return Err(format_error(
            report.failure.unwrap_or("RECOVERY_FAILED"),
            report.valid_prefix_bytes,
        ));
    }
    let mut source = File::open(source)?;
    if read_header(&mut source)? != report.segment_id {
        return Err(format_error("SOURCE_CHANGED", 0));
    }
    let mut writer = SegmentWriter::new(root, new_id, limits)?;
    let mut offset = HEADER_SIZE;
    for _ in 0..report.record_count {
        let frame = read_frame(&mut source, offset, limits)?
            .ok_or_else(|| format_error("SOURCE_CHANGED", offset))?;
        writer.append(&frame)?;
        offset += frame.encoded_len();
    }
    Ok((report, writer.seal()?))
}
