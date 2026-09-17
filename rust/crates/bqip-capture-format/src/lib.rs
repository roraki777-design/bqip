//! BQRC v1: bounded synchronous framing, local seal and non-destructive recovery.

use bqip_contracts::proto::{RawCaptureEvent, RawSegmentState};
use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub const HEADER_SIZE: u64 = 24;
pub const VERSION: u32 = 1;

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
        let checksum = crc32c::crc32c(&data[8..]);
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
    Uuid::from_slice(&data[8..]).map_err(|_| format_error("INVALID_SEGMENT_UUID", 8))
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
    if crc32c::crc32c(&data) != u32::from_be_bytes(checksum) {
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

pub fn recover(path: &Path, limits: Limits) -> Result<RecoveryReport> {
    let mut source = File::open(path)?;
    let segment_id = read_header(&mut source)?;
    let mut offset = HEADER_SIZE;
    let mut count = 0;
    loop {
        match read_frame(&mut source, offset, limits) {
            Ok(Some(frame)) => {
                count += 1;
                offset += frame.encoded_len();
            }
            Ok(None) => {
                return Ok(RecoveryReport {
                    segment_id,
                    record_count: count,
                    valid_prefix_bytes: offset,
                    state: if path.extension().is_some_and(|e| e == "sealed") {
                        RawSegmentState::SealedLocal
                    } else {
                        RawSegmentState::RecoveredPartial
                    },
                    failure: None,
                    failure_offset: None,
                })
            }
            Err(Error::Format {
                code,
                offset: failed_at,
            }) => {
                return Ok(RecoveryReport {
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
                })
            }
            Err(error) => return Err(error),
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
}

pub fn verify_segment(
    path: &Path,
    expected_sha256: [u8; 32],
    limits: Limits,
) -> Result<RecoveryReport> {
    let mut source = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    let actual: [u8; 32] = hash.finalize().into();
    if actual != expected_sha256 {
        return Err(format_error("SEGMENT_HASH_MISMATCH", 0));
    }
    let report = recover(path, limits)?;
    if let Some(code) = report.failure {
        return Err(format_error(code, report.failure_offset.unwrap_or(0)));
    }
    Ok(report)
}

pub struct SegmentWriter {
    pub segment_id: Uuid,
    pub partial_path: PathBuf,
    state: RawSegmentState,
    directory: PathBuf,
    file: File,
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
            file,
            limits,
            count: 0,
            bytes: HEADER_SIZE,
            hash: Sha256::new(),
        };
        writer.write(&header(segment_id))?;
        Ok(writer)
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        if let Err(error) = self.file.write_all(data) {
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
        let result: io::Result<()> = (|| {
            self.file.flush()?;
            self.file.sync_all()?;
            if path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "sealed file exists",
                ));
            }
            fs::rename(&self.partial_path, &path)?;
            File::open(&self.directory)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = result {
            self.state = RawSegmentState::Failed;
            return Err(error.into());
        }
        self.state = RawSegmentState::SealedLocal;
        Ok(SealedSegment {
            path,
            segment_id: self.segment_id,
            sha256: self.hash.clone().finalize().into(),
            record_count: self.count,
            byte_length: self.bytes,
        })
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
