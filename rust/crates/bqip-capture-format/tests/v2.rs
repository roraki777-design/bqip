//! AC-001B regressions: expectations are external fixtures, never another BQIP output.
use bqip_capture_format::*;
use bqip_contracts::{hex, identity::raw_hash, proto::RawSegmentState};
use serde_json::Value;
use std::fs;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;
use uuid::Uuid;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures/capture")
}
fn fixture(name: &str) -> Value {
    serde_json::from_slice(&fs::read(fixtures().join(name)).unwrap()).unwrap()
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn code<T>(result: Result<T>, expected: &str) {
    match result {
        Err(Error::Format { code, .. }) => assert_eq!(code, expected),
        Err(error) => panic!("unexpected error: {error}"),
        Ok(_) => panic!("expected {expected}"),
    }
}
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("bqip-v2-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn orphan(&self) -> PathBuf {
        let path = self.0.join("capture.sealed");
        fs::copy(fixtures().join("complete.bqrc"), &path).unwrap();
        path
    }
    fn original(&self) -> PathBuf {
        let path = self.orphan();
        let vector = fixture("seals-v2.json");
        fs::write(
            self.0.join("seal.json"),
            unhex(vector["seals"][0]["canonical_hex"].as_str().unwrap()),
        )
        .unwrap();
        path
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn header_error(name: &str) {
    let f = fixture("integrity-v2.json");
    let v = f["header_errors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap();
    match read_header(&mut Cursor::new(unhex(v["hex"].as_str().unwrap()))) {
        Err(Error::Format { code, offset }) => {
            assert_eq!(code, v["error"].as_str().unwrap());
            assert_eq!(offset, v["offset"].as_u64().unwrap());
        }
        result => panic!("unexpected {result:?}"),
    }
}

#[test]
fn header_record_bytes_and_crc_match_external_vectors() {
    let v = fixture("integrity-v2.json");
    let id = Uuid::parse_str(v["segment_id"].as_str().unwrap()).unwrap();
    let head = unhex(v["header_hex"].as_str().unwrap());
    assert_eq!(HEADER_SIZE, 28);
    assert_eq!(header(id), head);
    assert_eq!(
        format!("{:08x}", crc32c::crc32c(&head[..24])),
        v["header_crc32c_hex"]
    );
    assert_eq!(read_header(&mut Cursor::new(head)).unwrap(), id);
    let bytes = unhex(v["record_hex"].as_str().unwrap());
    let frame = Frame {
        metadata: b"ABC".to_vec(),
        payload: b"DEFGH".to_vec(),
    };
    assert_eq!(frame.encode(Limits::default()).unwrap(), bytes);
    assert_eq!(
        format!("{:08x}", crc32c::crc32c(&bytes[..bytes.len() - 4])),
        v["record_crc32c_hex"]
    );
    assert_eq!(
        read_frame(&mut Cursor::new(bytes), 0, Limits::default())
            .unwrap()
            .unwrap(),
        frame
    );
}

#[test]
fn length_prefix_partition_corruption_is_rejected() {
    let v = fixture("integrity-v2.json");
    let good = unhex(v["record_hex"].as_str().unwrap());
    let bad = unhex(v["record_errors"][0]["hex"].as_str().unwrap());
    assert_eq!(&good[8..], &bad[8..]);
    assert_eq!(&bad[..8], &[0, 0, 0, 4, 0, 0, 0, 4]);
    code(
        read_frame(&mut Cursor::new(bad), 0, Limits::default()),
        "CRC_MISMATCH",
    );
}
#[test]
fn segment_id_header_corruption_is_rejected() {
    header_error("segment_id_bit_flip");
}
#[test]
fn header_version_corruption_is_rejected() {
    header_error("version_bit_flip");
}
#[test]
fn all_external_corruption_errors_match() {
    let v = fixture("integrity-v2.json");
    for entry in v["header_errors"].as_array().unwrap() {
        header_error(entry["name"].as_str().unwrap());
    }
    for entry in v["record_errors"].as_array().unwrap() {
        code(
            read_frame(
                &mut Cursor::new(unhex(entry["hex"].as_str().unwrap())),
                0,
                Limits::default(),
            ),
            entry["error"].as_str().unwrap(),
        );
    }
    let head = unhex(v["header_hex"].as_str().unwrap());
    for n in 0..28 {
        code(
            read_header(&mut Cursor::new(&head[..n])),
            "TRUNCATED_HEADER",
        );
    }
}
#[test]
fn sealed_hash_is_computed_from_finalized_disk_bytes() {
    let root = Directory::new();
    let vectors = fixture("seals-v2.json");
    let expected = &vectors["seals"][0];
    let id = Uuid::parse_str(expected["body"]["segment_id"].as_str().unwrap()).unwrap();
    let mut writer = SegmentWriter::new(&root.0, id, Limits::default()).unwrap();
    for v in fixture("frames.json").as_array().unwrap() {
        writer
            .append(&Frame {
                metadata: unhex(v["metadata_hex"].as_str().unwrap()),
                payload: unhex(v["payload_hex"].as_str().unwrap()),
            })
            .unwrap();
    }
    let sealed = writer.seal().unwrap();
    let actual = fs::read(&sealed.path).unwrap();
    assert_eq!(actual, fs::read(fixtures().join("complete.bqrc")).unwrap());
    assert_eq!(sealed.sha256, raw_hash(&actual));
    assert_eq!(hex(&sealed.sha256), expected["body"]["segment_sha256"]);
    assert_eq!(
        sealed.seal_bytes().unwrap(),
        unhex(expected["canonical_hex"].as_str().unwrap())
    );
    assert_eq!(
        verify_segment(&sealed.path, Limits::default())
            .unwrap()
            .sha256,
        sealed.sha256
    );
    assert_eq!(writer.state(), RawSegmentState::SealedLocal);
    assert!(!sealed.path.with_file_name("seal.partial").exists());
}
#[test]
fn writer_expected_hash_mismatch_blocks_seal() {
    let root = Directory::new();
    let mut writer = SegmentWriter::new(&root.0, Uuid::new_v4(), Limits::default()).unwrap();
    writer
        .append(&Frame {
            metadata: b"ABC".to_vec(),
            payload: b"DEFGH".to_vec(),
        })
        .unwrap();
    // std::fs::File writes are unbuffered; flush the external handle before mutation.
    let mut disk = fs::OpenOptions::new()
        .write(true)
        .open(&writer.partial_path)
        .unwrap();
    disk.flush().unwrap();
    disk.seek(SeekFrom::Start(HEADER_SIZE)).unwrap();
    disk.write_all(&unhex(
        fixture("integrity-v2.json")["alternative_record_hex"]
            .as_str()
            .unwrap(),
    ))
    .unwrap();
    disk.flush().unwrap();
    drop(disk);
    code(writer.seal(), "WRITER_EXPECTED_HASH_MISMATCH");
    assert_eq!(writer.state(), RawSegmentState::Failed);
    assert!(!writer.partial_path.with_file_name("seal.json").exists());
    assert_eq!(
        recover(
            &writer.partial_path.with_file_name("capture.sealed"),
            Limits::default()
        )
        .unwrap()
        .state,
        RawSegmentState::RecoveredUnverified
    );
}
#[test]
fn seal_json_is_canonical_and_round_trips() {
    let root = Directory::new();
    let path = root.original();
    let verified = verify_segment(&path, Limits::default()).unwrap();
    let bytes = fs::read(path.with_file_name("seal.json")).unwrap();
    let vectors = fixture("seals-v2.json");
    assert_eq!(verified.seal_bytes().unwrap(), bytes);
    assert_eq!(hex(&raw_hash(&bytes)), vectors["seals"][0]["seal_sha256"]);
    assert_eq!(verified.seal_origin, SealOrigin::Original);
    assert_eq!(verified.byte_length, fs::metadata(&path).unwrap().len());
    assert_eq!(verified.record_count, 5);
    assert_eq!(
        recover(&path, Limits::default()).unwrap().state as i32,
        vectors["verified_state"].as_i64().unwrap() as i32
    );
}
#[test]
fn sealed_without_seal_json_is_not_sealed_local() {
    let root = Directory::new();
    let path = root.orphan();
    assert_eq!(
        recover(&path, Limits::default()).unwrap().state as i32,
        fixture("seals-v2.json")["orphan_state"].as_i64().unwrap() as i32
    );
    code(verify_segment(&path, Limits::default()), "SEAL_MISSING");
}
#[test]
fn orphan_sealed_segment_can_be_explicitly_recovered() {
    let root = Directory::new();
    let path = root.orphan();
    let before = fs::read(&path).unwrap();
    let actual = recover_seal(&path, Limits::default()).unwrap();
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(actual.seal_origin, SealOrigin::Recovered);
    let bytes = fs::read(path.with_file_name("seal.json")).unwrap();
    let vectors = fixture("seals-v2.json");
    assert_eq!(hex(&bytes), vectors["seals"][1]["canonical_hex"]);
    assert_eq!(hex(&raw_hash(&bytes)), vectors["seals"][1]["seal_sha256"]);
    assert_eq!(
        recover(&path, Limits::default()).unwrap().state as i32,
        vectors["verified_state"].as_i64().unwrap() as i32
    );
    assert_eq!(
        recover_seal(&path, Limits::default()).unwrap().seal_origin,
        SealOrigin::Recovered
    );
}
fn bad_seal(field: &str, value: Value, expected: &str) {
    let root = Directory::new();
    let path = root.original();
    let mut body = fixture("seals-v2.json")["seals"][0]["body"].clone();
    body[field] = value;
    // serde_json's ordered map emits canonical bytes for these ASCII string-only fixtures.
    let bytes = serde_json::to_vec(&body).unwrap();
    fs::write(path.with_file_name("seal.json"), &bytes).unwrap();
    code(verify_segment(&path, Limits::default()), expected);
    code(recover(&path, Limits::default()), expected);
    code(recover_seal(&path, Limits::default()), expected);
    assert_eq!(fs::read(path.with_file_name("seal.json")).unwrap(), bytes);
}
#[test]
fn bad_seal_hash_rejected() {
    bad_seal(
        "segment_sha256",
        "0".repeat(64).into(),
        "SEGMENT_HASH_MISMATCH",
    );
}
#[test]
fn bad_seal_record_count_rejected() {
    bad_seal("record_count", "4".into(), "SEAL_RECORD_COUNT_MISMATCH");
}
#[test]
fn bad_seal_byte_length_rejected() {
    bad_seal("byte_length", "1".into(), "SEAL_BYTE_LENGTH_MISMATCH");
}
#[test]
fn bad_seal_segment_id_rejected() {
    bad_seal(
        "segment_id",
        "00000000-0000-0000-0000-000000000000".into(),
        "SEAL_SEGMENT_ID_MISMATCH",
    );
}
#[test]
fn seal_partial_not_treated_as_valid_seal() {
    let root = Directory::new();
    let path = root.orphan();
    let partial = path.with_file_name("seal.partial");
    let original = unhex(
        fixture("seals-v2.json")["seals"][0]["canonical_hex"]
            .as_str()
            .unwrap(),
    );
    fs::write(&partial, original).unwrap();
    assert_eq!(
        recover(&path, Limits::default()).unwrap().state,
        RawSegmentState::RecoveredUnverified
    );
    assert!(partial.exists());
    assert_eq!(
        recover_seal(&path, Limits::default()).unwrap().seal_origin,
        SealOrigin::Recovered
    );
    assert!(!partial.exists());
    fs::write(&partial, b"incomplete newer evidence").unwrap();
    let before = fs::read(path.with_file_name("seal.json")).unwrap();
    recover_seal(&path, Limits::default()).unwrap();
    assert_eq!(fs::read(path.with_file_name("seal.json")).unwrap(), before);
    assert_eq!(fs::read(partial).unwrap(), b"incomplete newer evidence");
}
#[test]
fn invalid_seal_schema_or_noncanonical_json_rejected() {
    for (field, value) in [
        ("format_version", Value::from("1")),
        ("format_version", Value::from(2)),
        ("byte_length", "01".into()),
        ("byte_length", "18446744073709551616".into()),
        ("record_count", "-1".into()),
        ("seal_origin", "UNKNOWN".into()),
        ("segment_sha256", "F".repeat(64).into()),
        ("extra", "value".into()),
    ] {
        bad_seal(field, value, "INVALID_SEAL_SCHEMA");
    }
    let root = Directory::new();
    let path = root.original();
    let mut bytes = fs::read(path.with_file_name("seal.json")).unwrap();
    bytes.push(b'\n');
    fs::write(path.with_file_name("seal.json"), bytes).unwrap();
    code(
        verify_segment(&path, Limits::default()),
        "NON_CANONICAL_SEAL",
    );
    fs::write(path.with_file_name("seal.json"), b"{\"x\":1,\"x\":1}").unwrap();
    code(
        verify_segment(&path, Limits::default()),
        "INVALID_SEAL_JSON",
    );
}
#[test]
fn existing_seal_partial_blocks_normal_seal() {
    let root = Directory::new();
    let mut writer = SegmentWriter::new(&root.0, Uuid::new_v4(), Limits::default()).unwrap();
    writer
        .append(&Frame {
            metadata: b"ABC".to_vec(),
            payload: b"DEFGH".to_vec(),
        })
        .unwrap();
    let partial = writer.partial_path.with_file_name("seal.partial");
    fs::write(&partial, b"crash evidence").unwrap();
    assert!(writer.seal().is_err());
    assert_eq!(writer.state(), RawSegmentState::Failed);
    assert_eq!(fs::read(partial).unwrap(), b"crash evidence");
    assert!(!writer.partial_path.with_file_name("seal.json").exists());
    let recovered = recover_seal(
        &writer.partial_path.with_file_name("capture.sealed"),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(recovered.seal_origin, SealOrigin::Recovered);
}
