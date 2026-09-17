use bqip_capture_format::*;
use bqip_contracts::{
    hex,
    identity::raw_hash,
    proto::{CaptureId, ProcessingContext, RawCaptureEvent, RawSegmentState, ReplayMode},
};
use proptest::prelude::*;
use prost::Message;
use serde_json::Value;
use std::io::Cursor;
use std::path::PathBuf;
use uuid::Uuid;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures/capture")
}
fn vectors() -> Vec<Value> {
    serde_json::from_str(&std::fs::read_to_string(root().join("frames.json")).unwrap()).unwrap()
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
struct TestDirectory(PathBuf);
impl TestDirectory {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("bqip-test-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        Self(directory)
    }
}
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn normative_frames_payload_hashes_and_typed_protobuf() {
    for vector in vectors() {
        let bytes = unhex(vector["frame_hex"].as_str().unwrap());
        let frame = read_frame(&mut Cursor::new(&bytes), 0, Limits::default())
            .unwrap()
            .unwrap();
        assert_eq!(frame.encode(Limits::default()).unwrap(), bytes);
        assert_eq!(hex(&frame.payload), vector["payload_hex"].as_str().unwrap());
        assert_eq!(
            hex(&raw_hash(&frame.payload)),
            vector["raw_sha256"].as_str().unwrap()
        );
        let event = frame.decode_event().unwrap();
        assert_eq!(
            event.capture_id.as_ref().unwrap().capture_seq,
            vector["capture_seq"].as_u64()
        );
        assert_eq!(event.receive_timestamp_ns, Some(1234));
        assert_eq!(event.monotonic_ns_since_collector_start, Some(7));
        assert_eq!(
            event.payload_kind,
            Some(vector["payload_kind"].as_i64().unwrap() as i32)
        );
    }
}

#[test]
fn absent_fields_are_not_zero_and_semantic_replay_preserves_identity() {
    assert!(RawCaptureEvent::default().validate(&[]).is_err());
    for vector in vectors() {
        let event =
            RawCaptureEvent::decode(unhex(vector["metadata_hex"].as_str().unwrap()).as_slice())
                .unwrap();
        let replayed = RawCaptureEvent::decode(event.encode_to_vec().as_slice()).unwrap();
        assert_eq!(event.capture_id, replayed.capture_id);
        assert!(event.semantic_equal(&replayed));
    }
}

#[test]
fn every_truncated_frame_and_bad_crc_fail() {
    let bytes = unhex(vectors()[0]["frame_hex"].as_str().unwrap());
    for length in 1..bytes.len() {
        assert!(read_frame(&mut Cursor::new(&bytes[..length]), 0, Limits::default()).is_err());
    }
    let mut corrupt = bytes;
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(matches!(
        read_frame(&mut Cursor::new(corrupt), 0, Limits::default()),
        Err(Error::Format {
            code: "CRC_MISMATCH",
            ..
        })
    ));
    assert!(read_frame(&mut Cursor::new([255; 8]), 0, Limits::default()).is_err());
}

#[test]
fn local_seal_reopen_and_recovery_preserve_source() {
    let directory = TestDirectory::new();
    let mut writer = SegmentWriter::new(&directory.0, Uuid::new_v4(), Limits::default()).unwrap();
    let frame = Frame {
        metadata: vec![80, 1],
        payload: vec![0, 255, 1],
    };
    writer.append(&frame).unwrap();
    let sealed = writer.seal().unwrap();
    assert!(!writer.partial_path.exists());
    assert_eq!(
        sealed.sha256,
        raw_hash(&std::fs::read(&sealed.path).unwrap())
    );
    assert_eq!(
        verify_segment(&sealed.path, Limits::default())
            .unwrap()
            .record_count,
        1
    );
    let report = recover(&sealed.path, Limits::default()).unwrap();
    assert_eq!(report.record_count, 1);
    assert_eq!(report.state, RawSegmentState::SealedLocal);
    let source = root().join("truncated.bqrc");
    let before = std::fs::read(&source).unwrap();
    let (report, recovered) =
        recover_to(&source, &directory.0, Uuid::new_v4(), Limits::default()).unwrap();
    assert_eq!(report.failure, Some("TRUNCATED_RECORD"));
    assert_eq!(report.record_count, 4);
    assert_eq!(recovered.record_count, 4);
    assert_eq!(std::fs::read(source).unwrap(), before);
    assert!(recover_to(
        &root().join("corrupt.bqrc"),
        &directory.0,
        Uuid::new_v4(),
        Limits::default()
    )
    .is_err());
}

#[test]
fn failure_before_seal_and_existing_target_never_overwrite_evidence() {
    let directory = TestDirectory::new();
    let id = Uuid::new_v4();
    let partial;
    {
        let mut writer = SegmentWriter::new(&directory.0, id, Limits::default()).unwrap();
        writer
            .append(&Frame {
                metadata: vec![],
                payload: b"evidence".to_vec(),
            })
            .unwrap();
        partial = writer.partial_path.clone();
    }
    assert_eq!(
        recover(&partial, Limits::default()).unwrap().state,
        RawSegmentState::RecoveredPartial
    );
    assert!(SegmentWriter::new(&directory.0, id, Limits::default()).is_err());
    let mut writer = SegmentWriter::new(&directory.0, Uuid::new_v4(), Limits::default()).unwrap();
    let sealed = writer.partial_path.with_extension("sealed");
    std::fs::write(&sealed, b"existing evidence").unwrap();
    assert!(writer.seal().is_err());
    assert_eq!(writer.state(), RawSegmentState::Failed);
    assert_eq!(std::fs::read(sealed).unwrap(), b"existing evidence");
}

#[test]
fn crc32c_standard_and_every_golden_record() {
    let checks: Value =
        serde_json::from_str(&std::fs::read_to_string(root().join("crc32c.json")).unwrap())
            .unwrap();
    for vector in checks.as_array().unwrap() {
        let actual = crc32c::crc32c(&unhex(vector["input_hex"].as_str().unwrap()));
        assert_eq!(
            format!("{actual:08x}"),
            vector["crc32c_hex"].as_str().unwrap()
        );
    }
    for vector in vectors() {
        let frame = unhex(vector["frame_hex"].as_str().unwrap());
        let expected = u32::from_be_bytes(frame[frame.len() - 4..].try_into().unwrap());
        assert_eq!(crc32c::crc32c(&frame[..frame.len() - 4]), expected);
    }
}

#[test]
fn external_segment_hash_detects_whole_frame_deletion() {
    let directory = TestDirectory::new();
    let path = directory.0.join("capture.sealed");
    let bytes = std::fs::read(root().join("complete.bqrc")).unwrap();
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(root().join("segments.json")).unwrap())
            .unwrap();
    let hash: [u8; 32] = unhex(expected["complete_sha256"].as_str().unwrap())
        .try_into()
        .unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let seals: Value =
        serde_json::from_str(&std::fs::read_to_string(root().join("seals-v2.json")).unwrap())
            .unwrap();
    std::fs::write(
        path.with_file_name("seal.json"),
        unhex(seals["seals"][0]["canonical_hex"].as_str().unwrap()),
    )
    .unwrap();
    assert_eq!(
        verify_segment(&path, Limits::default()).unwrap().sha256,
        hash
    );
    assert_eq!(
        verify_segment(&path, Limits::default())
            .unwrap()
            .record_count,
        5
    );
    let boundary = expected["truncated_prefix_bytes"].as_u64().unwrap() as usize;
    std::fs::write(&path, &bytes[..boundary]).unwrap();
    let mut cursor = Cursor::new(&bytes[..boundary]);
    read_header(&mut cursor).unwrap();
    let mut count = 0;
    while read_frame(&mut cursor, HEADER_SIZE, Limits::default())
        .unwrap()
        .is_some()
    {
        count += 1;
    }
    assert_eq!(count, 4);
    assert!(matches!(
        verify_segment(&path, Limits::default()),
        Err(Error::Format {
            code: "SEAL_BYTE_LENGTH_MISMATCH",
            ..
        })
    ));
}

proptest! {
    #[test]
    fn replay_preserves_capture_identity(
        collector in any::<[u8; 16]>(), sequence in any::<u64>(),
        run_id in any::<[u8; 16]>(), processing_time in any::<i64>(),
    ) {
        let vector = &vectors()[0];
        let mut original = RawCaptureEvent::decode(
            unhex(vector["metadata_hex"].as_str().unwrap()).as_slice()).unwrap();
        original.capture_id = Some(CaptureId {
            collector_instance_id: Some(collector.to_vec()), capture_seq: Some(sequence),
        });
        let mut replayed = RawCaptureEvent::decode(original.encode_to_vec().as_slice()).unwrap();
        replayed.processing_context = Some(ProcessingContext {
            processing_timestamp_ns: Some(processing_time), code_commit: Some("fixture".into()),
            schema_version: Some(1), run_id: Some(run_id.to_vec()),
            replay_mode: Some(ReplayMode::VersionPinnedResearch as i32),
        });
        replayed.validate(&unhex(vector["payload_hex"].as_str().unwrap())).unwrap();
        prop_assert_eq!(&original.capture_id, &replayed.capture_id);
        prop_assert!(original.semantic_equal(&replayed));
    }

    #[test]
    fn frame_roundtrip(metadata in proptest::collection::vec(any::<u8>(), 0..256),
                       payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let frame = Frame { metadata, payload };
        let encoded = frame.encode(Limits::default()).unwrap();
        let decoded = read_frame(&mut Cursor::new(encoded), 0, Limits::default()).unwrap().unwrap();
        prop_assert_eq!(frame, decoded);
    }
}
