use bqip_capture_format::{read_frame, Frame, Limits};
use bqip_contracts::{binance, hex, identity::raw_hash, proto::*};
use prost::Message;
use serde_json::Value;
use std::io::Cursor;

#[test]
fn bqrc_replay_preserves_raw_lineage_and_normative_depth_bytes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/fixtures/binance-usdm");
    let payload = std::fs::read(root.join("raw/depth-ordinary.json")).unwrap();
    let raw = RawCaptureEvent {
        capture_id: Some(CaptureId {
            collector_instance_id: Some((0..16).collect()),
            capture_seq: Some(7),
        }),
        connection_id: Some((0..16).collect()),
        session_id: Some((16..32).collect()),
        transport: Some("WEBSOCKET".into()),
        source: Some("BINANCE".into()),
        endpoint: Some(binance::DEPTH_ENDPOINT.into()),
        channel: Some(binance::DEPTH_STREAM.into()),
        receive_timestamp_ns: Some(5000000),
        monotonic_ns_since_collector_start: Some(100),
        payload_kind: Some(PayloadKind::Text as i32),
        payload_length: Some(payload.len() as u32),
        raw_content_hash: Some(raw_hash(&payload).to_vec()),
        ..Default::default()
    };
    let frame = Frame {
        metadata: raw.encode_to_vec(),
        payload: payload.clone(),
    };
    let reread = read_frame(
        &mut Cursor::new(frame.encode(Limits::default()).unwrap()),
        0,
        Limits::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(reread.payload, payload);
    let decoded = reread.decode_event().unwrap();
    let event = binance::replay_depth(&decoded, &reread.payload).unwrap().0;
    assert_eq!(
        event.source.as_ref().unwrap().original_session_id,
        raw.session_id
    );
    assert_eq!(
        event
            .source
            .as_ref()
            .unwrap()
            .lineage
            .as_ref()
            .unwrap()
            .capture_id,
        raw.capture_id
    );
    let fixtures: Value =
        serde_json::from_slice(&std::fs::read(root.join("wire.json")).unwrap()).unwrap();
    let expected = fixtures
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["kind"] == "depth")
        .unwrap();
    assert_eq!(
        hex(&event.encode_to_vec()),
        expected["wire_hex"].as_str().unwrap()
    );
    let mut changed = payload;
    changed.push(b' ');
    assert!(binance::replay_depth(&decoded, &changed).is_err());
}
