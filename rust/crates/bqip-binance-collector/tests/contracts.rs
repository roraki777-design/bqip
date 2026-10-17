use bqip_contracts::proto::RawCaptureEvent;
use prost::Message;
use serde_json::Value;
fn bytes(value: &Value) -> Vec<u8> {
    value
        .as_str()
        .unwrap()
        .as_bytes()
        .chunks_exact(2)
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn additive_raw_contexts_match_independently_derived_wire_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../tests/fixtures/collector/wire.json"
    )))
    .unwrap();
    let expected = bytes(&fixture["typed_contexts_hex"]);
    let event = RawCaptureEvent::decode(expected.as_slice()).unwrap();
    assert!(event.http_source.is_some());
    assert!(event.stream_source.is_some());
    assert_eq!(event.encode_to_vec(), expected);
    let expected = bytes(&fixture["content_encoding_hex"]);
    let event = RawCaptureEvent::decode(expected.as_slice()).unwrap();
    assert_eq!(
        event.http_content_encoding,
        Some(bytes(&fixture["content_encoding_bytes_hex"]))
    );
    assert_eq!(event.encode_to_vec(), expected);
}
