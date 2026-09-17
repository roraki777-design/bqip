use bqip_contracts::{
    hex, identity, manifests,
    metadata::{MetadataCatalog, ResolverMode},
    proto::*,
};
use proptest::prelude::*;
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures");
    serde_json::from_str(&std::fs::read_to_string(root.join(name)).unwrap()).unwrap()
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn metadata(v: &Value) -> InstrumentMetadataVersion {
    let string = |key: &str| v[key].as_str().map(str::to_owned);
    InstrumentMetadataVersion {
        instrument_id: string("instrument_id"),
        metadata_version: string("metadata_version"),
        effective_from_ns: v["effective_from_ns"].as_i64(),
        effective_to_ns: v["effective_to_ns"].as_i64(),
        known_from_ns: v["known_from_ns"].as_i64(),
        known_to_ns: v["known_to_ns"].as_i64(),
        supersedes_version: string("supersedes_version"),
        product_type: string("product_type"),
        margin_asset: string("margin_asset"),
        settlement_asset: string("settlement_asset"),
        ..Default::default()
    }
}

#[test]
fn decimal_golden() {
    for v in fixture("contracts/decimals.json").as_array().unwrap() {
        let decimal = DecimalValue::parse(v["input"].as_str().unwrap()).unwrap();
        assert_eq!(decimal.coefficient.as_deref(), v["coefficient"].as_str());
        assert_eq!(decimal.scale, Some(v["scale"].as_u64().unwrap() as u32));
        decimal.validate().unwrap();
    }
    for text in ["", "+1", "1e3", " 1", "1 ", "NaN", "1.", ".1", "١", "--1"] {
        assert!(DecimalValue::parse(text).is_err(), "{text}");
    }
    assert!(DecimalValue {
        coefficient: None,
        scale: Some(0)
    }
    .validate()
    .is_err());
    assert!(DecimalValue {
        coefficient: Some("0".into()),
        scale: None
    }
    .validate()
    .is_err());
}

#[test]
fn identity_golden() {
    for v in fixture("identity/vectors.json").as_array().unwrap() {
        let parts: Vec<_> = v["components"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| (x[0].as_str().unwrap(), unhex(x[1].as_str().unwrap())))
            .collect();
        let refs: Vec<_> = parts.iter().map(|(k, v)| (*k, v.as_slice())).collect();
        assert_eq!(
            hex(&identity::preimage(&refs).unwrap()),
            v["preimage_hex"].as_str().unwrap()
        );
        assert_eq!(
            hex(&identity::logical_hash(&refs).unwrap()),
            v["sha256"].as_str().unwrap()
        );
    }
    assert_eq!(identity::enum_bytes("LIVE").unwrap(), b"LIVE");
    assert_eq!(
        DecimalValue::parse("123.4500")
            .unwrap()
            .identity_bytes()
            .unwrap(),
        b"12345:2"
    );
}

#[test]
fn metadata_golden_in_both_input_orders() {
    for v in fixture("metadata/vectors.json").as_array().unwrap() {
        let mut versions: Vec<_> = v["versions"]
            .as_array()
            .unwrap()
            .iter()
            .map(metadata)
            .collect();
        for _ in 0..2 {
            let catalog = MetadataCatalog::new(versions.clone()).unwrap();
            let mode = if v["mode"] == "AS_LIVED" {
                ResolverMode::AsLived
            } else {
                ResolverMode::CorrectedResearch
            };
            let result = catalog.resolve(
                "TEST-INSTRUMENT",
                v["event_time_ns"].as_i64().unwrap(),
                mode,
                v["as_of_ns"].as_i64(),
            );
            if let Some(expected) = v["error"].as_str() {
                assert_eq!(result.unwrap_err().0, expected);
            } else {
                assert_eq!(
                    result.unwrap().metadata_version.as_deref(),
                    v["expected"].as_str()
                );
            }
            versions.reverse();
        }
    }
}

#[test]
fn manifest_golden() {
    assert_eq!(
        manifests::canonical_manifest(&manifests::parse_manifest("{\"x\":-0}").unwrap()).unwrap(),
        b"{\"x\":0}"
    );
    for v in fixture("manifests/vectors.json").as_array().unwrap() {
        assert_eq!(
            hex(&manifests::canonical_manifest(&v["input"]).unwrap()),
            v["canonical_hex"].as_str().unwrap()
        );
        assert_eq!(
            hex(&manifests::manifest_hash(&v["input"]).unwrap()),
            v["sha256"].as_str().unwrap()
        );
        let mut envelope = ManifestEnvelope {
            schema_version: Some(1),
            canonical_body_json: Some(unhex(v["canonical_hex"].as_str().unwrap())),
            manifest_hash: Some(unhex(v["sha256"].as_str().unwrap())),
            ..Default::default()
        };
        envelope.validate().unwrap();
        envelope.canonical_body_json.as_mut().unwrap().push(b' ');
        assert_eq!(
            envelope.validate().unwrap_err().0,
            "NON_CANONICAL_MANIFEST_BODY"
        );
    }
    for text in [
        "{\"x\":1,\"x\":2}",
        "{\"x\":1.0}",
        "{\"x\":NaN}",
        "{\"x\":1e3}",
        "{\"x\":9007199254740992}",
        "{\"x\":\"\\ud800\"}",
        "[]",
    ] {
        assert!(manifests::parse_manifest(text).is_err(), "{text}");
    }
}

#[test]
fn independent_jcs_conformance() {
    let vectors = fixture("manifests/conformance.json");
    for vector in vectors["valid"].as_array().unwrap() {
        let document = manifests::parse_manifest(vector["input_json"].as_str().unwrap()).unwrap();
        assert_eq!(
            hex(&manifests::canonical_bytes(&document).unwrap()),
            vector["jcs_utf8_hex"].as_str().unwrap()
        );
        assert_eq!(
            hex(&manifests::canonical_manifest(&document).unwrap()),
            vector["manifest_utf8_hex"].as_str().unwrap()
        );
        assert_eq!(
            hex(&manifests::manifest_hash(&document).unwrap()),
            vector["manifest_sha256"].as_str().unwrap()
        );
    }
    for vector in vectors["invalid"].as_array().unwrap() {
        assert!(
            manifests::parse_manifest(vector["input_json"].as_str().unwrap()).is_err(),
            "{}",
            vector["name"]
        );
    }
}

#[test]
fn shared_enum_external_expected_values() {
    for (name, vectors) in fixture("contracts/enums.json").as_object().unwrap() {
        for vector in vectors.as_array().unwrap() {
            let number = vector["number"].as_i64().unwrap() as i32;
            let actual = match name.as_str() {
                "ReplayMode" => ReplayMode::try_from(number).unwrap().as_str_name(),
                "PayloadKind" => PayloadKind::try_from(number).unwrap().as_str_name(),
                "RawSegmentState" => RawSegmentState::try_from(number).unwrap().as_str_name(),
                "OperationalKind" => OperationalKind::try_from(number).unwrap().as_str_name(),
                "CauseCode" => CauseCode::try_from(number).unwrap().as_str_name(),
                "RawLossStatus" => RawLossStatus::try_from(number).unwrap().as_str_name(),
                "RealtimeContinuity" => RealtimeContinuity::try_from(number).unwrap().as_str_name(),
                _ => panic!("Unknown enum fixture family: {name}"),
            };
            assert_eq!(actual, vector["wire_name"].as_str().unwrap());
        }
    }
    assert!(CauseCode::try_from(999).is_err());
    assert!(RawLossStatus::try_from(999).is_err());
    assert!(RealtimeContinuity::try_from(999).is_err());
}

proptest! {
    #[test]
    fn decimal_normalization_idempotent(coefficient in any::<i128>(), scale in 0u32..100) {
        let first = DecimalValue::normalize(&coefficient.to_string(), scale).unwrap();
        let second = DecimalValue::normalize(first.coefficient.as_deref().unwrap(), first.scale.unwrap()).unwrap();
        prop_assert_eq!(first, second);
    }

    #[test]
    fn preimage_stable_and_order_sensitive(a in proptest::collection::vec(any::<u8>(), 0..100),
                                         b in proptest::collection::vec(any::<u8>(), 0..100)) {
        let first = [("left", a.as_slice()), ("right", b.as_slice())];
        let second = [("right", b.as_slice()), ("left", a.as_slice())];
        prop_assert_eq!(identity::preimage(&first).unwrap(), identity::preimage(&first).unwrap());
        prop_assert_ne!(identity::logical_hash(&first).unwrap(), identity::logical_hash(&second).unwrap());
    }

    #[test]
    fn manifest_ignores_excluded_hash(value in any::<i32>(), excluded in "[a-z0-9]{0,64}") {
        let base = serde_json::json!({"value": value});
        let annotated = serde_json::json!({"value": value, "manifest_hash": excluded});
        prop_assert_eq!(manifests::manifest_hash(&base).unwrap(), manifests::manifest_hash(&annotated).unwrap());
    }

    #[test]
    fn resolver_never_arbitrarily_picks_an_overlap(known in 11i64..1000) {
        let mut first = metadata(&fixture("metadata/vectors.json")[0]["versions"][0]);
        first.effective_to_ns = None;
        let mut second = first.clone();
        second.metadata_version = Some("unrelated".into());
        second.known_from_ns = Some(known);
        let catalog = MetadataCatalog::new(vec![first, second]).unwrap();
        prop_assert_eq!(catalog.resolve("TEST-INSTRUMENT", 150, ResolverMode::CorrectedResearch, None)
                       .unwrap_err().0, "METADATA_AMBIGUOUS");
    }
}
