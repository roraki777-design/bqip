use bqip_contracts::{
    binance::{
        self as b,
        book::{BookSync, BookSyncState},
    },
    hex, identity, manifests,
    metadata::{MetadataCatalog, ResolverMode},
    proto::*,
    ContractError,
};
use proptest::prelude::*;
use prost::Message;
use serde_json::{json, Value};
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../tests/fixtures/binance-usdm")
}
fn fixture(name: &str) -> Value {
    serde_json::from_slice(&std::fs::read(root().join(name)).unwrap()).unwrap()
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn payload(kind: &str) -> Vec<u8> {
    let name = match kind {
        "trade" => "trade-sell",
        "depth" => "depth-ordinary",
        "snapshot" => "snapshot-valid",
        "exchange" => "exchange-valid",
        _ => panic!("kind"),
    };
    std::fs::read(root().join(format!("raw/{name}.json"))).unwrap()
}
fn lineage(raw: &[u8]) -> SourceLineage {
    SourceLineage {
        venue_profile_id: Some(b::PROFILE_ID.into()),
        venue_profile_hash: Some(b::profile_hash().unwrap().to_vec()),
        capture_id: Some(CaptureId {
            collector_instance_id: Some((0..16).collect()),
            capture_seq: Some(7),
        }),
        raw_content_hash: Some(identity::raw_hash(raw).to_vec()),
        receive_timestamp_ns: Some(5000000),
    }
}
fn stream(kind: &str, raw: &[u8]) -> StreamSourceContext {
    StreamSourceContext {
        lineage: Some(lineage(raw)),
        transport: Some(SourceTransport::Websocket as i32),
        endpoint: Some(
            if kind == "trade" {
                b::TRADE_ENDPOINT
            } else {
                b::DEPTH_ENDPOINT
            }
            .into(),
        ),
        channel: Some(
            if kind == "trade" {
                b::TRADE_STREAM
            } else {
                b::DEPTH_STREAM
            }
            .into(),
        ),
        original_session_id: Some((16..32).collect()),
    }
}
fn http(kind: &str, raw: &[u8]) -> HttpSourceContext {
    HttpSourceContext {
        lineage: Some(lineage(raw)),
        transport: Some(SourceTransport::HttpRest as i32),
        method: Some("GET".into()),
        endpoint: Some(
            if kind == "snapshot" {
                "/fapi/v1/depth"
            } else {
                "/fapi/v1/exchangeInfo"
            }
            .into(),
        ),
        http_status: Some(200),
        query_parameters: if kind == "snapshot" {
            vec![
                QueryParameter {
                    name: Some("limit".into()),
                    value: Some("1000".into()),
                },
                QueryParameter {
                    name: Some("symbol".into()),
                    value: Some("BTCUSDT".into()),
                },
            ]
        } else {
            vec![]
        },
    }
}
fn decimal(d: &DecimalValue) -> Value {
    json!({"coefficient": d.coefficient, "scale": d.scale})
}
fn sides(levels: &[PriceLevel]) -> Value {
    json!(levels
        .iter()
        .map(|l| {
            let p = l.price.as_ref().unwrap();
            let q = l.quantity.as_ref().unwrap();
            vec![
                format!("{}:{}", p.coefficient.as_ref().unwrap(), p.scale.unwrap()),
                format!("{}:{}", q.coefficient.as_ref().unwrap(), q.scale.unwrap()),
            ]
        })
        .collect::<Vec<_>>())
}
fn book_view(book: &BookSync) -> Value {
    let (bids, asks) = book.trusted_book().unwrap();
    json!({"bids":sides(&bids),"asks":sides(&asks)})
}
fn parse(kind: &str, raw: &[u8]) -> Result<(Value, Vec<u8>, Vec<String>), ContractError> {
    match kind {
        "trade" => {
            let (v, drift) = b::parse_trade(raw, &stream(kind, raw))?;
            b::validation::trade(&v)?;
            Ok((
                json!({"price":decimal(v.price.as_ref().unwrap()),"quantity":decimal(v.quantity.as_ref().unwrap()),"quantity_ex_rpi":decimal(v.quantity_ex_rpi.as_ref().unwrap()),
                "aggregate_id":v.aggregate_id,"first_venue_trade_id":v.first_venue_trade_id,"last_venue_trade_id":v.last_venue_trade_id,"buyer_is_maker":v.buyer_is_maker,
                "aggressor":if v.aggressor==Some(2) {"SELL"} else {"BUY"},"granularity":"AGGREGATED","liquidation":"NOT_OBSERVED_FROM_THIS_SOURCE",
                "event_timestamp_ns":v.event_timestamp_ns,"trade_timestamp_ns":v.trade_timestamp_ns}),
                v.encode_to_vec(),
                drift,
            ))
        }
        "depth" => {
            let (v, drift) = b::parse_depth(raw, &stream(kind, raw))?;
            b::validation::depth(&v)?;
            Ok((
                json!({"first_update_id":v.first_update_id,"final_update_id":v.final_update_id,"previous_final_update_id":v.previous_final_update_id,
                "bids":sides(&v.bids),"asks":sides(&v.asks),"event_timestamp_ns":v.event_timestamp_ns,"transaction_timestamp_ns":v.transaction_timestamp_ns}),
                v.encode_to_vec(),
                drift,
            ))
        }
        "snapshot" => {
            let (v, drift) = b::parse_snapshot(raw, &http(kind, raw))?;
            b::validation::snapshot(&v)?;
            Ok((
                json!({"last_update_id":v.last_update_id,"bids":sides(&v.bids),"asks":sides(&v.asks),"event_timestamp_ns":v.event_timestamp_ns,"transaction_timestamp_ns":v.transaction_timestamp_ns}),
                v.encode_to_vec(),
                drift,
            ))
        }
        "exchange" => {
            let (v, drift) = b::parse_exchange_info(raw, &http(kind, raw), "observation-1")?;
            Ok((
                json!({"effective_time_basis":"UNKNOWN","effective_from_ns":v.effective_from_ns,"known_from_ns":v.known_from_ns,
                "tick_size":decimal(v.tick_size.as_ref().unwrap()),"lot_size":decimal(v.lot_size.as_ref().unwrap()),"venue_status":v.venue_status,"quantity_unit":v.quantity_unit,
                "minimum_quantity":decimal(v.minimum_quantity.as_ref().unwrap()),"maximum_quantity":decimal(v.maximum_quantity.as_ref().unwrap())}),
                v.encode_to_vec(),
                drift,
            ))
        }
        _ => panic!("fixture kind"),
    }
}
fn changed(kind: &str, changes: Value) -> Vec<u8> {
    let mut value: Value = serde_json::from_slice(&payload(kind)).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .extend(changes.as_object().unwrap().clone());
    serde_json::to_vec(&value).unwrap()
}
fn depth(changes: Value) -> BinanceDepthUpdate {
    let raw = changed("depth", changes);
    b::parse_depth(&raw, &stream("depth", &raw)).unwrap().0
}
fn snapshot() -> BinanceDepthSnapshot {
    let raw = payload("snapshot");
    b::parse_snapshot(&raw, &http("snapshot", &raw)).unwrap().0
}
fn running() -> BookSync {
    let mut book = BookSync::new(&(16..32).collect::<Vec<_>>()).unwrap();
    book.begin().unwrap();
    book.request_snapshot().unwrap();
    book
}
fn metadata(v: &Value) -> InstrumentMetadataVersion {
    let s = |k: &str| v[k].as_str().map(str::to_owned);
    InstrumentMetadataVersion {
        instrument_id: s("instrument_id"),
        metadata_version: s("metadata_version"),
        effective_from_ns: v["effective_from_ns"].as_i64(),
        effective_to_ns: v["effective_to_ns"].as_i64(),
        known_from_ns: v["known_from_ns"].as_i64(),
        known_to_ns: v["known_to_ns"].as_i64(),
        supersedes_version: s("supersedes_version"),
        product_type: s("product_type"),
        margin_asset: s("margin_asset"),
        settlement_asset: s("settlement_asset"),
        effective_time_basis: v["effective_time_basis"].as_i64().map(|n| n as i32),
        ..Default::default()
    }
}

#[test]
fn all_external_parser_vectors() {
    for v in fixture("parsing.json").as_array().unwrap() {
        let raw = std::fs::read(root().join(v["raw_file"].as_str().unwrap())).unwrap();
        assert_eq!(
            hex(&identity::raw_hash(&raw)),
            v["raw_sha256"].as_str().unwrap()
        );
        let result = parse(v["kind"].as_str().unwrap(), &raw);
        if let Some(error) = v["error"].as_str() {
            assert_eq!(result.unwrap_err().0, error, "{}", v["name"]);
        } else {
            assert_eq!(result.unwrap().0, v["expected"], "{}", v["name"]);
        }
    }
}
#[test]
fn profile_external_jcs_and_hash() {
    let v = fixture("profile.json");
    let profile = manifests::parse_manifest(b::PROFILE_JSON).unwrap();
    assert_eq!(profile, v["input"]);
    assert_eq!(
        hex(&manifests::canonical_manifest(&profile).unwrap()),
        v["canonical_hex"].as_str().unwrap()
    );
    assert_eq!(
        hex(&b::profile_hash().unwrap()),
        v["sha256"].as_str().unwrap()
    );
}
#[test]
fn temporal_external_vectors_both_orders() {
    for v in fixture("metadata.json").as_array().unwrap() {
        let mut versions: Vec<_> = v["versions"]
            .as_array()
            .unwrap()
            .iter()
            .map(metadata)
            .collect();
        for _ in 0..2 {
            let mode = if v["mode"] == "AS_LIVED" {
                ResolverMode::AsLived
            } else {
                ResolverMode::CorrectedResearch
            };
            let result = MetadataCatalog::new(versions.clone()).and_then(|catalog| {
                let r = catalog.resolve_with_basis(
                    "TEST",
                    v["event_time_ns"].as_i64().unwrap(),
                    mode,
                    v["as_of_ns"].as_i64(),
                )?;
                Ok((
                    r.version.metadata_version.clone(),
                    r.resolution_basis as i32,
                    r.quality,
                ))
            });
            if let Some(error) = v["error"].as_str() {
                assert_eq!(result.unwrap_err().0, error, "{}", v["name"]);
            } else {
                let (version, basis, quality) = result.unwrap();
                assert_eq!(version.as_deref(), v["expected"].as_str());
                assert_eq!(i64::from(basis), v["basis"].as_i64().unwrap());
                assert_eq!(quality, v["quality"].as_str());
            }
            versions.reverse();
        }
    }
}
#[test]
fn identity_external_preimages_and_hashes() {
    for v in fixture("identity.json").as_array().unwrap() {
        let parts: Vec<_> = v["components"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| (p[0].as_str().unwrap(), unhex(p[1].as_str().unwrap())))
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
        let actual = if parts[3].1 == b"AGG_TRADE" {
            b::trade_id(std::str::from_utf8(&parts[4].1).unwrap().parse().unwrap()).unwrap()
        } else {
            b::depth_id(
                &parts[4].1,
                std::str::from_utf8(&parts[5].1).unwrap().parse().unwrap(),
                std::str::from_utf8(&parts[6].1).unwrap().parse().unwrap(),
            )
            .unwrap()
        };
        assert_eq!(hex(&actual), v["sha256"].as_str().unwrap());
    }
}
#[test]
fn wire_external_vectors_and_decoded_validation() {
    for v in fixture("wire.json").as_array().unwrap() {
        let kind = v["kind"].as_str().unwrap();
        let raw = std::fs::read(root().join(v["raw_file"].as_str().unwrap())).unwrap();
        let expected = unhex(v["wire_hex"].as_str().unwrap());
        assert_eq!(parse(kind, &raw).unwrap().1, expected);
        assert_eq!(
            hex(&identity::raw_hash(&expected)),
            v["sha256"].as_str().unwrap()
        );
        match kind {
            "trade" => {
                b::validation::trade(&TradeEvent::decode(expected.as_slice()).unwrap()).unwrap()
            }
            "depth" => {
                b::validation::depth(&BinanceDepthUpdate::decode(expected.as_slice()).unwrap())
                    .unwrap()
            }
            "snapshot" => {
                b::validation::snapshot(&BinanceDepthSnapshot::decode(expected.as_slice()).unwrap())
                    .unwrap()
            }
            "exchange" => InstrumentMetadataVersion::decode(expected.as_slice())
                .unwrap()
                .validate_metadata()
                .unwrap(),
            _ => panic!("kind"),
        };
    }
}
#[test]
fn shared_new_enum_tags_external() {
    for (family, values) in fixture("enums.json").as_object().unwrap() {
        for (name, n) in values.as_object().unwrap() {
            let number = n.as_i64().unwrap() as i32;
            let actual = match family.as_str() {
                "EffectiveTimeBasis" => EffectiveTimeBasis::try_from(number).unwrap().as_str_name(),
                "MetadataResolutionBasis" => MetadataResolutionBasis::try_from(number)
                    .unwrap()
                    .as_str_name(),
                "TradeGranularity" => TradeGranularity::try_from(number).unwrap().as_str_name(),
                "AggressorSide" => AggressorSide::try_from(number).unwrap().as_str_name(),
                "LiquidationObservation" => LiquidationObservation::try_from(number)
                    .unwrap()
                    .as_str_name(),
                "SourceTransport" => SourceTransport::try_from(number).unwrap().as_str_name(),
                "CauseCode_additions" => CauseCode::try_from(number).unwrap().as_str_name(),
                _ => panic!("family"),
            };
            let prefix = match family.as_str() {
                "EffectiveTimeBasis" => "EFFECTIVE_TIME_BASIS",
                "MetadataResolutionBasis" => "METADATA_RESOLUTION_BASIS",
                "TradeGranularity" => "TRADE_GRANULARITY",
                "AggressorSide" => "AGGRESSOR_SIDE",
                "LiquidationObservation" => "LIQUIDATION_OBSERVATION",
                "SourceTransport" => "SOURCE_TRANSPORT",
                _ => "CAUSE_CODE",
            };
            assert_eq!(actual, format!("{prefix}_{name}"));
        }
    }
}
#[test]
fn bridge_boundary_external_vectors() {
    for v in fixture("boundaries.json").as_array().unwrap() {
        let mut book = running();
        book.ingest(&depth(json!({"U":v["U"],"u":v["u"]}))).unwrap();
        book.snapshot(&snapshot()).unwrap();
        assert_eq!(
            book.state().token(),
            v["state"].as_str().unwrap(),
            "{}",
            v["name"]
        );
    }
}
#[test]
fn external_sync_sequence() {
    let v = fixture("sync.json");
    let events: Vec<_> = v["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let raw = serde_json::to_vec(e).unwrap();
            b::parse_depth(&raw, &stream("depth", &raw)).unwrap().0
        })
        .collect();
    let mut book = running();
    book.ingest(&events[0]).unwrap();
    book.ingest(&events[1]).unwrap();
    book.snapshot(&snapshot()).unwrap();
    assert_eq!(book_view(&book), v["after_bridge"]);
    book.ingest(&events[2]).unwrap();
    assert_eq!(book_view(&book), v["after_next"]);
    assert_eq!(book.ingest(&events[3]).unwrap_err().0, "VENUE_GAP");
    assert_eq!(
        json!(book
            .transitions()
            .iter()
            .map(|s| s.token())
            .collect::<Vec<_>>()),
        v["transitions"]
    );
    assert!(book.trusted_book().is_err());
    assert!(book.ingest(&events[2]).is_err());
}
#[test]
fn content_duplicate_ignores_observation_hash_and_time() {
    let mut book = running();
    let first = depth(json!({}));
    book.ingest(&first).unwrap();
    let mut next = first.clone();
    let lineage = next.source.as_mut().unwrap().lineage.as_mut().unwrap();
    lineage.raw_content_hash = Some(vec![9; 32]);
    lineage.receive_timestamp_ns = Some(999);
    lineage.capture_id.as_mut().unwrap().capture_seq = Some(8);
    assert_eq!(book.ingest(&next).unwrap(), "DUPLICATE");
}
#[test]
fn pu_conflict_same_logical_identity() {
    let mut book = running();
    let first = depth(json!({}));
    let next = depth(json!({"pu":1}));
    assert_eq!(first.logical_event_id, next.logical_event_id);
    book.ingest(&first).unwrap();
    assert_eq!(book.ingest(&next).unwrap_err().0, "VENUE_IDENTITY_CONFLICT");
    assert_eq!(book.state(), BookSyncState::ResyncRequired);
}
#[test]
fn level_conflict_same_logical_identity() {
    let mut book = running();
    let first = depth(json!({}));
    let next = depth(json!({"b":[["100","3"]]}));
    assert_eq!(first.logical_event_id, next.logical_event_id);
    book.ingest(&first).unwrap();
    assert_eq!(book.ingest(&next).unwrap_err().0, "VENUE_IDENTITY_CONFLICT");
}
#[test]
fn session_scope_and_replayed_session_mismatch() {
    let raw = payload("depth");
    let mut src = stream("depth", &raw);
    src.original_session_id = Some((32..48).collect());
    let other = b::parse_depth(&raw, &src).unwrap().0;
    assert_ne!(other.logical_event_id, depth(json!({})).logical_event_id);
    assert_eq!(
        running().ingest(&other).unwrap_err().0,
        "ORIGINAL_SESSION_MISMATCH"
    );
    let raw = payload("trade");
    let mut src = stream("trade", &raw);
    let first = b::parse_trade(&raw, &src).unwrap().0;
    src.original_session_id = Some((32..48).collect());
    assert_eq!(
        first.logical_event_id,
        b::parse_trade(&raw, &src).unwrap().0.logical_event_id
    );
}
#[test]
fn buffer_owns_data_and_decimal_spelling_deduplicates() {
    let mut book = running();
    let mut event = depth(json!({}));
    book.ingest(&event).unwrap();
    event.bids[0].quantity.as_mut().unwrap().coefficient = Some("999".into());
    book.snapshot(&snapshot()).unwrap();
    assert_eq!(book_view(&book)["bids"][0], json!(["100:0", "2:0"]));
    assert_eq!(
        book.ingest(&depth(json!({"b":[["100.00","2.00"]]})))
            .unwrap(),
        "DUPLICATE"
    );
}
#[test]
fn missing_each_required_native_field_fails() {
    for (kind, fields) in [
        (
            "trade",
            vec!["e", "E", "s", "a", "p", "q", "nq", "f", "l", "T", "m"],
        ),
        ("depth", vec!["e", "E", "T", "s", "U", "u", "pu", "b", "a"]),
        ("snapshot", vec!["lastUpdateId", "E", "T", "bids", "asks"]),
    ] {
        for field in fields {
            let mut v: Value = serde_json::from_slice(&payload(kind)).unwrap();
            v.as_object_mut().unwrap().remove(field);
            assert!(
                parse(kind, &serde_json::to_vec(&v).unwrap()).is_err(),
                "{kind} {field}"
            );
        }
    }
}
#[test]
fn unknown_additive_fields_observable() {
    for kind in ["trade", "depth"] {
        let raw = changed(kind, json!({"newMetric":1.5}));
        assert!(parse(kind, &raw).unwrap().2.contains(&"newMetric".into()));
        let mut raw = payload(kind);
        raw.pop();
        raw.extend_from_slice(b",\"future\":-0}");
        assert!(parse(kind, &raw).unwrap().2.contains(&"future".into()));
    }
}
#[test]
fn duplicate_json_keys_and_nonfinite_rejected() {
    for suffix in [
        b",\"a\":8}".as_slice(),
        b",\"future\":NaN}",
        b",\"future\":\"\\ud800\"}",
    ] {
        let mut raw = payload("trade");
        raw.pop();
        raw.extend_from_slice(suffix);
        assert!(parse("trade", &raw).is_err());
    }
    let raw = String::from_utf8(changed("trade", json!({"a":"NEGATIVE_ZERO"})))
        .unwrap()
        .replace("\"NEGATIVE_ZERO\"", "-0");
    assert_eq!(
        parse("trade", raw.as_bytes()).unwrap_err().0,
        "PARSE_FAILURE"
    );
}
#[test]
fn http_status_and_payload_hash_are_checked() {
    let raw = payload("snapshot");
    let mut src = http("snapshot", &raw);
    src.http_status = Some(500);
    assert_eq!(
        b::parse_snapshot(&raw, &src).unwrap_err().0,
        "HTTP_STATUS_FAILURE"
    );
    src.http_status = Some(200);
    src.lineage.as_mut().unwrap().raw_content_hash = Some(vec![0; 32]);
    assert_eq!(
        b::parse_snapshot(&raw, &src).unwrap_err().0,
        "RAW_HASH_MISMATCH"
    );
}
#[test]
fn metadata_legacy_api_cannot_hide_unknown_effective_time() {
    let raw = payload("exchange");
    let value = b::parse_exchange_info(&raw, &http("exchange", &raw), "v1")
        .unwrap()
        .0;
    let catalog = MetadataCatalog::new(vec![value]).unwrap();
    assert_eq!(
        catalog
            .resolve(b::INSTRUMENT_ID, 1, ResolverMode::AsLived, Some(5000000))
            .unwrap_err()
            .0,
        "LABELED_METADATA_RESOLUTION_REQUIRED"
    );
}
#[test]
fn snapshot_does_not_skip_first_nonstale_event() {
    let mut book = running();
    book.ingest(&depth(json!({"U":101,"u":105}))).unwrap();
    book.ingest(&depth(json!({"U":98,"u":102}))).unwrap();
    book.snapshot(&snapshot()).unwrap();
    assert_eq!(book.state(), BookSyncState::Bridging);
    assert!(book.trusted_book().is_err());
}
#[test]
fn market_wire_presence_and_semantics_are_validated() {
    let raw = payload("trade");
    let valid = b::parse_trade(&raw, &stream("trade", &raw)).unwrap().0;
    let mut invalid = valid.clone();
    invalid.granularity = Some(TradeGranularity::Individual as i32);
    assert!(b::validation::trade(&invalid).is_err());
    invalid = valid;
    invalid.quantity_ex_rpi = None;
    assert!(b::validation::trade(&invalid).is_err());
    let mut event = depth(json!({}));
    event.logical_event_id = Some(vec![0; 32]);
    assert!(b::validation::depth(&event).is_err());
    let mut snap = snapshot();
    snap.last_update_id = None;
    assert!(b::validation::snapshot(&snap).is_err());
}

proptest! {
    #[test]
    fn property_decimal_exact(coefficient in 1u64..1_000_000_000, zeros in 0usize..15) {
        let input=format!("{coefficient}.{}", "0".repeat(zeros+1));let parsed=DecimalValue::parse(&input).unwrap();prop_assert_eq!(parsed.coefficient,Some(coefficient.to_string()));prop_assert_eq!(parsed.scale,Some(0));
    }
    #[test]
    fn property_timestamp_checked(ms in any::<u64>()) { let expected=i64::try_from(ms).ok().and_then(|n|n.checked_mul(1_000_000));prop_assert_eq!(b::ms_to_ns(ms).ok(),expected); }
    #[test]
    fn property_identity_namespace_and_session(id in any::<u64>(), session in any::<[u8;16]>()) {
        prop_assert_eq!(b::trade_id(id).unwrap(),b::trade_id(id).unwrap());let a=b::depth_id(&session,id,id).unwrap();let mut changed=session;changed[0]^=1;prop_assert_ne!(a,b::depth_id(&changed,id,id).unwrap());prop_assert_ne!(a,b::trade_id(id).unwrap());
    }
    #[test]
    fn property_absolute_and_zero_idempotent(quantity in 1u64..100000) {
        let mut book=running();book.ingest(&depth(json!({}))).unwrap();book.snapshot(&snapshot()).unwrap();
        for last in 103..110 { let event=depth(json!({"U":last,"u":last,"pu":last-1,"b":[["100",quantity.to_string()],["50","0"]],"a":[["101","0"]]}));book.ingest(&event).unwrap();prop_assert_eq!(book_view(&book),json!({"bids":[["100:0",format!("{quantity}:0")],["99:0","2:0"]],"asks":[]})); }
    }
    #[test]
    fn property_pu_mismatch_never_healthy(previous in 0u64..102) { let mut book=running();book.ingest(&depth(json!({}))).unwrap();book.snapshot(&snapshot()).unwrap();prop_assert_eq!(book.ingest(&depth(json!({"U":103,"u":104,"pu":previous}))).unwrap_err().0,"VENUE_GAP");prop_assert_eq!(book.state(),BookSyncState::ResyncRequired); }
    #[test]
    fn property_st_two_never_usdm(id in any::<u64>()) { let raw=changed("trade",json!({"a":id,"st":2}));prop_assert_eq!(parse("trade",&raw).unwrap_err().0,"PROFILE_MISMATCH"); }
    #[test]
    fn property_precisions_never_replace_filters(price in 0u32..30, quantity in 0u32..30) { let mut value:Value=serde_json::from_slice(&payload("exchange")).unwrap();value["symbols"][0]["pricePrecision"]=json!(price);value["symbols"][0]["quantityPrecision"]=json!(quantity);let raw=serde_json::to_vec(&value).unwrap();let metadata=b::parse_exchange_info(&raw,&http("exchange",&raw),"v1").unwrap().0;prop_assert_eq!(metadata.tick_size,Some(DecimalValue::parse("0.1").unwrap()));prop_assert_eq!(metadata.lot_size,Some(DecimalValue::parse("0.001").unwrap())); }
}
