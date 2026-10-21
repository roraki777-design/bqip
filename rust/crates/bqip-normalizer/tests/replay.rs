mod support;
use bqip_binance_collector::capture::Lane;
use bqip_contracts::{binance, hex, metadata::ResolverMode, proto::*};
use bqip_normalizer::{
    config::BUILD_COMMIT,
    input::{relative_path, Segment},
    metadata::Catalog,
    normalize::{
        metadata_observation, normalize_record, Outcome, OutcomeValue, Payload, RecordPosition,
    },
    plan::{Mode, ReplayPlan},
    transcript, Error, Replay,
};
use prost::Message;
use serde_json::{json, Value};
use std::{fs, io, path::Path, process::Command};
use support::*;
use uuid::Uuid;

fn golden() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../tests/fixtures/replay/golden.json"
    )))
    .unwrap()
}
fn replay(f: &Fixture) -> Replay<'_> {
    Replay {
        source_root: &f.source,
        plan: &f.plan,
        config: &f.config,
        catalog: Some(&f.catalog),
    }
}
fn position(lane: Lane) -> RecordPosition {
    RecordPosition {
        segment_id: Uuid::from_u128(1),
        segment_ordinal: 0,
        record_index: 0,
        lane,
        loss_recorded: false,
    }
}
fn outcomes(f: &Fixture) -> (Vec<Outcome>, Vec<u8>, bqip_normalizer::Summary) {
    let mut out = Vec::new();
    let mut bytes = Vec::new();
    let summary = replay(f)
        .execute(&context(1, 10), &mut bytes, |o| {
            out.push(o.clone());
            Ok(())
        })
        .unwrap();
    (out, bytes, summary)
}
fn d(c: &str, scale: u32) -> Option<DecimalValue> {
    Some(DecimalValue {
        coefficient: Some(c.into()),
        scale: Some(scale),
    })
}
fn levels(rows: &[(&str, &str)]) -> Vec<PriceLevel> {
    rows.iter()
        .map(|(p, q)| PriceLevel {
            price: d(p, 0),
            quantity: d(q, 0),
        })
        .collect()
}
fn bytes_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// Explicit decoded typed values, including original lineage, independent of replay implementation.
fn assert_semantics(o: &Outcome) {
    let g = golden();
    if let OutcomeValue::Market(e) = &o.value {
        assert_eq!(e.metadata_version, g["confirmed_metadata_version"]);
        assert_eq!(
            e.metadata_resolution_basis,
            MetadataResolutionBasis::EffectiveInterval
        );
        assert_eq!(e.lineage, o.lineage);
        assert_eq!(e.lineage.original_receive_timestamp_ns, 5_000_000);
        let seq = e.lineage.capture_id.capture_seq.unwrap();
        match &e.payload {
            Payload::Trade(t) => {
                let expected = TradeEvent {
                    instrument_id: Some(binance::INSTRUMENT_ID.into()),
                    venue_event_id: Some("7".into()),
                    logical_event_id: Some(bytes_hex(g["trade_logical_id"].as_str().unwrap())),
                    price: d("100", 0),
                    quantity: d("6", 3),
                    quantity_ex_rpi: d("4", 3),
                    event_timestamp_ns: Some(2_000_000),
                    trade_timestamp_ns: Some(1_000_000),
                    native_event_time_ms: Some(2),
                    native_trade_time_ms: Some(1),
                    aggressor: Some(AggressorSide::Buy as i32),
                    granularity: Some(TradeGranularity::Aggregated as i32),
                    aggregate_id: Some(7),
                    first_venue_trade_id: Some(10),
                    last_venue_trade_id: Some(12),
                    buyer_is_maker: Some(false),
                    liquidation: Some(LiquidationObservation::NotObservedFromThisSource as i32),
                    source: raw(Lane::Aggtrade, TRADE, seq, None).stream_source,
                };
                assert_eq!(*t, expected);
            }
            Payload::Depth(t) => assert_eq!(
                *t,
                BinanceDepthUpdate {
                    first_update_id: Some(98),
                    final_update_id: Some(102),
                    previous_final_update_id: Some(97),
                    native_event_time_ms: Some(2),
                    native_transaction_time_ms: Some(1),
                    event_timestamp_ns: Some(2_000_000),
                    transaction_timestamp_ns: Some(1_000_000),
                    bids: levels(&[("100", "2")]),
                    asks: levels(&[("101", "3")]),
                    logical_event_id: Some(bytes_hex(g["depth_logical_id"].as_str().unwrap())),
                    source: raw(Lane::Depth, DEPTH, seq, None).stream_source
                }
            ),
            Payload::Snapshot(t) => assert_eq!(
                *t,
                BinanceDepthSnapshot {
                    last_update_id: Some(100),
                    native_event_time_ms: Some(2),
                    native_transaction_time_ms: Some(1),
                    event_timestamp_ns: Some(2_000_000),
                    transaction_timestamp_ns: Some(1_000_000),
                    bids: levels(&[("100", "1"), ("99", "2")]),
                    asks: levels(&[("101", "4")]),
                    source: raw(Lane::Rest, SNAPSHOT, seq, Some("/fapi/v1/depth")).http_source
                }
            ),
        }
    }
}

#[test]
fn golden_all_mappings_accounting_typed_semantics_quality_identity() {
    let f = Fixture::new();
    let before = f.hashes();
    let (out, bytes, s) = outcomes(&f);
    let g = golden();
    assert_eq!(s.input_record_count, g["counts"]["input"].as_u64().unwrap());
    assert_eq!(
        s.normalized_count,
        g["counts"]["normalized"].as_u64().unwrap()
    );
    assert_eq!(s.metadata_observation_count, 1);
    assert_eq!(s.rejected_count, 1);
    assert_eq!(
        json!(out.iter().map(Outcome::name).collect::<Vec<_>>()),
        g["outcomes"]
    );
    out.iter().for_each(assert_semantics);
    assert_eq!(out[3].logical_event_id, out[4].logical_event_id);
    assert_ne!(out[3].lineage.capture_id, out[4].lineage.capture_id);
    assert_eq!(out[2].quality_codes, vec!["RAW_LOSS_RECORDED"]);
    assert_eq!(out[5].value, OutcomeValue::Rejected(Error::ParseFailure));
    if let OutcomeValue::Metadata(m) = &out[0].value {
        assert_eq!(
            m.metadata_version.as_deref(),
            g["metadata_observation_id"].as_str()
        );
        assert_eq!(
            m.effective_time_basis,
            Some(EffectiveTimeBasis::Unknown as i32)
        );
        assert_eq!(m.effective_from_ns, None);
        assert_eq!(m.known_from_ns, Some(5_000_000));
        assert_eq!(m.tick_size, d("1", 1));
        assert_eq!(m.lot_size, d("1", 3));
        assert_eq!(
            m.source,
            raw(Lane::Rest, EXCHANGE, 1, Some("/fapi/v1/exchangeInfo")).http_source
        );
    } else {
        panic!("metadata observation expected");
    }
    let transcript = transcript::verify(std::str::from_utf8(&bytes).unwrap()).unwrap();
    assert_eq!(transcript["transcript_hash"], s.transcript_hash);
    assert_eq!(before, f.hashes());
}

#[test]
fn catalog_unknown_correction_future_knowledge_and_ambiguity() {
    let c = config();
    let r = raw(Lane::Rest, EXCHANGE, 1, Some("/fapi/v1/exchangeInfo"));
    let unknown = metadata_observation(&r, EXCHANGE).unwrap().0;
    let cat = Catalog::new(vec![unknown], &c).unwrap();
    assert_eq!(
        cat.resolve(
            binance::INSTRUMENT_ID,
            Some(1_000_000),
            5_000_000,
            ResolverMode::CorrectedResearch
        )
        .unwrap_err(),
        Error::MetadataEffectiveTimeUnknown
    );
    let resolved = cat
        .resolve(
            binance::INSTRUMENT_ID,
            Some(1_000_000),
            5_000_000,
            ResolverMode::AsLived,
        )
        .unwrap();
    assert_eq!(
        resolved.resolution_basis,
        MetadataResolutionBasis::KnowledgeOnly
    );
    assert_eq!(resolved.quality, Some("METADATA_EFFECTIVE_TIME_UNKNOWN"));
    assert_eq!(
        cat.resolve(
            binance::INSTRUMENT_ID,
            Some(1_000_000),
            4_999_999,
            ResolverMode::AsLived
        )
        .unwrap_err(),
        Error::MetadataMissing
    );
    let v1 = confirmed("confirmed-v1", 5_000_000);
    let mut v2 = confirmed("corrected-v2", 9_000_000);
    v2.supersedes_version = v1.metadata_version.clone();
    v2.tick_size = d("2", 1);
    let catalog = Catalog::new(vec![v1.clone(), v2.clone()], &c).unwrap();
    let reordered = Catalog::new(vec![v2.clone(), v1.clone()], &c).unwrap();
    assert_eq!(catalog.digest(), reordered.digest());
    for cat in [&catalog, &reordered] {
        assert_eq!(
            cat.resolve(
                binance::INSTRUMENT_ID,
                Some(1_000_000),
                5_000_000,
                ResolverMode::AsLived
            )
            .unwrap()
            .version
            .metadata_version
            .as_deref(),
            Some("confirmed-v1")
        );
        assert_eq!(
            cat.resolve(
                binance::INSTRUMENT_ID,
                Some(1_000_000),
                5_000_000,
                ResolverMode::CorrectedResearch
            )
            .unwrap()
            .version
            .metadata_version
            .as_deref(),
            Some("corrected-v2")
        );
        assert_eq!(
            cat.resolve(
                binance::INSTRUMENT_ID,
                None,
                5_000_000,
                ResolverMode::CorrectedResearch
            )
            .unwrap_err(),
            Error::MetadataEventTimeMissing
        );
    }
    assert_ne!(
        catalog.digest(),
        Catalog::new(vec![v1.clone()], &c).unwrap().digest()
    );
    assert_eq!(
        Catalog::new(vec![v1.clone(), v1.clone()], &c).err(),
        Some(Error::Contract("DUPLICATE_METADATA_VERSION"))
    );
    v2.supersedes_version = None;
    for versions in [vec![v1.clone(), v2.clone()], vec![v2, v1]] {
        let cat = Catalog::new(versions, &c).unwrap();
        assert_eq!(
            cat.resolve(
                binance::INSTRUMENT_ID,
                Some(1_000_000),
                5_000_000,
                ResolverMode::CorrectedResearch
            )
            .unwrap_err(),
            Error::MetadataAmbiguous
        );
    }
}

#[test]
fn unknown_effective_metadata_rejects_markets_but_parser_success_is_counted() {
    let f = Fixture::new();
    let mut r = replay(&f);
    r.catalog = None;
    let s = r.execute(&context(1, 10), io::sink(), |_| Ok(())).unwrap();
    assert_eq!(
        (
            s.input_record_count,
            s.normalized_count,
            s.metadata_observation_count,
            s.rejected_count
        ),
        (6, 0, 1, 5)
    );
    assert_eq!(s.rejection_codes["METADATA_EFFECTIVE_TIME_UNKNOWN"], 4);
    assert_eq!(s.parser_successes["TradeEvent"], 2);
    assert_eq!(s.parser_successes["BinanceDepthUpdate"], 1);
    assert_eq!(s.parser_successes["BinanceDepthSnapshot"], 1);
}

#[test]
fn original_session_scope_and_metadata_id_recipe() {
    let f = Fixture::new();
    let a = raw(Lane::Depth, DEPTH, 10, None);
    let mut b = a.clone();
    let session = Uuid::parse_str("20212223-2425-2627-2829-2a2b2c2d2e2f")
        .unwrap()
        .as_bytes()
        .to_vec();
    b.session_id = Some(session.clone());
    b.stream_source.as_mut().unwrap().original_session_id = Some(session);
    let x = normalize_record(
        &a,
        DEPTH,
        &position(Lane::Depth),
        &f.catalog,
        &context(1, 10),
    )
    .unwrap();
    let y = normalize_record(
        &b,
        DEPTH,
        &position(Lane::Depth),
        &f.catalog,
        &context(2, 20),
    )
    .unwrap();
    assert_eq!(
        hex(x.logical_event_id.as_ref().unwrap()),
        golden()["depth_logical_id"]
    );
    assert_eq!(
        hex(y.logical_event_id.as_ref().unwrap()),
        golden()["other_session_depth_logical_id"]
    );
    assert_ne!(x.logical_event_id, y.logical_event_id);
    assert_eq!(x.lineage.capture_id, y.lineage.capture_id);
    let capture = raw(Lane::Rest, EXCHANGE, 1, Some("/fapi/v1/exchangeInfo"))
        .capture_id
        .unwrap();
    assert_eq!(
        binance::metadata_observation_id(&capture).unwrap(),
        format!("{}:{}", COLLECTOR, 1)
    );
    assert_eq!(
        binance::metadata_observation_id(&capture).unwrap(),
        golden()["metadata_observation_id"]
    );
}

#[test]
fn record_source_mismatches_and_encoding_are_stable_rejections() {
    let f = Fixture::new();
    let base = raw(Lane::Aggtrade, TRADE, 1, None);
    let mut cases = Vec::new();
    let mut r = base.clone();
    r.endpoint = Some(binance::DEPTH_ENDPOINT.into());
    cases.push((r, Error::ProfileMismatch));
    let mut r = base.clone();
    r.session_id = Some(vec![22; 16]);
    cases.push((r, Error::ProfileMismatch));
    let mut r = base.clone();
    r.stream_source
        .as_mut()
        .unwrap()
        .lineage
        .as_mut()
        .unwrap()
        .venue_profile_hash = Some(vec![0; 32]);
    cases.push((r, Error::ProfileMismatch));
    let mut r = base.clone();
    r.source = Some("OTHER".into());
    cases.push((r, Error::ProfileMismatch));
    let mut r = base.clone();
    r.channel = Some("unmapped".into());
    cases.push((r, Error::UnsupportedRawSource));
    let mut r = base;
    r.processing_context.as_mut().unwrap().schema_version = Some(2);
    cases.push((r, Error::UnsupportedSchemaVersion));
    for (r, expected) in cases {
        let o = normalize_record(
            &r,
            TRADE,
            &position(Lane::Aggtrade),
            &f.catalog,
            &context(1, 10),
        )
        .unwrap();
        assert_eq!(o.value, OutcomeValue::Rejected(expected));
    }
    let mut r = raw(Lane::Rest, SNAPSHOT, 1, Some("/fapi/v1/depth"));
    r.http_content_encoding = Some(b"gzip".to_vec());
    assert_eq!(
        normalize_record(
            &r,
            SNAPSHOT,
            &position(Lane::Rest),
            &f.catalog,
            &context(1, 10)
        )
        .unwrap()
        .value,
        OutcomeValue::Rejected(Error::UnsupportedContentEncoding)
    );
}

#[test]
fn json_rejections_and_additive_drift_preserve_exact_semantics() {
    let f = Fixture::new();
    let original = std::str::from_utf8(TRADE).unwrap();
    let cases = [
        (b"{".to_vec(), Error::ParseFailure),
        (
            original
                .replacen("{", "{\"e\":\"aggTrade\",", 1)
                .into_bytes(),
            Error::ParseFailure,
        ),
        (
            original.replace("100.00", "1e2").into_bytes(),
            Error::Contract("INVALID_VENUE_DECIMAL"),
        ),
    ];
    for (bytes, expected) in cases {
        let r = raw(Lane::Aggtrade, &bytes, 1, None);
        assert_eq!(
            normalize_record(
                &r,
                &bytes,
                &position(Lane::Aggtrade),
                &f.catalog,
                &context(1, 10)
            )
            .unwrap()
            .value,
            OutcomeValue::Rejected(expected)
        );
    }
    let bytes = original
        .replacen("{", "{\"z_future\":0,\"a_future\":true,", 1)
        .into_bytes();
    let r = raw(Lane::Aggtrade, &bytes, 1, None);
    let o = normalize_record(
        &r,
        &bytes,
        &position(Lane::Aggtrade),
        &f.catalog,
        &context(1, 10),
    )
    .unwrap();
    assert_eq!(o.schema_drift_paths, vec!["a_future", "z_future"]);
    assert_eq!(o.quality_codes, vec!["SCHEMA_DRIFT_OBSERVED"]);
    if let OutcomeValue::Market(e) = o.value {
        if let Payload::Trade(mut t) = e.payload {
            let (mut original, _) = binance::parse_trade(
                TRADE,
                &raw(Lane::Aggtrade, TRADE, 1, None).stream_source.unwrap(),
            )
            .unwrap();
            t.source = None;
            original.source = None;
            assert_eq!(t, original);
        } else {
            panic!("trade")
        }
    } else {
        panic!("market")
    }
}

#[test]
fn plan_pins_order_profile_commit_config_schema_and_duplicates() {
    let f = Fixture::new();
    let mut p = f.plan.clone();
    p.ordered_segments.reverse();
    p.plan_hash = p.digest().unwrap();
    assert_ne!(p.plan_hash, f.plan.plan_hash);
    p.validate(&f.config).unwrap();
    p.ordered_segments[0].capture_sha256 = "0".repeat(64);
    assert_eq!(p.validate(&f.config), Err(Error::ReplayPlanInvalid));
    let mut p = f.plan.clone();
    p.ordered_segments.push(p.ordered_segments[0].clone());
    p.plan_hash = p.digest().unwrap();
    assert_eq!(p.validate(&f.config), Err(Error::DuplicateReplaySegment));
    let mut p = f.plan.clone();
    p.venue_profile_hash = "0".repeat(64);
    p.plan_hash = p.digest().unwrap();
    assert_eq!(p.validate(&f.config), Err(Error::ReplayProfileMismatch));
    let mut p = f.plan.clone();
    p.normalizer_commit = "1".repeat(40);
    p.plan_hash = p.digest().unwrap();
    assert_eq!(p.validate(&f.config), Err(Error::NormalizerCommitMismatch));
    let mut c = f.config.clone();
    c.max_segment_bytes -= 1;
    assert_ne!(c.digest().unwrap(), f.config.digest().unwrap());
    assert_eq!(f.plan.validate(&c), Err(Error::NormalizerConfigMismatch));
    c.schema_version = 2;
    assert_eq!(c.validate(), Err(Error::UnsupportedSchemaVersion));
    let mut p = f.plan.clone();
    p.comments = Some("operational note".into());
    assert_eq!(p.digest().unwrap(), f.plan.plan_hash);
}

#[test]
fn plan_paths_and_symlink_escape_fail_closed() {
    for path in [
        "", "/tmp/x", "../x", "a/../b", "a\\b", "./x", "a//b", "C:/x",
    ] {
        assert_eq!(relative_path(path), Err(Error::ReplayPlanInvalid));
    }
    let f = Fixture::new();
    #[cfg(unix)]
    {
        let alias = f.source.join("escape");
        std::os::unix::fs::symlink(&f.base, &alias).unwrap();
        assert!(Segment::load(&f.source, "escape", &f.config).is_err());
    }
}

#[test]
fn source_change_before_or_during_replay_has_no_successful_transcript() {
    for mid_run in [false, true] {
        let f = Fixture::new();
        let path = f
            .source
            .join(&f.plan.ordered_segments[0].relative_path)
            .join("manifest.json");
        let mutate = || {
            let mut b = fs::read(&path).unwrap();
            b.push(b' ');
            fs::write(&path, b).unwrap();
        };
        if !mid_run {
            mutate();
        }
        let mut bytes = Vec::new();
        let mut seen = 0;
        let e = replay(&f)
            .execute(&context(1, 10), &mut bytes, |_| {
                seen += 1;
                if mid_run && seen == 1 {
                    mutate();
                }
                Ok(())
            })
            .unwrap_err();
        assert_eq!(e, Error::ReplayInputChanged);
        if !mid_run {
            assert_eq!(seen, 0);
            assert!(bytes.is_empty());
        }
        assert!(transcript::verify(std::str::from_utf8(&bytes).unwrap()).is_err());
    }
}

#[test]
fn source_replacement_of_already_consumed_segment_is_detected() {
    let f = Fixture::new();
    let path = f
        .source
        .join(&f.plan.ordered_segments[0].relative_path)
        .join("seal.json");
    let mut bytes = Vec::new();
    let mut seen = 0;
    assert!(replay(&f)
        .execute(&context(1, 10), &mut bytes, |_| {
            seen += 1;
            if seen == 6 {
                let mut data = fs::read(&path).unwrap();
                data[0] = b'!';
                fs::write(&path, data).unwrap();
            }
            Ok(())
        })
        .is_err());
    assert!(transcript::verify(std::str::from_utf8(&bytes).unwrap()).is_err());
}

#[test]
fn bad_crc_bad_seal_bad_manifest_and_invalid_source_emit_zero_records() {
    for name in bqip_normalizer::input::NAMES {
        let f = Fixture::new();
        let p = &f.plan.ordered_segments[1];
        let path = f.source.join(&p.relative_path).join(name);
        let mut bytes = fs::read(&path).unwrap();
        let n = bytes.len() - 1;
        bytes[n] ^= 1;
        fs::write(path, &bytes).unwrap();
        assert!(Segment::load(&f.source, &p.relative_path, &f.config).is_err());
        let mut seen = 0;
        assert!(replay(&f)
            .execute(&context(1, 1), io::sink(), |_| {
                seen += 1;
                Ok(())
            })
            .is_err());
        assert_eq!(seen, 0);
    }
}

#[test]
fn transcript_corruption_detected_and_stream_hash_equals_full_jcs() {
    let f = Fixture::new();
    let (_, bytes, s) = outcomes(&f);
    let mut v = transcript::verify(std::str::from_utf8(&bytes).unwrap()).unwrap();
    assert_eq!(
        bqip_normalizer::plan::digest_without(&v, "transcript_hash").unwrap(),
        s.transcript_hash
    );
    v["entries"][0]["capture_seq"] = "999".into();
    assert_eq!(
        transcript::verify(&v.to_string()),
        Err(Error::TranscriptInvalid)
    );
}

#[test]
fn negative_forensic_super_gate_and_live_rejection() {
    let f = Fixture::new();
    for mode in [Mode::AsLivedForensic, Mode::Live] {
        let mut p = f.plan.clone();
        p.replay_mode = mode;
        p.plan_hash = p.digest().unwrap();
        let mut bytes = Vec::new();
        let mut seen = 0;
        let r = Replay {
            source_root: &f.source,
            plan: &p,
            config: &f.config,
            catalog: Some(&f.catalog),
        };
        let e = r
            .execute(&context(1, 1), &mut bytes, |_| {
                seen += 1;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(
            e,
            if mode == Mode::AsLivedForensic {
                Error::ForensicProvenanceInsufficient
            } else {
                Error::ReplayPlanInvalid
            }
        );
        assert_eq!(seen, 0);
        assert!(bytes.is_empty());
    }
}

#[test]
fn deterministic_worker() {
    let Ok(base) = std::env::var("BQIP_REPLAY_TEST_BASE") else {
        return;
    };
    let base = Path::new(&base);
    let c = config();
    let plan = ReplayPlan::parse(&fs::read_to_string(base.join("plan.json")).unwrap()).unwrap();
    let meta =
        InstrumentMetadataVersion::decode(fs::read(base.join("confirmed.pb")).unwrap().as_slice())
            .unwrap();
    let catalog = Catalog::new(vec![meta], &c).unwrap();
    let run: u128 = std::env::var("BQIP_REPLAY_TEST_RUN")
        .unwrap()
        .parse()
        .unwrap();
    let out = base.join(format!("worker-{run}"));
    fs::create_dir(&out).unwrap();
    let ctx = context(run, run as i64 * 1000);
    let replay = Replay {
        source_root: &base.join("source"),
        plan: &plan,
        config: &c,
        catalog: Some(&catalog),
    };
    let mut checked = 0;
    let s = replay
        .execute(
            &ctx,
            fs::File::create(out.join("transcript.json")).unwrap(),
            |o| {
                assert_semantics(o);
                if matches!(o.value, OutcomeValue::Market(_)) {
                    checked += 1;
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(checked, 4);
    fs::write(out.join("report.json"), s.report(&plan, &ctx).to_string()).unwrap();
    fs::write(out.join("semantic-check.json"),json!({"decoded_typed_values_match_independent_golden":true,"checked_market_observations":checked}).to_string()).unwrap();
}

#[test]
fn determinism_super_gate_fresh_processes_and_different_runtime_provenance() {
    let f = Fixture::new();
    let before = f.hashes();
    f.metadata_file();
    fs::write(
        f.base.join("plan.json"),
        f.plan.value().unwrap().to_string(),
    )
    .unwrap();
    for run in [1, 2] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "deterministic_worker", "--nocapture"])
            .env("BQIP_REPLAY_TEST_BASE", &f.base)
            .env("BQIP_REPLAY_TEST_RUN", run.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        fs::read(f.base.join("worker-1/transcript.json")).unwrap(),
        fs::read(f.base.join("worker-2/transcript.json")).unwrap()
    );
    assert_eq!(
        fs::read(f.base.join("worker-1/semantic-check.json")).unwrap(),
        fs::read(f.base.join("worker-2/semantic-check.json")).unwrap()
    );
    assert_ne!(
        fs::read(f.base.join("worker-1/report.json")).unwrap(),
        fs::read(f.base.join("worker-2/report.json")).unwrap()
    );
    assert_eq!(before, f.hashes());
    // Optional sanitized acceptance evidence, never required by tests.
    if let Ok(path) = std::env::var("BQIP_REPLAY_EVIDENCE") {
        let path = Path::new(&path);
        fs::create_dir_all(path).unwrap();
        fs::copy(f.base.join("plan.json"), path.join("plan.json")).unwrap();
        for run in [1, 2] {
            for file in ["transcript.json", "report.json", "semantic-check.json"] {
                fs::copy(
                    f.base.join(format!("worker-{run}/{file}")),
                    path.join(format!("run-{run}-{file}")),
                )
                .unwrap();
            }
        }
        fs::write(
            path.join("source-hashes.json"),
            json!({"before":before,"after":f.hashes()}).to_string(),
        )
        .unwrap();
    }
}

#[test]
fn cli_run_verification_and_read_only_output_guard() {
    let f = Fixture::new();
    let plan = f.base.join("plan.json");
    fs::write(&plan, f.plan.value().unwrap().to_string()).unwrap();
    let binary = env!("CARGO_BIN_EXE_bqip-replay");
    let meta = f.metadata_file();
    let run = |state: &Path| {
        Command::new(binary)
            .arg("run")
            .arg("--source-root")
            .arg(&f.source)
            .arg("--state-root")
            .arg(state)
            .arg("--plan")
            .arg(&plan)
            .arg("--metadata")
            .arg(&meta)
            .output()
            .unwrap()
    };
    let out = f.base.join("cli");
    assert!(run(&out).status.success());
    assert!(!run(&out).status.success());
    assert!(!run(&f.source.join("forbidden")).status.success());
    assert!(!f.source.join("forbidden").exists());
    assert!(Command::new(binary)
        .args(["verify-transcript", "--transcript"])
        .arg(out.join("transcript.json"))
        .status()
        .unwrap()
        .success());
    let mut forensic = f.plan.clone();
    forensic.replay_mode = Mode::AsLivedForensic;
    forensic.plan_hash = forensic.digest().unwrap();
    fs::write(&plan, forensic.value().unwrap().to_string()).unwrap();
    let bad = run(&f.base.join("forensic"));
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("FORENSIC_PROVENANCE_INSUFFICIENT"));
    assert!(!f.base.join("forensic").exists());
}

#[test]
fn same_process_runtime_changes_only_operational_context() {
    let f = Fixture::new();
    let (a, bytes_a, s_a) = outcomes(&f);
    let mut b = Vec::new();
    let mut bytes_b = Vec::new();
    let s_b = replay(&f)
        .execute(&context(2, 99999), &mut bytes_b, |o| {
            b.push(o.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(bytes_a, bytes_b);
    assert_eq!(s_a.transcript_hash, s_b.transcript_hash);
    for (a, b) in a.iter().zip(&b) {
        assert_eq!(transcript::entry(a), transcript::entry(b));
        if let (OutcomeValue::Market(a), OutcomeValue::Market(b)) = (&a.value, &b.value) {
            assert_eq!(a.payload, b.payload);
            assert_ne!(a.processing_context, b.processing_context);
        }
    }
    assert_eq!(f.plan.normalizer_commit, BUILD_COMMIT);
}

use proptest::prelude::*;

#[test]
fn metadata_change_changes_transcript_and_old_catalog_still_reproduces() {
    let f = Fixture::new();
    let (_, original, first) = outcomes(&f);
    let v1 = f.catalog.versions()[0].clone();
    let mut v2 = confirmed("corrected-v2", 9_000_000);
    v2.supersedes_version = v1.metadata_version.clone();
    v2.tick_size = d("2", 1);
    let corrected = Catalog::new(vec![v1, v2], &f.config).unwrap();
    let mut r = replay(&f);
    r.catalog = Some(&corrected);
    let changed = r
        .execute(&context(3, 400), io::sink(), |o| {
            if let OutcomeValue::Market(e) = &o.value {
                assert_eq!(e.metadata_version, "corrected-v2");
            }
            Ok(())
        })
        .unwrap();
    assert_ne!(
        changed.metadata_catalog_digest,
        first.metadata_catalog_digest
    );
    assert_ne!(changed.transcript_hash, first.transcript_hash);
    assert_eq!(outcomes(&f).1, original);
    let empty = Catalog::new(Vec::new(), &f.config).unwrap();
    r.catalog = Some(&empty);
    let s = r.execute(&context(1, 2), io::sink(), |_| Ok(())).unwrap();
    assert_eq!(s.rejection_codes["METADATA_MISSING"], 4);
    let unrelated = Catalog::new(
        vec![confirmed("v1", 5_000_000), confirmed("other", 5_000_000)],
        &f.config,
    )
    .unwrap();
    r.catalog = Some(&unrelated);
    let s = r.execute(&context(1, 2), io::sink(), |_| Ok(())).unwrap();
    assert_eq!(s.rejection_codes["METADATA_AMBIGUOUS"], 4);
}

#[test]
fn bounded_source_catalog_and_transcript_writer_failure_are_visible() {
    let f = Fixture::new();
    let mut c = f.config.clone();
    c.max_segment_bytes = 1;
    assert_eq!(
        Segment::load(&f.source, &f.plan.ordered_segments[0].relative_path, &c).err(),
        Some(Error::CapacityExceeded)
    );
    c = f.config.clone();
    c.max_metadata_versions = 1;
    assert_eq!(
        Catalog::new(
            vec![confirmed("v1", 5_000_000), confirmed("v2", 6_000_000)],
            &c
        )
        .err(),
        Some(Error::CapacityExceeded)
    );
    struct Fails;
    impl io::Write for Fails {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("fault injection"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert_eq!(
        replay(&f)
            .execute(&context(1, 1), Fails, |_| Ok(()))
            .unwrap_err(),
        Error::OutputInvalid
    );
}

#[test]
fn strict_plan_json_and_all_segment_hashes_are_checked_before_any_callback() {
    let f = Fixture::new();
    let text = f.plan.value().unwrap().to_string();
    assert!(
        ReplayPlan::parse(&text.replacen("{", "{\"schema\":\"BQIP-REPLAY-PLAN-V1\",", 1)).is_err()
    );
    let mut p = f.plan.clone();
    p.ordered_segments[2].seal_sha256 = "0".repeat(64);
    p.plan_hash = p.digest().unwrap();
    let r = Replay {
        source_root: &f.source,
        plan: &p,
        config: &f.config,
        catalog: Some(&f.catalog),
    };
    let mut seen = 0;
    assert_eq!(
        r.execute(&context(1, 1), io::sink(), |_| {
            seen += 1;
            Ok(())
        })
        .unwrap_err(),
        Error::ReplayInputChanged
    );
    assert_eq!(seen, 0);
}

#[test]
fn atomic_source_replacement_and_final_segment_identity_mismatch_fail() {
    let f = Fixture::new();
    let path = f
        .source
        .join(&f.plan.ordered_segments[0].relative_path)
        .join("capture.sealed");
    let mut bytes = Vec::new();
    let mut seen = 0;
    assert!(replay(&f)
        .execute(&context(1, 1), &mut bytes, |_| {
            seen += 1;
            if seen == 2 {
                let replacement = f.base.join("replacement");
                let mut data = fs::read(&path).unwrap();
                data.pop();
                fs::write(&replacement, data).unwrap();
                fs::rename(replacement, &path).unwrap();
            }
            Ok(())
        })
        .is_err());
    assert!(transcript::verify(std::str::from_utf8(&bytes).unwrap()).is_err());
    let f = Fixture::new();
    let mut p = f.plan.clone();
    p.ordered_segments[0].segment_id = Uuid::from_u128(900).to_string();
    p.plan_hash = p.digest().unwrap();
    assert_eq!(
        p.verify_inputs(&f.source, &f.config),
        Err(Error::ReplayInputChanged)
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn property_capture_identity_decimal_and_runtime_invariance(seq in 1u64..10000, run in 1u128..10000, zeros in 1usize..8) {
        let f=Fixture::new();let text=std::str::from_utf8(TRADE).unwrap().replace("100.00",&format!("100.{}","0".repeat(zeros)));let bytes=text.as_bytes();
        let r=raw(Lane::Aggtrade,bytes,seq,None);let pos=position(Lane::Aggtrade);
        let a=normalize_record(&r,bytes,&pos,&f.catalog,&context(1,100)).unwrap();let b=normalize_record(&r,bytes,&pos,&f.catalog,&context(run,200)).unwrap();
        prop_assert_eq!(&a.lineage.capture_id,r.capture_id.as_ref().unwrap());prop_assert_eq!(transcript::entry(&a),transcript::entry(&b));
        if let OutcomeValue::Market(e)=a.value { if let Payload::Trade(t)=e.payload {prop_assert_eq!(t.price,d("100",0));} else {prop_assert!(false)} } else {prop_assert!(false)}
    }
}
