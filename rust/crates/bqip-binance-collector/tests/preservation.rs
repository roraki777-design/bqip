mod support;
use bqip_binance_collector::{
    capture::Lane,
    config::{backoff, Config},
    raw_writer::{Faults, RawWriter},
    realtime, recovery,
};
use std::time::Duration;
use support::*;
use tokio::sync::mpsc;

#[tokio::test]
async fn preservation_independence_stalled_realtime_keeps_every_payload() {
    let mut env = Env::new();
    env.config.raw_queues[0] = 1;
    let (c, s, e) = env.open(Lane::Depth);
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    let (tx, rx) = mpsc::channel(1);
    let mut expected = Vec::new();
    for n in 0..64 {
        let mut bytes = DEPTH.to_vec();
        bytes.extend(vec![b' '; n]);
        let record = env.record(Lane::Depth, c, s, &bytes);
        expected.push(bytes);
        writer.sink.append(record.clone()).await.unwrap();
        let epoch = env.meter.health(Lane::Depth).epoch.unwrap();
        realtime::handoff(&tx, record, epoch, &env.meter);
    }
    assert_eq!(rx.len(), 1);
    assert_eq!(
        env.meter.value(Lane::Depth, "local_capacity_drops_total"),
        63
    );
    assert!(!env.meter.trust(Lane::Depth, e));
    assert_eq!(env.meter.health(Lane::Depth).realtime, "BROKEN");
    writer.shutdown().await.unwrap();
    let records = env.records(Lane::Depth);
    assert_eq!(
        records
            .iter()
            .map(|f| f.payload.clone())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(env.meter.value(Lane::Depth, "raw_writer_failures_total"), 0);
    assert!(records.windows(2).all(|w| w[0]
        .decode_event()
        .unwrap()
        .capture_id
        .unwrap()
        .capture_seq
        < w[1].decode_event().unwrap().capture_id.unwrap().capture_seq));
    env.finish();
}
#[tokio::test]
async fn parser_independence_valid_bad_valid_remains_byte_exact() {
    let env = Env::new();
    let (c, s, e) = env.open(Lane::Aggtrade);
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Aggtrade,
        env.meter.clone(),
        Faults::default(),
    );
    let (tx, rx) = mpsc::channel(4);
    let consumer = tokio::spawn(realtime::trades(rx, env.meter.clone()));
    for bytes in [TRADE, b"{bad JSON", TRADE] {
        let r = env.record(Lane::Aggtrade, c, s, bytes);
        writer.sink.append(r.clone()).await.unwrap();
        tx.send(realtime::Item {
            record: r,
            epoch: e,
        })
        .await
        .unwrap();
    }
    drop(tx);
    consumer.await.unwrap().unwrap();
    writer.shutdown().await.unwrap();
    assert_eq!(
        env.records(Lane::Aggtrade)
            .iter()
            .map(|f| f.payload.as_slice())
            .collect::<Vec<_>>(),
        [TRADE, b"{bad JSON", TRADE]
    );
    assert_eq!(env.meter.value(Lane::Aggtrade, "parse_failures_total"), 1);
    assert_eq!(env.meter.value(Lane::Aggtrade, "parsed_total"), 2);
    assert_eq!(env.meter.health(Lane::Aggtrade).realtime, "BROKEN");
    env.finish();
}
#[tokio::test]
async fn writer_failure_revokes_trust_and_does_not_handoff_unwritten_event() {
    let env = Env::new();
    let (c, s, e) = env.open(Lane::Depth);
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults {
            append_at: Some(1),
            ..Faults::default()
        },
    );
    let (tx, rx) = mpsc::channel(8);
    let r = env.record(Lane::Depth, c, s, DEPTH);
    writer.sink.append(r.clone()).await.unwrap();
    realtime::handoff(&tx, r, e, &env.meter);
    assert!(writer
        .sink
        .append(env.record(Lane::Depth, c, s, DEPTH))
        .await
        .is_err());
    assert_eq!(rx.len(), 1);
    assert!(env.meter.health(Lane::Depth).raw_failed);
    assert!(!env.meter.trust(Lane::Depth, e));
    assert!(writer.shutdown().await.is_err());
    env.finish();
}
#[tokio::test]
async fn fence_and_seal_failures_cannot_be_graceful() {
    for faults in [
        Faults {
            fence: true,
            ..Faults::default()
        },
        Faults {
            seal: true,
            ..Faults::default()
        },
    ] {
        let env = Env::new();
        let (c, s, _) = env.open(Lane::Depth);
        let w = RawWriter::start(env.config.clone(), Lane::Depth, env.meter.clone(), faults);
        w.sink
            .append(env.record(Lane::Depth, c, s, DEPTH))
            .await
            .unwrap();
        assert!(w.shutdown().await.is_err());
        assert!(env.meter.health(Lane::Depth).raw_failed);
        env.finish();
    }
}
#[tokio::test]
async fn periodic_fence_age_rotation_and_independent_lane() {
    let mut env = Env::new();
    env.config.fence_ms = 5;
    env.config.segment_max_age_ms = 25;
    let (c, s, _) = env.open(Lane::Depth);
    let w = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    let r = env.record(Lane::Depth, c, s, DEPTH);
    let seq = r.seq();
    w.sink.append(r).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while env.meter.value(Lane::Depth, "raw_segments_sealed_total") == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(env.meter.health(Lane::Depth).fenced_seq, seq);
    w.sink
        .append(env.record(Lane::Depth, c, s, DEPTH))
        .await
        .unwrap();
    w.shutdown().await.unwrap();
    assert_eq!(env.paths(Lane::Depth).len(), 2);
    let (t, u, _) = env.open(Lane::Aggtrade);
    let trade = RawWriter::start(
        env.config.clone(),
        Lane::Aggtrade,
        env.meter.clone(),
        Faults::default(),
    );
    trade
        .sink
        .append(env.record(Lane::Aggtrade, t, u, TRADE))
        .await
        .unwrap();
    trade.shutdown().await.unwrap();
    assert_eq!(
        recovery::verify_spool(&env.config.spool_root).unwrap()["records_verified"],
        3
    );
    env.finish();
}
#[test]
fn configuration_bounds_and_duplicate_key_rejection() {
    let c = Config::default();
    c.validate(true).unwrap();
    let mut invalid = c.clone();
    invalid.raw_queues[0] = 0;
    assert!(invalid.validate(false).is_err());
    invalid = c.clone();
    invalid.snapshot_retry_min_ms = 1;
    assert!(invalid.validate(true).is_err());
    assert!(Config::parse(b"{\"spool_root\":\"a\",\"spool_root\":\"b\"}").is_err());
    assert_eq!(backoff(5, 20, 100), Duration::from_millis(20));
}
#[tokio::test]
async fn concurrent_lanes_share_unique_ingress_sequence_and_rotate_by_bytes() {
    let mut env = Env::new();
    env.config.segment_max_bytes = 1500;
    let (c, s, _) = env.open(Lane::Depth);
    let (tc, ts, _) = env.open(Lane::Aggtrade);
    let depth = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    let trade = RawWriter::start(
        env.config.clone(),
        Lane::Aggtrade,
        env.meter.clone(),
        Faults::default(),
    );
    tokio::join!(
        async {
            for _ in 0..20 {
                depth
                    .sink
                    .append(env.record(Lane::Depth, c, s, DEPTH))
                    .await
                    .unwrap();
            }
        },
        async {
            for _ in 0..20 {
                trade
                    .sink
                    .append(env.record(Lane::Aggtrade, tc, ts, TRADE))
                    .await
                    .unwrap();
            }
        }
    );
    depth.shutdown().await.unwrap();
    trade.shutdown().await.unwrap();
    assert!(env.paths(Lane::Depth).len() > 1);
    assert!(env.paths(Lane::Aggtrade).len() > 1);
    let mut records = env.records(Lane::Depth);
    records.extend(env.records(Lane::Aggtrade));
    let mut seq: Vec<_> = records
        .into_iter()
        .map(|f| {
            let e = f.decode_event().unwrap();
            assert!(e.monotonic_ns_since_collector_start.is_some());
            e.capture_id.unwrap().capture_seq.unwrap()
        })
        .collect();
    seq.sort();
    assert_eq!(seq, (1..=40).collect::<Vec<_>>());
    env.finish();
}
