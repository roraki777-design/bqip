mod support;
use bqip_binance_collector::{
    capture::Lane,
    config::Config,
    http::{RateGuard, Snapshot},
    realtime::{self, Item},
    telemetry::Epoch,
};
use bqip_contracts::{binance, proto::*};
use serde_json::{json, Value};
use support::*;
use tokio::{
    sync::{mpsc, watch},
    time::{sleep, timeout, Duration},
};
use uuid::Uuid;
fn snapshot(env: &Env, epoch: Epoch) -> Snapshot {
    let record = env.record(Lane::Rest, Uuid::new_v4(), Uuid::new_v4(), SNAPSHOT);
    let source = HttpSourceContext {
        lineage: Some(SourceLineage {
            venue_profile_id: Some(binance::PROFILE_ID.into()),
            venue_profile_hash: Some(binance::profile_hash().unwrap().to_vec()),
            capture_id: record.event.capture_id.clone(),
            raw_content_hash: record.event.raw_content_hash.clone(),
            receive_timestamp_ns: record.event.receive_timestamp_ns,
        }),
        transport: Some(SourceTransport::HttpRest as i32),
        method: Some("GET".into()),
        endpoint: Some("/fapi/v1/depth".into()),
        query_parameters: vec![
            QueryParameter {
                name: Some("limit".into()),
                value: Some("1000".into()),
            },
            QueryParameter {
                name: Some("symbol".into()),
                value: Some("BTCUSDT".into()),
            },
        ],
        http_status: Some(200),
    };
    Snapshot {
        epoch,
        value: binance::parse_snapshot(&record.payload, &source).unwrap().0,
    }
}
async fn until(mut check: impl FnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !check() {
            sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn healthy_bridge_gap_and_old_snapshot_cannot_restore_reconnected_session() {
    let env = Env::new();
    let (c, s, e) = env.open(Lane::Depth);
    let (tx, rx) = mpsc::channel(16);
    let (stx, srx) = mpsc::channel(1);
    let (dtx, drx) = watch::channel(None);
    let (stop, srxstop) = watch::channel(false);
    let task = tokio::spawn(realtime::depth(
        rx,
        srx,
        dtx,
        env.meter.clone(),
        env.config.clone(),
        srxstop,
    ));
    until(|| *drx.borrow() == Some(e)).await;
    tx.send(Item {
        record: env.record(Lane::Depth, c, s, DEPTH),
        epoch: e,
    })
    .await
    .unwrap();
    stx.send(snapshot(&env, e)).await.unwrap();
    until(|| env.meter.health(Lane::Depth).realtime == "HEALTHY").await;
    let mut gap: Value = serde_json::from_slice(DEPTH).unwrap();
    gap["U"] = json!(104);
    gap["u"] = json!(104);
    gap["pu"] = json!(0);
    tx.send(Item {
        record: env.record(Lane::Depth, c, s, &serde_json::to_vec(&gap).unwrap()),
        epoch: e,
    })
    .await
    .unwrap();
    until(|| env.meter.value(Lane::Depth, "venue_gaps_total") == 1).await;
    let (_, _, next) = env.open(Lane::Depth);
    until(|| *drx.borrow() == Some(next)).await;
    stx.send(snapshot(&env, e)).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    assert_ne!(env.meter.health(Lane::Depth).realtime, "HEALTHY");
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    env.finish();
}
#[tokio::test]
async fn conflicting_identity_survives_same_session_realtime_reset() {
    let env = Env::new();
    let (c, s, e) = env.open(Lane::Depth);
    let (tx, rx) = mpsc::channel(16);
    let (_stx, srx) = mpsc::channel(1);
    let (dtx, drx) = watch::channel(None);
    let (stop, srxstop) = watch::channel(false);
    let task = tokio::spawn(realtime::depth(
        rx,
        srx,
        dtx,
        env.meter.clone(),
        env.config.clone(),
        srxstop,
    ));
    until(|| *drx.borrow() == Some(e)).await;
    tx.send(Item {
        record: env.record(Lane::Depth, c, s, DEPTH),
        epoch: e,
    })
    .await
    .unwrap();
    until(|| env.meter.value(Lane::Depth, "parsed_total") == 1).await;
    env.meter
        .invalidate(Lane::Depth, "LOCAL_CAPACITY_DROP", "NONE");
    let next = env.meter.health(Lane::Depth).epoch.unwrap();
    until(|| *drx.borrow() == Some(next)).await;
    let mut changed: Value = serde_json::from_slice(DEPTH).unwrap();
    changed["b"][0][1] = json!("8");
    tx.send(Item {
        record: env.record(Lane::Depth, c, s, &serde_json::to_vec(&changed).unwrap()),
        epoch: next,
    })
    .await
    .unwrap();
    until(|| env.meter.value(Lane::Depth, "identity_conflicts_total") == 1).await;
    assert_ne!(env.meter.health(Lane::Depth).realtime, "HEALTHY");
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    env.finish();
}
#[tokio::test]
async fn bridge_and_session_budgets_are_enforced_without_poisoning_raw_health() {
    let mut env = Env::new();
    env.config.snapshot_buffer = 1;
    env.config.book_session_events = 3;
    let (c, s, e) = env.open(Lane::Depth);
    let (tx, rx) = mpsc::channel(16);
    let (_stx, srx) = mpsc::channel(1);
    let (dtx, drx) = watch::channel(None);
    let (stop, srxstop) = watch::channel(false);
    let task = tokio::spawn(realtime::depth(
        rx,
        srx,
        dtx,
        env.meter.clone(),
        env.config.clone(),
        srxstop,
    ));
    until(|| *drx.borrow() == Some(e)).await;
    for n in 0..8 {
        let mut data: Value = serde_json::from_slice(DEPTH).unwrap();
        data["U"] = json!(103 + n);
        data["u"] = json!(103 + n);
        data["pu"] = json!(102 + n);
        tx.send(Item {
            record: env.record(Lane::Depth, c, s, &serde_json::to_vec(&data).unwrap()),
            epoch: env.meter.health(Lane::Depth).epoch.unwrap(),
        })
        .await
        .unwrap();
        sleep(Duration::from_millis(5)).await;
    }
    until(|| env.meter.value(Lane::Depth, "parsed_total") == 8).await;
    assert!(!env.meter.health(Lane::Depth).raw_failed);
    assert_eq!(env.meter.health(Lane::Depth).realtime, "BROKEN");
    assert!(env.meter.value(Lane::Depth, "local_capacity_drops_total") >= 2);
    assert_eq!(*drx.borrow(), None);
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    env.finish();
}
#[tokio::test(start_paused = true)]
async fn snapshot_budget_uses_exponential_spacing_and_cooldown() {
    let cfg = Config::default();
    let mut guard = RateGuard::default();
    assert!(guard.ready());
    guard.consume(&cfg).unwrap();
    assert!(guard.consume(&cfg).is_err());
    tokio::time::advance(Duration::from_secs(5)).await;
    guard.consume(&cfg).unwrap();
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(!guard.ready());
    tokio::time::advance(Duration::from_secs(1)).await;
    guard.consume(&cfg).unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(!guard.ready());
    tokio::time::advance(Duration::from_secs(1)).await;
    guard.consume(&cfg).unwrap();
}
