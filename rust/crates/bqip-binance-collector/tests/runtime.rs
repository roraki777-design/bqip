mod support;
use bqip_binance_collector::{
    capture::Lane,
    http::{self, Rest},
    raw_writer::{Faults, RawWriter},
    runtime::{self, Endpoints},
    telemetry::Epoch,
};
use bqip_contracts::proto::{EffectiveTimeBasis, InstrumentMetadataVersion};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use serde_json::{json, Value};
use std::{
    fs,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use support::*;
use tokio::{
    io::AsyncWriteExt,
    net::TcpListener,
    sync::{mpsc, watch},
    time::{sleep, timeout, Duration},
};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;
async fn ws_peer(depth: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(s).await.unwrap();
        let mut ticker = tokio::time::interval(Duration::from_millis(5));
        let mut n = 0;
        loop {
            tokio::select! {_=ticker.tick()=>{let mut v:Value=serde_json::from_slice(if depth {DEPTH}else{TRADE}).unwrap();if depth&&n>0 {v["U"]=json!(102+n);v["u"]=json!(102+n);v["pu"]=json!(101+n);}
            if ws.send(WsMessage::Text(v.to_string().into())).await.is_err(){break;}n+=1;},message=ws.next()=>{match message{Some(Ok(WsMessage::Close(_)))=>{let _=ws.flush().await;break;},None|Some(Err(_))=>break,_=>{}}}}
        }
    });
    (url, task)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn integrated_runtime_delayed_snapshot_metadata_change_and_graceful_seal() {
    let env = Env::new();
    let (depth, dt) = ws_peer(true).await;
    let (aggtrade, tt) = ws_peer(false).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest = format!("http://{}", listener.local_addr().unwrap());
    let ht = tokio::spawn(async move {
        let mut observations = 0;
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let req = request(&mut s).await;
            let is_depth = String::from_utf8(req).unwrap().contains("/fapi/v1/depth?");
            let body = if is_depth {
                sleep(Duration::from_millis(120)).await;
                SNAPSHOT.to_vec()
            } else {
                observations += 1;
                let mut v: Value = serde_json::from_slice(EXCHANGE).unwrap();
                if observations >= 3 {
                    v["symbols"][0]["status"] = json!("HALT");
                }
                serde_json::to_vec(&v).unwrap()
            };
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if s.write_all(head.as_bytes()).await.is_ok() {
                let _ = s.write_all(&body).await;
            }
        }
    });
    let mut cfg = env.config.clone();
    cfg.spool_root = env.base.join("runtime-spool");
    cfg.metadata_refresh_ms = 70;
    cfg.fence_ms = 25;
    cfg.segment_max_age_ms = 200;
    cfg.snapshot_retry_min_ms = 20;
    cfg.snapshot_retry_max_ms = 40;
    cfg.snapshot_cooldown_ms = 100;
    let (_stop, rx) = watch::channel(false);
    let report = timeout(
        Duration::from_secs(10),
        runtime::run(
            cfg.clone(),
            Endpoints {
                depth,
                aggtrade,
                rest,
            },
            Duration::from_millis(700),
            env.base.join("runtime.jsonl"),
            rx,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    dt.await.unwrap();
    tt.await.unwrap();
    ht.abort();
    let _ = ht.await;
    assert_eq!(report["shutdown_ok"], true, "{report}");
    assert!(report["verification"]["records_verified"].as_u64().unwrap() > 20);
    assert!(
        report["summary"]["lanes"][0]["counters"]["resync_completed_total"]
            .as_u64()
            .unwrap()
            >= 1,
        "{report}"
    );
    assert!(
        report["summary"]["lanes"][1]["counters"]["parsed_total"]
            .as_u64()
            .unwrap()
            > 5
    );
    assert_eq!(
        report["summary"]["lanes"][2]["counters"]["metadata_observations_total"],
        2
    );
    let metadata: Vec<_> = fs::read_dir(cfg.spool_root.join("metadata"))
        .unwrap()
        .map(|p| {
            InstrumentMetadataVersion::decode(fs::read(p.unwrap().path()).unwrap().as_slice())
                .unwrap()
        })
        .collect();
    assert_eq!(metadata.len(), 2);
    for value in metadata {
        assert_eq!(
            value.effective_time_basis,
            Some(EffectiveTimeBasis::Unknown as i32)
        );
        assert_eq!(value.effective_from_ns, None);
    }
    env.finish();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rapid_resync_generations_do_not_reset_guard_or_overlap_requests() {
    let mut env = Env::new();
    env.config.snapshot_retry_min_ms = 20;
    env.config.snapshot_retry_max_ms = 40;
    env.config.snapshot_attempt_budget = 2;
    env.config.snapshot_cooldown_ms = 100;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(AtomicUsize::new(0));
    let (a, m, n) = (active.clone(), maximum.clone(), snapshots.clone());
    let server = tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let (a, m, n) = (a.clone(), m.clone(), n.clone());
            tokio::spawn(async move {
                let req = request(&mut s).await;
                let is_depth = String::from_utf8(req).unwrap().contains("/fapi/v1/depth?");
                let current = a.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(current, Ordering::SeqCst);
                if is_depth {
                    n.fetch_add(1, Ordering::SeqCst);
                }
                sleep(Duration::from_millis(30)).await;
                let body = if is_depth { SNAPSHOT } else { EXCHANGE };
                s.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
                s.write_all(body).await.unwrap();
                a.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    let writer = RawWriter::start(
        env.config.clone(),
        Lane::Rest,
        env.meter.clone(),
        Faults::default(),
    );
    let rest = Rest {
        client: http::client(&env.config).unwrap(),
        base,
        config: env.config.clone(),
        ingress: env.ingress.clone(),
        raw: writer.sink.clone(),
        meter: env.meter.clone(),
    };
    let (desired, drx) = watch::channel(None);
    let (results, mut rrx) = mpsc::channel(1);
    let (stop, srx) = watch::channel(false);
    let consumer = tokio::spawn(async move { while rrx.recv().await.is_some() {} });
    let task = tokio::spawn(rest.run(drx, results, srx));
    let session = Uuid::new_v4();
    for generation in 0..100 {
        desired.send_replace(Some(Epoch {
            session,
            generation,
        }));
        sleep(Duration::from_millis(3)).await;
    }
    stop.send_replace(true);
    task.await.unwrap().unwrap();
    consumer.await.unwrap();
    server.abort();
    let _ = server.await;
    writer.shutdown().await.unwrap();
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert!((3..=10).contains(&snapshots.load(Ordering::SeqCst)));
    assert!(!env.records(Lane::Rest).is_empty());
    env.finish();
}
