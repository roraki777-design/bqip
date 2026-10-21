mod support;
use bqip_binance_collector::{
    capture::{Ingress, Lane},
    recovery,
};
use bqip_raw_archive::{
    config::Config,
    key, local, receipt,
    uploader::{Archive, Runner},
    Kind,
};
use std::{fs, sync::Arc, time::Duration};
use support::*;
use uuid::Uuid;

#[tokio::test]
async fn exact_bytes_all_lanes_manifest_last_source_read_only() {
    let e = Env::new();
    let ds = Lane::ALL.map(|l| e.segment(l));
    let before = source_snapshot(&e.config.spool_root);
    for d in &ds {
        let s = e.load(d);
        let k = e.keys(d);
        e.archive.archive(d.clone()).await.unwrap();
        let remote = e.store.objects();
        for i in 0..3 {
            assert_eq!(remote[&k[i]], s.objects[i]);
        }
        assert!(receipt::valid(&e.config, &s));
        assert_eq!(&e.store.creations()[e.store.creations().len() - 3..], &k);
    }
    assert_eq!(source_snapshot(&e.config.spool_root), before);
    assert!(e.archive.scan(false).unwrap().is_empty());
    assert_eq!(e.meter.value("remote_conflicts_total"), 0);
    assert_eq!(
        e.meter.summary()["backlog"]["archive_remote_durable_segments"],
        3
    );
}
#[tokio::test]
async fn idempotent_retry_and_lost_receipt_reconstruct_without_creation() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    e.archive.archive(d.clone()).await.unwrap();
    let creates = e.store.creations();
    e.archive.archive(d.clone()).await.unwrap();
    fs::remove_file(e.receipt(&d)).unwrap();
    e.restart().once(false).await.unwrap();
    assert_eq!(e.store.creations(), creates);
    assert!(receipt::valid(&e.config, &e.load(&d)));
    assert_eq!(e.meter.value("remote_conflicts_total"), 0);
}
#[tokio::test]
async fn different_precommit_objects_conflict_never_overwrite() {
    for i in 0..2 {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let k = e.keys(&d);
        let before = source_snapshot(&e.config.spool_root);
        e.store.set(&k[i], b"different".to_vec());
        assert_eq!(
            e.archive.archive(d.clone()).await.unwrap_err().kind,
            Kind::RemoteObjectConflict
        );
        assert_eq!(e.store.objects()[&k[i]], b"different");
        assert!(!e.store.objects().contains_key(&k[2]));
        assert!(!e.receipt(&d).exists());
        assert_eq!(source_snapshot(&e.config.spool_root), before);
    }
}
#[tokio::test]
async fn lost_ack_each_object_resolves_exact_get() {
    for i in 0..3 {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let k = e.keys(&d);
        e.store.fault(Op::Create, &k[i], 0, Action::LostAck);
        e.archive.archive(d.clone()).await.unwrap();
        assert_eq!(e.store.creations().len(), 3);
        assert!(receipt::valid(&e.config, &e.load(&d)));
    }
}
#[tokio::test]
async fn crash_after_raw_seal_or_manifest_restarts_idempotently() {
    for boundary in 1..=3 {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let k = e.keys(&d);
        if boundary < 3 {
            e.store.fault(
                Op::Create,
                &k[boundary],
                0,
                Action::Fail(Kind::UploadFailed),
            );
        } else {
            e.store
                .fault(Op::Get, &k[2], 1, Action::Fail(Kind::UploadFailed));
        }
        assert!(e.archive.archive(d.clone()).await.is_err());
        assert_eq!(e.store.creations().len(), boundary);
        assert!(!e.receipt(&d).exists());
        e.restart().archive(d.clone()).await.unwrap();
        assert_eq!(e.store.creations().len(), 3);
        assert!(receipt::valid(&e.config, &e.load(&d)));
    }
}
#[tokio::test]
async fn interrupted_receipt_temporary_file_is_not_success() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let s = e.load(&d);
    let k = e.keys(&d);
    for (key, object) in k.iter().zip(&s.objects) {
        e.store.set(key, object.clone());
    }
    let p = e.receipt(&d);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    let temp = p.with_file_name(".receipt-interrupted.tmp");
    fs::write(&temp, b"incomplete").unwrap();
    assert!(!receipt::valid(&e.config, &s));
    e.restart().once(false).await.unwrap();
    assert!(receipt::valid(&e.config, &s));
    assert!(temp.exists());
    assert!(e.store.creations().is_empty());
}
#[tokio::test]
async fn failure_after_full_verification_before_receipt_rename_is_not_durable() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let s = e.load(&d);
    let k = e.keys(&d);
    let p = e.receipt(&d);
    // The last full GET creates a destination collision after invalidation,
    // so the real uploader reaches receipt publication and its rename fails.
    e.store
        .fault(Op::Get, &k[1], 0, Action::CreateDirectory(p.clone()));
    assert_eq!(
        e.archive.archive(d.clone()).await.unwrap_err().kind,
        Kind::StateIo
    );
    assert_eq!(e.meter.value("remote_verify_successes_total"), 3);
    assert_eq!(e.store.creations().len(), 3);
    assert!(!receipt::valid(&e.config, &s));
    assert_eq!(e.archive.scan(false).unwrap().len(), 1);
    fs::remove_dir(&p).unwrap();
    e.restart().once(false).await.unwrap();
    assert!(receipt::valid(&e.config, &s));
    assert_eq!(e.store.creations().len(), 3);
}

#[tokio::test]
async fn source_change_during_remote_verification_prevents_receipt() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let k = e.keys(&d);
    e.store.fault(
        Op::Get,
        &k[1],
        0,
        Action::ChangeLocal(d.directory.join("capture.sealed"), b"corrupt".to_vec()),
    );
    assert_eq!(
        e.archive.archive(d.clone()).await.unwrap_err().kind,
        Kind::LocalSourceInvalid
    );
    assert_eq!(e.meter.value("remote_verify_successes_total"), 3);
    assert!(!e.receipt(&d).exists());
}

#[test]
fn scan_capacity_fails_visibly_without_upload() {
    let e = Env::with(|c| c.max_scanned_segments = 1);
    e.segment(Lane::Depth);
    e.segment(Lane::Aggtrade);
    assert_eq!(e.archive.scan(false).unwrap_err().kind, Kind::ScanCapacity);
    assert!(e.store.calls().is_empty());
}

#[tokio::test]
async fn graceful_stop_drains_active_once_work_without_scheduling_remaining_segments() {
    let e = Env::new();
    for _ in 0..2 {
        let d = e.segment(Lane::Depth);
        e.store.fault(Op::Get, &e.keys(&d)[2], 0, Action::Stall);
    }
    let (tx, rx) = tokio::sync::oneshot::channel();
    let a = e.archive.clone();
    let job = tokio::spawn(async move {
        a.once_until(false, async {
            rx.await.unwrap();
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while e.store.calls().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tx.send(()).unwrap();
    job.await.unwrap().unwrap();
    assert_eq!(e.store.creations().len(), 3);
    assert_eq!(e.archive.scan(false).unwrap().len(), 1);
    assert_eq!(e.meter.value("active_uploads"), 0);
    assert_eq!(e.meter.value("peak_active_uploads"), 1);
}
#[tokio::test]
async fn corrupt_receipt_is_rejected_and_remote_reverified() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    e.archive.archive(d.clone()).await.unwrap();
    fs::write(e.receipt(&d), b"{}").unwrap();
    let calls = e.store.calls().len();
    assert_eq!(e.restart().scan(false).unwrap().len(), 1);
    e.restart().once(false).await.unwrap();
    assert!(e.store.calls().len() > calls);
    assert!(receipt::valid(&e.config, &e.load(&d)));
    assert_eq!(e.store.creations().len(), 3);
}
#[tokio::test]
async fn committed_missing_or_tampered_object_is_divergence_without_repair() {
    for mutation in 0..5 {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let k = e.keys(&d);
        e.archive.archive(d.clone()).await.unwrap();
        let creates = e.store.creations();
        if mutation < 2 {
            e.store.remove(&k[mutation]);
        } else {
            let i = mutation - 2;
            let mut b = e.store.objects()[&k[i]].clone();
            b[0] ^= 1;
            e.store.set(&k[i], b);
        }
        let before = e.store.objects();
        assert_eq!(
            e.restart().archive(d.clone()).await.unwrap_err().kind,
            Kind::RemoteArchiveDivergence
        );
        assert_eq!(e.store.objects(), before);
        assert_eq!(e.store.creations(), creates);
        assert!(!e.receipt(&d).exists());
    }
}
#[tokio::test]
async fn full_get_detects_tamper_even_with_unchanged_etag() {
    for i in 0..3 {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let k = e.keys(&d);
        e.store
            .fault(Op::Get, &k[i], usize::from(i == 2), Action::Corrupt);
        assert_eq!(
            e.archive.archive(d.clone()).await.unwrap_err().kind,
            Kind::RemoteHashMismatch
        );
        assert!(!e.receipt(&d).exists());
        assert!(e.meter.value("remote_verify_failures_total") > 0);
    }
}
#[tokio::test]
async fn local_corruption_each_object_rejects_before_remote_io() {
    for i in 0..3 {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let p = d.directory.join(local::NAMES[i]);
        let mut b = fs::read(&p).unwrap();
        b[0] ^= 1;
        fs::write(&p, &b).unwrap();
        assert_eq!(
            e.archive.archive(d.clone()).await.unwrap_err().kind,
            Kind::LocalSourceInvalid
        );
        assert!(e.store.calls().is_empty());
        assert_eq!(fs::read(&p).unwrap(), b);
    }
}
#[tokio::test]
async fn accepted_possible_loss_evidence_remains_archivable() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let v = recovery::manifest(
        &d.directory.join("capture.sealed"),
        d.lane,
        "RECOVERED",
        "POSSIBLE",
        Some(&d.id.to_string()),
    )
    .unwrap();
    fs::write(
        d.directory.join("manifest.json"),
        bqip_contracts::manifests::canonical_bytes(&v).unwrap(),
    )
    .unwrap();
    e.archive.archive(d.clone()).await.unwrap();
    assert!(receipt::valid(&e.config, &e.load(&d)));
}
#[tokio::test]
async fn same_segment_concurrent_workers_create_each_key_once() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let second = e.restart();
    let (a, b) = tokio::join!(e.archive.archive(d.clone()), second.archive(d.clone()));
    a.unwrap();
    b.unwrap();
    assert_eq!(e.store.creations().len(), 3);
    assert!(receipt::valid(&e.config, &e.load(&d)));
    assert_eq!(e.meter.value("remote_conflicts_total"), 0);
}
#[tokio::test]
async fn permanent_auth_and_capability_failure_do_not_busy_retry() {
    for kind in [Kind::ArchiveAuthFailure, Kind::ArchiveCapabilityUnsupported] {
        let e = Env::new();
        let d = e.segment(Lane::Depth);
        let k = e.keys(&d);
        e.store.fault(Op::Get, &k[2], 0, Action::Fail(kind));
        let mut r = Runner::new(e.archive.clone());
        r.tick().await.unwrap();
        r.drain().await.unwrap();
        let n = e.store.calls().len();
        for _ in 0..5 {
            r.tick().await.unwrap();
        }
        assert_eq!(e.store.calls().len(), n);
        assert!(!e.receipt(&d).exists());
        assert!(e.store.creations().is_empty());
    }
}
#[tokio::test(start_paused = true)]
async fn retry_backoff_survives_scans_and_recovers() {
    let e = Env::with(|c| {
        c.retry_min_ms = 50;
        c.retry_max_ms = 200;
    });
    let d = e.segment(Lane::Depth);
    let k = e.keys(&d);
    e.store
        .fault(Op::Create, &k[0], 0, Action::Fail(Kind::UploadRetryable));
    let mut r = Runner::new(e.archive.clone());
    r.tick().await.unwrap();
    r.drain().await.unwrap();
    let calls = e.store.calls().len();
    for _ in 0..4 {
        r.tick().await.unwrap();
    }
    assert_eq!(e.store.calls().len(), calls);
    tokio::time::advance(Duration::from_millis(50)).await;
    r.tick().await.unwrap();
    r.drain().await.unwrap();
    assert!(receipt::valid(&e.config, &e.load(&d)));
    assert_eq!(
        [1, 2, 3, 4, 100].map(|n| e.config.backoff(n).as_millis()),
        [50, 100, 200, 200, 200]
    );
}
#[tokio::test]
async fn oversized_valid_source_remains_pending_with_zero_upload() {
    let e = Env::with(|c| c.max_single_put_bytes = 4096);
    let d = write_segment(&e.config.spool_root, Lane::Depth, Some(8192));
    let before = source_snapshot(&e.config.spool_root);
    assert_eq!(
        e.archive.archive(d.clone()).await.unwrap_err().kind,
        Kind::ArchiveObjectTooLarge
    );
    assert!(e.store.calls().is_empty());
    assert_eq!(
        e.archive.once(false).await.unwrap_err().kind,
        Kind::ArchiveObjectTooLarge
    );
    e.archive.scan(false).unwrap();
    assert_eq!(e.meter.summary()["backlog"]["archive_pending_segments"], 1);
    assert_eq!(source_snapshot(&e.config.spool_root), before);
}
#[tokio::test]
async fn namespace_change_does_not_trust_other_receipt() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    e.archive.archive(d.clone()).await.unwrap();
    let mut c = e.config.clone();
    c.prefix = "another".into();
    let a = Arc::new(Archive::new(c.clone(), e.store.clone(), e.meter.clone(), BASE).unwrap());
    assert_eq!(a.scan(false).unwrap().len(), 1);
    a.once(false).await.unwrap();
    assert_ne!(c.namespace_id().unwrap(), e.config.namespace_id().unwrap());
    assert_eq!(e.store.creations().len(), 6);
}
#[tokio::test]
async fn empty_active_and_incomplete_spool_are_ignored() {
    let e = Env::new();
    e.archive.once(false).await.unwrap();
    let root = recovery::lane_root(&e.config.spool_root, Lane::Depth);
    let p = root.join(Uuid::new_v4().to_string());
    fs::create_dir_all(&p).unwrap();
    fs::write(p.join("capture.partial"), b"active").unwrap();
    let p = root.join(Uuid::new_v4().to_string());
    fs::create_dir(&p).unwrap();
    fs::write(p.join("capture.sealed"), b"not-finalized").unwrap();
    e.archive.once(false).await.unwrap();
    assert!(e.store.calls().is_empty());
}
#[cfg(unix)]
#[tokio::test]
async fn symlinks_and_path_escape_never_upload_external_file() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let p = d.directory.join("capture.sealed");
    let external = e.temp.path().join("outside");
    fs::rename(&p, &external).unwrap();
    std::os::unix::fs::symlink(&external, &p).unwrap();
    assert_eq!(
        e.archive.archive(d.clone()).await.unwrap_err().kind,
        Kind::LocalSourceInvalid
    );
    let mut escaped = d.clone();
    escaped.directory = e.temp.path().to_owned();
    assert!(e.archive.archive(escaped).await.is_err());
    assert!(e.store.calls().is_empty());
}
#[test]
fn unsafe_config_and_excessive_bounds_are_rejected() {
    for prefix in ["", "/lead", "a//b", "..", "a/../b", "a\\b", "a\nb"] {
        let c = Config {
            prefix: prefix.into(),
            ..Default::default()
        };
        assert!(c.validate().is_err());
        assert!(key::keys(prefix, Lane::Depth, Uuid::nil()).is_err());
    }
    for endpoint in [
        "http://example.com",
        "https://u:p@example.com",
        "https://example.com/path",
        "https://example.com?token=x",
    ] {
        assert!(Config {
            endpoint: endpoint.into(),
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    let mut c = Config::default();
    c.archive_state_root = c.spool_root.join("state");
    assert!(c.validate().is_err());
    assert!(Config {
        max_inflight_segments: 8,
        ..Default::default()
    }
    .validate()
    .is_err());
}
#[tokio::test]
async fn unknown_put_outcome_with_unreadable_get_stays_retryable() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let k = e.keys(&d);
    e.store.fault(Op::Create, &k[0], 0, Action::LostAck);
    for _ in 0..3 {
        e.store
            .fault(Op::Get, &k[0], 0, Action::Fail(Kind::UploadRetryable));
    }
    assert_eq!(
        e.archive.archive(d.clone()).await.unwrap_err().kind,
        Kind::UploadRetryable
    );
    assert!(!e.receipt(&d).exists());
    e.restart().archive(d.clone()).await.unwrap();
    assert_eq!(e.store.creations().len(), 3);
}
#[tokio::test]
async fn concurrency_cap_and_timeout_release_all_permits() {
    let e = Env::with(|c| c.max_inflight_segments = 2);
    let ds = (0..4).map(|_| e.segment(Lane::Depth)).collect::<Vec<_>>();
    let mut tasks = tokio::task::JoinSet::new();
    for d in ds {
        let k = e.keys(&d);
        e.store.fault(Op::Create, &k[0], 0, Action::Stall);
        let a = e.archive.clone();
        tasks.spawn(async move { a.archive(d).await });
    }
    while let Some(r) = tasks.join_next().await {
        assert_eq!(r.unwrap().unwrap_err().kind, Kind::UploadRetryable);
    }
    assert_eq!(e.meter.value("peak_active_uploads"), 2);
    assert_eq!(e.meter.value("active_uploads"), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preservation_independence_remote_stalls_actual_raw_writer_keeps_sealing() {
    use bqip_binance_collector::{
        config::Config as CollectorConfig,
        raw_writer::{Faults, RawWriter},
        recovery::SpoolGuard,
        telemetry::Meter as CollectorMeter,
    };
    let e = Env::with(|c| c.request_timeout_ms = 10000);
    let d = e.segment(Lane::Depth);
    let first = source_snapshot(&e.config.spool_root);
    let k = e.keys(&d);
    e.store.fault(Op::Get, &k[2], 0, Action::Stall);
    let guard = SpoolGuard::acquire(&e.config.spool_root).unwrap();
    let (meter, logger) = CollectorMeter::new(&e.temp.path().join("raw.jsonl"), 1024).unwrap();
    recovery::recover_spool(&e.config.spool_root, &meter).unwrap();
    let c = CollectorConfig {
        spool_root: e.config.spool_root.clone(),
        ..Default::default()
    };
    let raw = RawWriter::start(c, Lane::Depth, meter.clone(), Faults::default());
    let ingress = Ingress::new(BASE).unwrap();
    let a = e.archive.clone();
    let job = tokio::spawn(async move { a.archive(d).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while e.store.calls().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    e.archive.scan(false).unwrap();
    let before = e.meter.summary()["backlog"].clone();
    for _ in 0..3 {
        raw.sink
            .append(Arc::new(record(
                &ingress,
                Lane::Depth,
                DEPTH.to_vec(),
                ingress.stamp().unwrap(),
            )))
            .await
            .unwrap();
        raw.sink.boundary().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    e.archive.scan(false).unwrap();
    let after = e.meter.summary()["backlog"].clone();
    for key in ["archive_pending_segments", "archive_pending_bytes"] {
        assert!(after[key].as_u64().unwrap() > before[key].as_u64().unwrap());
    }
    assert!(
        after["oldest_unuploaded_age_seconds"].as_f64().unwrap()
            > before["oldest_unuploaded_age_seconds"].as_f64().unwrap()
    );
    assert!(!meter.health(Lane::Depth).raw_failed);
    assert_eq!(meter.value(Lane::Depth, "raw_segments_sealed_total"), 3);
    assert_eq!(e.meter.value("peak_active_uploads"), 1);
    raw.shutdown().await.unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(e.meter.value("active_uploads"), 0);
    logger.finish().unwrap();
    guard.release().unwrap();
    let after = source_snapshot(&e.config.spool_root);
    for (p, evidence) in first {
        assert_eq!(after[&p], evidence);
    }
}
#[test]
fn byte_validator_matches_disk_and_rejects_corruption() {
    let e = Env::new();
    let d = e.segment(Lane::Depth);
    let s = e.load(&d);
    assert_eq!(
        s.manifest,
        recovery::verify_manifest(&d.directory.join("capture.sealed"), d.lane).unwrap()
    );
    for i in 0..3 {
        for position in [0, s.objects[i].len() / 2, s.objects[i].len() - 1] {
            let mut objects = s.objects.clone();
            objects[i][position] ^= 1;
            assert!(recovery::verify_manifest_bytes(
                &objects[0],
                &objects[1],
                &objects[2],
                d.lane,
                d.id
            )
            .is_err());
        }
    }
}
#[test]
fn independent_namespace_and_root_only_receipt_hash_vectors() {
    let v: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../tests/fixtures/archive/vectors.json"
    )))
    .unwrap();
    let c = Config::default().validate().unwrap();
    assert_eq!(
        c.namespace_bytes().unwrap(),
        v["namespace"]["canonical"].as_str().unwrap().as_bytes()
    );
    assert_eq!(c.namespace_id().unwrap(), v["namespace"]["sha256"]);
    assert_eq!(
        receipt::hash(&v["receipt"]["value"]).unwrap(),
        v["receipt"]["sha256"]
    );
    let mut changed = v["receipt"]["value"].clone();
    changed["nested"]["receipt_hash"] = "different".into();
    assert_ne!(receipt::hash(&changed).unwrap(), v["receipt"]["sha256"]);
}
