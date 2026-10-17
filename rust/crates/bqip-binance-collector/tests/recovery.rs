mod support;
use bqip_binance_collector::{
    capture::Lane,
    raw_writer::{Faults, RawWriter},
    recovery::{self, SpoolGuard, LIMITS},
};
use bqip_capture_format::{self as bqrc, Frame, SegmentWriter};
use std::{fs, io::Write};
use support::*;
use uuid::Uuid;

#[test]
fn crash_recovery_retains_partial_and_is_idempotent() {
    let env = Env::new();
    let (c, s, _) = env.open(Lane::Depth);
    let mut writer = SegmentWriter::new(
        &recovery::lane_root(&env.config.spool_root, Lane::Depth),
        Uuid::new_v4(),
        LIMITS,
    )
    .unwrap();
    for _ in 0..3 {
        let r = env.record(Lane::Depth, c, s, DEPTH);
        writer
            .append(&Frame::from_event(&r.event, r.payload.clone()).unwrap())
            .unwrap();
    }
    let partial = writer.partial_path.clone();
    drop(writer);
    fs::OpenOptions::new()
        .append(true)
        .open(&partial)
        .unwrap()
        .write_all(&[0, 0, 0])
        .unwrap();
    let original = fs::read(&partial).unwrap();
    recovery::recover_spool(&env.config.spool_root, &env.meter).unwrap();
    assert_eq!(fs::read(&partial).unwrap(), original);
    assert_eq!(env.records(Lane::Depth).len(), 3);
    assert_eq!(env.meter.value(Lane::Depth, "recovered_segments_total"), 1);
    recovery::recover_spool(&env.config.spool_root, &env.meter).unwrap();
    assert_eq!(env.paths(Lane::Depth).len(), 1);
    let m = recovery::verify_manifest(&env.paths(Lane::Depth)[0], Lane::Depth).unwrap();
    assert_eq!(m["raw_loss_status"], "POSSIBLE");
    assert_eq!(m["close_reason"], "RECOVERED");
    env.finish();
}
#[test]
fn corrupt_crc_is_not_truncated_and_blessed() {
    let env = Env::new();
    let (c, s, _) = env.open(Lane::Depth);
    let mut w = SegmentWriter::new(
        &recovery::lane_root(&env.config.spool_root, Lane::Depth),
        Uuid::new_v4(),
        LIMITS,
    )
    .unwrap();
    let r = env.record(Lane::Depth, c, s, DEPTH);
    w.append(&Frame::from_event(&r.event, r.payload.clone()).unwrap())
        .unwrap();
    let p = w.partial_path.clone();
    drop(w);
    let mut b = fs::read(&p).unwrap();
    let last = b.len() - 1;
    b[last] ^= 1;
    fs::write(&p, &b).unwrap();
    assert!(recovery::recover_spool(&env.config.spool_root, &env.meter).is_err());
    assert_eq!(fs::read(p).unwrap(), b);
    env.finish();
}
#[tokio::test]
async fn orphan_seal_has_recovered_origin() {
    let env = Env::new();
    let (c, s, _) = env.open(Lane::Depth);
    let w = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    w.sink
        .append(env.record(Lane::Depth, c, s, DEPTH))
        .await
        .unwrap();
    w.shutdown().await.unwrap();
    let p = env.paths(Lane::Depth)[0].clone();
    fs::remove_file(p.with_file_name("seal.json")).unwrap();
    fs::remove_file(p.with_file_name("manifest.json")).unwrap();
    recovery::recover_spool(&env.config.spool_root, &env.meter).unwrap();
    assert_eq!(
        bqrc::verify_segment(&p, LIMITS).unwrap().seal_origin,
        bqrc::SealOrigin::Recovered
    );
    env.finish();
}
#[tokio::test]
async fn manifest_seal_and_segment_tampering_all_fail() {
    let env = Env::new();
    let (c, s, _) = env.open(Lane::Depth);
    let w = RawWriter::start(
        env.config.clone(),
        Lane::Depth,
        env.meter.clone(),
        Faults::default(),
    );
    for _ in 0..2 {
        w.sink
            .append(env.record(Lane::Depth, c, s, DEPTH))
            .await
            .unwrap();
    }
    w.shutdown().await.unwrap();
    let p = env.paths(Lane::Depth)[0].clone();
    let manifest = p.with_file_name("manifest.json");
    let original = fs::read(&manifest).unwrap();
    for field in [
        "segment_sha256",
        "record_count",
        "first_capture_seq",
        "last_capture_seq",
        "venue_profile_hash",
    ] {
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        value[field] = "wrong".into();
        value["manifest_hash"] =
            recovery::hex(&bqip_contracts::manifests::manifest_hash(&value).unwrap()).into();
        fs::write(
            &manifest,
            bqip_contracts::manifests::canonical_bytes(&value).unwrap(),
        )
        .unwrap();
        assert!(
            recovery::verify_manifest(&p, Lane::Depth).is_err(),
            "{field}"
        );
    }
    fs::write(&manifest, &original).unwrap();
    assert!(recovery::publish_manifest(&p, Lane::Depth, "SHUTDOWN", "NONE", None).is_err());
    let seal = p.with_file_name("seal.json");
    let original_seal = fs::read(&seal).unwrap();
    fs::write(&seal, b"{}").unwrap();
    assert!(recovery::verify_manifest(&p, Lane::Depth).is_err());
    fs::write(&seal, original_seal).unwrap();
    let mut bytes = fs::read(&p).unwrap();
    bytes.truncate(bytes.len() - 3);
    fs::write(&p, bytes).unwrap();
    assert!(recovery::verify_manifest(&p, Lane::Depth).is_err());
    env.finish();
}
#[test]
fn ownership_and_early_manifest_are_fail_closed() {
    let env = Env::new();
    assert!(SpoolGuard::acquire(&env.config.spool_root).is_err());
    assert!(recovery::publish_manifest(
        &env.config.spool_root.join("absent/capture.sealed"),
        Lane::Depth,
        "SHUTDOWN",
        "NONE",
        None
    )
    .is_err());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/tmp", env.config.spool_root.join("bad-link")).unwrap();
        assert!(recovery::recover_spool(&env.config.spool_root, &env.meter).is_err());
        fs::remove_file(env.config.spool_root.join("bad-link")).unwrap();
    }
    env.finish();
}
