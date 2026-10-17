use bqip_binance_collector::{telemetry::sync_dir, Kind};

#[test]
fn existing_directory_is_supported_and_missing_path_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    sync_dir(directory.path()).unwrap();
    let error = sync_dir(&directory.path().join("missing")).unwrap_err();
    assert_eq!(error.kind, Kind::SealManifest);
}

#[cfg(windows)]
#[test]
fn windows_directory_validation_rejects_regular_files() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("file");
    std::fs::write(&path, b"data").unwrap();
    let error = sync_dir(&path).unwrap_err();
    assert_eq!(error.kind, Kind::SealManifest);
}
