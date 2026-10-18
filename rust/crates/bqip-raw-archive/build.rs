use std::{env, path::PathBuf, process::Command};
fn main() {
    println!("cargo:rerun-if-env-changed=BQIP_BUILD_COMMIT");
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("crate path")).join("../../..");
    for name in [".git/HEAD", ".git/refs/heads/brief-004", ".git/packed-refs"] {
        println!("cargo:rerun-if-changed={}", root.join(name).display());
    }
    let commit = env::var("BQIP_BUILD_COMMIT")
        .ok()
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(root)
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
        })
        .unwrap_or_else(|| "UNVERIFIED".into());
    println!("cargo:rustc-env=BQIP_CODE_COMMIT={}", commit.trim());
}
