use std::{env, path::PathBuf, process::Command};
fn main() {
    println!("cargo:rerun-if-env-changed=BQIP_BUILD_COMMIT");
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"))
        .join("../../..");
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        root.join(".git/refs/heads/brief-003").display()
    );
    let commit = env::var("BQIP_BUILD_COMMIT")
        .ok()
        .or_else(|| {
            Command::new("git")
                .arg("rev-parse")
                .arg("HEAD")
                .current_dir(root)
                .output()
                .ok()
                .filter(|o| o.status.success())
                .and_then(|o| String::from_utf8(o.stdout).ok())
        })
        .unwrap_or_else(|| "UNVERIFIED".into());
    println!("cargo:rustc-env=BQIP_CODE_COMMIT={}", commit.trim());
}
