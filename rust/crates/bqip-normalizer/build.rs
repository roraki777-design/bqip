use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=BQIP_BUILD_COMMIT");
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("crate path")).join("../../..");
    let git = |args: &[&str]| {
        Command::new("git")
            .args(args)
            .current_dir(&root)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_owned())
    };
    for name in ["HEAD", "refs/heads/brief-005", "packed-refs"] {
        if let Some(path) = git(&["rev-parse", "--git-path", name]) {
            println!("cargo:rerun-if-changed={}", root.join(path).display());
        }
    }
    let commit = env::var("BQIP_BUILD_COMMIT")
        .ok()
        .or_else(|| git(&["rev-parse", "HEAD"]))
        .filter(|s| {
            s.len() == 40
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .unwrap_or_else(|| "UNVERIFIED".into());
    println!("cargo:rustc-env=BQIP_CODE_COMMIT={commit}");
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.lock").display()
    );
}
