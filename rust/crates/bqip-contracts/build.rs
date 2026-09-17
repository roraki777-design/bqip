fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../../../proto");
    let names = ["common", "capture", "metadata", "manifests", "operational"];
    let files: Vec<_> = names
        .iter()
        .map(|name| root.join(format!("bqip/v1/{name}.proto")))
        .collect();
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    prost_build::Config::new().compile_protos(&files, &[root])?;
    Ok(())
}
