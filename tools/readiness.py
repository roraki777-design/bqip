"""Fail visibly when Foundation, Brief 002 or Brief 003 deliverables are absent."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REQUIRED = [
    "Cargo.lock",
    "python/requirements-dev.lock",
    "python/src/bqip/v1/common_pb2.py",
    "python/src/bqip/v1/capture_pb2.py",
    "python/src/bqip/v1/metadata_pb2.py",
    "python/src/bqip/v1/manifests_pb2.py",
    "python/src/bqip/v1/operational_pb2.py",
    "tests/protobuf/test_binding.py",
]
REQUIRED.extend(f"python/src/bqip/v1/{name}_pb2.pyi"
                for name in ("common", "capture", "metadata", "manifests", "operational"))
REQUIRED.extend([
    "proto/bqip/v1/source.proto",
    "proto/bqip/v1/market.proto",
    "python/src/bqip/v1/source_pb2.py",
    "python/src/bqip/v1/source_pb2.pyi",
    "python/src/bqip/v1/market_pb2.py",
    "python/src/bqip/v1/market_pb2.pyi",
    "config/venues/binance/usdm/btcusdt-perpetual-v1.json",
    "docs/venues/binance-usdm/sources-v1.json",
    "docs/venues/binance-usdm/README.md",
    "docs/adr/ADR-015-canonical-trade-source-granularity.md",
    "tests/test_binance.py",
    "tests/protobuf/test_binance_binding.py",
    "rust/crates/bqip-contracts/tests/binance.rs",
    "rust/crates/bqip-capture-format/tests/binance_replay.rs",
])
REQUIRED.extend(f"tests/fixtures/binance-usdm/{name}.json" for name in
                ("parsing", "metadata", "identity", "wire", "profile", "boundaries", "sync", "enums"))


REQUIRED.extend([
    "docs/briefs/Brief-003.txt",
    "rust/crates/bqip-binance-collector/Cargo.toml",
    "rust/crates/bqip-binance-collector/tests/transports.rs",
    "rust/crates/bqip-binance-collector/tests/preservation.rs",
    "rust/crates/bqip-binance-collector/tests/recovery.rs",
    "rust/crates/bqip-binance-collector/tests/realtime.rs",
    "rust/crates/bqip-binance-collector/tests/runtime.rs",
    "rust/crates/bqip-binance-collector/tests/contracts.rs",
    "rust/crates/bqip-binance-collector/examples/collector_benchmark.rs",
    "tools/run_collector_benchmark.py",
    "config/runtime/binance-btcusdt-collector-v1.json",
    "docs/collectors/binance-usdm-btcusdt-v1.md",
    "docs/collectors/binance-runtime-sources-v1.json",
    "tests/fixtures/collector/wire.json",
    "tests/protobuf/test_collector_binding.py",
])


def main() -> int:
    missing = [name for name in REQUIRED if not (ROOT / name).is_file()]
    for name in missing:
        print(f"BLOCKED: missing required deliverable: {name}")
    return int(bool(missing))


if __name__ == "__main__":
    raise SystemExit(main())
