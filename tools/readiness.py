"""Fail visibly when Brief 001 deliverables have not actually been supplied."""

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


def main() -> int:
    missing = [name for name in REQUIRED if not (ROOT / name).is_file()]
    for name in missing:
        print(f"BLOCKED: missing required deliverable: {name}")
    return int(bool(missing))


if __name__ == "__main__":
    raise SystemExit(main())
