"""Generate Python bindings using the pinned system protoc.

No dependency is installed by this script. Generated bindings require the
official protobuf Python runtime pinned in python/requirements.txt.
Rust generation is performed by prost-build in bqip-contracts/build.rs.
"""

import argparse
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def generate(destination: Path) -> None:
    files = sorted((ROOT / "proto/bqip/v1").glob("*.proto"))
    subprocess.run([
        "protoc", f"--proto_path={ROOT / 'proto'}",
        f"--python_out={destination}", f"--pyi_out={destination}",
        *(str(path) for path in files),
    ], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="regenerate in a temporary directory and reject stale bindings")
    arguments = parser.parse_args()
    version = subprocess.run(["protoc", "--version"], check=True, capture_output=True,
                             text=True).stdout.strip()
    if version != "libprotoc 33.6":
        raise RuntimeError(f"Expected libprotoc 33.6, received {version!r}")
    if not arguments.check:
        generate(ROOT / "python/src")
        return
    with tempfile.TemporaryDirectory(prefix="bqip-protobuf-check-") as temporary:
        destination = Path(temporary)
        generate(destination)
        mismatches = []
        for generated in sorted(destination.rglob("*")):
            if not generated.is_file():
                continue
            relative = generated.relative_to(destination)
            committed = ROOT / "python/src" / relative
            if not committed.is_file() or committed.read_bytes() != generated.read_bytes():
                mismatches.append(str(relative))
        if mismatches:
            raise SystemExit("STALE_OR_MISSING_PROTOBUF: " + ", ".join(mismatches))


if __name__ == "__main__":
    main()
