"""Record the exact checkout, toolchain and lockfile evidence used for validation."""

import hashlib
import importlib.metadata
import json
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def command(*arguments: str) -> str:
    return subprocess.run(arguments, cwd=ROOT, check=True, capture_output=True,
                          text=True).stdout.strip()


def main() -> None:
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
              for name in ("FILES.sha256", "Cargo.lock", "python/requirements-dev.lock")}
    evidence = {
        "os": platform.platform(),
        "distribution": platform.freedesktop_os_release().get("PRETTY_NAME", "unknown"),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "rust": command("rustc", "--version"),
        "cargo": command("cargo", "--version"),
        "protoc": command("protoc", "--version"),
        "protobuf_runtime": importlib.metadata.version("protobuf"),
        "cargo_audit": command("cargo", "audit", "--version"),
        "gitleaks": command("gitleaks", "version"),
        "ruff": importlib.metadata.version("ruff"),
        "mypy": importlib.metadata.version("mypy"),
        "types_protobuf": importlib.metadata.version("types-protobuf"),
        "git_commit": command("git", "rev-parse", "HEAD"),
        "lockfile_hashes": hashes,
    }
    print(json.dumps(evidence, indent=2), flush=True)
    if (sys.version_info[:2] != (3, 12)
            or not evidence["rust"].startswith("rustc 1.85.0 ")
            or not evidence["cargo"].startswith("cargo 1.85.0 ")
            or evidence["protoc"] != "libprotoc 33.6"
            or evidence["protobuf_runtime"] != "6.33.6"
            or evidence["cargo_audit"] != "cargo-audit 0.22.1"
            or evidence["gitleaks"] != "8.24.2"
            or evidence["ruff"] != "0.11.4"
            or evidence["mypy"] != "1.15.0"
            or evidence["types_protobuf"] != "6.32.1.20260221"):
        raise SystemExit("TOOLCHAIN_VERSION_MISMATCH")


if __name__ == "__main__":
    main()
