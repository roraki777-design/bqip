"""Run every implemented local gate, reporting absent tools as failures."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "python/src")
    checks = [
        ("Toolchain and checkout evidence", [sys.executable, "tools/environment.py"], ROOT),
        ("Rust formatting", ["cargo", "fmt", "--check"], ROOT),
        ("Rust lint", ["cargo", "clippy", "--locked", "--all-targets", "--all-features", "--", "-D", "warnings"], ROOT),
        ("Rust unit/property/golden", ["cargo", "test", "--workspace", "--locked"], ROOT),
        ("Python lint", [sys.executable, "-m", "ruff", "check", "src", "../tests", "../tools"], ROOT / "python"),
        ("Python typing", [sys.executable, "-m", "mypy"], ROOT / "python"),
        ("Python unit/golden/fault", [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py", "-v"], ROOT),
        ("Python Protobuf contracts", [sys.executable, "-m", "unittest", "discover", "-s", "tests/protobuf", "-p", "test_*.py", "-v"], ROOT),
        ("Generated contract freshness", [sys.executable, "tools/generate_protobuf.py", "--check"], ROOT),
        ("Rust dependency audit", ["cargo", "audit", "--deny", "warnings"], ROOT),
        ("Python dependency audit", [sys.executable, "-m", "pip_audit", "--strict", "-r", "python/requirements-dev.lock"], ROOT),
        ("Secret scan", ["gitleaks", "dir", ".", "--redact", "--no-banner"], ROOT),
        ("Brief deliverable prerequisites", [sys.executable, "tools/readiness.py"], ROOT),
    ]
    failures = []
    for name, command, directory in checks:
        print(f"\nCHECK: {name}", flush=True)
        try:
            result = subprocess.run(command, cwd=directory, env=environment, check=False)
            passed = result.returncode == 0
        except FileNotFoundError as error:
            print(f"BLOCKED: {error}", flush=True)
            passed = False
        print(f"{'PASS' if passed else 'FAIL'}: {name}", flush=True)
        if not passed:
            failures.append(name)
    print(f"\n{len(checks) - len(failures)}/{len(checks)} checks passed", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
