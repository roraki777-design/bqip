"""Finite in-memory benchmarks; no cloud credentials or third-party Python tools."""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.samples <= 100:
        parser.error("samples must be in 1..100")
    subprocess.run(["cargo", "build", "--locked", "--release", "-p", "bqip-raw-archive",
                    "--example", "archive_benchmark"], cwd=ROOT, check=True)
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    binary = target / "release/examples/archive_benchmark"
    if os.name == "nt":
        binary = binary.with_suffix(".exe")
    points = []
    for mib in (1, 8, 32, 64):
        start = time.monotonic()
        with tempfile.TemporaryFile() as output:
            proc = subprocess.Popen([str(binary), str(mib * 1048576), str(args.samples)],
                                    cwd=ROOT, stdout=output)
            rss = cpu = None
            if hasattr(os, "wait4"):
                _, status, usage = os.wait4(proc.pid, 0)
                proc.returncode = os.waitstatus_to_exitcode(status)
                rss = usage.ru_maxrss * (1 if platform.system() == "Darwin" else 1024)
                cpu = usage.ru_utime + usage.ru_stime
            else:
                proc.wait()
            if proc.returncode:
                raise RuntimeError(f"benchmark failed: {proc.returncode}")
            output.seek(0)
            result = json.load(output)
        samples = result["samples"]
        put_ms = sum(s["upload_latency_ms"]["total_ms"] for s in samples)
        verify_ms = sum(s["verification_latency_ms"]["total_ms"] for s in samples)
        points.append({"mib": mib, "peak_rss_bytes": rss, "process_cpu_seconds": cpu,
                       "process_wall_seconds": time.monotonic() - start,
                       "upload_bytes_per_second": sum(s["upload_bytes"] for s in samples) / (put_ms / 1000),
                       "verification_bytes_per_second": sum(s["verified_download_bytes"] for s in samples) / (verify_ms / 1000),
                       **result})
    report = {"scope": "Synthetic in-memory conditional store, not real S3 throughput; does not close B2/B4.",
              "resource_scope": "Per-point child process, including synthetic fixture creation and validation.",
              "os": platform.platform(), "architecture": platform.machine(),
              "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "points": points}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"points": len(points), "output": str(args.output)}))


if __name__ == "__main__":
    main()
