"""Bounded release replay benchmark. Fixture preparation is excluded from measurements."""
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
    parser.add_argument("--cycles", type=int, default=100)
    args = parser.parse_args()
    if not 1 <= args.cycles <= 100:
        parser.error("base cycles must be 1..100")
    if not hasattr(os, "wait4"):
        parser.error("this measurement harness requires POSIX wait4 for per-process CPU/RSS")
    subprocess.run(["cargo", "build", "--release", "--locked", "-p", "bqip-normalizer",
                    "--bin", "bqip-replay", "--example", "replay_workload"], cwd=ROOT, check=True)
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    binary = target / "release/bqip-replay"
    fixture = target / "release/examples/replay_workload"
    points = []
    with tempfile.TemporaryDirectory(prefix="bqip-replay-benchmark-") as temporary:
        for multiplier in (1, 5, 10):
            root = Path(temporary) / f"workload-{multiplier}"
            subprocess.run([str(fixture), str(root), str(args.cycles * multiplier)], check=True,
                           stdout=subprocess.DEVNULL)
            out = root / "run"
            start = time.monotonic()
            with tempfile.TemporaryFile() as stdout:
                proc = subprocess.Popen([str(binary), "run", "--source-root", str(root / "source"),
                                         "--state-root", str(out), "--plan", str(root / "plan.json"),
                                         "--metadata", str(root / "confirmed.pb")], stdout=stdout)
                _, status, usage = os.wait4(proc.pid, 0)
                proc.returncode = os.waitstatus_to_exitcode(status)
                wall = time.monotonic() - start
                if proc.returncode:
                    raise RuntimeError(f"replay benchmark failed: {proc.returncode}")
            report = json.loads((out / "report.json").read_text())
            seconds = int(report["elapsed_ns"]) / 1e9
            points.append({
                "multiplier": multiplier, "input_records": int(report["input_record_count"]),
                "records_per_second": int(report["input_record_count"]) / seconds,
                "raw_mib_per_second": int(report["raw_bytes"]) / (1048576 * seconds),
                "normalized_successes_per_second": int(report["normalized_count"]) / seconds,
                "rejections_per_second": int(report["rejected_count"]) / seconds,
                "transcript_seconds": int(report["transcript_time_ns"]) / 1e9,
                "transcript_fraction": int(report["transcript_time_ns"]) / int(report["elapsed_ns"]),
                "peak_rss_bytes": usage.ru_maxrss * (1 if platform.system() == "Darwin" else 1024),
                "process_cpu_seconds": usage.ru_utime + usage.ru_stime,
                "process_wall_seconds": wall, "run_report": report,
            })
    report = {
        "scope": "Offline replay, accepted synthetic sealed segments; does not close B2/B4.",
        "base_workload": f"{args.cycles} cycles: exchangeInfo, snapshot, depth, two trades, malformed RAW",
        "resource_scope": "Fresh child per point, validation + normalization + transcript I/O; preparation excluded.",
        "transcript_overhead": "Measured serialization, payload digest, JCS, hash and write time; no throughput target.",
        "os": platform.platform(), "architecture": platform.machine(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "points": points,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"points": len(points), "output": str(args.output)}))


if __name__ == "__main__":
    main()
