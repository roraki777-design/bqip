"""Measure finite synthetic collector process resources without new dependencies."""
from __future__ import annotations

import argparse
import json
import resource
import subprocess
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--count", default=1000, type=int)
    args = parser.parse_args()
    start = time.monotonic()
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    result = subprocess.run(
        [str(args.binary.resolve()), str(args.output.resolve()), str(args.count)],
        check=False,
    )
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if result.returncode:
        return result.returncode
    report = json.loads((args.output / "benchmark.json").read_text())
    report["process_resources"] = {
        "wall_seconds": time.monotonic() - start,
        "user_cpu_seconds": after.ru_utime - before.ru_utime,
        "system_cpu_seconds": after.ru_stime - before.ru_stime,
        "peak_rss_kib_linux": after.ru_maxrss,
        "scope": "entire benchmark child, all four scenarios; Linux ru_maxrss in KiB",
        "disk_scope": "logical output file sizes, not device write amplification",
    }
    (args.output / "benchmark-with-resources.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
