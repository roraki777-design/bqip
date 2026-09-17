# BQIP — Brief #001 contract kernel

**Status: BLOCKED pending independent acceptance, including a real CI run.**

BQIP is a BTC quantitative data/research platform. Foundation supplies exact,
versioned contracts and reproducible evidence before venue integration or research
features. This repository implements only Implementation Brief #001 / AC-001
against Master Architecture v1.0 FROZEN. Do not start Brief #002.

## Contents

* Five normative Protobuf IDLs; official committed Python `.py`/`.pyi` output.
* Two Rust crates: contract primitives and local BQRC framing/recovery.
* Independent Python primitives and adapters for the official Protobuf runtime.
* External golden expectations, JCS conformance, property and fault tests.
* Resolved Cargo and hash-locked Python dependencies, read-only CI, four proposed ADRs.

## Supported validation environment

Linux x86_64 / POSIX filesystem; CI targets Ubuntu 24.04. Python 3.12.x (CI:
3.12.14), Rust/cargo 1.85.0 with rustfmt/clippy, protoc 33.6, protobuf runtime
6.33.6, cargo-audit 0.22.1 and Gitleaks 8.24.2. Python's lock targets CPython 3.12
on Linux x86_64; other platforms need separately resolved and reviewed artifacts.
Install Git, a C linker/toolchain, curl and unzip before provisioning these tools.
The pinned tools must be on PATH. No implicit dependency installation occurs in
`tools/checks.py` or the Protobuf generator.

```bash
rustup toolchain install 1.85.0 --profile minimal --component rustfmt --component clippy
cargo install cargo-audit --version 0.22.1 --locked
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --require-hashes -r python/requirements-dev.lock
```

The exact protoc and Gitleaks release downloads and SHA-256 checks are in
`.github/workflows/ci.yml`. cargo-audit 0.21.2 was demonstrated incompatible with
CVSS 4.0 entries in the current RustSec database; 0.22.1 compiles with Rust 1.85.0.
No advisory database filtering or vulnerability suppression is used.

## Reproduce all gates from committed files

Clone the repository into a new directory and provision the tools above. For a
cache-independent check, use an empty CARGO_HOME and CARGO_TARGET_DIR and a new
Python virtual environment; install Python dependencies with `--no-cache-dir`.
Tool binaries are prerequisites, not project build caches.

From the root:

```bash
python3 tools/generate_protobuf.py
python3 tools/generate_protobuf.py --check
git diff --exit-code -- python/src/bqip/v1
python3 tools/checks.py
```

`tools/checks.py` runs 13 gates, including tool versions/git/lock evidence, fmt,
clippy, Rust unit/property/golden tests, Ruff, strict mypy, Python unit/golden/fault
and Protobuf tests, generated freshness, both dependency audits, secret scan and
required files. Missing tools and dependencies fail visibly; no test skips them.
CI regenerates bindings, checks the committed output, then invokes the same script.
The environment record prints the git commit and FILES.sha256/lockfile hashes.
The delivery evidence records the clean-checkout run separately so reporting does
not mutate the tested commit. A local green run is not a substitute for GitHub CI.

## Individual gates

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked
python3 -m ruff check --config python/pyproject.toml python/src tests tools
python3 -m mypy --config-file python/pyproject.toml python/src/bqip
PYTHONPATH=python/src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=python/src python3 -m unittest discover -s tests/protobuf -p 'test_*.py' -v
cargo audit --deny warnings
python3 -m pip_audit --strict -r python/requirements-dev.lock
gitleaks dir . --redact --no-banner
```

Regeneration uses official protoc 33.6; Rust's build generates from the same IDL.
Never edit generated bindings. types-protobuf is development-only and does not
replace runtime behavior. Narrow generated-file lint/type exceptions are explained
in pyproject.toml; handwritten adapters remain strictly checked.

## Explicit scope and evidence limits

No exchange adapter/connection, Binance, Book Service, async runtime, NATS,
database, Parquet, object-store client, cloud deployment, trading credentials,
orders or execution. Remote segment states are contract values only.

Tests exercise local fsync/rename, reopen, truncation, CRC faults and preservation
of existing evidence. A structurally valid segment does not prove no complete
frame disappeared: compare its hash against external segment integrity evidence.
No real power-loss durability or production performance claim is made.

See `docs/IMPLEMENTATION_REPORT.md`, `docs/contracts/`, `tests/fixtures/README.md`
and the accompanying validation evidence. ADR-001/002/003/008 remain
**PROPOSED / IMPLEMENTED FOR REVIEW**, regardless of test results.

## Authority

Master Architecture PDF SHA-256:
`bd0496d108018730662ceed48735b4c1114589f735202ff422372b56cb559eb3`.
AC-001, Brief #001 and Completion Directive #001-A define this implementation.
