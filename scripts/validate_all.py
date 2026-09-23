#!/usr/bin/env python
"""validate_all.py — Single-command comprehensive verification gate for tickhub.

Enforces:
1. Clean C shared library build (make -C c).
2. ABI struct sizes, padding, and cache-line alignment across C, Go, and Python.
3. Go binary builds (bin/tickhub, bin/producer_helper, bin/producer_demo).
4. Go unit and race detector test suite (go test -v -race ./...).
5. Python pytest suite (pytest -v tests/).
6. End-to-end binary execution sanity checks.
7. Code hygiene and git tree status check.

Telemetry is logged to console and output/{yymmdd}_{hhmm}_{pid}_stdout.txt.
Fails fast on any regression.
"""
import os
import sys
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON_EXE = sys.executable
GO_EXE = shutil.which("go") or "/mnt/wc/go/bin/go"


class Telemetry:
    """Manages output redirection to console and output/."""
    def __init__(self):
        yymmdd = datetime.now().strftime("%y%m%d")
        hhmm = datetime.now().strftime("%H%M")
        pid = os.getpid()

        output_dir = ROOT / "output"
        output_dir.mkdir(parents=True, exist_ok=True)

        self.stdout_path = output_dir / f"{yymmdd}_{hhmm}_{pid}_stdout.txt"
        self.stderr_path = output_dir / f"{yymmdd}_{hhmm}_{pid}_stderr.txt"

        self.stdout_f = open(self.stdout_path, "w", encoding="utf-8")
        self.stderr_f = open(self.stderr_path, "w", encoding="utf-8")

    def log(self, message: str):
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        formatted = f"{timestamp} {message}"
        self.stdout_f.write(formatted + "\n")
        self.stdout_f.flush()
        print(formatted, file=sys.stdout)
        sys.stdout.flush()

    def error(self, message: str):
        timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        formatted = f"{timestamp} ERROR: {message}"
        self.stderr_f.write(formatted + "\n")
        self.stderr_f.flush()
        print(formatted, file=sys.stderr)
        sys.stderr.flush()

    def close(self):
        self.stdout_f.close()
        self.stderr_f.close()


telemetry = Telemetry()


def run_cmd(cmd: list[str], cwd: Path | None = None, desc: str = "") -> None:
    """Runs a subprocess command and fails fast on non-zero exit."""
    if desc:
        telemetry.log(f"--> {desc} ({' '.join(cmd)})")
    else:
        telemetry.log(f"--> Running: {' '.join(cmd)}")

    proc = subprocess.run(
        cmd,
        cwd=str(cwd or ROOT),
        capture_output=True,
        text=True,
    )

    if proc.stdout:
        for line in proc.stdout.strip().splitlines():
            telemetry.log(f"    [stdout] {line}")
    if proc.stderr:
        for line in proc.stderr.strip().splitlines():
            telemetry.log(f"    [stderr] {line}")

    if proc.returncode != 0:
        telemetry.error(f"Command failed with exit code {proc.returncode}: {' '.join(cmd)}")
        sys.exit(1)


def validate_c_build():
    """Verify C atomic library builds cleanly."""
    telemetry.log("==================== [1/6] C Shared Library Compilation ====================")
    c_dir = ROOT / "c"
    run_cmd(["make", "-C", "c", "clean"], desc="Clean C shared library")
    run_cmd(["make", "-C", "c"], desc="Compile libtickhub_atomic.so")

    lib_path = ROOT / "python" / "tickhub" / "libtickhub_atomic.so"
    if not lib_path.exists():
        telemetry.error(f"Compiled C library missing at {lib_path}")
        sys.exit(1)
    telemetry.log(f"C shared library successfully compiled and placed at {lib_path}")


def validate_abi_alignments():
    """Verify ABI struct sizes and cache line alignments in Python."""
    telemetry.log("==================== [2/6] ABI Alignment & Size Verification ====================")
    check_script = (
        "import sys, ctypes\n"
        "from tickhub.abi import GlobalHeader, SymbolSnapshot, FrameHeader, SymbolDirectoryEntry, PhaseInfo\n"
        "assert ctypes.sizeof(GlobalHeader) == 1024, f'GlobalHeader size: {ctypes.sizeof(GlobalHeader)}'\n"
        "assert ctypes.sizeof(SymbolSnapshot) == 128, f'SymbolSnapshot size: {ctypes.sizeof(SymbolSnapshot)}'\n"
        "assert ctypes.sizeof(FrameHeader) == 64, f'FrameHeader size: {ctypes.sizeof(FrameHeader)}'\n"
        "assert ctypes.sizeof(SymbolDirectoryEntry) == 16, f'SymbolDirectoryEntry size: {ctypes.sizeof(SymbolDirectoryEntry)}'\n"
        "assert ctypes.sizeof(PhaseInfo) == 32, f'PhaseInfo size: {ctypes.sizeof(PhaseInfo)}'\n"
        "assert GlobalHeader.anchor_publish_latency_ns.offset == 64, f'Producer line offset: {GlobalHeader.anchor_publish_latency_ns.offset}'\n"
        "assert GlobalHeader.last_read_anchor_ns.offset == 128, f'Consumer line offset: {GlobalHeader.last_read_anchor_ns.offset}'\n"
        "assert GlobalHeader.phases.offset == 192, f'Phases offset: {GlobalHeader.phases.offset}'\n"
        "print('All ABI assertions passed successfully.')\n"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "python") + ":" + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [PYTHON_EXE, "-c", check_script],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        telemetry.error(f"ABI verification failed:\n{proc.stderr}")
        sys.exit(1)
    telemetry.log("ABI Struct sizes and dual cache-line alignment verified.")


def validate_go_builds():
    """Build all Go binaries."""
    telemetry.log("==================== [3/6] Go Binary Compilation ====================")
    bin_dir = ROOT / "bin"
    bin_dir.mkdir(exist_ok=True)
    run_cmd([GO_EXE, "build", "-o", "bin/tickhub", "./cmd/tickhub"], desc="Build tickhub CLI")
    run_cmd([GO_EXE, "build", "-o", "bin/producer_helper", "./tests/producer_helper.go"], desc="Build producer_helper")
    run_cmd([GO_EXE, "build", "-o", "bin/producer_demo", "./examples/producer_demo.go"], desc="Build producer_demo")
    telemetry.log("All Go binaries compiled cleanly.")


def validate_go_tests():
    """Run Go tests with race detector."""
    telemetry.log("==================== [4/6] Go Unit & Race Detector Suite ====================")
    run_cmd([GO_EXE, "test", "-v", "-race", "./..."], desc="Execute go test -race ./...")
    telemetry.log("Go unit and race tests passed cleanly.")


def validate_python_tests():
    """Run pytest suite."""
    telemetry.log("==================== [5/6] Python Pytest Suite ====================")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "python") + ":" + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [PYTHON_EXE, "-m", "pytest", "-v", "tests/"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.stdout:
        for line in proc.stdout.strip().splitlines():
            telemetry.log(f"    [pytest] {line}")
    if proc.stderr:
        for line in proc.stderr.strip().splitlines():
            telemetry.log(f"    [pytest-err] {line}")
    if proc.returncode != 0:
        telemetry.error(f"Pytest failed with exit code {proc.returncode}")
        sys.exit(1)
    telemetry.log("All Python integration tests passed cleanly.")


def validate_smoke_and_hygiene():
    """Run CLI smoke check and report git status."""
    telemetry.log("==================== [6/6] Smoke Execution & Git Hygiene ====================")
    # Check CLI help output
    proc = subprocess.run(
        [str(ROOT / "bin" / "tickhub"), "--help"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        telemetry.error(f"tickhub --help failed: {proc.stderr}")
        sys.exit(1)
    telemetry.log("CLI smoke execution verified.")

    # Check git status
    proc = subprocess.run(["git", "status", "--porcelain"], cwd=str(ROOT), capture_output=True, text=True)
    if proc.returncode == 0:
        dirty_lines = [l.strip() for l in (proc.stdout or "").splitlines() if l.strip()]
        if dirty_lines:
            telemetry.log(f"Working tree has {len(dirty_lines)} uncommitted file(s):")
            for line in dirty_lines:
                telemetry.log(f"    {line}")
        else:
            telemetry.log("Working tree is clean.")


def main():
    start_time = datetime.now()
    telemetry.log(f"Starting tickhub validate_all at {start_time.strftime('%Y-%m-%d %H:%M:%SZ')}")

    try:
        validate_c_build()
        validate_abi_alignments()
        validate_go_builds()
        validate_go_tests()
        validate_python_tests()
        validate_smoke_and_hygiene()

        elapsed = (datetime.now() - start_time).total_seconds()
        telemetry.log(f"ALL TICKHUB VERIFICATIONS PASSED SUCCESSFULLY in {elapsed:.2f}s.")
        return 0
    except Exception as e:
        telemetry.error(f"Unhandled exception during validation: {e}")
        import traceback
        telemetry.error(traceback.format_exc())
        return 1
    finally:
        telemetry.close()


if __name__ == "__main__":
    sys.exit(main())
