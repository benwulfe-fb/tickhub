import os
import re
import subprocess
import time
from pathlib import Path
import numpy as np
import pytest

from tickhub import TickHubReader

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKHUB_BIN = REPO_ROOT / "bin" / "tickhub"
CONFIG_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "config_golden.yaml"
DATALAKE_DIR = REPO_ROOT / "tests" / "fixtures" / "golden" / "datalake"
GOLDEN_METRICS_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "golden_1hz_metrics.npy"
GOLDEN_ANCHORS_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "golden_anchors.npy"
SHM_FILE = "/dev/shm/tickhub_golden_test"

THROUGHPUT_FLOOR_TICKS_PER_SEC = 50000.0
EXECUTION_BUDGET_CEILING_MS = 100.0


@pytest.fixture(autouse=True)
def clean_shm():
    """Ensure private SHM segment is unlinked before and after test execution."""
    if os.path.exists(SHM_FILE):
        try:
            os.unlink(SHM_FILE)
        except OSError:
            pass
    try:
        yield
    finally:
        if os.path.exists(SHM_FILE):
            try:
                os.unlink(SHM_FILE)
            except OSError:
                pass


def test_golden_replay_bit_identical_and_perf():
    assert TICKHUB_BIN.exists(), f"tickhub binary not found at {TICKHUB_BIN}. Run `go build -o bin/tickhub ./cmd/tickhub`."
    assert GOLDEN_METRICS_PATH.exists(), f"Golden metrics reference missing: {GOLDEN_METRICS_PATH}"
    assert GOLDEN_ANCHORS_PATH.exists(), f"Golden anchors reference missing: {GOLDEN_ANCHORS_PATH}"

    golden_metrics = np.load(GOLDEN_METRICS_PATH)
    golden_anchors = np.load(GOLDEN_ANCHORS_PATH)

    # 1. Run replay via subprocess and measure round-trip wall-clock time
    t_start = time.perf_counter()
    cmd = [
        str(TICKHUB_BIN),
        "replay",
        "--config", str(CONFIG_PATH),
        "--datalake", str(DATALAKE_DIR),
        "--date", "2026-05-06",
        "--no-unlink",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)

    # 2. Extract metrics and anchors via TickHubReader
    with TickHubReader(CONFIG_PATH) as reader:
        cursors, _ = reader.begin_all()
        num_frames = int((reader.last_written_anchor_ns - reader.first_anchor_ns) // reader.cadence_ns) + 1

        extracted_records = []
        extracted_anchors = []
        mat = np.zeros((1, 5), dtype=np.float64)

        for i in range(num_frames):
            extracted_anchors.append(cursors[0].target_anchor_ns)
            failed = reader.load_sync(cursors, mat)
            assert not failed, f"Frame {i} load failed for symbol: {failed}"
            extracted_records.append(mat[0].copy())
            reader.next(cursors)

    t_end = time.perf_counter()
    total_elapsed_ms = (t_end - t_start) * 1000.0

    actual_metrics = np.array(extracted_records, dtype=np.float64)
    actual_anchors = np.array(extracted_anchors, dtype=np.int64)

    # 3. Assert bit-identical match with golden reference
    np.testing.assert_array_equal(
        actual_anchors,
        golden_anchors,
        err_msg="Golden anchor timestamps mismatch!",
    )
    np.testing.assert_equal(
        actual_metrics,
        golden_metrics,
        err_msg="Golden 1Hz metrics diverge from frozen bit-identical reference!",
    )

    # 4. Parse throughput from Go daemon stdout / stderr (Go log.Printf outputs to stderr)
    combined_output = f"{res.stdout}\n{res.stderr}"
    match = re.search(r"Completed replay of \d+ ticks in .*?\(([\d\.]+)\s+ticks/sec\)", combined_output)
    assert match is not None, f"Could not parse replay throughput from output:\n{combined_output}"
    observed_throughput = float(match.group(1))

    # 5. Performance regression gates with explicit diagnostic telemetry
    print(f"\n[GOLDEN TEST TELEMETRY] Throughput: {observed_throughput:.1f} ticks/s (floor: {THROUGHPUT_FLOOR_TICKS_PER_SEC:.1f})")
    print(f"[GOLDEN TEST TELEMETRY] Round-trip latency: {total_elapsed_ms:.2f} ms (ceiling: {EXECUTION_BUDGET_CEILING_MS:.1f} ms)")

    assert observed_throughput >= THROUGHPUT_FLOOR_TICKS_PER_SEC, (
        f"Replay throughput regression: observed {observed_throughput:.1f} ticks/s < floor {THROUGHPUT_FLOOR_TICKS_PER_SEC:.1f} ticks/s"
    )
    assert total_elapsed_ms <= EXECUTION_BUDGET_CEILING_MS, (
        f"Execution budget exceeded: elapsed {total_elapsed_ms:.1f} ms > ceiling {EXECUTION_BUDGET_CEILING_MS:.1f} ms"
    )
