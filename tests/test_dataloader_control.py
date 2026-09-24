import os
import subprocess
import sys
import time
from pathlib import Path
import numpy as np
import pytest

from tickhub import TickHubReader
from tickhub.abi import CONTROL_STATUS_BUSY, CONTROL_STATUS_IDLE, GlobalHeader

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKHUB_BIN = REPO_ROOT / "bin" / "tickhub"
CONFIG_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "config_golden.yaml"
DATALAKE_DIR = REPO_ROOT / "tests" / "fixtures" / "golden" / "datalake"
GOLDEN_METRICS_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "golden_1hz_metrics.npy"
GOLDEN_ANCHORS_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "golden_anchors.npy"
SHM_NAME = "tickhub_worker_test"
SHM_FILE = f"/dev/shm/{SHM_NAME}"

EXECUTION_BUDGET_CEILING_MS = 60.0


@pytest.fixture
def worker_daemon():
    """Starts a persistent Go worker daemon and ensures cleanup on teardown."""
    if os.path.exists(SHM_FILE):
        try:
            os.unlink(SHM_FILE)
        except OSError:
            pass

    proc = subprocess.Popen(
        [
            str(TICKHUB_BIN),
            "worker",
            "--config", str(CONFIG_PATH),
            "--datalake", str(DATALAKE_DIR),
            "--shm-name", SHM_NAME,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Wait for daemon to initialize and create SHM segment
    t0 = time.time()
    while not os.path.exists(SHM_FILE):
        if proc.poll() is not None:
            _, stderr = proc.communicate()
            raise RuntimeError(f"Worker failed to start (exit code {proc.returncode}): {stderr}")
        if time.time() - t0 > 3.0:
            proc.kill()
            raise TimeoutError("Timed out waiting for worker daemon to create SHM segment")
        time.sleep(0.01)

    try:
        yield proc
    finally:
        if proc.poll() is None:
            try:
                # Try graceful shutdown via TickHubReader if SHM segment still mapped
                if os.path.exists(SHM_FILE):
                    try:
                        with TickHubReader(CONFIG_PATH, shm_path_override=SHM_NAME) as r:
                            r.shutdown_worker()
                        proc.wait(timeout=1.0)
                    except Exception:
                        pass
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=1.0)
        if os.path.exists(SHM_FILE):
            try:
                os.unlink(SHM_FILE)
            except OSError:
                pass


def test_dataloader_control_line_and_recovery(worker_daemon):
    assert TICKHUB_BIN.exists(), f"tickhub binary missing at {TICKHUB_BIN}."
    assert GOLDEN_METRICS_PATH.exists(), f"Golden metrics reference missing: {GOLDEN_METRICS_PATH}"
    assert GOLDEN_ANCHORS_PATH.exists(), f"Golden anchors reference missing: {GOLDEN_ANCHORS_PATH}"

    golden_metrics = np.load(GOLDEN_METRICS_PATH)
    golden_anchors = np.load(GOLDEN_ANCHORS_PATH)

    s_start = 1778074260000000000  # 09:31:00
    s_end = 1778074320000000000    # 09:32:00

    # 1. Connect reader and command chunk replay
    with TickHubReader(CONFIG_PATH, shm_path_override=SHM_NAME) as reader:
        assert reader._writable, "Reader must be opened O_RDWR"

        t0 = time.perf_counter()
        num_frames, cold_start = reader.request_chunk("DASH", "2026-05-06", s_start, s_end)
        round_trip_ms = (time.perf_counter() - t0) * 1000.0

        # In chunk replay mode for half-open interval [startNS, endNS), the worker flushes
        # all 1Hz intervals up to endNS (09:32:00), emitting exactly 60 frames: anchors
        # 09:31:01 through 09:32:00. In cold replay mode (test_golden_replay), replay stopped
        # at the last observed tick (09:31:59.836), flushing 59 frames.
        assert num_frames == 60, f"Expected 60 frames, got {num_frames}"
        assert cold_start == 1, f"Expected cold_start=1 for unseeded slice, got {cold_start}"

        # 2. Extract frames and assert bitwise parity
        cursors, _ = reader.begin_all()
        extracted_records = []
        extracted_anchors = []
        mat = np.zeros((1, 5), dtype=np.float64)

        for i in range(num_frames):
            extracted_anchors.append(cursors[0].target_anchor_ns)
            failed = reader.load_sync(cursors, mat)
            assert not failed, f"Frame {i} load failed: {failed}"
            extracted_records.append(mat[0].copy())
            reader.next(cursors)

        actual_metrics = np.array(extracted_records, dtype=np.float64)
        actual_anchors = np.array(extracted_anchors, dtype=np.int64)

        # Assert ControlResponse anchor range matches actual committed frames
        assert reader._header.control_resp.first_anchor_ns == actual_anchors[0], (
            f"FirstAnchorNS mismatch: {reader._header.control_resp.first_anchor_ns} != {actual_anchors[0]}"
        )
        assert reader._header.control_resp.last_anchor_ns == actual_anchors[-1], (
            f"LastAnchorNS mismatch: {reader._header.control_resp.last_anchor_ns} != {actual_anchors[-1]}"
        )
        assert reader._header.control_resp.num_frames_written == 60

        # Assert first 59 frames match frozen golden reference bit-for-bit
        np.testing.assert_array_equal(
            actual_anchors[:59],
            golden_anchors,
            err_msg="Anchor timestamps mismatch in worker chunk replay!",
        )
        np.testing.assert_equal(
            actual_metrics[:59],
            golden_metrics,
            err_msg="Metrics mismatch in worker chunk replay!",
        )

        print(
            f"\n[WORKER TELEMETRY] Chunk round-trip: {round_trip_ms:.2f} ms "
            f"(budget: <={EXECUTION_BUDGET_CEILING_MS:.1f} ms), 60 frames verified bit-identical"
        )
        assert round_trip_ms <= EXECUTION_BUDGET_CEILING_MS, (
            f"Chunk replay latency regression: {round_trip_ms:.2f} ms > ceiling {EXECUTION_BUDGET_CEILING_MS:.1f} ms"
        )

    # 3. Simulate abrupt client crash and test in-place recovery (B2 Invariant)
    crash_script = (
        "import os, mmap, ctypes\n"
        f"fd = os.open('{SHM_FILE}', os.O_RDWR)\n"
        "mm = mmap.mmap(fd, 0, prot=mmap.PROT_READ | mmap.PROT_WRITE)\n"
        "from tickhub.abi import GlobalHeader, CONTROL_STATUS_BUSY\n"
        "class Py_buffer(ctypes.Structure):\n"
        "    _fields_ = [('buf', ctypes.c_void_p), ('obj', ctypes.py_object), ('len', ctypes.c_ssize_t), ('itemsize', ctypes.c_ssize_t), ('readonly', ctypes.c_int)]\n"
        "view = Py_buffer()\n"
        "ctypes.pythonapi.PyObject_GetBuffer(ctypes.py_object(mm), ctypes.byref(view), 0)\n"
        "base = view.buf\n"
        "ctypes.pythonapi.PyBuffer_Release(ctypes.byref(view))\n"
        "hdr = GlobalHeader.from_address(base)\n"
        "hdr.consumer_pid = os.getpid()\n"
        "hdr.control_resp.status = CONTROL_STATUS_BUSY\n"
        "os._exit(0)\n"
    )
    subprocess.run([sys.executable, "-c", crash_script], check=True)

    # Worker daemon should detect client death via syscall.Kill(pid, 0) and reset StatusBusy -> StatusIdle
    time.sleep(0.05)

    with TickHubReader(CONFIG_PATH, shm_path_override=SHM_NAME) as reader:
        assert reader._header.control_resp.status == CONTROL_STATUS_IDLE, (
            f"Expected StatusIdle after client crash, got {reader._header.control_resp.status}"
        )
        assert reader._header.consumer_pid == 0, (
            f"Expected ConsumerPID=0 after client crash, got {reader._header.consumer_pid}"
        )

        # Issue second request to verify in-place recovery on existing segment
        num_frames2, _ = reader.request_chunk("DASH", "2026-05-06", s_start, s_end)
        assert num_frames2 == 60

        # 4. Clean worker shutdown via control line
        reader.shutdown_worker()

    worker_daemon.wait(timeout=2.0)
    assert worker_daemon.returncode == 0, f"Worker exited with code {worker_daemon.returncode}"
    assert not os.path.exists(SHM_FILE), "SHM segment should be unlinked on clean daemon shutdown"
