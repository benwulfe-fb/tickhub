import ctypes
import os
import signal
import subprocess
import time
from pathlib import Path

import numpy as np
import pytest
import torch

from tickhub.abi import (
    FLAG_COLD_START,
    GlobalHeader,
    RECOVERY_MODE_COLD_START,
    RECOVERY_MODE_WARM_RESIDENT_GAP,
    RECOVERY_MODE_WARM_SUB_CADENCE,
)
from tickhub.atomic import load_acquire_i64
from tickhub.shm import HeartbeatTimeoutError, TickHubReader

ROOT = Path(__file__).resolve().parent.parent
HELPER_BIN = ROOT / "bin" / "producer_helper"

TEST_CONFIG = {
    "shm": {
        "name": "tickhub_test_recovery",
        "max_frames": 256,
        "cadence_interval": 1_000_000_000,
    },
    "features": ["ret_1s", "ret_5s", "ret_15s", "vol_1s", "spread_bps"],
    "unique_symbols": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMZN"],
    "phases": [
        {
            "id": 0,
            "name": "phase_0ms",
            "offset_ms": 0,
            "symbols": ["SPY", "QQQ", "AAPL", "MSFT"],
        },
        {
            "id": 1,
            "name": "phase_500ms",
            "offset_ms": 500,
            "symbols": ["SPY", "QQQ", "NVDA", "AMZN"],
        },
    ],
    "auto_realign": True,
}


import select


def wait_for_ready(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    start = time.time()
    while time.time() - start < timeout:
        if proc.poll() is not None:
            raise RuntimeError(f"Producer exited prematurely with code {proc.returncode}")
        r, _, _ = select.select([proc.stdout], [], [], 0.05)
        if r:
            line = proc.stdout.readline()
            if b"READY" in line:
                return
    raise TimeoutError("Producer did not output READY in time")


def cleanup_shm(name: str):
    path = f"/dev/shm/tickhub_{name}" if not name.startswith("tickhub_") else f"/dev/shm/{name}"
    if os.path.exists(path):
        try:
            os.unlink(path)
        except OSError:
            pass


def test_sub_cadence_warm_recovery():
    """Verify daemon kill and sub-cadence restart preserves cursors and updates BootID."""
    shm_name = "test_subcadence_rec"
    cleanup_shm(shm_name)
    cfg = dict(TEST_CONFIG)
    cfg["shm"] = dict(TEST_CONFIG["shm"])
    cfg["shm"]["name"] = shm_name

    start_anchor = 1700000000_000_000_000

    # 1. Start initial daemon (produces 5 frames and stays resident)
    proc1 = subprocess.Popen(
        [
            str(HELPER_BIN),
            "-shm",
            shm_name,
            "-anchor",
            str(start_anchor),
            "-frames",
            "5",
            "-interval",
            "20",
            "-no-unlink",
            "-daemon",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_ready(proc1)
        reader = TickHubReader(cfg)
        all_cursors, _ = reader.begin_all()
        p0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]
        for c in p0_cursors:
            c.target_anchor_ns = start_anchor

        boot_id_1 = reader.boot_id
        gen_1 = reader.generation
        assert boot_id_1 != 0
        assert gen_1 == 1

        # Read first batch
        out1 = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)
        reader.load_sync(p0_cursors, out1)
        assert out1[0, 0] > 0

        # Advance cursors for next batch (frame 5)
        for c in p0_cursors:
            c.target_anchor_ns = start_anchor + 5 * 1_000_000_000

        # 2. Kill daemon process cleanly leaving SHM resident
        proc1.kill()
        proc1.wait()

        # Simulate sub-cadence downtime: set last_written_anchor to now - 200ms
        hdr_ptr = ctypes.cast(reader._base_addr, ctypes.POINTER(GlobalHeader))
        hdr_ptr.contents.last_written_anchor_ns = time.time_ns() - 200_000_000

        # 3. Start replacement daemon in recovery mode (< 1s downtime)
        proc2 = subprocess.Popen(
            [
                str(HELPER_BIN),
                "-shm",
                shm_name,
                "-anchor",
                str(start_anchor),
                "-recovery",
                "-no-unlink",
                "-start-frame",
                "5",
                "-frames",
                "5",
                "-interval",
                "20",
                "-daemon",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            wait_for_ready(proc2)

            # 4. Reader re-attaches and loads second batch seamlessly
            out2 = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)
            failed = reader.load_sync(p0_cursors, out2)
            assert len(failed) == 0

            # Verify lifecycle metadata updated
            assert reader.boot_id != boot_id_1
            assert reader.generation == gen_1 + 1
            assert reader.is_warm_recovered is True
            assert reader.recovery_mode == RECOVERY_MODE_WARM_SUB_CADENCE
            assert reader.is_cold_start is False

            reader.close()
        finally:
            proc2.kill()
            proc2.wait()
    finally:
        cleanup_shm(shm_name)


def test_resident_gap_recovery():
    """Verify daemon restart with multi-second gap flags ColdStart and advances cursors."""
    shm_name = "test_gap_rec"
    cleanup_shm(shm_name)
    cfg = dict(TEST_CONFIG)
    cfg["shm"] = dict(TEST_CONFIG["shm"])
    cfg["shm"]["name"] = shm_name

    start_anchor = 1700000000_000_000_000

    # 1. Start initial daemon
    proc1 = subprocess.Popen(
        [
            str(HELPER_BIN),
            "-shm",
            shm_name,
            "-anchor",
            str(start_anchor),
            "-frames",
            "5",
            "-interval",
            "20",
            "-no-unlink",
            "-daemon",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_ready(proc1)
        reader = TickHubReader(cfg)
        all_cursors, _ = reader.begin_all()
        p0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]
        for c in p0_cursors:
            c.target_anchor_ns = start_anchor

        out = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)
        reader.load_sync(p0_cursors, out)

        proc1.kill()
        proc1.wait()

        # Artificially modify LastWrittenAnchorNS in SHM to simulate 5 seconds of downtime
        hdr_ptr = ctypes.cast(reader._base_addr, ctypes.POINTER(GlobalHeader))
        hdr_ptr.contents.last_written_anchor_ns = time.time_ns() - 5_000_000_000

        # Target upcoming anchor after gap
        for c in p0_cursors:
            c.target_anchor_ns = start_anchor + 10 * 1_000_000_000

        # 2. Start replacement daemon in recovery mode
        proc2 = subprocess.Popen(
            [
                str(HELPER_BIN),
                "-shm",
                shm_name,
                "-anchor",
                str(start_anchor),
                "-recovery",
                "-no-unlink",
                "-start-frame",
                "10",
                "-frames",
                "5",
                "-interval",
                "20",
                "-daemon",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            wait_for_ready(proc2)

            # Reader syncs across gap
            out2 = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)
            failed = reader.load_sync(p0_cursors, out2)
            assert len(failed) == 0

            assert reader.recovery_mode == RECOVERY_MODE_WARM_RESIDENT_GAP
            assert reader.is_warm_recovered is True
            reader.close()
        finally:
            proc2.kill()
            proc2.wait()
    finally:
        cleanup_shm(shm_name)


def test_heartbeat_timeout_detection():
    """Verify HeartbeatTimeoutError raised when daemon heartbeat stalls > 3.0s."""
    shm_name = "test_hb_timeout"
    cleanup_shm(shm_name)
    cfg = dict(TEST_CONFIG)
    cfg["shm"] = dict(TEST_CONFIG["shm"])
    cfg["shm"]["name"] = shm_name

    proc = subprocess.Popen(
        [str(HELPER_BIN), "-shm", shm_name, "-frames", "5", "-interval", "20", "-no-unlink", "-daemon"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_ready(proc)
        reader = TickHubReader(cfg)

        # Heartbeat is currently fresh
        assert reader.reconnect_if_needed() is False

        # Kill producer so background heartbeat ticker stops
        proc.kill()
        proc.wait()

        # Set heartbeat 4.0s in the past
        hdr_ptr = ctypes.cast(reader._base_addr, ctypes.POINTER(GlobalHeader))
        hdr_ptr.contents.heartbeat_ns = time.time_ns() - 4_000_000_000

        with pytest.raises(HeartbeatTimeoutError) as exc_info:
            reader.reconnect_if_needed()
        assert "heartbeat stalled > 3.0s" in str(exc_info.value)

        reader.close()
    finally:
        cleanup_shm(shm_name)


def test_cold_start_flag_detection():
    """Verify FlagColdStart detection via is_cold_start property."""
    shm_name = "test_cold_flag"
    cleanup_shm(shm_name)
    cfg = dict(TEST_CONFIG)
    cfg["shm"] = dict(TEST_CONFIG["shm"])
    cfg["shm"]["name"] = shm_name

    proc = subprocess.Popen(
        [str(HELPER_BIN), "-shm", shm_name, "-anchor", "-1", "-frames", "3", "-interval", "20", "-cold-start", "-no-unlink", "-daemon"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_ready(proc)
        reader = TickHubReader(cfg)

        # Frame was published with FlagColdStart
        assert reader.is_cold_start is True

        proc.kill()
        proc.wait()

        # Start second producer without cold start
        proc2 = subprocess.Popen(
            [str(HELPER_BIN), "-shm", shm_name, "-anchor", "-1", "-frames", "3", "-start-frame", "3", "-interval", "20", "-recovery", "-daemon"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            wait_for_ready(proc2)
            assert reader.is_cold_start is False
            reader.close()
        finally:
            proc2.kill()
            proc2.wait()
    finally:
        cleanup_shm(shm_name)
