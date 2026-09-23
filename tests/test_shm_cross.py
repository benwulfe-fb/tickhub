import asyncio
import os
import subprocess
import time
from pathlib import Path
import pytest
import numpy as np
import torch

from tickhub import TickHubReader, SymbolCursor, LaggedAnchorError

PRODUCER_BIN = Path(__file__).parent.parent / "bin" / "producer_helper"
SHM_NAME = "tickhub_test_cross"

TEST_CONFIG = {
    "shm": {"name": SHM_NAME},
    "cadence_interval": 1_000_000_000,
    "features": ["ret_1s", "ret_5s", "ret_15s", "vol_1s", "spread_bps"],
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
}


@pytest.fixture(scope="module")
def producer_process():
    # Start Go producer in daemon mode
    proc = subprocess.Popen(
        [str(PRODUCER_BIN), "-shm", SHM_NAME, "-frames", "50", "-interval", "20", "-daemon"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Wait for READY line
    line = proc.stdout.readline()
    assert "READY" in line, f"Producer failed to start: {proc.stderr.read()}"

    yield proc

    proc.terminate()
    try:
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        proc.kill()


def test_shm_metadata_and_snapshots(producer_process):
    with TickHubReader(TEST_CONFIG) as hub:
        assert hub.status == 2  # StatusRunning
        assert hub.num_phases == 2
        assert hub.max_frames == 256
        assert len(hub.symbols) == 6
        assert "SPY" in hub.symbols
        assert "QQQ" in hub.symbols
        assert hub.phases == ["phase_0ms", "phase_500ms"]

        # SeqLock Top-of-Book Snapshots
        snap_spy = hub.snapshot("SPY")
        assert snap_spy["symbol"] == "SPY"
        assert snap_spy["bid_px"] == 100.0
        assert snap_spy["ask_px"] == 100.05
        assert snap_spy["spread"] == 0.05
        assert snap_spy["midprice"] == 100.025

        snap_aapl = hub.snapshot("AAPL")
        assert snap_aapl["symbol"] == "AAPL"
        assert snap_aapl["bid_px"] == 200.0


def test_phase_symbol_partitioning_and_cursors(producer_process):
    with TickHubReader(TEST_CONFIG) as hub:
        all_cursors, _ = hub.begin_all()
        assert len(all_cursors) == 8  # 4 symbols in phase 0 + 4 symbols in phase 1

        # Partition via standard Python list comprehension
        phase_0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]
        phase_1_cursors = [c for c in all_cursors if c.phase == "phase_500ms"]

        assert len(phase_0_cursors) == 4
        assert [c.symbol for c in phase_0_cursors] == ["SPY", "QQQ", "AAPL", "MSFT"]

        assert len(phase_1_cursors) == 4
        assert [c.symbol for c in phase_1_cursors] == ["SPY", "QQQ", "NVDA", "AMZN"]

        # Cross assets exist in both collections
        assert any(c.symbol == "SPY" for c in phase_0_cursors)
        assert any(c.symbol == "SPY" for c in phase_1_cursors)


def test_metric_matrix_loading_and_cross_assets(producer_process):
    with TickHubReader(TEST_CONFIG) as hub:
        all_cursors, _ = hub.begin_all()
        p0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]
        p1_cursors = [c for c in all_cursors if c.phase == "phase_500ms"]

        # Target known anchor from synthetic producer
        start_anchor = 1700000000_000_000_000
        for c in p0_cursors:
            c.target_anchor_ns = start_anchor
        for c in p1_cursors:
            c.target_anchor_ns = start_anchor + 500_000_000

        # PyTorch pre-allocated output matrix
        out_p0 = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)
        out_p1 = torch.zeros((len(p1_cursors), 5), dtype=torch.float64)

        # Synchronous load
        failed = hub.load_sync(p0_cursors, out_p0)
        assert len(failed) == 0

        # Verify bit-exact float64 metrics for Phase 0
        # Formula from producer_helper: float64(anchor) + s_idx*1000.0 + feat_idx*0.1
        for s_idx in range(4):
            for f_idx in range(5):
                expected = float(start_anchor) + float(s_idx) * 1000.0 + float(f_idx) * 0.1
                actual = out_p0[s_idx, f_idx].item()
                assert actual == expected, f"Mismatch at ({s_idx}, {f_idx}): {actual} != {expected}"

        # Load Phase 1
        failed1 = hub.load_sync(p1_cursors, out_p1)
        assert len(failed1) == 0

        # Verify bit-exact float64 metrics for Phase 1 (anchor + 500ms)
        p1_anchor = start_anchor + 500_000_000
        for s_idx in range(4):
            for f_idx in range(5):
                expected = float(p1_anchor) + float(s_idx) * 1000.0 + float(f_idx) * 0.1
                actual = out_p1[s_idx, f_idx].item()
                assert actual == expected

        # Cross-asset verification: SPY (index 0) in Phase 0 vs Phase 1
        spy_p0_ret = out_p0[0, 0].item()
        spy_p1_ret = out_p1[0, 0].item()
        assert spy_p0_ret != spy_p1_ret
        assert spy_p1_ret - spy_p0_ret == 500_000_000.0


def test_async_load_and_catchup(producer_process):
    with TickHubReader(TEST_CONFIG) as hub:
        all_cursors, _ = hub.begin_all()
        p0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]
        start_anchor = 1700000000_000_000_000
        for c in p0_cursors:
            c.target_anchor_ns = start_anchor

        out_p0 = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)

        async def run_async_steps():
            # Step 1: initial load
            failed = await hub.load(p0_cursors, out_p0)
            assert len(failed) == 0

            # Step 2: advance cursors
            hub.next(p0_cursors)
            assert p0_cursors[0].target_anchor_ns == start_anchor + 1_000_000_000

            # Step 3: load next frame
            failed = await hub.load(p0_cursors, out_p0)
            assert len(failed) == 0
            assert out_p0[0, 0].item() == float(start_anchor + 1_000_000_000)

        asyncio.run(run_async_steps())


def test_lagged_anchor_error_and_rebegin(producer_process):
    with TickHubReader(TEST_CONFIG) as hub:
        all_cursors, _ = hub.begin_all()
        p0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]

        # Deliberately set target anchor far in the past before ring buffer
        p0_cursors[0].target_anchor_ns = 100_000_000_000  # 100 seconds after epoch 1970
        out = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)

        with pytest.raises(LaggedAnchorError):
            hub.load_sync(p0_cursors, out)

        # Call rebegin on the lagged cursor
        p0_cursors[0].rebegin()
        assert p0_cursors[0].target_anchor_ns > 1700000000_000_000_000
