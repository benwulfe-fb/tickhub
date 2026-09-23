import asyncio
import gc
import os
import subprocess
import time
from pathlib import Path
import numpy as np
import torch

from tickhub import TickHubReader, SymbolCursor

PRODUCER_BIN = Path(__file__).parent.parent / "bin" / "producer_helper"
SHM_NAME = "tickhub_benchmark"

BENCH_CONFIG = {
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


def run_benchmark():
    # Start Go producer in daemon mode
    proc = subprocess.Popen(
        [str(PRODUCER_BIN), "-shm", SHM_NAME, "-frames", "200", "-interval", "5", "-daemon"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    line = proc.stdout.readline()
    assert "READY" in line, f"Producer failed: {proc.stderr.read()}"

    try:
        with TickHubReader(BENCH_CONFIG) as hub:
            print("\n" + "=" * 60)
            print("  TICKHUB ZERO-COPY SHM LATENCY BENCHMARK")
            print("=" * 60)

            # 1. Benchmark SeqLock Snapshot Read Latency
            N_SNAP = 50_000
            start = time.perf_counter_ns()
            for _ in range(N_SNAP):
                _ = hub.snapshot("SPY")
            elapsed_ns = time.perf_counter_ns() - start
            snap_lat_ns = elapsed_ns / N_SNAP
            print(f"1. SeqLock Snapshot Read Latency: {snap_lat_ns:.1f} ns ({snap_lat_ns / 1000.0:.3f} µs) per read")

            # 2. Benchmark Metric Matrix Extraction into PyTorch Tensor
            all_cursors, _ = hub.begin_all()
            p0_cursors = [c for c in all_cursors if c.phase == "phase_0ms"]
            out_tensor = torch.zeros((len(p0_cursors), 5), dtype=torch.float64)

            # Target fixed committed anchor
            start_anchor = 1700000000_000_000_000
            for c in p0_cursors:
                c.target_anchor_ns = start_anchor

            N_LOAD = 20_000
            # Warm up
            for _ in range(100):
                hub.load_sync(p0_cursors, out_tensor)

            start = time.perf_counter_ns()
            for _ in range(N_LOAD):
                hub.load_sync(p0_cursors, out_tensor)
            elapsed_ns = time.perf_counter_ns() - start
            load_lat_ns = elapsed_ns / N_LOAD
            print(f"2. Zero-Copy Tensor Load (4 symbols x 5 feats): {load_lat_ns:.1f} ns ({load_lat_ns / 1000.0:.3f} µs) per frame")

            # 3. Microsecond Spin & Jitter Test
            N_JITTER = 1000
            sleep_delays = []
            for _ in range(N_JITTER):
                target_sleep = 0.0005  # 500 µs
                t0 = time.perf_counter()
                time.sleep(target_sleep)
                t1 = time.perf_counter()
                sleep_delays.append((t1 - t0) * 1e6)  # µs

            mean_sleep = np.mean(sleep_delays)
            p99_sleep = np.percentile(sleep_delays, 99)
            print(f"3. OS Sleep Jitter (500 µs requested): mean={mean_sleep:.1f} µs, p99={p99_sleep:.1f} µs")

            print("=" * 60)
            print("BENCHMARK COMPLETED SUCCESSFULLY\n")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    run_benchmark()
