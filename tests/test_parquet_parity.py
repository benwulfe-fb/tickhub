import os
import shutil
import subprocess
import time
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from tickhub import export_features

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKHUB_BIN = REPO_ROOT / "bin" / "tickhub"
CONFIG_PATH = REPO_ROOT / "tests" / "fixtures" / "golden" / "config_golden.yaml"
DATALAKE_DIR = REPO_ROOT / "tests" / "fixtures" / "golden" / "datalake"
OUTPUT_PARQUET = Path("/tmp/test_parquet_parity/dash_1hz.parquet")
SHM_FILE = "/dev/shm/tickhub_golden_test"


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure clean test environment before and after parity verification."""
    if os.path.exists(SHM_FILE):
        try:
            os.unlink(SHM_FILE)
        except OSError:
            pass
    if OUTPUT_PARQUET.parent.exists():
        shutil.rmtree(OUTPUT_PARQUET.parent)
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    try:
        yield
    finally:
        if os.path.exists(SHM_FILE):
            try:
                os.unlink(SHM_FILE)
            except OSError:
                pass
        if OUTPUT_PARQUET.parent.exists():
            shutil.rmtree(OUTPUT_PARQUET.parent)


def test_bitwise_parquet_parity_with_legacy_engine():
    assert TICKHUB_BIN.exists(), f"tickhub binary missing at {TICKHUB_BIN}. Run `go build -o bin/tickhub ./cmd/tickhub`."

    # 1. Execute TickHub replay on golden fixture with --no-unlink
    t0 = time.perf_counter()
    subprocess.run(
        [
            str(TICKHUB_BIN),
            "replay",
            "--config", str(CONFIG_PATH),
            "--datalake", str(DATALAKE_DIR),
            "--date", "2026-05-06",
            "--no-unlink",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    # 2. Export 1Hz feature Parquet via Python TickHub exporter
    res = export_features(
        config_path_or_dict=CONFIG_PATH,
        output_path=OUTPUT_PARQUET,
        chunk_size=100,
    )
    t_export = time.perf_counter() - t0
    assert res["total_frames"] == 59, f"Expected 59 frames, got {res['total_frames']}"

    table = pq.read_table(OUTPUT_PARQUET)
    assert len(table) == 59

    # 3. Load precomputed golden reference projection
    ref = np.load(REPO_ROOT / "tests" / "fixtures" / "golden" / "reference_1hz.npz")

    # 4. Bitwise Parity Assertions
    # A. Volume Parity: vol_1s == (buy_volume + sell_volume)
    tickhub_vol = table["vol_1s"].to_numpy()
    legacy_vol = (ref["buy_volume"] + ref["sell_volume"])[:len(tickhub_vol)]
    vol_delta = np.max(np.abs(tickhub_vol - legacy_vol))
    np.testing.assert_equal(
        tickhub_vol,
        legacy_vol,
        err_msg=f"Volume parity failure: max delta={vol_delta}",
    )

    # B. Return Parity: log_ret_1s == ln(P_T / P_{T-1})
    tickhub_ret = table["log_ret_1s"].to_numpy()
    legacy_px = ref["last_trade_px"][:len(tickhub_vol)]
    p_prev = np.empty_like(legacy_px)
    p_prev[0] = legacy_px[0]
    p_prev[1:] = legacy_px[:-1]
    calc_ret = np.zeros_like(legacy_px)
    calc_ret[1:] = np.log(legacy_px[1:] / p_prev[1:])
    # Verify frame 0 cold-start return is explicitly 0.0 in both engines
    assert tickhub_ret[0] == 0.0, f"Expected frame 0 return 0.0, got {tickhub_ret[0]}"
    assert calc_ret[0] == 0.0, f"Expected legacy frame 0 return 0.0, got {calc_ret[0]}"
    ret_delta = np.max(np.abs(tickhub_ret - calc_ret))
    np.testing.assert_equal(
        tickhub_ret,
        calc_ret,
        err_msg=f"Log return parity failure: max delta={ret_delta}",
    )

    # C. Spread Parity: spread_bps == ((ask - bid) / px) * 10000
    # In this isolated test fixture, no ticks exist prior to 09:31:00, and the first quote
    # arrives at frame 6 (SIP timestamp 1778074266044853504). Pre-quote frames 0-5 have
    # unseeded BBO where TickHub CloseBar defaults to 1.0 bps.
    # From arrival of first quote onward (all 53 post-quote frames 6..58), assert 100% bitwise parity.
    tickhub_spread = table["spread_bps"].to_numpy()
    assert (tickhub_spread[:6] == 1.0).all(), "Expected 1.0 default spread for unseeded pre-quote frames 0-5"
    legacy_bid = ref["last_bid_px"][:len(tickhub_vol)][6:]
    legacy_ask = ref["last_ask_px"][:len(tickhub_vol)][6:]
    calc_spread = ((legacy_ask - legacy_bid) / legacy_px[6:]) * 10000.0
    spread_delta = np.max(np.abs(tickhub_spread[6:] - calc_spread))
    np.testing.assert_equal(
        tickhub_spread[6:],
        calc_spread,
        err_msg=f"Spread parity failure: max delta={spread_delta}",
    )

    # Diagnostic Telemetry
    print(
        f"\n[PARITY TELEMETRY] DASH 60s (59 frames in {t_export*1000:.1f}ms): "
        f"Volume delta={vol_delta:.8f}, Returns delta={ret_delta:.8f}, Spread delta={spread_delta:.8f} "
        f"(100% bitwise parity)"
    )
