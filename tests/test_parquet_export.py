import os
import subprocess
import time
from pathlib import Path
import pyarrow.parquet as pq
import pytest

from tickhub import export_features

TICKHUB_BIN = Path(__file__).parent.parent / "bin" / "tickhub"
CONFIG_PATH = Path(__file__).parent.parent / "examples" / "config_datalake.yaml"
OUTPUT_PARQUET = Path("/tmp/test_export_auto/features.parquet")


def test_historical_replay_and_export():
    if OUTPUT_PARQUET.parent.exists():
        import shutil
        shutil.rmtree(OUTPUT_PARQUET.parent)
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    # 1. Start Go TickHub replay daemon
    proc = subprocess.Popen(
        [
            str(TICKHUB_BIN),
            "replay",
            "--config",
            str(CONFIG_PATH),
            "--datalake",
            "/mnt/wc/datalake",
            "--date",
            "2026-05-06",
            "--max-ticks",
            "10000",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Allow daemon to open files and SHM segment
    time.sleep(0.5)

    try:
        # 2. Run Python feature exporter
        res = export_features(
            config_path_or_dict=CONFIG_PATH,
            output_path=OUTPUT_PARQUET,
            max_frames=30,
            chunk_size=10,
        )

        assert res["total_frames"] == 30
        assert res["total_rows"] == 120  # 4 symbols total per frame across 2 phases * 30 frames
        assert res["overruns"] == 0
        assert Path(res["output_file"]).exists()

        # 3. Verify Parquet content
        table = pq.read_table(res["output_file"])
        assert len(table) == 120
        assert "anchor_ns" in table.column_names
        assert "phase" in table.column_names
        assert "symbol" in table.column_names
        assert "log_ret_1s" in table.column_names
        assert "spread_bps" in table.column_names

        # Verify no NaNs
        for col in ["log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"]:
            vals = table[col].to_numpy()
            assert not any(v != v for v in vals), f"NaN detected in {col}"

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
