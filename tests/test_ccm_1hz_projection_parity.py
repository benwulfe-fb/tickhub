#!/usr/bin/env python3
"""
test_ccm_1hz_projection_parity.py — Verifies bit-identical 1Hz primitive and MetricsEngine
parity between TickHub 44-slot SHM (`TickHubReader.load_window_1hz`) and CCM's Python/Numba
`project_to_1hz(..., window_rooted=True)` across 0ms and 500ms phase anchors.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from tickhub import TickHubReader
from tickhub.shm import _CCM_INT64_KEYS, _CCM_NAN_KEYS, _CCM_ZERO_KEYS

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKHUB_BIN = REPO_ROOT / "bin" / "tickhub"
CONFIG_LIVE_72 = REPO_ROOT / "config" / "live_72.yaml"
DATALAKE_DIR = Path("/mnt/wc/datalake")
CCM_SRC_DIR = Path("/mnt/wc/src")
DATE_STR = "2026-08-03"
SHM_NAME = "tickhub_ccm_parity_test"
SHM_FILE = f"/dev/shm/{SHM_NAME}"
NS_PER_S = 1_000_000_000


def _load_arena_from_parquet(symbol: str, date_str: str, max_sip_ns: int):
    """Loads raw quotes and trades up to max_sip_ns (inclusive, matching TickStoreImpl.snapshot)."""
    from ccm.marketdata.projection import TickArena

    day_dir = DATALAKE_DIR / date_str / symbol[0].upper()
    q_path = day_dir / f"{symbol}.quotes.parquet"
    t_path = day_dir / f"{symbol}.trades.parquet"

    q_tbl = pq.read_table(q_path)
    t_tbl = pq.read_table(t_path)

    q_sip_all = q_tbl["sip_timestamp"].to_numpy()
    t_sip_all = t_tbl["sip_timestamp"].to_numpy()

    q_cut = int(np.searchsorted(q_sip_all, max_sip_ns, side="right"))
    t_cut = int(np.searchsorted(t_sip_all, max_sip_ns, side="right"))

    q_bid_raw = q_tbl["bid_price"].to_numpy()[:q_cut]
    q_ask_raw = q_tbl["ask_price"].to_numpy()[:q_cut]
    q_valid = (q_bid_raw > 0.0) & (q_ask_raw > 0.0)
    q_end = int(np.sum(q_valid))

    t_px_raw = t_tbl["price"].to_numpy()[:t_cut]
    t_valid = t_px_raw > 0.0
    t_end = int(np.sum(t_valid))

    arena = TickArena(size=max(q_end + 16, t_end + 16, 1024))
    arena.q_bid[:q_end] = q_bid_raw[q_valid]
    arena.q_ask[:q_end] = q_ask_raw[q_valid]
    arena.q_szb[:q_end] = q_tbl["bid_size"].to_numpy()[:q_cut][q_valid]
    arena.q_sza[:q_end] = q_tbl["ask_size"].to_numpy()[:q_cut][q_valid]
    arena.q_sip[:q_end] = q_sip_all[:q_cut][q_valid]
    arena.q_part[:q_end] = q_tbl["participant_timestamp"].to_numpy()[:q_cut][q_valid]
    arena.q_exch[:q_end] = q_tbl["bid_exchange"].to_numpy()[:q_cut][q_valid].astype(np.int16)
    arena.q_exch_ask[:q_end] = q_tbl["ask_exchange"].to_numpy()[:q_cut][q_valid].astype(np.int16)
    arena.q_end = q_end

    arena.t_px[:t_end] = t_px_raw[t_valid]
    arena.t_sz[:t_end] = t_tbl["size"].to_numpy()[:t_cut][t_valid]
    arena.t_sip[:t_end] = t_sip_all[:t_cut][t_valid]
    arena.t_part[:t_end] = t_tbl["participant_timestamp"].to_numpy()[:t_cut][t_valid]
    arena.t_exch[:t_end] = t_tbl["exchange"].to_numpy()[:t_cut][t_valid].astype(np.int16)
    if "trf_id" in t_tbl.column_names:
        arena.t_trf_id[:t_end] = t_tbl["trf_id"].to_numpy(zero_copy_only=False)[:t_cut][t_valid].astype(np.float64)
    arena.t_end = t_end

    return arena.get_as_dict()


@pytest.fixture
def ccm_worker():
    if os.path.exists(SHM_FILE):
        try:
            os.unlink(SHM_FILE)
        except OSError:
            pass

    proc = subprocess.Popen(
        [
            str(TICKHUB_BIN),
            "worker",
            "--config", str(CONFIG_LIVE_72),
            "--datalake", str(DATALAKE_DIR),
            "--shm-name", SHM_NAME,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    deadline = time.time() + 5.0
    while not os.path.exists(SHM_FILE) and time.time() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(f"Worker exited prematurely:\n{out}\n{err}")
        time.sleep(0.02)

    try:
        yield proc
    finally:
        if proc.poll() is None:
            try:
                if os.path.exists(SHM_FILE):
                    with TickHubReader(CONFIG_LIVE_72, shm_path_override=SHM_NAME) as r:
                        r.shutdown_worker()
                    proc.wait(timeout=1.5)
            except Exception:
                pass
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
        if os.path.exists(SHM_FILE):
            try:
                os.unlink(SHM_FILE)
            except OSError:
                pass


def test_ccm_1hz_projection_and_metrics_parity(ccm_worker):
    if not CCM_SRC_DIR.exists() or not (DATALAKE_DIR / DATE_STR).exists():
        pytest.skip("CCM source or datalake not mounted")

    if str(CCM_SRC_DIR) not in sys.path:
        sys.path.insert(0, str(CCM_SRC_DIR))

    from ccm.marketdata.projection import project_to_1hz
    from ccm.metrics.engine.engine import CROSS_ASSET_ALLOWED_SET, DAGContextFactory, MetricsEngine
    from ccm.metrics.registry import MetricsRegistry

    registry = MetricsRegistry.get_universal_registry()
    span_s = 360

    # 2026-08-03 10:00:00 ET = 14:00:00 UTC = 1785765600000000000 ns
    base_anchor_0ms = 1785765600000000000
    base_anchor_500ms = base_anchor_0ms + 500_000_000

    test_symbols = ["SPY", "QQQ", "ALAB", "ANET"]
    core_features = ["mid", "spread_bps", "vwap_10s", "quote_rate_10s", "nbbo_staleness_ms"]
    available_features = [f for f in core_features if f in registry]

    with TickHubReader(CONFIG_LIVE_72, shm_path_override=SHM_NAME) as reader:
        for anchor_ns in (base_anchor_0ms, base_anchor_500ms):
            anchor_floor_ns = (anchor_ns // NS_PER_S) * NS_PER_S
            s_start = anchor_floor_ns - span_s * NS_PER_S
            s_end = anchor_floor_ns + NS_PER_S

            shm_projs = {}
            py_projs = {}

            for sym in test_symbols:
                n_frames, _ = reader.request_chunk(sym, DATE_STR, s_start, anchor_ns, timeout_s=5.0)
                assert n_frames >= span_s, f"Expected >= {span_s} frames for {sym}, got {n_frames}"

                proj_shm = reader.load_window_1hz(sym, anchor_ns, span_s=span_s)
                assert proj_shm is not None, f"load_window_1hz returned None for {sym} @ {anchor_ns}"

                raw_dict = _load_arena_from_parquet(sym, DATE_STR, anchor_ns)
                proj_py = project_to_1hz(
                    raw_dict,
                    s_start=s_start,
                    s_end=s_end,
                    window_rooted=True,
                )

                # 1. Verify anchor_timestamp, quote_idx, trade_idx across all 361 rows (0..span_s)
                np.testing.assert_array_equal(
                    proj_shm["anchor_timestamp"],
                    proj_py["anchor_timestamp"][: span_s + 1],
                    err_msg=f"{sym} @ {anchor_ns}: anchor_timestamp mismatch",
                )
                np.testing.assert_array_equal(
                    proj_shm["quote_idx"],
                    proj_py["quote_idx"][: span_s + 1],
                    err_msg=f"{sym} @ {anchor_ns}: quote_idx mismatch",
                )
                np.testing.assert_array_equal(
                    proj_shm["trade_idx"],
                    proj_py["trade_idx"][: span_s + 1],
                    err_msg=f"{sym} @ {anchor_ns}: trade_idx mismatch",
                )

                # 2. Verify all 34 1Hz primitives across the 360-second window [:span_s]
                max_abs_err = 0.0
                for k in _CCM_ZERO_KEYS + _CCM_NAN_KEYS:
                    a_shm = proj_shm[k][:span_s]
                    a_py = proj_py[k][:span_s]
                    np.testing.assert_allclose(
                        a_shm,
                        a_py,
                        rtol=1e-12,
                        atol=1e-7,
                        equal_nan=True,
                        err_msg=f"{sym} @ {anchor_ns}: primitive '{k}' [:span_s] mismatch",
                    )
                    valid_mask = ~(np.isnan(a_shm) & np.isnan(a_py))
                    if np.any(valid_mask):
                        err = float(np.max(np.abs(a_shm[valid_mask] - a_py[valid_mask])))
                        if err > max_abs_err:
                            max_abs_err = err

                for k in _CCM_INT64_KEYS:
                    np.testing.assert_array_equal(
                        proj_shm[k][:span_s],
                        proj_py[k][:span_s],
                        err_msg=f"{sym} @ {anchor_ns}: int64 timestamp '{k}' [:span_s] mismatch",
                    )

                # 3. Verify terminal row [span_s] point-in-time state as of anchor_ns
                for term_k in ("last_bid_px", "last_ask_px", "last_bid_sz", "last_ask_sz", "last_trade_px", "sip_timestamp"):
                    np.testing.assert_allclose(
                        proj_shm[term_k][span_s],
                        proj_py[term_k][span_s],
                        rtol=1e-12,
                        atol=1e-7,
                        equal_nan=True,
                        err_msg=f"{sym} @ {anchor_ns}: terminal '{term_k}' [span_s] mismatch",
                    )

                pub_lat_ns = int(reader._header.anchor_publish_latency_ns)
                assert 0 < pub_lat_ns < 15_000_000, (
                    f"{sym} @ {anchor_ns}: 1Hz publish latency SLO violated ({pub_lat_ns} ns >= 15,000,000 ns)"
                )
                assert int(reader._header.last_read_anchor_ns) > 0, (
                    f"{sym} @ {anchor_ns}: expected last_read_anchor_ns > 0 in replay mode"
                )
                print(
                    f"[TELEMETRY] {sym} @ {anchor_ns}: max_abs_err={max_abs_err:.3e}, "
                    f"publish_latency_ns={pub_lat_ns} ({pub_lat_ns / 1e6:.3f} ms)"
                )

                shm_projs[sym] = proj_shm
                py_projs[sym] = proj_py

            # 4. Verify downstream MetricsEngine parity on primary symbols with SPY/QQQ cross-assets
            if available_features:
                DAGContextFactory._cache.clear()
                for ca in ("SPY", "QQQ"):
                    MetricsEngine().execute(
                        market_data={ca: shm_projs[ca]},
                        registry=registry,
                        sample_len=span_s,
                        anchor_timestamp=anchor_ns,
                        is_cross_asset=True,
                        features=sorted(CROSS_ASSET_ALLOWED_SET),
                    )
                shm_ctx = MetricsEngine().execute(
                    market_data={"ALAB": shm_projs["ALAB"]},
                    registry=registry,
                    sample_len=span_s,
                    anchor_timestamp=anchor_ns,
                    features=available_features,
                )["ALAB"]
                shm_arrays = {k: np.array(v, copy=True) for k, v in shm_ctx.arrays.items()}

                DAGContextFactory._cache.clear()
                for ca in ("SPY", "QQQ"):
                    MetricsEngine().execute(
                        market_data={ca: py_projs[ca]},
                        registry=registry,
                        sample_len=span_s,
                        anchor_timestamp=anchor_ns,
                        is_cross_asset=True,
                        features=sorted(CROSS_ASSET_ALLOWED_SET),
                    )
                py_ctx = MetricsEngine().execute(
                    market_data={"ALAB": py_projs["ALAB"]},
                    registry=registry,
                    sample_len=span_s,
                    anchor_timestamp=anchor_ns,
                    features=available_features,
                )["ALAB"]

                for feat_k in available_features:
                    if feat_k in shm_arrays and feat_k in py_ctx.arrays:
                        np.testing.assert_allclose(
                            shm_arrays[feat_k],
                            np.asarray(py_ctx.arrays[feat_k]),
                            rtol=1e-12,
                            atol=1e-7,
                            equal_nan=True,
                            err_msg=f"ALAB @ {anchor_ns}: MetricsEngine feature '{feat_k}' mismatch",
                        )
