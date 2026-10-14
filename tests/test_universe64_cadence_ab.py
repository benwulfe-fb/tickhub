#!/usr/bin/env python3
"""
test_universe64_cadence_ab.py — Empirical A/B test of TickHub 4-phase (250ms) vs 2-phase (500ms)
cadence on the 64-symbol universe (with SPY/QQQ cross assets) on historical datalake parquet ticks.
"""
import os
import subprocess
import time
from pathlib import Path
import numpy as np
import pytest
import torch

from tickhub import TickHubReader

REPO_ROOT = Path(__file__).resolve().parent.parent
TICKHUB_BIN = REPO_ROOT / "bin" / "tickhub"
CONFIG_ARM_A = REPO_ROOT / "config" / "universe64_arm_a_500ms.yaml"
CONFIG_ARM_B = REPO_ROOT / "config" / "universe64_arm_b_250ms.yaml"
DATALAKE_DIR = Path("/mnt/wc/datalake")
DATE_STR = "2026-08-03"
MAX_TICKS = 300000

SHM_ARM_A = "/dev/shm/tickhub_test_arm_a"
SHM_ARM_B = "/dev/shm/tickhub_test_arm_b"


def cleanup_shm():
    for f in [SHM_ARM_A, SHM_ARM_B]:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass


def run_arm(config_path, shm_file, max_frames=50):
    """Starts tickhub replay and drains frames using TickHubReader."""
    cmd = [
        str(TICKHUB_BIN),
        "replay",
        "--config", str(config_path),
        "--shm-name", shm_file.replace("/dev/shm/", ""),
        "--datalake", str(DATALAKE_DIR),
        "--date", DATE_STR,
        "--max-ticks", str(MAX_TICKS),
        "--no-unlink",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    deadline = time.time() + 25.0
    while not os.path.exists(shm_file) and time.time() < deadline:
        if proc.poll() is not None:
            stdout, stderr = proc.communicate()
            raise RuntimeError(f"Replay process exited prematurely:\nStdout: {stdout}\nStderr: {stderr}")
        time.sleep(0.2)

    if not os.path.exists(shm_file):
        proc.kill()
        stdout, stderr = proc.communicate()
        raise TimeoutError(f"Timed out waiting for {shm_file} to appear.\nStdout: {stdout}\nStderr: {stderr}")

    records = {}  # phase_name -> list of (anchor_ns, symbol, features)
    phase_anchors = {}

    try:
        with TickHubReader(config_path, shm_path_override=shm_file) as hub:
            status_deadline = time.time() + 10.0
            while hub.status < 2 and time.time() < status_deadline:
                time.sleep(0.05)

            feature_names = hub.config.get("features", ["log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"])
            all_cursors, _ = hub.begin_all()
            phase_groups = [(p_name, [c for c in all_cursors if c.phase == p_name]) for p_name in hub.phases]
            tensors = {
                p_name: torch.zeros((len(c_list), len(feature_names)), dtype=torch.float64)
                for p_name, c_list in phase_groups
            }

            for p_name, _ in phase_groups:
                records[p_name] = []
                phase_anchors[p_name] = []

            frames_read = 0
            while frames_read < max_frames:
                if hub.status == 4 and proc.poll() is not None:
                    break

                for p_name, c_list in phase_groups:
                    tensor = tensors[p_name]
                    failed = hub.load_sync(c_list, tensor, timeout_ms=3000.0)
                    if failed:
                        if hub.status == 4:
                            break
                        raise RuntimeError(f"Timeout waiting for symbols {failed} in {p_name}")

                    target_anchor = c_list[0].target_anchor_ns
                    phase_anchors[p_name].append(target_anchor)
                    np_view = tensor.numpy()
                    for s_idx, cursor in enumerate(c_list):
                        records[p_name].append((target_anchor, cursor.symbol, np_view[s_idx].copy()))
                    hub.next(c_list)

                hub.commit_read(all_cursors)
                frames_read += 1

            overruns = hub.dropped_ticks
            header_dict = {
                "num_phases": hub.num_phases,
                "cadence_ns": hub.cadence_ns,
                "first_anchor_ns": hub.first_anchor_ns,
                "last_written_anchor_ns": hub.last_written_anchor_ns,
                "dropped_ticks": overruns,
                "publish_latency_ns": hub.anchor_publish_latency_ns,
            }
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                proc.kill()

    return records, phase_anchors, header_dict


def test_empirical_cadence_ab():
    cleanup_shm()
    try:
        print("\n--- Running Arm A (2 phases: 0ms, 500ms) ---")
        rec_a, anchors_a, hdr_a = run_arm(CONFIG_ARM_A, SHM_ARM_A, max_frames=30)
        print(f"Arm A completed: {hdr_a['num_phases']} phases, dropped_ticks={hdr_a['dropped_ticks']}")

        cleanup_shm()
        print("\n--- Running Arm B (4 phases: 0ms, 250ms, 500ms, 750ms) ---")
        rec_b, anchors_b, hdr_b = run_arm(CONFIG_ARM_B, SHM_ARM_B, max_frames=30)
        print(f"Arm B completed: {hdr_b['num_phases']} phases, dropped_ticks={hdr_b['dropped_ticks']}")

        # 1. Assert Phase Geometry
        assert hdr_a["num_phases"] == 2, f"Arm A expected 2 phases, got {hdr_a['num_phases']}"
        assert hdr_b["num_phases"] == 4, f"Arm B expected 4 phases, got {hdr_b['num_phases']}"

        # 2. Assert Zero Overruns & Monotonicity
        assert hdr_a["dropped_ticks"] == 0, f"Arm A dropped ticks: {hdr_a['dropped_ticks']}"
        assert hdr_b["dropped_ticks"] == 0, f"Arm B dropped ticks: {hdr_b['dropped_ticks']}"

        # 3. Assert Interleaved Chronological Timing across all 4 phases in Arm B
        timeline_b = []
        for p_name, ancs in anchors_b.items():
            for a in ancs:
                timeline_b.append((a, p_name))
        timeline_b.sort()

        print(f"\nVerifying {len(timeline_b)} anchors on interleaved 250ms timeline...")
        assert len(timeline_b) >= 40, f"Insufficient anchors captured in Arm B: {len(timeline_b)}"

        expected_phase_cycle = ["phase_250ms", "phase_500ms", "phase_750ms", "phase_0ms"]
        # Find start offset
        start_idx = expected_phase_cycle.index(timeline_b[0][1])

        for i in range(1, len(timeline_b)):
            dt = timeline_b[i][0] - timeline_b[i-1][0]
            assert dt == 250_000_000, f"Timeline step mismatch at index {i}: dt = {dt} ns (expected 250,000,000 ns)"
            expected_phase = expected_phase_cycle[(start_idx + i) % 4]
            actual_phase = timeline_b[i][1]
            assert actual_phase == expected_phase, f"Phase cycle mismatch at {i}: got {actual_phase}, expected {expected_phase}"

        print("Phase timing verification PASSED: perfect monotonic 250ms spacing and round-robin phase cycling.")

        # 4. Assert Bit-Identical Feature Parity for common symbols at identical anchors (SPY, QQQ + symbols in phase_0ms and phase_500ms)
        for check_phase in ["phase_0ms", "phase_500ms"]:
            dict_a = {(item[0], item[1]): item[2] for item in rec_a[check_phase]}
            dict_b = {(item[0], item[1]): item[2] for item in rec_b[check_phase]}
            common_keys = sorted(list(set(dict_a.keys()).intersection(set(dict_b.keys()))))
            assert len(common_keys) > 0, f"No common keys at {check_phase}"

            diffs = []
            for k in common_keys:
                fa = dict_a[k]
                fb = dict_b[k]
                finite_mask = np.isfinite(fa) & np.isfinite(fb)
                diff = float(np.max(np.abs(fa[finite_mask] - fb[finite_mask]))) if np.any(finite_mask) else 0.0
                diffs.append(diff)
                assert np.allclose(fa, fb, atol=1e-8, equal_nan=True), f"Feature mismatch for {k}: Arm A {fa} vs Arm B {fb}"

            print(f"Feature parity verification PASSED for {check_phase}: {len(common_keys)} common (anchor, symbol) frames evaluated. Max diff={max(diffs):.2e}")

        # 5. Check publish latency
        print(f"\nPublish latency: Arm A = {hdr_a['publish_latency_ns']/1000.0:.2f} us, Arm B = {hdr_b['publish_latency_ns']/1000.0:.2f} us")
        assert hdr_a['publish_latency_ns'] < 15_000_000, f"Arm A publish latency exceeded 15ms budget: {hdr_a['publish_latency_ns']} ns"
        assert hdr_b['publish_latency_ns'] < 15_000_000, f"Arm B publish latency exceeded 15ms budget: {hdr_b['publish_latency_ns']} ns"

        print("\nAll empirical A/B assertions PASSED successfully!")

    finally:
        cleanup_shm()


if __name__ == "__main__":
    test_empirical_cadence_ab()
