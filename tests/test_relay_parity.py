import os
import signal
import socket
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

PRIMARY_SHM = "/dev/shm/tickhub_relay_primary"
REPLICA_SHM = "/dev/shm/tickhub_relay_replica"


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def clean_shm():
    for f in (PRIMARY_SHM, REPLICA_SHM):
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass
    try:
        yield
    finally:
        for f in (PRIMARY_SHM, REPLICA_SHM):
            if os.path.exists(f):
                try:
                    os.unlink(f)
                except OSError:
                    pass


def test_relay_offline_datalake_bitwise_parity():
    assert TICKHUB_BIN.exists(), f"tickhub binary not found at {TICKHUB_BIN}"
    assert GOLDEN_METRICS_PATH.exists(), f"Golden metrics missing: {GOLDEN_METRICS_PATH}"
    assert GOLDEN_ANCHORS_PATH.exists(), f"Golden anchors missing: {GOLDEN_ANCHORS_PATH}"

    golden_metrics = np.load(GOLDEN_METRICS_PATH)
    golden_anchors = np.load(GOLDEN_ANCHORS_PATH)

    # 1. Replay golden dataset into primary SHM
    replay_cmd = [
        str(TICKHUB_BIN),
        "replay",
        "--config", str(CONFIG_PATH),
        "--datalake", str(DATALAKE_DIR),
        "--date", "2026-05-06",
        "--shm-name", "tickhub_relay_primary",
        "--no-unlink",
    ]
    subprocess.run(replay_cmd, check=True, capture_output=True, text=True)
    assert os.path.exists(PRIMARY_SHM), f"Primary SHM segment not created at {PRIMARY_SHM}"

    # 2. Start relay-server on dynamic loopback port
    port = get_free_port()
    addr = f"127.0.0.1:{port}"

    srv_cmd = [
        str(TICKHUB_BIN),
        "relay-server",
        "--shm", "tickhub_relay_primary",
        "--addr", addr,
        "--from-start",
    ]
    srv_proc = subprocess.Popen(srv_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Allow server to bind
    time.sleep(0.05)

    # 3. Start relay-client to replicate into tickhub_relay_replica
    cli_cmd = [
        str(TICKHUB_BIN),
        "relay-client",
        "--addr", addr,
        "--shm", "tickhub_relay_replica",
        "--no-unlink",
    ]
    t_relay_start = time.perf_counter()
    cli_proc = subprocess.Popen(cli_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        # 4. Wait for replication to complete
        deadline = time.time() + 5.0
        replica_ready = False

        with TickHubReader(CONFIG_PATH, shm_path_override="tickhub_relay_primary") as prim_reader:
            target_last_anchor = prim_reader.last_written_anchor_ns
            assert target_last_anchor > 0, "Primary has no committed anchors"

            while time.time() < deadline:
                if os.path.exists(REPLICA_SHM):
                    try:
                        with TickHubReader(CONFIG_PATH, shm_path_override="tickhub_relay_replica") as repl_reader:
                            if repl_reader.last_written_anchor_ns == target_last_anchor:
                                replica_ready = True
                                break
                    except Exception:
                        pass
                time.sleep(0.01)

            t_relay_end = time.perf_counter()
            relay_duration_ms = (t_relay_end - t_relay_start) * 1000.0

            assert replica_ready, (
                f"Relay client failed to replicate up to anchor {target_last_anchor} within timeout"
            )

            # 5. Extract and verify metrics between Primary and Replica
            with TickHubReader(CONFIG_PATH, shm_path_override="tickhub_relay_replica") as repl_reader:
                assert repl_reader.first_anchor_ns == prim_reader.first_anchor_ns, (
                    f"First anchor mismatch: {repl_reader.first_anchor_ns} vs {prim_reader.first_anchor_ns}"
                )
                assert repl_reader.last_written_anchor_ns == prim_reader.last_written_anchor_ns, (
                    f"Last anchor mismatch: {repl_reader.last_written_anchor_ns} vs {prim_reader.last_written_anchor_ns}"
                )

                num_frames = int((prim_reader.last_written_anchor_ns - prim_reader.first_anchor_ns) // prim_reader.cadence_ns) + 1

                prim_cursors, _ = prim_reader.begin_all()
                repl_cursors, _ = repl_reader.begin_all()

                prim_records = []
                repl_records = []
                prim_anchors = []
                repl_anchors = []

                num_syms = len(prim_cursors)
                mat_prim = np.zeros((num_syms, 5), dtype=np.float64)
                mat_repl = np.zeros((num_syms, 5), dtype=np.float64)

                for i in range(num_frames):
                    prim_anchors.append(prim_cursors[0].target_anchor_ns)
                    repl_anchors.append(repl_cursors[0].target_anchor_ns)

                    failed_p = prim_reader.load_sync(prim_cursors, mat_prim)
                    failed_r = repl_reader.load_sync(repl_cursors, mat_repl)
                    assert not failed_p, f"Primary frame {i} load failed: {failed_p}"
                    assert not failed_r, f"Replica frame {i} load failed: {failed_r}"

                    prim_records.append(mat_prim.copy())
                    repl_records.append(mat_repl.copy())

                    prim_reader.next(prim_cursors)
                    repl_reader.next(repl_cursors)

                actual_prim_metrics = np.array(prim_records, dtype=np.float64)
                actual_repl_metrics = np.array(repl_records, dtype=np.float64)
                actual_repl_anchors = np.array(repl_anchors, dtype=np.int64)

                # Strict Bitwise Identity Assertions across all symbols and features
                np.testing.assert_array_equal(
                    actual_repl_anchors,
                    golden_anchors,
                    err_msg="Replica anchors do not match golden reference!",
                )
                np.testing.assert_equal(
                    actual_repl_metrics,
                    actual_prim_metrics,
                    err_msg="Replica 1Hz metrics diverge from Primary SHM!",
                )
                np.testing.assert_equal(
                    actual_repl_metrics[:, 0, :],
                    golden_metrics,
                    err_msg="Replica 1Hz metrics diverge from Golden reference!",
                )

                # 6. Verify Top-of-Book Snapshots
                for sym in prim_reader.symbols:
                    prim_snap = prim_reader.snapshot(sym)
                    repl_snap = repl_reader.snapshot(sym)

                    assert repl_snap["bid_px"] == prim_snap["bid_px"], f"Snapshot bid_px mismatch for {sym}"
                    assert repl_snap["ask_px"] == prim_snap["ask_px"], f"Snapshot ask_px mismatch for {sym}"
                    assert repl_snap["midprice"] == prim_snap["midprice"], f"Snapshot midprice mismatch for {sym}"
                    assert repl_snap["spread"] == prim_snap["spread"], f"Snapshot spread mismatch for {sym}"
                    assert repl_snap["sip_timestamp_ns"] == prim_snap["sip_timestamp_ns"], f"Snapshot sip_ts mismatch for {sym}"

                max_diff = np.max(np.abs(actual_repl_metrics - actual_prim_metrics))
                latency_per_frame_us = (relay_duration_ms * 1000.0) / max(num_frames, 1)

                print(f"\n[RELAY TELEMETRY] Primary vs Replica: {max_diff:.8f} difference across {num_frames} frames.")
                print(f"[RELAY TELEMETRY] Snapshots: 100% bit-identical across all symbols.")
                print(f"[RELAY TELEMETRY] Total replication time: {relay_duration_ms:.2f} ms ({latency_per_frame_us:.2f} µs/bar)")

                # Performance budget: <= 500 µs per bar
                assert latency_per_frame_us <= 500.0, (
                    f"Relay latency budget exceeded: {latency_per_frame_us:.2f} µs/bar > 500.0 µs/bar"
                )

    finally:
        # Terminate client and server
        for proc in (cli_proc, srv_proc):
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
