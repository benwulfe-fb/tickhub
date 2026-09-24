#!/usr/bin/env python3
"""Reference execution engine client consuming 1Hz market data from POSIX SHM.

Simulates downstream quantitative model execution (e.g. ccm-live on VM or ccm-paper on WSL).
Attaches to /dev/shm/<name>, loops on 1Hz anchors, reads SeqLock snapshots and feature
matrices, and outputs structured JSONL telemetry tagged with anchor_ns.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from typing import Optional

import numpy as np

from tickhub.shm import TickHubReader


def run_engine(
    config_path: str,
    shm_name: str,
    role: str,
    duration_s: float = 20.0,
    output_path: Optional[str] = None,
) -> None:
    stop_event = False

    def handle_sig(sig, frame):
        nonlocal stop_event
        stop_event = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    out_file = None
    if output_path:
        out_file = open(output_path, "w", encoding="utf-8")

    sys.stdout.write(f"[{role.upper()}] Starting engine client against /dev/shm/{shm_name} (PID: {os.getpid()})\n")
    sys.stdout.flush()

    # Retry opening SHM until producer creates it
    t0 = time.time()
    reader = None
    while not stop_event and (time.time() - t0) < 15.0:
        try:
            reader = TickHubReader(config_path_or_dict=config_path, shm_path_override=shm_name)
            break
        except FileNotFoundError:
            time.sleep(0.2)

    if reader is None:
        sys.stderr.write(f"[{role.upper()}] Fatal: could not open /dev/shm/{shm_name} within 15s\n")
        sys.exit(1)

    with reader:
        cursors, _ = reader.begin_all(history_steps=0)
        n_symbols = len(cursors)
        if n_symbols == 0:
            sys.stderr.write(f"[{role.upper()}] Warning: zero symbols configured in SHM\n")

        feat_matrix = np.zeros((n_symbols, 5), dtype=np.float64)
        symbols = reader.symbols
        start_time = time.time()
        anchors_read = 0

        while not stop_event and (time.time() - start_time) < duration_s:
            t_load_0 = time.perf_counter_ns()
            try:
                failed = reader.load_sync(cursors, feat_matrix, timeout_ms=1200.0)
            except Exception as e:
                sys.stderr.write(f"[{role.upper()}] Exception in load_sync: {type(e).__name__}: {e}\n")
                sys.stderr.flush()
                time.sleep(0.05)
                continue

            load_time_us = (time.perf_counter_ns() - t_load_0) / 1000.0
            anchor_ns = cursors[0].target_anchor_ns if cursors else 0
            wall_ns = time.time_ns()

            if failed:
                sys.stderr.write(f"[{role.upper()}] Timed out on {len(failed)} symbols @ anchor {anchor_ns}: {failed[:5]}\n")
                sys.stderr.flush()

            # Read SeqLock snapshots for all symbols
            snapshots_dict = {}
            for sym in symbols:
                try:
                    s = reader.snapshot(sym)
                    snapshots_dict[sym] = {
                        "bid": s["bid_px"],
                        "ask": s["ask_px"],
                        "mid": s["midprice"],
                        "last": s["last_trade_px"],
                        "spread": s["spread"],
                        "size": s["last_trade_sz"],
                    }
                except (KeyError, RuntimeError):
                    pass

            first_sym = symbols[0] if symbols else ""
            sample_snap = snapshots_dict.get(first_sym, {})

            record = {
                "role": role,
                "anchor_ns": anchor_ns,
                "local_wall_ns": wall_ns,
                "n_symbols": n_symbols,
                "load_time_us": round(load_time_us, 2),
                "committed_frames": reader.committed_frames if hasattr(reader, "committed_frames") else 0,
                "sample_symbol": first_sym,
                "sample_mid": sample_snap.get("mid", 0.0),
                "sample_bid": sample_snap.get("bid", 0.0),
                "sample_ask": sample_snap.get("ask", 0.0),
                "sample_last": sample_snap.get("last", 0.0),
                "sample_spread": sample_snap.get("spread", 0.0),
                "snapshots": snapshots_dict,
                "features": feat_matrix.tolist(),
                "failed_symbols": failed,
            }

            line = json.dumps(record)
            if out_file:
                out_file.write(line + "\n")
                out_file.flush()

            # Emit log every frame
            sys.stdout.write(
                f"[{role.upper()}] Anchor {anchor_ns} | load: {load_time_us:.1f}µs | "
                f"{first_sym} mid: {sample_snap.get('mid', 0.0):.2f}, spread: {sample_snap.get('spread', 0.0):.4f}\n"
            )
            sys.stdout.flush()

            reader.next(cursors)
            anchors_read += 1

    if out_file:
        out_file.close()

    sys.stdout.write(f"[{role.upper()}] Completed session: {anchors_read} anchors read in {time.time() - start_time:.2f}s\n")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="TickHub Reference Engine Client")
    parser.add_argument("--config", default="config/live_72.yaml", help="Path to config yaml")
    parser.add_argument("--shm", default="tickhub_live", help="SHM segment name")
    parser.add_argument("--role", default="prod", choices=["prod", "paper"], help="Role identifier")
    parser.add_argument("--duration", type=float, default=20.0, help="Run duration in seconds")
    parser.add_argument("--output", default=None, help="JSONL output path")
    args = parser.parse_args()

    run_engine(args.config, args.shm, args.role, args.duration, args.output)


if __name__ == "__main__":
    main()
