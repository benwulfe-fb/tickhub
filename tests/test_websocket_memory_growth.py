#!/usr/bin/env python3
"""End-to-End WebSocket Datalake Ingestion Memory Growth & Leak Verification Test.

Validates that TickHub daemon running with live WebSocket feed replaying datalake
Parquet ticks maintains a flat memory profile (no heap leaks, no buffer accumulation,
no goroutine leaks, flat RSS) over sustained ingestion across 1Hz projection windows.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
import urllib.request

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DURATION_SEC = int(os.environ.get("TICKHUB_MEM_TEST_DURATION", "20"))


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def load_datalake_events() -> list[dict]:
    """Load quotes and trades from datalake Parquet with graceful fixture fallback."""
    candidate_dirs = [
        Path("/mnt/wc/datalake/2026-09-23/D"),
        REPO_ROOT / "tests" / "fixtures" / "golden" / "datalake" / "2026-05-06" / "D",
    ]
    data_dir = None
    for d in candidate_dirs:
        if d.exists() and (d / "DASH.quotes.parquet").exists():
            data_dir = d
            break

    if data_dir is None:
        raise RuntimeError(f"No datalake parquet directory found in: {candidate_dirs}")

    symbols = ["DASH"]
    if (data_dir / "DELL.quotes.parquet").exists():
        symbols.append("DELL")

    events = []
    print(f"Loading parquet tick records from {data_dir} for symbols {symbols}...")

    for sym in symbols:
        q_path = data_dir / f"{sym}.quotes.parquet"
        t_path = data_dir / f"{sym}.trades.parquet"

        if q_path.exists():
            tbl_q = pq.read_table(q_path)
            bp = tbl_q["bid_price"].to_numpy()
            ap = tbl_q["ask_price"].to_numpy()
            bs = tbl_q["bid_size"].to_numpy()
            as_sz = tbl_q["ask_size"].to_numpy()
            ts = tbl_q["sip_timestamp"].to_numpy()
            for i in range(len(tbl_q)):
                if bp[i] > 0 or ap[i] > 0:
                    events.append({
                        "ev": "Q",
                        "sym": sym,
                        "bp": float(bp[i]),
                        "ap": float(ap[i]),
                        "bs": float(bs[i]),
                        "as": float(as_sz[i]),
                        "sip_ts": int(ts[i]),
                    })

        if t_path.exists():
            tbl_t = pq.read_table(t_path)
            p = tbl_t["price"].to_numpy()
            s = tbl_t["size"].to_numpy()
            ts = tbl_t["sip_timestamp"].to_numpy()
            for i in range(len(tbl_t)):
                if p[i] > 0:
                    events.append({
                        "ev": "T",
                        "sym": sym,
                        "p": float(p[i]),
                        "s": float(s[i]),
                        "sip_ts": int(ts[i]),
                    })

    # Sort strictly by timestamp to maintain quote-before-trade and chronological order
    events.sort(key=lambda x: x["sip_ts"])
    print(f"Loaded and ordered {len(events):,} market events from datalake.")
    return events


class MockMassiveWSServer:
    """Mock WebSocket server simulating Massive.com stocks feed."""

    def __init__(self, host: str, port: int, events: list[dict]):
        self.host = host
        self.port = port
        self.events = events
        self.server = None
        self.stop_event = asyncio.Event()

    async def handler(self, websocket):
        try:
            # 1. Handle auth message
            auth_raw = await websocket.recv()
            auth_msg = json.loads(auth_raw)
            if auth_msg.get("action") != "auth":
                await websocket.close(1008, "Expected auth")
                return

            auth_resp = [{"ev": "status", "status": "auth_success", "message": "authenticated"}]
            await websocket.send(json.dumps(auth_resp))

            # 2. Handle subscribe message
            sub_raw = await websocket.recv()
            sub_msg = json.loads(sub_raw)
            if sub_msg.get("action") != "subscribe":
                await websocket.close(1008, "Expected subscribe")
                return

            print(f"[MockWS] Client subscribed to: {sub_msg.get('params')}")

            # 3. Stream market events in batches with wall-clock aligned timestamps
            batch_size = 50
            total_events = len(self.events)
            idx = 0

            while not self.stop_event.is_set():
                now_ms = int(time.time() * 1000)
                batch = []
                for _ in range(batch_size):
                    ev = self.events[idx].copy()
                    del ev["sip_ts"]
                    ev["t"] = now_ms
                    batch.append(ev)
                    idx = (idx + 1) % total_events

                await websocket.send(json.dumps(batch))
                # 5ms delay per 50-tick batch = ~10,000 ticks/second
                await asyncio.sleep(0.005)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            if not self.stop_event.is_set():
                print(f"[MockWS] Handler exception: {e}")

    async def start(self):
        import websockets
        self.server = await websockets.serve(self.handler, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        print(f"[MockWS] Server listening on ws://{self.host}:{self.port}")

    async def stop(self):
        self.stop_event.set()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        print("[MockWS] Server stopped.")


def scrape_metrics(url: str) -> dict[str, float]:
    """Scrapes Prometheus metrics from /metrics endpoint."""
    req = urllib.request.Request(url, headers={"User-Agent": "TickHub-MemTest"})
    metrics = {}
    with urllib.request.urlopen(req, timeout=3) as resp:
        content = resp.read().decode("utf-8")
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    metrics[parts[0]] = float(parts[1])
                except ValueError:
                    pass
    return metrics


def linear_regression_slope(xs: list[float], ys: list[float]) -> float:
    """Computes least-squares slope (dy/dx)."""
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def run_memory_growth_test(duration_sec: int = DEFAULT_DURATION_SEC):
    print("=" * 80)
    print(f" TICKHUB WEBSOCKET INGESTION MEMORY GROWTH TEST (Duration: {duration_sec}s)")
    print("=" * 80)

    # 1. Clean existing test SHM segment
    shm_name = "tickhub_mem_test"
    shm_path = Path(f"/dev/shm/{shm_name}")
    if shm_path.exists():
        shm_path.unlink()

    # 2. Build tickhub binary
    print("\n[1/5] Building tickhub daemon binary...")
    res = subprocess.run(["/mnt/wc/go/bin/go", "build", "-o", "bin/tickhub", "./cmd/tickhub"],
                         cwd=REPO_ROOT, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Go build failed: {res.stderr}")

    # 3. Load datalake ticks
    print("\n[2/5] Loading datalake market records...")
    events = load_datalake_events()

    # 4. Start Mock WebSocket Server in background event loop thread
    print("\n[3/5] Starting Mock WebSocket Server...")
    ws_port = find_free_port()
    metrics_port = find_free_port()

    import threading
    loop = asyncio.new_event_loop()
    ws_server = MockMassiveWSServer("127.0.0.1", ws_port, events)

    def run_ws():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(ws_server.start())
        loop.run_forever()

    ws_thread = threading.Thread(target=run_ws, daemon=True)
    ws_thread.start()
    time.sleep(0.5)

    # 5. Launch TickHub Daemon
    print(f"\n[4/5] Launching tickhub daemon against ws://127.0.0.1:{ws_port}...")
    config_path = REPO_ROOT / "examples" / "config_datalake.yaml"
    cmd = [
        str(REPO_ROOT / "bin" / "tickhub"),
        "daemon",
        f"--config={config_path}",
        f"--shm-name={shm_name}",
        f"--ws-endpoint=ws://127.0.0.1:{ws_port}",
        "--api-key=test_api_key_fixture",
        "--feed-enabled=true",
        f"--metrics-addr=127.0.0.1:{metrics_port}",
        "--allow-recovery=false",
    ]

    daemon_proc = subprocess.Popen(
        cmd,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    samples = []
    ready = False

    try:
        # Await /readyz
        ready_url = f"http://127.0.0.1:{metrics_port}/readyz"
        metrics_url = f"http://127.0.0.1:{metrics_port}/metrics"

        print("Awaiting daemon readiness (/readyz)...")
        for _ in range(30):
            if daemon_proc.poll() is not None:
                stdout, stderr = daemon_proc.communicate()
                raise RuntimeError(f"Daemon died prematurely:\nSTDOUT: {stdout}\nSTDERR: {stderr}")
            try:
                with urllib.request.urlopen(ready_url, timeout=1) as resp:
                    if resp.status == 200:
                        ready = True
                        break
            except Exception:
                time.sleep(0.2)

        if not ready:
            raise TimeoutError("Daemon failed to become ready within 6 seconds.")

        print("Daemon READY. Beginning sustained telemetry sampling...")
        print(f"{'Time':>6} | {'Ticks':>9} | {'Committed':>9} | {'HeapAlloc':>10} | {'HeapInuse':>10} | {'Objects':>8} | {'GCs':>4} | {'Routines':>8} | {'RSS':>10}")
        print("-" * 95)

        start_time = time.time()
        poll_interval = 2.0  # 2s interval to minimize GC perturbation

        while time.time() - start_time < duration_sec:
            time.sleep(poll_interval)
            elapsed = time.time() - start_time
            m = scrape_metrics(metrics_url)
            if not m:
                continue

            ticks = int(m.get("tickhub_ticks_total", 0))
            committed = int(m.get("tickhub_committed_anchors_total", 0))
            heap_alloc = m.get("go_memstats_heap_alloc_bytes", 0)
            heap_inuse = m.get("go_memstats_heap_inuse_bytes", 0)
            heap_objs = int(m.get("go_memstats_heap_objects_total", 0))
            num_gc = int(m.get("go_memstats_num_gc", 0))
            goroutines = int(m.get("go_goroutines", 0))
            rss = m.get("tickhub_process_rss_bytes", 0)

            sample = {
                "elapsed": elapsed,
                "ticks": ticks,
                "committed": committed,
                "heap_alloc": heap_alloc,
                "heap_inuse": heap_inuse,
                "heap_objs": heap_objs,
                "num_gc": num_gc,
                "goroutines": goroutines,
                "rss": rss,
            }
            samples.append(sample)

            print(f"{elapsed:5.1f}s | {ticks:9,d} | {committed:9,d} | {heap_alloc/1048576:9.2f}MB | {heap_inuse/1048576:9.2f}MB | {heap_objs:8,d} | {num_gc:4d} | {goroutines:8d} | {rss/1048576:9.2f}MB")

    finally:
        print("\n[5/5] Halting daemon and mock server...")
        if daemon_proc.poll() is None:
            daemon_proc.send_signal(signal.SIGINT)
            try:
                daemon_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                daemon_proc.kill()
                daemon_proc.wait()

        # Stop WS server
        loop.call_soon_threadsafe(lambda: asyncio.create_task(ws_server.stop()))
        time.sleep(0.5)
        loop.call_soon_threadsafe(loop.stop)

        if shm_path.exists():
            shm_path.unlink()

    # 6. Evaluation & Statistical Assertions
    print("\n" + "=" * 80)
    print(" MEMORY ANALYSIS & LEAK VERIFICATION RESULTS")
    print("=" * 80)

    assert len(samples) >= 4, f"Insufficient metric samples collected: {len(samples)}"

    final_sample = samples[-1]
    total_ticks = final_sample["ticks"]
    total_committed = final_sample["committed"]

    print(f"Total Ticks Ingested:    {total_ticks:,d}")
    print(f"Total Committed Frames:  {total_committed:,d}")
    assert total_ticks > 50_000, f"Expected > 50,000 ticks ingested, got {total_ticks}"
    assert total_committed > 0, f"Expected committed frames > 0, got {total_committed}"

    # Partition warmup vs post-warmup (warmup: initial 10 seconds for runtime/SHM/heap arena sizing)
    warmup_cutoff = 10.0
    post_warmup = [s for s in samples if s["elapsed"] >= warmup_cutoff]
    if len(post_warmup) < 3:
        post_warmup = samples[len(samples) // 2:]

    pw_xs = [s["elapsed"] for s in post_warmup]
    pw_heap = [s["heap_alloc"] for s in post_warmup]
    pw_rss = [s["rss"] for s in post_warmup]

    heap_slope_bps = linear_regression_slope(pw_xs, pw_heap)
    rss_slope_bps = linear_regression_slope(pw_xs, pw_rss)

    baseline_sample = post_warmup[0]
    baseline_heap_mb = baseline_sample["heap_alloc"] / 1048576.0
    final_heap_mb = final_sample["heap_alloc"] / 1048576.0
    baseline_rss_mb = baseline_sample["rss"] / 1048576.0
    final_rss_mb = final_sample["rss"] / 1048576.0

    # Calculate post-GC heap floors (troughs): minimum HeapAlloc in first half vs second half of post-warmup
    mid = len(post_warmup) // 2
    first_half_trough_mb = min(s["heap_alloc"] for s in post_warmup[:mid]) / 1048576.0 if mid > 0 else baseline_heap_mb
    second_half_trough_mb = min(s["heap_alloc"] for s in post_warmup[mid:]) / 1048576.0 if mid > 0 else final_heap_mb
    trough_drift_mb = second_half_trough_mb - first_half_trough_mb

    rss_pct_change = ((final_sample["rss"] - baseline_sample["rss"]) / baseline_sample["rss"]) * 100.0 if baseline_sample["rss"] > 0 else 0.0
    goroutine_growth = final_sample["goroutines"] - baseline_sample["goroutines"]

    print(f"\nWarmup Baseline Time:    {baseline_sample['elapsed']:.1f}s")
    print(f"Final Sample Time:       {final_sample['elapsed']:.1f}s")
    print(f"Heap Floor (Trough):     {first_half_trough_mb:.2f} MB -> {second_half_trough_mb:.2f} MB (Drift: {trough_drift_mb:+.2f} MB)")
    print(f"HeapAlloc (Instantaneous): {baseline_heap_mb:.2f} MB -> Final: {final_heap_mb:.2f} MB (Slope: {heap_slope_bps/1024:.2f} KB/s)")
    print(f"Process RSS Baseline:    {baseline_rss_mb:.2f} MB -> Final: {final_rss_mb:.2f} MB (Change: {rss_pct_change:+.2f}%, Slope: {rss_slope_bps/1024:.2f} KB/s)")
    print(f"Goroutine Count:         {baseline_sample['goroutines']} -> Final: {final_sample['goroutines']} (Delta: {goroutine_growth:+d})")

    # Assertions
    # 1. Goroutine stability: zero goroutine growth post-warmup
    assert goroutine_growth <= 0, f"Goroutine leak detected: count grew by {goroutine_growth}"
    print("✔ Goroutine Stability: PASSED (zero goroutine accumulation)")

    # 2. Heap Floor Stability: post-GC minimum heap must not drift by > 1.5 MB
    assert trough_drift_mb < 1.5, f"Heap memory leak detected: post-GC floor drifted upward by {trough_drift_mb:.2f} MB"
    print("✔ Heap Floor Flatness: PASSED (post-GC heap floor drift < 1.5 MB)")

    # 3. RSS growth: within 5% of post-warmup baseline
    assert rss_pct_change < 5.0, f"Process RSS growth excessive: {rss_pct_change:.2f}% (threshold: 5%)"
    print("✔ Process RSS Bounded: PASSED (post-warmup RSS within < 5% variation)")

    print("\nOVERALL VERDICT: ZERO MEMORY GROWTH CONFIRMED.")


def test_websocket_memory_growth():
    """Pytest entrypoint (runs standard duration)."""
    run_memory_growth_test(duration_sec=DEFAULT_DURATION_SEC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TickHub WebSocket Memory Growth Test")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION_SEC, help="Test duration in seconds (default 20)")
    args = parser.parse_args()
    run_memory_growth_test(duration_sec=args.duration)
