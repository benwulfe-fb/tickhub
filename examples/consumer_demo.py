#!/usr/bin/env python3
"""TickHub Consumer Demo: Asynchronous multi-phase 1Hz engine reading from POSIX SHM.

Demonstrates:
- SSoT YAML configuration loading.
- Universal cursor initialization via begin_all().
- Phase partitioning via standard Python list comprehensions.
- Zero-copy extraction into pre-allocated PyTorch tensors.
- Lock-free top-of-book SeqLock snapshots (< 2 µs).
- Async load with non-blocking sleep and automatic stale cursor catch-up.
"""

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

import torch

from tickhub import LaggedAnchorError, SymbolCursor, TickHubReader


async def run_phase_worker(
    hub: TickHubReader,
    phase_name: str,
    cursors: list[SymbolCursor],
    tensor: torch.Tensor,
    max_steps: int = 0,
):
    """Processes 1Hz frames for a specific phase, catching up sequentially if stale."""
    step = 0
    print(f"[{phase_name}] Started worker with {len(cursors)} symbols: {[c.symbol for c in cursors]}")

    while True:
        try:
            # 1. Await load for this phase (sleeps until window if on-time, or drains immediately if stale)
            failed = await hub.load(cursors, tensor)
            if failed:
                print(f"[{phase_name}] Warning: {len(failed)} symbols timed out: {failed}")

            # Extract sample values (zero copy from SHM directly into PyTorch tensor)
            spy_idx = next(i for i, c in enumerate(cursors) if c.symbol == "SPY")
            spy_ret = tensor[spy_idx, 0].item()
            anchor_dt = time.strftime("%H:%M:%S", time.gmtime(cursors[0].target_anchor_ns / 1e9))
            ms = (cursors[0].target_anchor_ns // 1_000_000) % 1000

            # Also query lock-free top-of-book snapshot for SPY
            snap = hub.snapshot("SPY")

            print(
                f"[{phase_name}] Bar @ {anchor_dt}.{ms:03d} | "
                f"SPY ret_1s={spy_ret:+.6f} | MidPx=${snap['midprice']:.2f} | "
                f"Spread=${snap['spread']:.2f}"
            )

            # 2. Advance all cursors by one cadence interval
            hub.next(cursors)

            # 3. Catch-up loop: if any cursor in this phase is stale, drain immediately at CPU speed
            while True:
                stale_cursors = [c for c in cursors if c.is_stale]
                if not stale_cursors:
                    break
                print(f"[{phase_name}] Catching up {len(stale_cursors)} stale cursors (staleness={stale_cursors[0].staleness_ns/1e6:.1f}ms)...")
                await hub.load(stale_cursors, tensor)
                hub.next(stale_cursors)

            step += 1
            if max_steps > 0 and step >= max_steps:
                print(f"[{phase_name}] Completed {step} steps.")
                break

        except LaggedAnchorError as e:
            print(f"[{phase_name}] Lagged behind SHM ring buffer: {e}. Resynchronizing cursors...")
            for c in cursors:
                c.rebegin()


async def main():
    parser = argparse.ArgumentParser(description="TickHub Async Consumer Demo")
    parser.add_argument("--config", default="examples/config.yaml", help="Path to config.yaml")
    parser.add_argument("--steps", type=int, default=10, help="Number of 1Hz steps to consume (0 = infinite)")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    print("=" * 65)
    print("  TICKHUB ASYNC CONSUMER ENGINE")
    print("=" * 65)

    with TickHubReader(config_path) as hub:
        print(f"Connected to SHM /dev/shm/{hub.config['shm']['name']}")
        print(f"Phases: {hub.phases}")
        print(f"Directory Symbols ({len(hub.symbols)}): {hub.symbols}")
        print(f"Cadence: {hub.cadence_ns / 1e9:.1f}s | Watermark: {hub.watermark_buffer_ns / 1e6:.1f}ms\n")

        # 1. Initialize all cursors across all phases
        all_cursors, _ = hub.begin_all(history_steps=0)

        # 2. Filter into phase-specific collections using standard list comprehensions
        phase_workers = []
        for phase_name in hub.phases:
            phase_cursors = [c for c in all_cursors if c.phase == phase_name]
            num_feats = len(hub.config.get("features", []))

            # Preallocate PyTorch tensor [N_symbols, N_features]
            phase_tensor = torch.zeros((len(phase_cursors), num_feats), dtype=torch.float64)

            phase_workers.append(
                run_phase_worker(
                    hub=hub,
                    phase_name=phase_name,
                    cursors=phase_cursors,
                    tensor=phase_tensor,
                    max_steps=args.steps,
                )
            )

        # 3. Run all phase workers concurrently on the asyncio event loop
        await asyncio.gather(*phase_workers)

    print("\nConsumer demo completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
