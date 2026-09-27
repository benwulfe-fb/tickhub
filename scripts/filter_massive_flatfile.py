#!/usr/bin/env python3
"""
filter_massive_flatfile.py — Memory-bounded filter & merge for Massive.com SIP flat files.

Extracts symbols in the configured live universe (e.g. config/live_72.yaml) from raw
Massive.com trades and quotes CSV.GZ archives and outputs a single, chronologically
sorted, compressed CSV stream with Quote-Before-Trade tie-breaking.

Memory complexity: O(1) via streaming generator merge.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import heapq
import logging
import os
import sys
import time
from pathlib import Path
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("filter_massive")


def load_universe(config_path: str | Path) -> set[str]:
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    symbols = set(cfg.get("unique_symbols", []))
    if not symbols:
        raise ValueError(f"No unique_symbols found in {config_path}")
    logger.info(f"Loaded {len(symbols)} target symbols from {config_path}")
    return symbols


def trade_generator(trades_path: str | Path, symbols: set[str], max_rows: int = -1):
    count = 0
    with gzip.open(trades_path, "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            sym_idx = header.index("ticker")
            ts_idx = header.index("sip_timestamp")
            px_idx = header.index("price")
            sz_idx = header.index("size")
        except (StopIteration, ValueError) as e:
            raise ValueError(f"Unexpected trades header in {trades_path}: {e}") from e

        for parts in reader:
            if len(parts) <= max(sym_idx, ts_idx, px_idx, sz_idx):
                continue
            sym = parts[sym_idx]
            if sym in symbols:
                try:
                    ts = int(parts[ts_idx])
                    px = parts[px_idx]
                    sz = parts[sz_idx]
                except (ValueError, IndexError):
                    continue
                # Order key: (timestamp, 1) -> Quotes (0) break ties before Trades (1)
                record = f"T,{sym},{ts},0,0,0,0,{px},{sz}\n"
                yield (ts, 1, record)
                count += 1
                if max_rows > 0 and count >= max_rows:
                    break


def quote_generator(quotes_path: str | Path, symbols: set[str], max_rows: int = -1):
    count = 0
    with gzip.open(quotes_path, "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            sym_idx = header.index("ticker")
            ts_idx = header.index("sip_timestamp")
            bp_idx = header.index("bid_price")
            bs_idx = header.index("bid_size")
            ap_idx = header.index("ask_price")
            as_idx = header.index("ask_size")
        except (StopIteration, ValueError) as e:
            raise ValueError(f"Unexpected quotes header in {quotes_path}: {e}") from e

        for parts in reader:
            if len(parts) <= max(sym_idx, ts_idx, bp_idx, bs_idx, ap_idx, as_idx):
                continue
            sym = parts[sym_idx]
            if sym in symbols:
                try:
                    ts = int(parts[ts_idx])
                    bp = parts[bp_idx]
                    bs = parts[bs_idx]
                    ap = parts[ap_idx]
                    as_ = parts[as_idx]
                except (ValueError, IndexError):
                    continue
                # Order key: (timestamp, 0) -> Quotes (0) break ties before Trades (1)
                record = f"Q,{sym},{ts},{bp},{bs},{ap},{as_},0,0\n"
                yield (ts, 0, record)
                count += 1
                if max_rows > 0 and count >= max_rows:
                    break


def merge_and_filter(
    trades_path: str | Path,
    quotes_path: str | Path,
    config_path: str | Path,
    output_path: str | Path,
    max_rows: int = -1,
):
    symbols = load_universe(config_path)
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t_gen = trade_generator(trades_path, symbols, max_rows)
    q_gen = quote_generator(quotes_path, symbols, max_rows)

    merged = heapq.merge(q_gen, t_gen, key=lambda item: (item[0], item[1]))

    t0 = time.time()
    total_written = 0
    q_count = 0
    t_count = 0

    open_fn = gzip.open if str(out_path).endswith(".gz") else open
    mode = "wt" if str(out_path).endswith(".gz") else "w"

    with open_fn(out_path, mode, encoding="utf-8") as out:
        out.write("event_type,symbol,sip_timestamp_ns,bid_px,bid_sz,ask_px,ask_sz,trade_px,trade_sz\n")
        last_log = time.time()

        for ts, tie_breaker, record in merged:
            out.write(record)
            total_written += 1
            if tie_breaker == 0:
                q_count += 1
            else:
                t_count += 1

            if time.time() - last_log >= 5.0:
                elapsed = time.time() - t0
                rate = total_written / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Written {total_written:,} events ({q_count:,} Q, {t_count:,} T) "
                    f"at {rate:,.0f} ev/s..."
                )
                last_log = time.time()

    elapsed = time.time() - t0
    rate = total_written / elapsed if elapsed > 0 else 0
    file_mb = out_path.stat().st_size / (1024 * 1024)
    logger.info(
        f"Completed: {total_written:,} total events ({q_count:,} quotes, {t_count:,} trades) "
        f"in {elapsed:.1f}s ({rate:,.0f} ev/s). Output: {out_path} ({file_mb:.1f} MB)"
    )


def main():
    parser = argparse.ArgumentParser(description="Filter and merge raw Massive.com flat files.")
    parser.add_argument("--trades", required=True, help="Path to raw trades_*.csv.gz")
    parser.add_argument("--quotes", required=True, help="Path to raw quotes_*.csv.gz")
    parser.add_argument("--config", default="config/live_72.yaml", help="Path to live symbols config")
    parser.add_argument("--output", required=True, help="Path to output .csv.gz")
    parser.add_argument("--max-rows", type=int, default=-1, help="Max rows per stream for testing")
    args = parser.parse_args()

    merge_and_filter(args.trades, args.quotes, args.config, args.output, args.max_rows)


if __name__ == "__main__":
    main()
