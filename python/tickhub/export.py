#!/usr/bin/env python3
"""TickHub Parquet Feature Exporter.

Streams 1Hz multi-phase metric frames from POSIX SHM in replay mode
and writes partitioned Parquet feature datasets using PyArrow.
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch

from tickhub.shm import LaggedAnchorError, SymbolCursor, TickHubReader


def export_features(
    config_path_or_dict: Union[str, Path, dict],
    output_path: Union[str, Path],
    max_frames: int = 0,
    chunk_size: int = 1000,
    shm_name_override: Optional[str] = None,
) -> dict:
    """Exports 1Hz feature matrices from SHM replay into a compressed Parquet file.

    Guarantees:
    - Continuous commit_read signals backpressure to Go producer without I/O stalls.
    - Staged writing to `.tmp` file followed by atomic `os.replace` on completion.
    - End-to-end row reconciliation assertion.
    """
    output_file = Path(output_path).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = output_file.with_suffix(".parquet.tmp")

    with TickHubReader(config_path_or_dict, shm_path_override=shm_name_override) as hub:
        feature_names = hub.config.get("features", ["log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"])

        # PyArrow Schema definition
        arrow_fields = [
            ("anchor_ns", pa.int64()),
            ("phase", pa.string()),
            ("symbol", pa.string()),
        ]
        for f in feature_names:
            arrow_fields.append((f, pa.float64()))
        schema = pa.schema(arrow_fields)

        all_cursors, _ = hub.begin_all()
        phase_groups = [
            (p_name, [c for c in all_cursors if c.phase == p_name])
            for p_name in hub.phases
        ]

        # Preallocate PyTorch tensors per phase
        tensors = {
            p_name: torch.zeros((len(c_list), len(feature_names)), dtype=torch.float64)
            for p_name, c_list in phase_groups
        }

        writer = pq.ParquetWriter(str(tmp_file), schema, compression="zstd", compression_level=3)
        total_rows_written = 0
        total_frames_processed = 0

        # Memory buffer for chunked writes
        col_data = {name: [] for name, _ in arrow_fields}

        def flush_chunk():
            nonlocal total_rows_written
            if not col_data["anchor_ns"]:
                return
            arrays = [pa.array(col_data[name], type=schema.field(name).type) for name, _ in arrow_fields]
            batch_table = pa.Table.from_arrays(arrays, schema=schema)
            writer.write_table(batch_table)
            total_rows_written += len(col_data["anchor_ns"])
            for name, _ in arrow_fields:
                col_data[name].clear()

        start_time = time.perf_counter()

        try:
            done = False
            while not done:
                # 1. Drain frames across all phases
                for p_name, c_list in phase_groups:
                    tensor = tensors[p_name]
                    failed = hub.load_sync(c_list, tensor, timeout_ms=1000.0)
                    if failed:
                        if hub.status == 4:  # StatusClosed
                            done = True
                            break
                        raise RuntimeError(f"Timeout waiting for symbols {failed} in {p_name} at anchor {c_list[0].target_anchor_ns}")

                    np_view = tensor.numpy()
                    target_anchor = c_list[0].target_anchor_ns

                    for s_idx, cursor in enumerate(c_list):
                        col_data["anchor_ns"].append(target_anchor)
                        col_data["phase"].append(p_name)
                        col_data["symbol"].append(cursor.symbol)
                        for f_idx, f_name in enumerate(feature_names):
                            col_data[f_name].append(float(np_view[s_idx, f_idx]))

                    hub.next(c_list)

                if done:
                    break

                # 2. Advance consumer control line: wakes Go producer immediately
                hub.commit_read(all_cursors)
                total_frames_processed += 1

                # 3. Periodically flush to disk
                if total_frames_processed % chunk_size == 0:
                    flush_chunk()

                # Check termination
                if max_frames > 0 and total_frames_processed >= max_frames:
                    break
                if hub.status == 4:  # StatusClosed
                    break

        except Exception as e:
            if tmp_file.exists():
                tmp_file.unlink(missing_ok=True)
            raise e
        finally:
            flush_chunk()
            writer.close()

        elapsed_sec = time.perf_counter() - start_time

        # 4. End-to-end reconciliation and atomic rename
        expected_rows = sum(len(c_list) for _, c_list in phase_groups) * total_frames_processed
        assert total_rows_written == expected_rows, (
            f"Row count reconciliation failed: written={total_rows_written}, expected={expected_rows}"
        )
        assert hub.dropped_ticks == 0, f"Overrun detected during replay: dropped_ticks={hub.dropped_ticks}"

        # Atomic rename to final output path
        os.replace(str(tmp_file), str(output_file))

        throughput = total_frames_processed / elapsed_sec if elapsed_sec > 0 else 0
        return {
            "output_file": str(output_file),
            "total_frames": total_frames_processed,
            "total_rows": total_rows_written,
            "elapsed_sec": elapsed_sec,
            "bars_per_sec": throughput,
            "overruns": hub.dropped_ticks,
        }


def main():
    parser = argparse.ArgumentParser(description="TickHub Feature Parquet Exporter")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--output", required=True, help="Output .parquet file path")
    parser.add_argument("--max-frames", type=int, default=0, help="Max frames to export (0 = until closed)")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Flush chunk size in frames")
    parser.add_argument("--shm", default=None, help="SHM segment name override")
    args = parser.parse_args()

    res = export_features(
        config_path_or_dict=args.config,
        output_path=args.output,
        max_frames=args.max_frames,
        chunk_size=args.chunk_size,
        shm_name_override=args.shm,
    )
    print(f"Export completed in {res['elapsed_sec']:.2f}s:")
    print(f"  Total frames: {res['total_frames']} ({res['bars_per_sec']:.1f} bars/sec)")
    print(f"  Total rows:   {res['total_rows']}")
    print(f"  Overruns:     {res['overruns']}")
    print(f"  Destination:  {res['output_file']}")


if __name__ == "__main__":
    main()
