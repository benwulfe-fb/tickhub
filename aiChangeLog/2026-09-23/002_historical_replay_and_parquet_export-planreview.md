# Plan Review — aiChangeLog/2026-09-23/002_historical_replay_and_parquet_export.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-23 19:42:17Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

# Plan Review — aiChangeLog/2026-09-23/002_historical_replay_and_parquet_export.md

## 1. Summary of the Proposal

Go TickHub adds `ModeHistoricalReplay` to stream historical Parquet ticks through existing `pkg/project` 1Hz projection engine into SHM at maximum CPU rate. `pkg/feed` implements K-way min-heap merge across daily trade and quote Parquet files with deterministic tie-breaking (quotes before trades, then symbol ASCII). Flow control via `LastReadAnchorNS` pauses producer when SHM ring buffer fills, preventing frame overruns. Python `export.py` drains SHM frames, advances `LastReadAnchorNS` per batch to maintain in-memory throughput, writes 1,000s chunks to `features.parquet.tmp`, reconciles row counts against expected frames, and atomically renames output to `features.parquet`. Downstream training memory-maps exported Parquet files directly, eliminating raw tick decompression and seek bottlenecks while preserving Go projection logic as Single Source of Truth.

## 2. Simplest Sufficient Design

Plan is at minimum sufficient design. `pkg/feed` K-way merger, `ModeHistoricalReplay` flow control, and Python `export.py` are each strictly load-bearing to eliminate duplicate feature logic in Python, avoid fatal random Parquet seek latency during training, and guarantee lossless export. No material cut available.

## 3. Blocking Defects

None.

## 4. Non-Blocking Observations

1. Multi-cursor flow control in `export.py` should commit `min(c.target_anchor_ns)` rather than `max` if phase cursors step asynchronously, preventing ring buffer overwrite of lagging phase.
2. Dual consumer liveness check combines `syscall.Kill(pid, 0)` with `ConsumerHeartbeat` progression every 100ms; adequate in practice, though SHM generation counter remains strictly cleaner against PID recycling.
3. K-way min-heap opens $2N$ Parquet files concurrently (200 descriptors for 100 symbols); verify process `ulimit -n` accommodates open file table during batch run.
4. Replay mode repurposes `GlobalHeader.DroppedTickCount` for ring buffer overrun tracking; verify downstream monitoring does not conflate historical backpressure overruns with live network packet drops.

## 5. Methodological & Data-Alignment Concerns

None. Running authoritative Go `pkg/project` engine guarantees zero train-serve feature skew. Microstructure metrics strictly enforce backward-looking half-open intervals $[T - \Delta, T)$ with deterministic forward-fill on illiquid bars, preventing look-ahead leakage.

## 6. Missing Telemetry

None.

## 7. Verdict

**Verdict: Proceed-with-noted-risks**

Prior blocking defect B1 is resolved: § Architecture §1 and § Telemetry item 3 now explicitly specify the decoupled `commit_read` durability contract, transactional `.tmp` file staging with atomic `os.replace`, and mandatory end-to-end row reconciliation (`parquet_table.num_rows == total_expected_frames * num_symbols`). Empirical profiling in § Measurement item 1 confirms the 3.71M samples/sec precomputed array throughput advantage over raw tick IPC seeks. The design is minimal, preserves the production Go engine as SSoT, and is ready for implementation.
