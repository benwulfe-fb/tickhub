# Plan Review — aiChangeLog/2026-09-23/002_historical_replay_and_parquet_export.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-23 19:39:58Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: B3 halt 1/2 (UNCOUNTED — round budget unchanged, still 1/2) · Verdict parsed: **Blocked (B3)**_

---

## 1. Summary of the Proposal

Go TickHub already runs live streaming with `project1hz`. This plan adds: (a) a `ModeHistoricalReplay` to the Go producer that replays raw Parquet ticks through the existing projection engine at max CPU speed, throttled by a SHM flow-control line (`LastReadAnchorNS`); (b) a Python exporter that drains SHM frames and writes daily 1Hz feature Parquets; (c) a `pkg/feed` K-way merge layer to chronologically unify per-symbol trade/quote Parquet files as the replay input source. The downstream benefit is that PyTorch training memory-maps the precomputed 1Hz Parquets rather than running live IPC, and the Python bot can run unmodified for backtesting.

---

## 2. Simplest Sufficient Design

The stated goal is: precomputed 1Hz feature Parquets, zero duplication of projection logic, zero train-serve skew.

The minimum design that achieves this:
1. `pkg/feed` K-way merger — required, no cut possible.
2. `ModeHistoricalReplay` + flow-control in the Go producer — required.
3. Python SHM exporter (`export.py`) + `commit_read` — required.

**The plan is at or near minimum.** The SHM flow-control mechanism, K-way merger, and Python exporter are each load-bearing. No material cut is available without abandoning the SSoT constraint or the IPC reuse goal.

---

## 3. Blocking Defects

**B3 — Unmeasured foundation for the "GPU starvation / on-the-fly IPC is a bottleneck" claim.**

The Problem Statement (§ "Training Bottleneck with On-the-Fly IPC") asserts two quantitative root causes as the justification for the entire design:

> *"Warmup window reconstruction: Random access samples … require seeking back 15s–60s in raw Parquet tick files to reconstruct rolling feature state, thrashing disk I/O and CPU decompression."*
> *"GPU Starvation: Raw tick decompression and chronological sorting cannot match modern GPU consumption rates (thousands of samples per second)."*

These are **attributed, quantitative performance claims** — the design is built to solve them. Neither claim is accompanied by a measurement. The changelog presents no profiler output, no DataLoader throughput number, no GPU utilization trace, no observed seek latency, and no tick decompression benchmark from the actual data layout (NVMe vs spinning, compression codec, row-group size). The Measurement section (§ "Measurement") lists *target* numbers for the new design but presents no baseline from the current path.

Specifically missing:
- **Seek/reconstruct cost**: What is the measured p50/p99 latency to seek back 15–60s in one of the actual `*.trades.parquet` files and re-project a warm window? Run `time python -c "import pyarrow.parquet as pq; ..."` on a real datalake file.
- **DataLoader throughput vs GPU demand**: What samples/sec does the current (or hypothetical on-the-fly) DataLoader actually sustain? What is the observed GPU utilization %? Has the training loop even been written yet?

Without these two numbers the B3 verdict applies: the design cannot be adjudicated from the text.

**Verdict: `Blocked (B3)`**

---

## 4. Non-Blocking Observations

1. Consumer-liveness check uses `syscall.Kill(pid, 0)` (Step 1) — PID recycling on Linux can cause a live unrelated process to be mistaken for the original consumer; a shared-memory generation counter is more robust.
2. The flush interval "every 10,000 seconds" (Step 4) exceeds a full trading day (23,400s); if the exporter crashes mid-day the in-memory accumulation is lost.
3. Monotonicity assertion (§ Telemetry, item 2) uses `<=` but the merger description says "strictly chronological" — ties between a trade and a quote at the same nanosecond are legal; the tie-break rule should be stated and tested.
4. Output path in Step 4 (`<date>.parquet`) is a single file; the Architecture section (§ 4) says "chunked" writes via `ParquetWriter` — the file-per-day vs partitioned layout choice should be pinned before implementation to avoid schema churn in training code.
5. `vol_1s` is defined as "total trade volume in past 1 second" (§ 3) but `log_ret_1s` window is `[T-1s, T)` — confirm `vol_1s` uses the same half-open interval so features are temporally consistent.

---

## 5. Methodological & Data-Alignment Concerns

N/A

---

## 6. Missing Telemetry

The plan's own Measurement §1 and §2 require `overrun_count == 0` and `> 100,000 bars/sec`. These are stated but no mechanism to *record* `overrun_count` during a replay run is described — it must be a persistent counter in `GlobalHeader` that the exporter can read and assert on after the run, not just an in-process check.

---

## 7. Verdict

**Verdict: Blocked (B3)**

The entire architectural choice — SHM replay + precomputed Parquets rather than on-the-fly DataLoader IPC — rests on the claim that on-the-fly seek/reconstruct and tick decompression will starve the GPU. Neither the seek cost nor the DataLoader throughput gap has been measured against the actual datalake. Until both baselines exist, it is equally plausible that a simpler approach (e.g., pre-sorting tick Parquets once offline and reading them sequentially per worker) achieves the same goal at a fraction of the implementation cost. The plan should instrument the current (or simplest alternative) path first; if the measurements confirm the bottleneck, this design is well-scoped and can proceed immediately.
