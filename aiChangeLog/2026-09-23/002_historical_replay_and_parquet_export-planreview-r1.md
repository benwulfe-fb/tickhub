# Plan Review — aiChangeLog/2026-09-23/002_historical_replay_and_parquet_export.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-23 19:41:20Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

# Plan Review — aiChangeLog/2026-09-23/002_historical_replay_and_parquet_export.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 2 · Generated: 2026-09-23T12:40:34-07:00 · Tool: planreview (Antigravity `agy`)_

_Budget: Round 2 of 2_

---

## 1. Summary of the Proposal

Go TickHub adds `ModeHistoricalReplay`: the producer reads raw daily Parquet ticks via a `pkg/feed` K-way min-heap merger, runs them through the existing `pkg/project` 1Hz projection engine, and writes 1Hz metric frames into SHM at maximum CPU speed. A new `LastReadAnchorNS` flow-control line prevents the producer from overwriting unconsumed frames (lossless). A new Python `export.py` drains SHM, accumulates rows, flushes every 1,000 seconds to `/mnt/wc/datalake/features1hz/date=YYYY-MM-DD/features.parquet`. Downstream training memory-maps these precomputed Parquets for ~3.7M samples/sec random access. The same path serves golden-replay backtesting.

---

## 2. Simplest Sufficient Design

Plan is at or near minimum. `pkg/feed`, `ModeHistoricalReplay` + flow-control, and `export.py` are each individually load-bearing for SSoT + lossless export. No material cut available.

---

## 3. Blocking Defects

**Prior B3 — Closed.** The current changelog (§ Measurement, item 1) now provides empirical profiling: single-symbol 60s window seek p50 = 3.38 ms, p99 = 4.48 ms; 100-symbol extrapolation = 338 ms/sample = 2.96 samples/sec; precomputed slice = 0.27 µs p50, 3.71M samples/sec. Both numbers are present (cost and attribution). B3 is resolved.

**B1 — Flush interval (1,000 seconds) exceeds the ring-buffer capacity, creating a loss window that violates the plan's own "zero frame loss" invariant.**

The plan states (§ Measurement, item 3): "zero frames may be dropped or overwritten before being read by the consumer (`overrun_count == 0`)." The plan also states (§ Architecture §4): "Flushes chunks every 1,000 seconds." The flow-control mechanism (`LastReadAnchorNS`) prevents the Go producer from overwriting SHM frames before the consumer *reads* them from SHM. But `commit_read` (Architecture §1, Python Consumer step 3) must be called to advance `LastReadAnchorNS` and unblock the producer. The exporter cannot call `commit_read` on frames it has accumulated in memory but not yet flushed — or rather, it *can* call `commit_read` before flushing to disk. The plan does not specify whether `commit_read` is called per-frame as frames are consumed from SHM, or only after the 1,000-second flush to Parquet. If `commit_read` is deferred until the Parquet flush: the producer is throttled to the 1,000-second flush cadence and the "100,000 bars/sec" throughput target (§ Measurement, item 2) is unreachable — a full day's 23,400 bars cannot be emitted without the consumer processing them continuously. If `commit_read` is called per-frame (decoupled from the Parquet flush): an exporter crash between `commit_read` and the next Parquet flush silently drops all frames acknowledged-but-not-written, violating the zero-loss invariant the plan claims (§ Measurement, item 3). The plan does not specify this decoupling contract or provide a recovery/WAL path for the acknowledged-but-unflushed window. This is not a line-level detail — it is the fundamental durability contract of the exporter, and its omission means the design cannot be implemented without an assumption the plan neither states nor establishes.

---

## 4. Non-Blocking Observations

1. **PID liveness vs. generation counter** (prior review non-blocking #1, not claimed resolved): `syscall.Kill(pid, 0)` check remains in Step 1; PID recycling on Linux can produce a false-alive verdict. Low-probability in practice given `ConsumerHeartbeat` as a secondary check, but the combined guard is weaker than a SHM generation counter.
2. **Monotonicity assertion uses `<=`** (prior review non-blocking #3): § Telemetry item 2 asserts `tick[i].SIPTimestampNS <= tick[i+1].SIPTimestampNS`; the tie-break rule now exists (§ Architecture §2), but the test should assert the tie-break order specifically, not just weak monotonicity.
3. **`vol_1s` window consistency** (prior review non-blocking #5, not claimed resolved): `log_ret_1s` window is `[T-1s, T)` (§ Architecture §3); `vol_1s` definition in §3 says "past 1 second" without confirming the same half-open interval. Should be pinned in the spec.
4. **Parquet output layout schema** (prior review non-blocking #4, not claimed resolved): Step 4 and § Architecture §4 specify `features.parquet` as a single file per day with `date=YYYY-MM-DD` Hive partitioning — layout is now pinned. No issue; this prior observation is closed.
5. **`overrun_count` persistence mechanism** (prior review Missing Telemetry, not claimed resolved): Plan now says "recorded persistently in `GlobalHeader.DroppedTickCount` or dedicated counter" (§ Measurement item 3) — the "or" leaves the field name unresolved at implementation time. Pin to one field name.

---

## 5. Methodological & Data-Alignment Concerns

N/A

---

## 6. Missing Telemetry

The B1 defect requires the plan to specify: after a replay run, assert that every frame emitted from Go and acknowledged via `commit_read` was written to Parquet (row count in output file == total frames produced). Without this end-to-end row-count reconciliation, a crash-in-flight loss is undetectable. This is not currently in the Telemetry section.

---

## 7. Verdict

**Verdict: Blocked (B1)**

The `commit_read` / Parquet flush decoupling contract is unspecified, and both resolutions are defective: deferring `commit_read` to flush cadence makes the 100,000 bars/sec throughput target unreachable; decoupling `commit_read` from flush creates an unacknowledged loss window on exporter crash. The plan simultaneously promises zero frame loss (§ Measurement item 3) and 100,000 bars/sec throughput (§ Measurement item 2) without specifying the mechanism that reconciles them. The fix is small — state explicitly that `commit_read` is called per-frame (decoupled from Parquet flush), add a WAL or atomic rename to cover the in-memory window, and add the end-to-end row-count reconciliation check — but the current text leaves this as an unresolved implementation assumption.
