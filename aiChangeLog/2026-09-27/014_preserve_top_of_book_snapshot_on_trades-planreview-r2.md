# Plan Review — aiChangeLog/2026-09-27/014_preserve_top_of_book_snapshot_on_trades.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-27 22:11:50Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

# Plan Review — aiChangeLog/2026-09-27/014_preserve_top_of_book_snapshot_on_trades.md

_Model: Gemini 3.8 Flash · Round: 2 · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 (final) · Verdict parsed: **Proceed**_

---

### 1. Summary of the Proposal

Plan fixes top-of-book snapshot wiping and hot-path heap allocations in `pkg/project/projector.go`. Replaces per-tick `&shm.SymbolSnapshot{}` allocation with preallocated `p.snapshots []shm.SymbolSnapshot` slice sized by unique symbol count in `NewProjector`. `updateSnapshot` mutates staging snapshot fields in place (preserving quotes on trades, and trade info on quotes) and calls `p.producer.WriteSnapshot(uIdx, snap)`. `SeedState` populates initial state pre-ingestion. Explicit single-goroutine invariant formally documents isolation between `p.snapshots` and external SHM SeqLock readers, closing prior B2. Adds unit test `TestProjectorSnapshotPersistenceAndZeroAlloc` asserting cross-update persistence, boundary symbol coverage, and zero allocations via `testing.AllocsPerRun`.

---

### 2. Simplest Sufficient Design

Plan is already at minimum. Single slice indexed by existing `uIdx` without extra locks, wrappers, or files.

---

### 3. Blocking Defects

None.

*Prior B2 resolution verified*: Section "Concurrency & Threading Invariant" establishes that `p.snapshots` is private to the single ingestion goroutine running `IngestTick`/`updateSnapshot` (with `SeedState` strictly pre-ingestion). External readers access `/dev/shm` directly through SeqLock store-release/load-acquire fences via `Producer.Snapshot()`, eliminating unsynchronized access to `p.snapshots`.

---

### 4. Non-Blocking Observations

1. If quote tick arrives with one-sided zero (`tick.BidPx == 0` or `tick.AskPx == 0`), `Midprice` and `Spread` retain previous values rather than invalidating.
2. Silent return on out-of-bounds `uIdx` in `updateSnapshot` lacks a debug metric or counter for dropped updates.

---

### 5. Methodological & Data-Alignment Concerns

ABI layout of `shm.SymbolSnapshot` (128 bytes) remains unchanged across Go, C, and Python. SeqLock write fences in `WriteSnapshot` continue to guarantee reader atomicity. 1Hz rolling window math, tie-breaking, and $[T-1\text{s}, T)$ interval boundaries in `project1hz` are untouched.

---

### 6. Missing Telemetry

None.

---

### 7. Verdict

**Verdict: Proceed**

The plan successfully remediates the prior B2 concurrency objection with an explicit single-goroutine ownership model. It resolves both the top-of-book wiping defect and the hot-path allocation violation with the smallest sufficient change, preserves ABI stability, and includes complete regression test coverage with zero-allocation assertions.
