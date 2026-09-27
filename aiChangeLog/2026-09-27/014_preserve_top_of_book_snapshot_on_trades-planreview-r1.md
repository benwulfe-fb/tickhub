# Plan Review — aiChangeLog/2026-09-27/014_preserve_top_of_book_snapshot_on_trades.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-27 22:11:22Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## Review: `014_preserve_top_of_book_snapshot_on_trades.md`

---

### 1. Summary of the Proposal

`updateSnapshot` currently allocates a fresh zero-valued `&shm.SymbolSnapshot{}` on every tick, then populates only trade fields on trade ticks (zeroing quotes) or only quote fields on quote ticks (zeroing last-trade). Fix: add `snapshots []shm.SymbolSnapshot` to `Projector`, allocated once in `NewProjector`. `updateSnapshot` takes a pointer into that slice, mutates only the relevant fields, and calls `WriteSnapshot`. `SeedState` seeds the preallocated entry before ticks arrive. One new test validates field persistence and zero allocations via `testing.AllocsPerRun`.

---

### 2. Simplest Sufficient Design

Plan is at minimum. The only state needed is one `shm.SymbolSnapshot` per symbol; the plan adds exactly that. No excess abstractions, no new files beyond the test. **Already minimal.**

---

### 3. Blocking Defects

**B2 — Concurrent read/write on `snapshots` slice without SeqLock protection (race condition on the projector-side staging buffer).**

The plan introduces `p.snapshots []shm.SymbolSnapshot` as a per-symbol staging buffer mutated by `updateSnapshot` and read (for seeding) by `SeedState`. The changelog states the test must pass with `-race` (`go test -v -race ./pkg/project/...`). If `updateSnapshot` is called from a hot-path goroutine and `SeedState` or any status/metrics reader calls `p.producer.Snapshot(uIdx)` concurrently — or if the projector ever calls `updateSnapshot` concurrently for different symbols without a lock — then the `p.snapshots` slice elements are subject to unsynchronized concurrent access. The plan does not describe any mutex, per-symbol lock, or channel discipline protecting `p.snapshots` between goroutines. `shm.SymbolSnapshot` is 128 bytes; the Go memory model does not guarantee atomic 128-byte struct reads/writes, so the race detector will fire even on non-overlapping field writes if two goroutines touch the same struct concurrently.

The plan's own Telemetry section (`cmd/tickhub/status.go` and `/metrics` calling `p.producer.Snapshot(uIdx)`) implies concurrent readers exist. If `WriteSnapshot` reads from `snap *shm.SymbolSnapshot` while `updateSnapshot` is writing to `p.snapshots[uIdx]`, the staging buffer is the race surface, not shared memory (which has SeqLock protection). The plan is silent on this.

**Resolution required before implementation**: either document that `updateSnapshot` is always called on a single goroutine (and `SeedState` only before the hot path starts), or add a per-symbol lock or atomic copy discipline to the staging buffer access.

---

### 4. Non-Blocking Observations

1. `snap.RecvTimestampNS = tick.SIPTimestampNS` — both fields set to the same value; if `RecvTimestampNS` is meant to be wall-clock arrival time, this is a latency-measurement bug.
2. `SeedState` calls `WriteSnapshot` immediately after seeding; if `NewProjector` hasn't established a valid SHM segment yet, this could write to an uninitialised producer — the ordering dependency is not stated.
3. The test only covers two symbols; testing an index-boundary symbol (index 0 and `len-1`) would give the bounds-check assertion real coverage.
4. `SeedState` does not update `Spread` or `Midprice` if only one of `lastBid`/`lastAsk` is positive but nonzero — consistent with quote logic, but worth a comment.
5. Zero-alloc guarantee via `testing.AllocsPerRun` will break if `WriteSnapshot` internally allocates (e.g., an interface boxing); the test implicitly constrains `WriteSnapshot`'s implementation.

---

### 5. Methodological & Data-Alignment Concerns

**ABI**: `p.snapshots` is a Go slice of `shm.SymbolSnapshot` (128 bytes each). Go may insert padding between slice elements only if the element size is not a multiple of its alignment. `SymbolSnapshot` is 128 bytes; as long as its alignment requirement is ≤ 128, the slice backing array is contiguous and `&p.snapshots[uIdx]` passes the correct pointer to `WriteSnapshot`. This is fine provided the struct's declared alignment hasn't changed — the plan correctly notes the 128-byte layout is preserved.

**1Hz semantics / forward-fill / half-open interval**: this change is entirely in `updateSnapshot` (per-tick, not per-window). The 1Hz `project1hz` logic is untouched. No look-ahead or interval-boundary concern applies here.

**Quote-before-trade tie-breaking**: unchanged by this plan; tie-breaking lives in the tick ingestion order, not in `updateSnapshot`.

---

### 6. Missing Telemetry

The plan should assert in the test (or in a post-deploy check) that after the fix, **no symbol ever emits a snapshot with `BidPx == 0 && LastTradePx > 0`** (the original defect's observable symptom). The Telemetry section gestures at this but frames it as a manual Python check on a live VM, not an automated regression guard. A single assertion on the mock producer's captured snapshots in `TestProjectorSnapshotPersistenceAndZeroAlloc` would make this machine-verifiable.

---

### 7. Verdict

**Verdict: Blocked (B2)**

The staging buffer `p.snapshots []shm.SymbolSnapshot` is written by `updateSnapshot` and potentially read concurrently by `SeedState` or metrics paths. The plan introduces no synchronisation on this buffer and mandates `-race` testing, which will surface the race. The fix is a one-line invariant statement (single-goroutine discipline) or a trivial per-symbol lock — either resolves the block without inflating the plan's size.
