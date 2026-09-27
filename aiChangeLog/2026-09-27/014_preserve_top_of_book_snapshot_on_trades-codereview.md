# Code Review — aiChangeLog/2026-09-27/014_preserve_top_of_book_snapshot_on_trades.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-27 22:14:54Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

`projector.go`: Adds `snapshots []shm.SymbolSnapshot` field to `Projector`, allocated in `NewProjector` with `make([]shm.SymbolSnapshot, len(uniqueSymbols))`. `SeedState` now populates and publishes the pre-feed staging snapshot for a symbol. `updateSnapshot` replaces the per-tick `&shm.SymbolSnapshot{}` heap alloc with an in-place mutation of `p.snapshots[uIdx]`, updating only the fields relevant to trade or quote, preserving the other side's state, then calling `p.producer.WriteSnapshot(uIdx, snap)`.

`projector_test.go`: Adds `TestProjectorSnapshotPersistenceAndZeroAlloc` covering: seed+publish verification, quote persistence of last trade, trade non-wiping of bid/ask/mid, boundary symbol `SPY` (index 2), and `testing.AllocsPerRun(1000, ...)` zero-allocation assertion on the hot path.

---

## 2. Correctness & Concurrency Bugs

**No blocking defects found.**

- `p.snapshots` is owned exclusively by the single ingestion goroutine; `SeedState` is called strictly pre-ingestion. External readers hit SHM SeqLock only. No races on `p.snapshots`. Race detector will confirm.
- Bounds check `uIdx < 0 || uIdx >= len(p.snapshots)` at top of `updateSnapshot` is correct and sufficient.
- `SeedState` has `uIdx >= 0 && uIdx < len(p.snapshots)` guard before slice access. Correct.
- `RecvTimestampNS` deviation noted: the diff sets `snap.RecvTimestampNS = tick.SIPTimestampNS` unconditionally, rather than the conditional `if tick.RecvTimestampNS > 0 { ... } else { tick.SIPTimestampNS }` stated in the ledger (§ Proposed Code Changes, `updateSnapshot`, timestamp block). This is a minor conservatism (always uses SIP as fallback) but deviates from the plan. **Not a data-loss or correctness defect** — SIPTimestamp is a valid monotonic fallback — so this does not warrant a DENY.

---

## 3. Projection Math & Temporal Parity

- `updateSnapshot` does not touch `project1hz` logic. Rolling window math, `[T-1s, T)` half-open interval, illiquid forward-fill, vol computation, log-ret, spread_bps, and tie-breaking are all untouched by this diff. SSoT invariant preserved.
- `Midprice` and `Spread` in snapshot (real-time BBO cache) are distinct from 1Hz projected `midprice`/`spread_bps`. No leakage.
- Quote-before-trade tie-breaking in `IngestTick` is unchanged. This change only affects the snapshot cache written after ingestion, not the ordering of tick processing itself.

---

## 4. Deviations from the Approved Plan

One minor deviation:

- **`RecvTimestampNS` assignment** (ledger line: *"if `tick.RecvTimestampNS > 0`, else `tick.SIPTimestampNS`"*): implementation unconditionally assigns `tick.SIPTimestampNS` to both `SIPTimestampNS` and `RecvTimestampNS`. Original pre-change code also used `tick.SIPTimestampNS` for both (the old struct literal set `RecvTimestampNS: tick.SIPTimestampNS`), so this is status-quo-preserving. The ledger described an enhancement (propagate real recv timestamp when available) that was not implemented. **Non-blocking**: the old behavior is retained; no regression.

No other deviations from the approved plan.

---

## 5. Systems & Performance Violations

**None found.**

- `updateSnapshot` is now alloc-free in steady state: no `&shm.SymbolSnapshot{}` construction, no interface boxing, no map reads. The `testing.AllocsPerRun(1000, ...)` test enforces this at runtime.
- `p.snapshots` is a flat `[]shm.SymbolSnapshot` slice (value-typed, not pointer slice). No per-element heap escapes.
- `WriteSnapshot` takes a `*shm.SymbolSnapshot` pointing into the slice; no copy allocation.
- Cache-line isolation: `p.snapshots` is the staging buffer in Go heap, not in the SHM segment. It is not on the Producer or Consumer cache lines in `/dev/shm`. `WriteSnapshot` uses SeqLock store-release fences to publish atomically. Dual cache-line isolation (producer at offset 64, consumer at offset 128) in `GlobalHeader` is untouched.
- `SeedState` heap-allocates nothing new (no `make`, no `&T{}`).

---

## 6. Telemetry / Verification Gaps

- Test uses a real `shm.CreateProducer` (not a mock), so the unit test exercises the actual SeqLock round-trip through `/dev/shm`. `prod.Snapshot(uIdx)` reads back through the SeqLock — strong verification.
- Plan mentions verifying `BidSz`/`AskSz` propagation; test ingests `BidSz: 10, AskSz: 20` and `BidSz: 50, AskSz: 60` but does not assert on `BidSz`/`AskSz` in the snapshot return. Minor gap but not blocking — the core regression (trade-wipes-bid) is fully covered.
- `MSFT` (index 1) receives no ticks in the test; its zero-value snapshot is never validated. Acceptable — boundary coverage at 0 and 2 is sufficient.

---

## 7. Verdict & Remediation

**APPROVED.** Both defects from the ledger (quote/trade wiping and hot-path heap allocation) are correctly remediated. The implementation is minimal, race-clean, ABI-stable, and zero-alloc in steady state. The one plan deviation (`RecvTimestampNS` conditional) preserves prior behavior and introduces no regression. The test is substantive and exercises real SHM SeqLock round-trips. No blocking issues.
