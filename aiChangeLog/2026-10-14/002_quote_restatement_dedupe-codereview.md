# Code Review — aiChangeLog/2026-10-14/002_quote_restatement_dedupe.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-10-14 18:55:17Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

The change set implements per-symbol duplicate NBBO quote restatement suppression in `TickHub` for the CCM 44-slot projection pipeline (`isCCM44 == true`), aligning Go projection semantics with `ccm/live/massive_feed.py:510-528` and `ccm_train/tape_source.py:71-105`:
- **`pkg/project/window.go`**: Adds deduplication state (`hasDedupBaseline`, `dedupBidPx`, `dedupAskPx`, `dedupBidSz`, `dedupAskSz`, `dedupDroppedQuotes`) to `SymbolHistory`, initializes/resets them in `InitCCM44()`, and implements `AcceptQuote(bidPx, askPx, bidSz, askSz float64) bool` and `DedupDroppedQuotes() uint64`. Retains existing validity guards inside `IngestPreWindowCCMQuote` and `IngestCCMQuote` as a defensive backstop per the Round 1 plan review observation.
- **`pkg/project/projector.go`**: Gates `TickQuote` processing in both `IngestPreWindowTick` and `IngestTick` (when `p.isCCM44` is true) on `hist.AcceptQuote(...)` prior to `updateSnapshot`, clock progression (`closeSecondBar` / `closeSymbolSecondBar`), phase closing, or quote accumulator mutation.
- **`tests/test_ccm_1hz_projection_parity.py`**: Updates `_load_arena_from_parquet` to filter consecutive duplicate valid quotes before populating the reference `TickArena`.
- **`pkg/project/dedup_test.go`**: Adds unit tests covering `AcceptQuote` edge cases (invalid/zero/negative/NaN prices, `[B, Z(px=0), B]` separator immunity, price/size updates, IEEE 754 `NaN != NaN` size handling) and `Projector` integration tests (`IngestTick` and `IngestPreWindowTick`).

## 2. Correctness & Concurrency Bugs

None identified.
- `SymbolHistory` fields are mutated exclusively by the single-threaded `Projector` ingestion path (`IngestPreWindowTick` and `IngestTick`), introducing no data races.
- Invalid prices (`!(bidPx > 0.0 && askPx > 0.0)`, which also catches `NaN` prices via IEEE 754 negation) return `false` immediately without mutating the baseline or incrementing `dedupDroppedQuotes`.
- `NaN` sizes evaluate `h.dedupBidSz == bidSz` or `h.dedupAskSz == askSz` as `false` under IEEE 754 rules, ensuring quotes with `NaN` sizes are never suppressed as duplicates.

## 3. Projection Math & Temporal Parity

- **Wire-Level Suppression Parity**: Filtering in `IngestTick` before live flushing, historical phase closing, whole-second bar closing, `updateSnapshot`, and `IngestCCMQuote` ensures suppressed duplicate restatements do not advance bar/phase clocks, alter top-of-book snapshots, or inflate `bid_quote_count`, `ask_quote_count`, `nbbo_quote_rate_primitive`, or `nbbo_staleness_ms_primitive`.
- **Pre-Window Continuity**: Filtering in `IngestPreWindowTick` with the same `AcceptQuote` predicate ensures the deduplication baseline transitions seamlessly from pre-window warmup into the active projection window, and suppressed pre-window quotes do not update `updateSnapshot` or `ccmCumQuotes`.
- **Half-Open Interval & Tie-Breaking**: Preserved without modification.

## 4. Deviations from the Approved Plan

None. The implementation follows the approved ledger (`aiChangeLog/2026-10-14/002_quote_restatement_dedupe.md`) and incorporates the Round 1 plan review non-blocking recommendation by keeping the defensive `!(bidPx > 0.0 && askPx > 0.0)` guard inside `IngestPreWindowCCMQuote` and `IngestCCMQuote`.

## 5. Systems & Performance Violations

None.
- **Zero-Allocation Hot Path**: `AcceptQuote` performs only scalar `float64` and `bool` comparisons/assignments on `SymbolHistory` with zero heap allocations.
- **Cross-Language ABI & Cache-Line Isolation**: `/dev/shm` layouts (`GlobalHeader`, `SymbolSnapshot`, `FrameHeader`) are untouched.

## 6. Telemetry / Verification Gaps

None. `SymbolHistory.DedupDroppedQuotes()` exposes the per-symbol suppressed quote counter, and `pkg/project/dedup_test.go` along with `tests/test_ccm_1hz_projection_parity.py` verify both unit-level predicate edge cases and end-to-end 1Hz projection parity.

## 7. Verdict & Remediation

**APPROVED** — The change is surgical, allocation-free on the hot path, faithful to the CCM Python reference specification, and thoroughly covered by unit and parity tests.
