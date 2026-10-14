# Implementation Ledger: Duplicate NBBO Restatement Suppression in TickHub

**Date:** 2026-10-14  
**Author:** Benjamin Wulfe / Claude (Junior Agent)  
**Status:** In Review  
**Target Git Tree:** `/mnt/wc/worktrees/tickhub_dedupe` (branch `feat/tickhub-quote-dedupe`)  

---

## 1. Goal & Context

In live trading and offline training (`ccm/live/massive_feed.py:510-528`, `ccm_train/tape_source.py:71-105`), duplicate NBBO quote restatements emitted by data feeds (e.g., massive.com artifacts where consecutive quotes have identical bid price, ask price, bid size, and ask size) are suppressed at the wire. These duplicate restatements do not represent true market events; when unsuppressed, they artificially inflate tick intensity, quote count, and related microstructure intensity primitives (`bid_quote_count`, `ask_quote_count`, `nbbo_quote_rate_primitive`, `nbbo_staleness_ms_primitive`).

Previously, `TickHub` (`pkg/project/window.go`, `pkg/project/projector.go`) ingested all quote ticks with `bidPx > 0 && askPx > 0` directly into cumulative state and 1Hz bar accumulators without checking for duplicate NBBO restatements.

This ledger implements duplicate-NBBO-restatement suppression natively in `TickHub` matching the exact specification and edge cases of `ccm/live/massive_feed.py` and `ccm_train/tape_source.py`:
1. Invalid-priced quotes (`!(bidPx > 0.0 && askPx > 0.0)`) are dropped without updating the deduplication baseline.
2. Consecutive identical valid quotes (`dedupBidPx == bidPx && dedupAskPx == askPx && dedupBidSz == bidSz && dedupAskSz == askSz`) are suppressed and increment `dedupDroppedQuotes`.
3. NaN-sized quotes never compare equal (`NaN == NaN` is false in Go, matching Python IEEE 754 float tuple semantics), so quotes with NaN sizes are always retained.
4. Suppressed duplicate quotes do not advance the projection clock, do not trigger whole-second bar or phase closes, do not update top-of-book / micro snapshots, and do not increment bar quote counters.

---

## 2. SOLID Adherence & Mathematical Specification

- **Single Responsibility Principle (SRP):** `SymbolHistory` owns the per-symbol deduplication baseline state (`hasDedupBaseline`, `dedupBidPx`, `dedupAskPx`, `dedupBidSz`, `dedupAskSz`, `dedupDroppedQuotes`) and the unified decision predicate (`AcceptQuote`). `Projector` owns wire-level quote filtering, clock progression, and bar boundary gating.
- **Open/Closed Principle (OCP):** Deduplication applies strictly to the CCM 44-slot projection pipeline (`isCCM44 = true`) without disrupting legacy 5-feature rolling returns.
- **Parity Specification (`ccm/live/massive_feed.py` & `ccm_train/tape_source.py`):**
  - **Predicate:** For quote $Q = (bp, ap, bs, as)$:
    - If $\neg(bp > 0 \land ap > 0)$: return `false` without modifying baseline. (Invalid quotes do not act as separators between identical NBBOs; e.g. sequence $[B, Z(px=0), B]$ correctly drops the second $B$).
    - If $hasBaseline \land dedupBidPx = bp \land dedupAskPx = ap \land dedupBidSz = bs \land dedupAskSz = as$: increment $dedupDroppedQuotes$ and return `false`.
    - Otherwise: set $hasBaseline = true$, $dedupBidPx = bp$, $dedupAskPx = ap$, $dedupBidSz = bs$, $dedupAskSz = as$, and return `true`.
  - **Single Wire Gate:** In `Projector.IngestTick`, if $tick.Type == TickQuote$:
    ```go
    if p.isCCM44 && tick.Type == feed.TickQuote {
        uIdx, ok := p.symToUnique[tick.Symbol]
        if !ok {
            return nil
        }
        if !p.uniqueHistories[uIdx].AcceptQuote(tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz) {
            return nil
        }
    }
    ```
    If `AcceptQuote` returns `false`, clock progression ($closeSecondBar$, $closePhase$), snapshot updates ($updateSnapshot$), and accumulator updates ($IngestCCMQuote$) are completely bypassed with zero side effects.
  - **Pre-Window Ingestion:** `Projector.IngestPreWindowTick` gates on `hist.AcceptQuote(...)` before calling `updateSnapshot` and `IngestPreWindowCCMQuote`.

---

## 3. Proposed Code Changes

### [MODIFY] `pkg/project/window.go`
- Add deduplication state fields to `SymbolHistory`:
  ```go
  hasDedupBaseline   bool
  dedupBidPx         float64
  dedupAskPx         float64
  dedupBidSz         float64
  dedupAskSz         float64
  dedupDroppedQuotes uint64
  ```
- Add helper methods to `SymbolHistory`:
  - `AcceptQuote(bidPx, askPx, bidSz, askSz float64) bool`: evaluates deduplication, latches baseline on accepted quotes, increments `dedupDroppedQuotes` on duplicates.
  - `DedupDroppedQuotes() uint64`: returns total suppressed duplicate quotes.
- In `InitCCM44()`: reset `hasDedupBaseline = false`, zero `dedup*` floats, and reset `dedupDroppedQuotes = 0`.
- In `IngestPreWindowCCMQuote` and `IngestCCMQuote`: remove redundant validity checks; rely on wire-level `AcceptQuote` filtering.

### [MODIFY] `pkg/project/projector.go`
- In `Projector.IngestTick`: at the beginning of `if p.isCCM44 && tick.Type == feed.TickQuote`, gate on `!hist.AcceptQuote(tick.BidPx, tick.AskPx, tick.BidSz, tick.AskSz)` -> `return nil`.
- In `Projector.IngestPreWindowTick`: gate on `!hist.AcceptQuote(...)` before calling `updateSnapshot` and `IngestPreWindowCCMQuote`.

### [ADD] `pkg/project/dedup_test.go`
- Unit tests verifying:
  - Consecutive identical quotes are suppressed and increment `dedupDroppedQuotes`.
  - Distinct price or size changes are accepted and update the baseline.
  - Quotes with invalid prices (`px <= 0` or `NaN`) are rejected without changing the baseline (`[B, Z(px=0), B]` correctly suppresses the second `B`).
  - NaN sizes are always accepted and never compare equal.
  - Clock advancement, whole-second bar aggregation, and micro snapshot updates are not triggered by duplicate quotes.

---

## 4. Telemetry Plan

- `SymbolHistory.DedupDroppedQuotes()` exposes total suppressed restatements per symbol.
- Unit and integration tests assert exact counts of suppressed duplicate quotes.

---

## 5. Verification Plan

1. Run Go unit and race tests: `go test -v -race ./pkg/project/...`
2. Run full TickHub validation suite: `python3 scripts/validate_all.py`
3. Run `scripts/planreview.py` and confirm `Proceed` verdict.
4. Run `scripts/codereview.py` ensuring all quality gates pass.
5. Rebuild `/mnt/wc/tickhub/bin/tickhub` and run external parity test `pytest tests/ccm/test_tickhub_feed.py` (with `_populate_store_from_parquet` using `_dedupe_quote_mask`) before merging into `main`.

---

## 6. Plan Review Remediation (Round 1 — `Proceed`)

- Verdict: `Proceed` (0 blocking defects).
- Non-blocking observations carried to implementation and `codereview.py`:
  1. Retain `if !(bidPx > 0.0 && askPx > 0.0) { return }` inside `IngestCCMQuote` and `IngestPreWindowCCMQuote` as a defensive backstop for direct callers.
  2. `SymbolHistory.DedupDroppedQuotes()` provides unit and integration test verification.

---

## 7. Checklist

- [x] Write ledger and pass `scripts/planreview.py` (Round 1 — `Proceed`)
- [x] Implement deduplication in `pkg/project/window.go` and `pkg/project/projector.go`
- [x] Add unit tests in `pkg/project/dedup_test.go`
- [x] Pass `scripts/validate_all.py` in `/mnt/wc/worktrees/tickhub_dedupe`
- [x] Pass `scripts/codereview.py` (Round 1 — `APPROVED`)
- [ ] Rebuild `/mnt/wc/tickhub/bin/tickhub` and pass parity test in `ccm_tickhub_1hz`
- [ ] Merge into `main` and push to `origin/main`

