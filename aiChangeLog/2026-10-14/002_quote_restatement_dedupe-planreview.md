# Plan Review — aiChangeLog/2026-10-14/002_quote_restatement_dedupe.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-10-14 18:46:34Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Proceed**_

---

## 1. Summary of the Proposal

The plan adds per-symbol wire-level duplicate NBBO quote restatement suppression to `TickHub` (`pkg/project/window.go` and `pkg/project/projector.go`) when running in CCM 44-slot mode (`isCCM44 = true`), achieving parity with `ccm/live/massive_feed.py:510-528` and `ccm_train/tape_source.py:71-105`. Specifically, `SymbolHistory` gains a 4-tuple baseline (`dedupBidPx`, `dedupAskPx`, `dedupBidSz`, `dedupAskSz`), a `hasDedupBaseline` flag, a `dedupDroppedQuotes` counter, and an `AcceptQuote` predicate. `Projector.IngestTick` and `Projector.IngestPreWindowTick` gate incoming `TickQuote` events on `AcceptQuote` before advancing the projection clock, closing 1Hz bars/phases, updating snapshots, or mutating bar accumulators.

## 2. Simplest Sufficient Design

The plan is already at the minimum required to achieve its stated goal.

## 3. Blocking Defects

None.

## 4. Non-Blocking Observations

- Removing the `bidPx > 0 && askPx > 0` validity guard inside `IngestCCMQuote` and `IngestPreWindowCCMQuote` (`pkg/project/window.go`) assumes no existing unit tests or internal callers invoke those methods directly without first calling `AcceptQuote`.
- `SymbolHistory.DedupDroppedQuotes()` is only inspected in tests; if live daemon observability needs this counter later, it is not currently aggregated or logged at the `Projector` level.

## 5. Methodological & Data-Alignment Concerns

- **1Hz Projection & Clock Semantics:** Gating `Projector.IngestTick` on `AcceptQuote` prior to `closeSecondBar` and `closePhase` ensures duplicate restatements do not artificially advance the watermark clock or alter `nbbo_staleness_ms_primitive`, `nbbo_quote_rate_primitive`, `bid_quote_count`, or `ask_quote_count`.
- **Pre-Window State Alignment:** Applying the same `AcceptQuote` gate in `Projector.IngestPreWindowTick` ensures the deduplication baseline carries cleanly across the pre-window boundary into the active projection window.
- **Cross-Language ABI Parity:** All new fields reside strictly in Go heap/struct state (`SymbolHistory`); `/dev/shm` layouts remain untouched.

## 6. Missing Telemetry

None.

## 7. Verdict

**Verdict: Proceed**

The proposed change is minimal, surgical, and zero-allocation on the hot path. It places the deduplication state and predicate on `SymbolHistory` and gates `Projector.IngestTick` and `Projector.IngestPreWindowTick` before any clock progression, snapshot update, or accumulator mutation occurs, matching the stated Python reference semantics including invalid-price ignore and IEEE 754 `NaN` size handling.
