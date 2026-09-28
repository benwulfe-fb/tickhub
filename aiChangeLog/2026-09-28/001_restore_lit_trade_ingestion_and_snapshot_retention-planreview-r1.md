# Plan Review — aiChangeLog/2026-09-28/001_restore_lit_trade_ingestion_and_snapshot_retention.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-28 16:16:38Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

## 1. Summary of the Proposal

Three bugs silenced all trade ingestion during a live session: (1) `RawMassiveEvent.Ds` was typed `float64` but the wire format delivers a JSON string, causing `json.Unmarshal` to fail; (2) that failure was silently dropped in `readPump()`; (3) in the Python consumer, trade dedup used `sip_ts` which updates on every quote, causing phantom duplicate suppression. The fix is: introduce `FlexFloat64`/`FlexInt64` custom unmarshalers in `massive_ws.go`, log parse errors, increment `snap.Conditions` on trade ingestion as a monotonic sequence counter, and update the Python consumer to key dedup on that counter.

---

## 2. Simplest Sufficient Design

The minimum fix is:
1. Fix `Ds` type (and whichever other fields are actually string-typed on the wire).
2. Log the parse error instead of dropping it.
3. Fix the Python dedup key.

The plan is at or near that minimum — `FlexFloat64` and `FlexInt64` are the right idiomatic Go pattern for mixed-type JSON fields; `snap.Conditions` repurposing is zero-ABI-cost. No excess structure to cut.

---

## 3. Blocking Defects

**B1 — Partial field fix, goal defeated for other string-typed fields:**

The root cause description (§1) says *"`Ds` was declared as `float64`"* and that is the stated failure. The fix widens `Ds`, `P`, `S`, `Bp`, `Ap`, `Bs`, `As`, and `T` all to Flex types. That breadth is fine — but the plan never states which of those other fields (`Bp`, `Ap`, `Bs`, `As`, `P`, `S`, `T`) are actually delivered as JSON strings on the wire. If none of the quote fields are string-typed, the extra Flex wrapping is harmless. But if `T` (timestamp nanoseconds) or `P` (trade price) are *not* string-typed and the Flex unmarshaler silently coerces a missing or null field to zero, a trade could be ingested with `Price == 0` and `Timestamp == 0` — indistinguishable from "no trade" in the snapshot. The plan does not state the actual wire schema for these fields, and the test matrix (§3, `massive_ws_test.go`) only covers `ds` and `s`; there is no test for `P == 0` on null/missing or for `T` overflow/truncation. This is not a hypothetical: if `T` silently zero-coerces, quote-before-trade tie-breaking (GEMINI.md invariant) fails and projection timestamps are poisoned. **The plan must specify the actual Massive.com wire types for every field it is widening, and add test coverage for null/missing values on `P` and `T`.**

**B1 — `snap.Conditions` repurposing is race-unsafe under SeqLock:**

GEMINI.md names `SymbolSnapshot` as SeqLock-protected (128-byte struct, §5 ABI Parity). SeqLock correctness requires that the writer increments a version word, writes payload fields, then increments again; the reader spins until both version reads are equal and odd/even consistent. `snap.Conditions++` (§3, `projector.go`) is added inside `updateSnapshot()`. The plan does not state whether this increment happens *inside* or *outside* the SeqLock critical section. If it is outside, a concurrent reader can observe `Conditions` incremented while payload (`LastTradePx`, `LastTradeSz`) is not yet visible, producing a spurious `t_seq` change with stale price/size — exactly the phantom-trade scenario the plan is trying to prevent. The plan must explicitly place the `Conditions` write within the SeqLock write region, and the Go race test alone will not catch this (SeqLock is typically implemented with atomics, not `sync.Mutex`).

---

## 4. Non-Blocking Observations

1. `FlexInt64` on `T` (nanosecond timestamp): JSON numbers decode as `float64` in Go's standard library, which loses precision above 2^53 — a nanosecond epoch timestamp overflows that. If the wire sends `T` as a number rather than a string, the float64 intermediate in `FlexInt64` will silently truncate it. Use `json.Number` or `json.RawMessage` internally.
2. The fallback `ev.S.Float64()` when `ds` is missing (§3, `ParseMassiveEvents`) will double-count size if both `ds` and `s` are present and differ; log when the fallback is taken.
3. `ccm/live/tickhub_feed.py` fallback `if t_seq > 0 else (t_px, t_sz, sip_ts)` means the first trade ever (before any `Conditions` increment, if the reader starts mid-session) silently reverts to the broken dedup key; consider initializing `t_seq` sentinel to `-1` or requiring `t_seq >= 0` always.
4. Verification step 4 (live canary) has no defined rollback criterion — specify the observable that triggers rollback (e.g., "if `compacted_trades` remains 0 after 60 s of live feed, revert binary").
5. The checklist (§6) does not include updating `ccm/tests/` for the Python dedup change, creating a test gap in the `src` repo.

---

## 5. Methodological & Data-Alignment Concerns

**ABI / SeqLock (covered as B1 above).** No look-ahead or half-open interval impact: the fix is in ingestion, not projection math. No cross-language ABI size impact: `Conditions` is an existing 128-byte field repurposed in-place; C and Python readers that currently read `conditions` for its original purpose (trade condition flags) will now see a monotone counter — if any C or Python path branches on `Conditions` semantics today, this is a silent semantic change. The plan does not audit existing consumers of `Conditions`.

---

## 6. Missing Telemetry

The plan's telemetry (§4) covers parse error counts and snapshot spot-checks, which is sufficient to confirm ingestion is live. One gap: **count of trades rejected due to `Price == 0` or `Size == 0` after Flex parsing.** Without it, silent zero-coercion from the B1 risk above would not surface in telemetry.

---

## 7. Verdict

**Verdict: Blocked (B1)**

Two B1 defects must be resolved before implementation. First, the plan widens six fields beyond the stated root cause without documenting wire types or covering null/missing in tests — if `P` or `T` silently zero-coerces, the fix ships a new data-corruption path. Second, `snap.Conditions++` placement relative to the SeqLock write boundary is unspecified; if placed outside, readers can observe a spurious `t_seq` increment ahead of the price/size writes, recreating the phantom-trade problem. Both are straightforward to resolve in the ledger before coding begins.
