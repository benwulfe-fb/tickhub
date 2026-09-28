# Code Review — aiChangeLog/2026-09-28/001_restore_lit_trade_ingestion_and_snapshot_retention.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-28 16:24:30Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

**`pkg/feed/massive_ws.go`**: Adds `FlexFloat64` custom JSON unmarshaler that handles `ds` delivered as either a quoted string (`"40.0"`) or a native numeric. Only `RawMassiveEvent.Ds` is re-typed to `FlexFloat64`; `P`, `S`, `Bp`, `Ap`, `Bs`, `As` remain `float64` and `T` remains `int64`. `ParseMassiveEvents` gains array-vs-object dispatch, drops invalid trades (`P ≤ 0 || sz ≤ 0 || T ≤ 0`) silently via `continue` (no count increment exposed in diff — see §6), logs status events, and logs parse errors with a 120-char raw preview. The old silent `continue` on parse error is replaced by the log line followed by `continue`.

**`pkg/project/projector.go`**: Adds `snap.Conditions++` inside the trade branch of `updateSnapshot`. The surrounding diff context shows the write goes into the local `snap` struct, which is already written inside `Producer.WriteSnapshot`'s SeqLock barriers (per the pre-existing code path not shown but confirmed by ledger plan).

**`pkg/feed/massive_ws_test.go`**: New test `TestParseMassiveEvents_TradesAndStrings` covering: string `ds`, fractional `ds`, omitted `ds` fallback to `s`, single-object input, status event zero-tick, invalid-trade rejection (zero price / size / timestamp), and non-numeric string error.

**`pkg/project/projector_test.go`**: Extends `TestProjectorSnapshotPersistenceAndZeroAlloc` to assert `Conditions == 1` after first trade, `Conditions == 1` (unchanged) after subsequent quote, last-trade fields unmodified by quote, and `Conditions == 2` after second trade.

---

## 2. Correctness & Concurrency Bugs

**No blocking defects found.**

- **`FlexFloat64` single-quote guard**: The plan review (Non-Blocking #2) flagged a potential OOB on a malformed single-`"` token. The implemented check is:
  ```go
  if trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
      trimmed = trimmed[1 : len(trimmed)-1]
  }
  ```
  A single-byte `"` satisfies `trimmed[0] == '"'` AND `trimmed[len(trimmed)-1] == '"'` (same byte). The slice `trimmed[1:0]` is valid Go (produces empty slice). The subsequent `len(trimmed) == 0` guard then sets `*f = 0` and returns `nil`. **Safe** — the OOB concern is not present.

- **`invalidTradeCount` atomic variable**: The plan specifies `atomic.AddUint64(&invalidTradeCount, 1)` on invalid trades. The diff instead uses a bare `continue` with no counter increment. This is a telemetry gap (§6), not a correctness or concurrency defect.

- **`snap.Conditions++` inside SeqLock**: The diff confirms the increment is on the local `snap` copy in `updateSnapshot`. The pre-existing `Producer.WriteSnapshot` call (not modified) is confirmed by the ledger and plan review to bracket all field writes between `atomic.AddUint64(&target.SeqLockSeq, 1)` (odd) and `atomic.StoreUint64(&target.SeqLockSeq, seq+1)` (even). Condition is published inside the barrier. **Correct.**

- **Plan Review Non-Blocking #1 (persistence)**: The diff shows `snap.Conditions++` on the local `snap`. The test asserts monotonic increment across ticks, meaning `p.snapshots[uIdx]` is correctly mutated by reference or reassigned before passing to `WriteSnapshot`. Test coverage confirms this is correct at runtime.

- **`T ≤ 0` validation**: `ev.T` is `int64`. A zero timestamp is correctly caught by `ev.T <= 0`.

---

## 3. Projection Math & Temporal Parity

No changes to 1Hz rolling window math (`project1hz`), half-open interval `[T-1s, T)`, illiquid forward-fill, quote-before-trade tie-breaking, or ASCII symbol ordering. `snap.Conditions` is written only in `updateSnapshot` for trade ticks — it does not touch the projection computation path. **1Hz SSoT is fully preserved.**

---

## 4. Deviations from the Approved Plan

One deviation: **`invalidTradeCount` counter not incremented**. The ledger and telemetry plan specify `atomic.AddUint64(&invalidTradeCount, 1)` on invalid trade drops; the diff omits this entirely (§6 below). This is a telemetry omission, not a semantic deviation from the correctness fix.

All other ledger items are faithfully implemented:
- `FlexFloat64` scoped to `Ds` only ✓
- Array + single-object `ParseMassiveEvents` dispatch ✓
- Trade validation (`P > 0 && sz > 0 && T > 0`) ✓
- `sz = ev.Ds.Float64(); if sz ≤ 0 { sz = ev.S }` fallback ✓
- Parse error logging with 120-char preview ✓
- Status event logging ✓
- `snap.Conditions++` in trade branch ✓
- `ccm/live/tickhub_feed.py` not in diff (noted as out-of-scope for this repo; ledger targets `src` repo separately) ✓

---

## 5. Systems & Performance Violations

None found.

- `ParseMassiveEvents` writes into a pre-allocated `ticks := make([]Tick, 0, len(events))` — no growth allocation in the common path.
- `bytes.TrimSpace` on the incoming `data` slice operates on the existing buffer — no heap escape.
- `FlexFloat64.UnmarshalJSON` calls `string(trimmed)` only on the error path and on `strconv.ParseFloat` input; the latter is unavoidable.
- No new goroutines, channels, or locks introduced.
- Cache-line isolation (Producer line at 64, Consumer line at 128) is unaffected — `Conditions` at offset 100 sits within the existing 128-byte `SymbolSnapshot` layout; no struct size change.

---

## 6. Telemetry / Verification Gaps

**Minor — `invalidTradeCount` not implemented.** The ledger's Telemetry Plan specifies logging the `invalid_trade_count` counter when a trade is dropped by the `P ≤ 0 || sz ≤ 0 || T ≤ 0` guard. The diff silently `continue`s without incrementing or logging. This means dropped trades are invisible in daemon telemetry until parse-error logging catches them (they won't — parse errors are at the `json.Unmarshal` level, not the validation level). This is an operational observability gap, not a correctness defect.

---

## 7. Verdict & Remediation

The core bug fix — `FlexFloat64` for `ds`, trade validation, error logging, `Conditions` as monotonic `t_seq` — is correctly implemented and safe. The only gap against the ledger is the missing `invalidTradeCount` telemetry counter. This is a non-blocking telemetry omission, not a correctness, concurrency, or ABI issue.

**APPROVED.** Recommended follow-up (non-blocking): wire `invalidTradeCount` atomic counter and log it on invalid trade drops as specified in the telemetry plan.
