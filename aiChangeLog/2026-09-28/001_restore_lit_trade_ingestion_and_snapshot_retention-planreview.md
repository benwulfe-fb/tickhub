# Plan Review — aiChangeLog/2026-09-28/001_restore_lit_trade_ingestion_and_snapshot_retention.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-28 16:18:20Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

# Plan Review — aiChangeLog/2026-09-28/001_restore_lit_trade_ingestion_and_snapshot_retention.md

_Model: Gemini 3.8 Flash · Round: 2 · Tool: planreview.py_

---

## 1. Summary of the Proposal

Massive.com WebSocket feeds deliver fractional trade size `ds` as a JSON string (e.g., `"ds":"40.0"`), causing Go's standard `json.Unmarshal` to fail against `RawMassiveEvent.Ds float64` and `readPump()` to silently drop all trade messages. Consequently, shared memory snapshots retained `last_trade_px: 0.0` and `vol_1s: 0.0`, blocking engine Midday Mode entry qualification. 

The revised plan:
1. Implements `FlexFloat64` custom unmarshaling strictly for `Ds`, while locking all other fields (`P`, `S`, `Bp`, `Ap`, `Bs`, `As`, `T`) to native numeric types (`float64`/`int64`).
2. Validates incoming trades (`P > 0`, `Sz > 0`, `T > 0`), drops invalid frames, and logs parse errors with raw payload previews.
3. Repurposes the unwritten `SymbolSnapshot.Conditions uint32` field (offset 100) as a monotonic trade sequence counter (`t_seq`), updated and published strictly inside the `Producer.WriteSnapshot` SeqLock write barrier.
4. Updates Python `TickHubLiveFeed` to dedup incoming trades using `current_t = t_seq`, eliminating phantom duplicate trades previously triggered by quote arrivals.

---

## 2. Simplest Sufficient Design

The plan is at the simplest sufficient design: surgical modifications to `massive_ws.go`, `projector.go`, and `tickhub_feed.py`, reusing existing struct fields without ABI alterations or unnecessary abstractions.

---

## 3. Blocking Defects

None.

Both Round 1 B1 blocking defects have been fully resolved:
- **Wire types and field scope**: Widening is now restricted strictly to `Ds`, wire formats are documented with verbatim frames, numeric types for timestamps and prices are preserved, and explicit validation drops non-positive values.
- **SeqLock publication**: The plan explicitly locates the `snap.Conditions` write inside `Producer.WriteSnapshot` between the odd-increment and even-store SeqLock memory barriers.

---

## 4. Non-Blocking Observations

1. `pkg/project/projector.go:updateSnapshot`: Ensure `p.snapshots[uIdx]` is mutated by reference or reassigned (`p.snapshots[uIdx] = snap`) so `Conditions` persists monotonically across ticks rather than resetting.
2. `pkg/feed/massive_ws.go:FlexFloat64.UnmarshalJSON`: Add a `len(trimmed) >= 2` guard before `trimmed[1 : len(trimmed)-1]` slicing to defensively prevent index out-of-bounds panics on malformed single-character raw tokens (`'"'`).
3. `pkg/feed/massive_ws.go`: Ensure `invalidTradeCount` is allocated with 64-bit alignment if declared as a package-level or struct variable for safe atomic operations across architectures.
4. `ccm/live/tickhub_feed.py`: If the Python reader starts mid-session with `t_seq > 0`, `self._last_trade.get(sym) != t_seq` evaluates to true and dispatches the snapshot's existing trade once; confirm downstream engine execution handles initial snapshot trade dispatch cleanly.

---

## 5. Methodological & Data-Alignment Concerns

- **Cross-Language ABI Parity**: `SymbolSnapshot` struct size remains invariant at 128 bytes. Reusing `Conditions uint32` at offset 100 preserves identical memory layouts across Go, C, and Python without ABI version bumps.
- **1Hz Projection Semantics**: 1Hz rolling window calculation and illiquid forward-fill logic in `pkg/project/projector.go` remain untouched SSoT; restoring trade ingestion re-enables non-zero `vol_1s` computation.
- **SeqLock Atomic Safety**: Publishing `target.Conditions` between `atomic.AddUint64(&target.SeqLockSeq, 1)` and `atomic.StoreUint64(&target.SeqLockSeq, seq+1)` guarantees concurrent readers in Python/C cannot observe torn state or uncoordinated sequence jumps.

---

## 6. Missing Telemetry

None.

The telemetry plan comprehensively covers invalid trade drops (`invalid_trade_count`), parse failure logging with raw message previews, daemon tick throughput, SHM snapshot inspection, and engine-level `compacted_trades`.

---

## 7. Verdict

**Verdict: Proceed**

The author has resolved all Round 1 blocking concerns. Wire formats are grounded in verbatim payloads, type widening is confined strictly to `ds`, invalid trades are rejected prior to projection, `snap.Conditions` publication is safely contained within SeqLock barriers, and a concrete rollback criterion is defined. The plan is minimal, structurally sound, and ready for implementation.
