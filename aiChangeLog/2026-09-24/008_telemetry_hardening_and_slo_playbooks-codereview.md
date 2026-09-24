# Code Review — aiChangeLog/2026-09-24/008_telemetry_hardening_and_slo_playbooks.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-24 20:07:25Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

Four surgical telemetry fixes across three Go files and one Python file:

- **`pkg/project/projector.go`**: Captures `startNS := time.Now().UnixNano()` at entry of `closePhase`, computes `publishLatNS` after all feature calculation and `CommitSymbolMetrics` calls, passes it to the renamed `CommitFrameFinalizeWithLatency`.
- **`pkg/shm/producer.go`**: New `CommitFrameFinalizeWithLatency(anchorNS, publishLatNS int64)` atomically stores `publishLatNS` into `AnchorPublishLatencyNS` (when `> 0`), then atomically stores `anchorNS` into `LastWrittenAnchorNS`, logging `[SLO-VIOLATION]` if latency exceeds `15 * time.Millisecond`. Old `CommitFrameFinalize` becomes a zero-latency shim calling the new function. Adds `"log"` import.
- **`pkg/relay/client.go`**: Changes sequence gap condition from `seq != lastPacketSeq+1` to `seq > lastPacketSeq+1` (addresses planreview observation #1), immediately logs `[SLO-VIOLATION]` with expected/got/gap fields. Adds immediate `[SLO-VIOLATION]` log when `latencyUS > 10000`.
- **`python/tickhub/shm.py`**: Adds `import logging`, module-level `logger = logging.getLogger("tickhub.shm")` with `NullHandler`. In `reconnect_if_needed` logs `[RESILIENCE]` on boot ID change and conditional cold-start warning. In `_load_into_matrix` logs `[SLO-VIOLATION] [RESILIENCE]` with gap in ms before the existing realign logic.

---

## 2. Correctness & Concurrency Bugs

**No blocking defects found.** Observations:

- **`producer.go`: Atomic write order** — `AnchorPublishLatencyNS` is stored before `LastWrittenAnchorNS`. This is the correct order: consumers who gate on `LastWrittenAnchorNS` will always see a coherent latency value for the frame. On x86 TSO, two independent `StoreInt64` calls with `LOCK XCHG`/`MOV` semantics are sequentially consistent; on arm64 the Go runtime emits `STLR` for atomic stores, preserving this order. Consistent with planreview observation #3. No bug.
- **`producer.go`: `publishLatNS > 0` guard** — The shim passes `0`, so `AnchorPublishLatencyNS` is not written when called via the backwards-compatible path. This is the specified behavior (backwards-compatible helper "defaulting latency to 0"). Correct.
- **`relay/client.go`: `seq > lastPacketSeq+1` with uint types** — If `seq` and `lastPacketSeq` are unsigned, wrap-around on reconnect could produce a spuriously large difference. However, the fix correctly avoids the negative-gap issue noted in planreview observation #1 (`!=` → `>`). Whether the underlying type risks wrap is outside the visible diff; the sign of `gap` is safely `int64`-cast.
- **`shm.py`: Log before realign** — The warning is emitted before `target_anchor = phase_latest` and `cursor.target_anchor_ns = phase_latest`, so the logged `target` value correctly reflects the stale anchor. No ordering issue.

---

## 3. Projection Math & Temporal Parity

- **Measurement scope**: `startNS` is captured at the very top of `closePhase`, before `pCfg` and slot lookups. This is slightly wider than pure feature-calculation + commit time, but negligible (struct field reads) and fully deterministic. No look-ahead leak introduced.
- **`anchorNS` origin**: `anchorNS` is passed in from the caller (bar-logical timestamp `T`); it is not derived from `time.Now()`. The `publishLatNS` measurement is additive instrumentation only — it does not alter what gets committed to the SHM frame. No temporal parity impact.
- **Half-open interval semantics**: The change touches only the commit path, not feature calculation or `CommitSymbolMetrics`. No change to $[T-1\text{s}, T)$ interval logic, forward-fill, or symbol ordering.
- **SSoT**: `AnchorPublishLatencyNS` remains the sole writable field for publish latency; no duplicate accounting introduced.

---

## 4. Deviations from the Approved Plan

All four planreview non-blocking observations addressed or accounted for:

| Observation | Status |
|---|---|
| #1: `seq > lastPacketSeq+1` to prevent negative gap math | ✅ Implemented exactly |
| #2: Hardcoded 10ms threshold (consider configurability) | Non-blocking; not implemented. Acceptable — ledger does not require it. |
| #3: Atomic write order `AnchorPublishLatencyNS` before `LastWrittenAnchorNS` | ✅ Implemented correctly |
| #4: Callers must attach handlers to surface NullHandler warnings | Non-blocking; outside scope of this diff. Acceptable. |

Ledger plan adherence:
- `CommitFrameFinalize` retained as backwards-compatible shim ✅
- Sequence gap immediate logging ✅
- `latencyUS > 10000` WAN budget check ✅
- Python `[RESILIENCE]` and `[SLO-VIOLATION]` structured log events ✅
- `NullHandler` initialization ✅

No unjustified deviations.

---

## 5. Systems & Performance Violations

- **Hot path allocations**: The `[SLO-VIOLATION]` log paths in Go use `log.Printf` with format strings — these only execute when a violation occurs, so they are not in the 1Hz hot path. The non-violation path is two `atomic.StoreInt64` calls: zero allocation. ✅
- **Python `_load_into_matrix`**: The `logger.warning` f-string is only evaluated inside the `if self._auto_realign` branch, which is the exceptional overrun case. Not on the normal read path. ✅
- **Cache-line isolation**: `AnchorPublishLatencyNS` at offset 64 remains on the Producer cache line. The diff does not touch Consumer cache line layout. ✅
- **`log` import in `producer.go`**: Minimal; `log.Printf` is stdlib, no new dependency. ✅

---

## 6. Telemetry / Verification Gaps

Matching planreview §6 (Missing Telemetry, non-blocking):

1. **No unit test for non-zero `AnchorPublishLatencyNS`**: The diff adds no test asserting that `CommitFrameFinalizeWithLatency` with a synthetic `publishLatNS > 0` results in a readable non-zero value and triggers the SLO log. The ledger smoke-check item ("Verify `AnchorPublishLatencyNS` is non-zero after committing frames") implies this is manual, not automated.
2. **No integration test with injected sequence gap**: The immediate `[SLO-VIOLATION]` relay log is not exercised by a new test in this diff.

Both are pre-existing planreview observations carried forward as known gaps — they do not block approval per the plan verdict. No new gaps introduced by this implementation.

---

## 7. Verdict & Remediation

All four telemetry visibility gaps are closed exactly as specified. The implementation is surgical: no unrelated code touched, no speculative abstractions, no new dependencies beyond stdlib `log`/`logging`. Concurrency correctness is sound (correct atomic store ordering, no races introduced). The `seq > lastPacketSeq+1` fix from planreview observation #1 is applied. Python logging follows the NullHandler convention. The backwards-compatible `CommitFrameFinalize` shim is present. No blocking defects.
