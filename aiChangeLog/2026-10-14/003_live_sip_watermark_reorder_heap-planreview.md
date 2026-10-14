# Plan Review — aiChangeLog/2026-10-14/003_live_sip_watermark_reorder_heap.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-10-14 19:18:43Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

# Plan Review — aiChangeLog/2026-10-14/003_live_sip_watermark_reorder_heap.md

_Model: Gemini 3.8 Flash · Round: 2/2 (Final) · Tool: planreview.py_

---

## 1. Summary of the Proposal

The proposal remediates cross-channel WebSocket transit skew (up to 536ms between quote/trade channels) and premature wall-clock bar closing in `tickhub` (`ccm-live-1`). In `Projector` (`pkg/project/projector.go`), incoming live ticks immediately update `SymbolSnapshot` and `MicroTracker` with 0ms delay for live execution, while bar aggregation pushes ticks to a concrete, type-specialized `tickMinHeap` ordered deterministically by `(SIPTimestampNS, TickQuote < TickTrade, Symbol, arrival seq)`. Ticks are buffered for 600ms (`watermarkBufferNS = 600_000_000`) and drained in sorted order into `ingestSortedCCM44Tick` for quote deduplication (`AcceptQuote`) and 1Hz bar aggregation. Late ticks arriving older than already-committed bars are dropped and tracked via `lateDroppedTicks` in `GlobalHeader.DroppedTickCount`. In `cmd/tickhub/daemon.go`, wall-clock flushing is replaced by `VirtualStreamNow() - watermarkBufferNS`, which advances stream time 1:1 with wall clock during quiet periods and halts to flush trailing ticks.

---

## 2. Simplest Sufficient Design

The plan represents the minimum sufficient design to guarantee deterministic 1Hz bar projection across multiplexed live channels. A watermark-based min-heap reorder buffer with stream-time virtual advancement is the canonical solution for multiplexed streaming data, and implementing concrete `PushItem`/`PopItem`/`PeekItem` methods on `tickMinHeap` eliminates standard library interface boxing allocations on the hot path without external abstractions.

---

## 3. Blocking Defects

None.

---

## 4. Non-Blocking Observations

1. Anchor `VirtualStreamNow` to `maxSIPTimestampNS` (updating wall time only when `maxSIP` advances) rather than `lastReceivedSIP` to prevent virtual stream clock jitter when out-of-order ticks arrive.
2. Guard `VirtualStreamNow` against uninitialized stream state (`maxSIPTimestampNS == 0`) to prevent `time.Now().Sub(time.Time{})` `int64` overflow before the first tick arrives.
3. Explicitly increment `p.liveSeq++` before pushing items to `p.liveHeap` as specified in Section 5.
4. Verify boundary ticks with `SIPTimestampNS == targetEndNS` are attributed to $[targetEndNS, targetEndNS + 1\text{s})$ per half-open interval semantics during `Flush`.
5. Emit a rate-limited log warning on the first occurrence of `lateDroppedTicks > 0` to alert operators if live transit skew exceeds 600ms.

---

## 5. Methodological & Data-Alignment Concerns

- **Half-Open Interval $[T-1\text{s}, T)$ Semantics:** When draining `liveHeap` up to `targetEndNS` with `<=`, confirm that `ingestSortedCCM44Tick` treats a tick at exact timestamp `targetEndNS` as the opening tick of the succeeding interval $[targetEndNS, targetEndNS + 1\text{s})$ rather than folding it into the closing bar.
- **Wire Snapshot vs. Bar Accumulator Decoupling:** Updating `SymbolSnapshot` at 0ms delay while buffering bar aggregation by 600ms intentionally decouples execution state from 1Hz bar features; verify that `ingestSortedCCM44Tick` derives spread and midprice from its internal sorted stream state rather than reading mutable fields from `SymbolSnapshot`.
- **SHM ABI Parity:** `GlobalHeader`, `SymbolSnapshot`, and `FrameHeader` layouts remain untouched; late tick telemetry reuses the existing `DroppedTickCount` field in `GlobalHeader`.

---

## 6. Missing Telemetry

None.

---

## 7. Verdict

**Verdict: Proceed-with-noted-risks**

The round 1 blocking defects (stream stall, `container/heap` interface boxing, missing ASCII tie-breaking, and late-tick accounting) have been fully resolved with solid empirical attribution and zero-allocation concrete heap methods. The noted risks—specifically guarding `VirtualStreamNow` against uninitialized zero-time overflow at startup and anchoring virtual time to `maxSIPTimestampNS` to eliminate clock jitter on out-of-order arrivals—are implementation-line details to be addressed during coding and verified in code review.
