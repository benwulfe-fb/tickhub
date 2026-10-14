# Code Review — aiChangeLog/2026-10-14/003_live_sip_watermark_reorder_heap.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-10-14 20:01:54Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

### 1. Summary of the Implemented Change

The implementation introduces a SIP-timestamp watermark reorder buffer (`tickMinHeap`) in [`pkg/project/projector.go`](file:///mnt/wc/worktrees/tickhub_dedupe/pkg/project/projector.go) and updates [`cmd/tickhub/daemon.go`](file:///mnt/wc/worktrees/tickhub_dedupe/cmd/tickhub/daemon.go) to flush 1Hz bars using virtual SIP stream time (`VirtualStreamNow(now) - watermarkBufferNS`, defaulting to 100ms) instead of raw host wall-clock time.
- **Concrete Reorder Min-Heap (`pkg/project/projector.go`)**: Implements a zero-allocation `tickMinHeap` over `[]liveTickItem` with custom `siftUp`/`siftDown` and 4-key total ordering: `(SIPTimestampNS, TickQuote < TickTrade, Symbol ASCII, arrival seq)`.
- **Decoupled Wire Snapshot vs. Sorted Bar Projection (`pkg/project/projector.go`)**: Live ticks immediately update `SymbolSnapshot` and `MicroTracker` at 0ms wire arrival (`p.updateSnapshot(uIdx, tick)`), while queuing into `liveHeap` and draining into `ingestSortedCCM44Tick` once `SIPTimestampNS <= maxSIPTimestampNS - watermarkBufferNS` or during `Flush(targetEndNS)`. Late tail ticks arriving with `ts < symNextSecondNS - cadenceNS` increment `lateDroppedTicks`, log a one-time warning, and route through `IngestPreWindowCCMTrade`/`IngestPreWindowCCMQuote` to update prevailing state without corrupting closed or active bar accumulators.
- **Exact Point-in-Time `MicroTracker` (`pkg/project/window.go`)**: Replaces the 250ms discretized bucket ring buffer with fixed-size circular arrays (`quotes [4096]microQuote`, `trades [1024]microTrade`) and implements hybrid Lee-Ready trade classification (`classifyTrade` with 1ms SIP lookback, venue participant timestamp check, and tick-rule fallback) on trade arrival.
- **Multi-Phase History Alignment (`pkg/shm/producer.go`)**: Aligns `ReadHistoryBars` anchor calculation to the phase's `OffsetMS` (`((lastAnchor - offsetNS) / cadenceNS) * cadenceNS + offsetNS`).
- **Verification (`pkg/project/live_reorder_test.go`, `pkg/project/dedup_test.go`, `pkg/project/projector_test.go`)**: Adds unit tests covering out-of-order live vs. historical replay bit-identity, zero-allocation heap push/pop, virtual stream-time idle drain, and late tail-tick routing.

### 2. Correctness & Concurrency Bugs

None found.
- `tickMinHeap` bounds checks in `PopItem` (`n > 1` before `siftDown(0)`) and `siftDown` (`left >= n`) are sound, and all callers (`IngestTick`, `Flush`) guard `PeekItem()`/`PopItem()` with `p.liveHeap.Len() > 0`.
- `VirtualStreamNow` (`pkg/project/projector.go:295-304`) guards `p.maxSIPTimestampNS == 0` by returning `0` and clamps negative `elapsed` to `0`, preventing `int64` underflow/overflow at startup or under NTP adjustments.
- `MicroTracker` circular index arithmetic uses bitwise masking `& (maxMicroQuotes - 1)` and `& (maxMicroTrades - 1)` with power-of-two array sizes (`4096` and `1024`), preventing out-of-bounds indexing.

### 3. Projection Math & Temporal Parity

- **Half-Open Interval $[T - 1\text{s}, T)$**: In `ingestSortedCCM44Tick` (`pkg/project/projector.go:531-536`), `secReady := p.nextSecondAnchorNS > 0 && ts >= p.nextSecondAnchorNS` closes $[S - 1\text{s}, S)$ before the tick at `ts == S` is ingested into `SymbolHistory`, preserving half-open $[S - 1\text{s}, S)$ semantics. Terminal as-of phase snapshots (`ts > target`, `pkg/project/projector.go:522`) preserve right-closed `side='right'` semantics.
- **Quote-Before-Trade & Symbol Tie-Breaking**: `tickMinHeap.less` (`pkg/project/projector.go:24-36`) orders `TickQuote` before `TickTrade` at identical `SIPTimestampNS`, then breaks ties by `a.tick.Symbol < b.tick.Symbol`, and finally by monotonic `a.seq < b.seq`.
- **Hybrid Lee-Ready Parity**: `MicroTracker.classifyTrade` (`pkg/project/window.go:408-503`) matches `SymbolHistory.classifyCCMTrade` and `project_to_1hz` (1ms SIP lookback via binary search, same-venue quote check `candB.sipNS <= tsSIP && candB.partNS <= tsPart && candB.sipNS > candA.sipNS`, and tick-rule fallback).

### 4. Deviations from the Approved Plan

- **Late-Tick State Maintenance (`pkg/project/projector.go:507-567`)**: Whereas the ledger prose in Section 5 described returning early on late ticks (`ts < symNextSecondNS[uIdx] - cadenceNS`), the implementation increments `lateDroppedTicks`, skips bar-closing and current-bar accumulation, and routes the tick to `IngestPreWindowCCMTrade` / `IngestPreWindowCCMQuote`. This is strictly superior because it keeps prevailing BBO/last-trade state and Lee-Ready tick-rule baselines accurate without mutating current-bar accumulators, and is explicitly tested in `TestLiveLateTailTickRouting`.
- **All Plan Review Noted Risks Remediated**:
  1. `VirtualStreamNow` is anchored to `maxSIPTimestampNS` and `maxSIPWallTime` (updated only when `ts > p.maxSIPTimestampNS`).
  2. `VirtualStreamNow` returns `0` when `maxSIPTimestampNS == 0`.
  3. `p.liveSeq++` increments monotonically before every `PushItem`.
  4. Boundary ticks at `targetEndNS` close the prior bar before accumulating into the new bar.
  5. Rate-limited warning (`p.warnedLateDrop`) logs on the first late tick drop.

### 5. Systems & Performance Violations

None.
- `tickMinHeap` is pre-allocated with capacity `16384` in `NewProjector` (`pkg/project/projector.go:236`) and confirmed zero-allocation in `TestLiveReorderZeroAllocPushPop`.
- `MicroTracker` uses fixed-size value arrays (`[4096]microQuote`, `[1024]microTrade`, `[256]ccmQuoteEntry`) inline within `Projector.microTrackers`, avoiding heap allocations on the hot path.
- Shared memory `GlobalHeader` cache-line isolation between producer (offset 64) and consumer (offset 128) is untouched.

### 6. Telemetry / Verification Gaps

None. `projector.LateDroppedTicks()` is wired into `prod.PublishTelemetry(0, watermarkBufferNS, projector.LateDroppedTicks(), tickCount)` in `cmd/tickhub/daemon.go:352`, and `pkg/project/live_reorder_test.go` verifies replay parity, zero allocations, idle stream drain, and late tail-tick handling.

### 7. Verdict & Remediation

**APPROVED** — The implementation is clean, zero-allocation on the hot path, adheres to all TickHub 1Hz projection invariants, and resolves all plan review observations.
