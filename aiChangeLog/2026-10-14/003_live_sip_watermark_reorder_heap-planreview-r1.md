# Plan Review — aiChangeLog/2026-10-14/003_live_sip_watermark_reorder_heap.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-10-14 19:15:09Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

The proposal introduces a SIP-watermark-based reordering min-heap (`tickMinHeap`) inside `Projector` (`pkg/project/projector.go`) for live CCM44 mode (`isLiveMode && isCCM44`) with a 600ms watermark buffer (`watermarkBufferNS = 600_000_000`). Upon wire arrival in `IngestTick`, quote deduplication (`AcceptQuote`) and `SymbolSnapshot`/`MicroTracker` updates (`updateSnapshot`) execute immediately at 0ms delay, while bar aggregation ticks are pushed onto `liveHeap` ordered by `(SIPTimestampNS, TickQuote < TickTrade, arrival seq)` and drained into `ingestSortedCCM44Tick` once `SIPTimestampNS <= maxSIPTimestampNS - watermarkBufferNS`. Concurrently, `cmd/tickhub/daemon.go` updates `--watermark-buffer` default to `600ms` and switches `flushNow` in CCM44 mode from wall-clock time (`time.Now() - 50ms`) to stream time (`maxSIP - watermarkBufferNS`).

## 2. Simplest Sufficient Design

The plan is near the minimum sufficient design for live cross-channel tick reordering, with one minor redundant drain path noted in observations.

## 3. Blocking Defects

- **B1 goal-defeating — Missing ASCII Symbol Tie-Breaking on Equal SIP Timestamps**:
  - **Grounding**: Section 1 states the goal of *"guaranteeing bit-identical bar placement between live streaming and historical replay"*, and Section 4 (`[ADD] pkg/project/live_reorder_test.go`) requires that *"Resulting 44-slot frames match bit-for-bit with sorted historical replay."* However, Section 3 specifies the `tickMinHeap` total ordering as:
    1. Primary key: `a.tick.SIPTimestampNS < b.tick.SIPTimestampNS`
    2. Secondary key: `TickQuote` precedes `TickTrade`
    3. Tertiary key: FIFO arrival sequence `a.seq < b.seq`
  - **Why it fails**: In live WebSocket streaming, ticks across symbols arrive in nondeterministic wire order (`a.seq`), whereas historical replay (`pkg/feed/merger.go`) breaks timestamp ties deterministically across parallel feeds (and per-symbol state initialization/anchor advancement or cross-symbol tie-breaking). Furthermore, even within the same symbol or across symbols, if any feature or state depends on deterministic tie-breaking, or if `AcceptQuote` is evaluated at wire arrival (`IngestTick`) *before* `tickMinHeap` rather than in sorted order, consider both:
    1. Quote deduplication (`AcceptQuote`) runs in `IngestTick` *before* pushing to `liveHeap`. Because the changelog proves in Section 2 that within each symbol's quote stream there was an out-of-order quote (`q_ooo = 1`), evaluating `AcceptQuote` on wire arrival order rather than post-heap sorted order can drop or accept different quotes than sorted historical replay if `AcceptQuote` is stateful or order-dependent.
    2. More critically, what happens when a live tick arrives with `tick.SIPTimestampNS < p.maxSIPTimestampNS - p.watermarkBufferNS` (e.g., during a brief network stall > 600ms or after a quiet period followed by a burst)? The plan drains `liveHeap` up to `maxSIP - watermarkBufferNS` and closes bars in `ingestSortedCCM44Tick` / `Flush(targetEndNS)`. If a late tick arrives after its bar $[S-1\text{s}, S)$ has already been closed by `maxSIP - watermarkBufferNS`, the plan does not specify whether `ingestSortedCCM44Tick` corrupts already-closed bar state, panics, or drops the late tick and increments `DroppedTickCount`.

- **B2 loss / data integrity — Stream-Time Stall During Low-Activity / Halt Regimes (`flushNow` Driven Solely by `maxSIP - watermarkBufferNS`)**:
  - **Grounding**: Section 3 (`Stream-Time Flush in Daemon`) and Section 4 (`[MODIFY] cmd/tickhub/daemon.go`) state: *"In `flushNow`: when `projector.IsCCM44()`, flush up to `maxSIP - watermarkBufferNS` (only when `maxSIP > watermarkBufferNS`), bypassing host wall-clock time."* And in `IngestTick`: *"Drain all items where `p.liveHeap[0].tick.SIPTimestampNS <= p.maxSIPTimestampNS - p.watermarkBufferNS`."*
  - **Why it fails**: `maxSIPTimestampNS` only advances when a new tick arrives over the WebSocket. If a tick arrives at $T = 10.000\text{s}$ (closing bar $[9\text{s}, 10\text{s})$) sits in `liveHeap`, it cannot be drained or flushed until `maxSIPTimestampNS >= 10.600\text{s}`. If the feed goes quiet for >600ms (e.g., market close at 16:00:00, symbol halts, sparse pre-market/after-hours, or brief WebSocket pause), `maxSIPTimestampNS` stops advancing. Because `flushNow` bypasses host wall-clock time completely and only flushes to `maxSIP - watermarkBufferNS`, the last 600ms of ticks trapped in `liveHeap` will **never drain** and the final integer-second bar $[S-1\text{s}, S)$ will **never close** until another tick arrives (which at market close or disconnect never happens). Even worse, `IngestTick` already drains `liveHeap` up to `maxSIP - watermarkBufferNS` on every tick arrival, meaning `flushNow(maxSIP - watermarkBufferNS)` in `daemon.go` is a complete no-op whenever no new ticks have arrived! `maxSIP - watermarkBufferNS` in `flushNow` literally never advances between calls to `IngestTick`. Thus, `daemon.go`'s periodic 5ms flush ceases to advance bars during any idle period >= 5ms.

- **B2 performance / hot-path allocation — `container/heap` Interface Boxing on Every Live Tick**:
  - **Grounding**: Section 4 (`[MODIFY] pkg/project/projector.go`) defines `type tickMinHeap []liveTickItem` implementing Go's standard library `container/heap.Interface` (`Push(x any)` and `Pop() any`).
  - **Why it fails**: `container/heap.Push(&p.liveHeap, item)` and `container/heap.Pop(&p.liveHeap)` box `liveTickItem` (which embeds `feed.Tick` + `uint64`) into an `any` (`interface{}`) on every single live tick push and pop, causing 2 heap allocations per live tick on the hot path and violating TickHub's zero-allocation hot-path invariant. Type-specialized siftUp/siftDown methods on `tickMinHeap` (avoiding `container/heap`'s `any` boxing) require ~25 lines of Go and allocate zero bytes once the backing slice is pre-allocated.

## 4. Non-Blocking Observations

1. Calling `AcceptQuote` in `IngestTick` before `updateSnapshot` is right for deduplicating snapshots, but verify `AcceptQuote` is also identical to what historical replay executes in `ingestSortedCCM44Tick` so historical replay does not skip `AcceptQuote`.
2. In `Flush(targetEndNS)` and `IngestTick`, both paths use `<=` (`SIPTimestampNS <= targetEndNS`), whereas half-open bars $[S-1\text{s}, S)$ exclude boundary ticks at $S$; ensure draining `<= targetEndNS` before closing bars up to `targetEndNS` does not prematurely fold a tick at exact timestamp `targetEndNS` into $[targetEndNS-1\text{s}, targetEndNS)$.
3. If `p.liveSeq` increments per tick, increment `p.liveSeq++` explicitly before pushing to `p.liveHeap` (omitted in the pseudocode snippet in Section 4).

## 5. Methodological & Data-Alignment Concerns

- **Half-Open Interval $[T-1\text{s}, T)$ Boundary in `Flush(targetEndNS)`**:
  In Section 4, `Flush(targetEndNS)` drains all heap items with `SIPTimestampNS <= targetEndNS` into `ingestSortedCCM44Tick`, and then closes second bars and phases up to `targetEndNS`. If `ingestSortedCCM44Tick` processes a tick with `SIPTimestampNS == targetEndNS`, that tick belongs to $[targetEndNS, targetEndNS + 1\text{s})$ and will advance `nextSecondAnchorNS` to `targetEndNS + 1s`. Verify that draining `<= targetEndNS` vs `< targetEndNS` does not cause mismatch with `Flush(targetEndNS)` semantics.
- **BBO Update Timing (`updateSnapshot` vs Bar Projection)**:
  Running `updateSnapshot` immediately on wire arrival while delaying `ingestSortedCCM44Tick` by 600ms means `SymbolSnapshot` (SeqLock table) leads the 1Hz ring buffer by up to 600ms (which is intentional per Section 3), provided `ingestSortedCCM44Tick` maintains its own internal per-symbol BBO state for bar spread/midprice calculation and does NOT read from `SymbolSnapshot`. Confirm `ingestSortedCCM44Tick` does not read `SymbolSnapshot` state mutated 600ms in the future.

## 6. Missing Telemetry

- **Late-tick drop / out-of-watermark counter**: The plan relies on a 600ms watermark buffer (`0 / 694,356` ticks exceeded 600ms in one 760s capture). In live production, network stalls or exchange bursts can exceed 600ms. `Projector` must track (e.g., via `DroppedTickCount` in `GlobalHeader` or a late-tick counter) whenever a tick arrives with `SIPTimestampNS` older than the already-flushed bar boundary so silent bar corruption or drops are immediately visible.

## 7. Verdict

**Verdict: Blocked (B2)**

The plan has a critical stream-time liveness defect (B2) and a hot-path allocation defect (B2), alongside an unhandled late-tick/tie-breaking boundary (B1). Specifically, because `IngestTick` already drains `liveHeap` up to `maxSIP - watermarkBufferNS` on every tick arrival, changing `daemon.go:flushNow` to flush exclusively to `maxSIP - watermarkBufferNS` makes the periodic timer flush a no-op when no ticks arrive: `maxSIP` never advances during quiet intervals, symbol halts, or at the 16:00:00 market close, leaving the last 600ms of ticks permanently stuck in `liveHeap` and preventing the current 1Hz bar from closing. Additionally, using `container/heap.Push`/`Pop` boxes `liveTickItem` into `any` on every tick (violating the zero-allocation hot path), and the plan must define how ticks arriving older than the last flushed bar (`SIPTimestampNS < flushedAnchorNS - 1s`) are guarded and counted in `DroppedTickCount`.
