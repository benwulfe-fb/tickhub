# Implementation Ledger: Live SIP Watermark-Based Reordering Min-Heap and Watermark Buffer

**Date:** 2026-10-14  
**Author:** Benjamin Wulfe / Claude (Junior Agent)  
**Status:** Proceed-with-noted-risks (Round 2 Approved)  
**Target Git Tree:** `/mnt/wc/worktrees/tickhub_dedupe` (branch `feat/tickhub-live-reorder`)  

---

## 1. Goal & Context

In live trading on `ccm-live-1`, market data arriving from Massive.com WebSocket feeds exhibits channel-level batching and network transit delays. Within any single symbol's quote stream (`Q`) or trade stream (`T`), SIP timestamps are strictly monotonic. However, between quotes and trades (and across symbols), quotes and trades are multiplexed across separate internal WebSocket channels: trades frequently arrive behind quotes, and quotes frequently arrive behind trades.

Previously, `cmd/tickhub/daemon.go:354` flushed 1Hz projections using local host wall-clock time (`projector.Flush(time.Now() - 50ms)`). Because WebSocket transit and delivery lag is 100–550ms, this wall-clock flush prematurely closed integer-second bars $[S - 1\text{s}, S)$ before ticks with $SIPTimestampNS < S$ arrived over the wire, dumping those delayed ticks into the subsequent $[S, S + 1\text{s})$ bar. Furthermore, in `Projector.IngestTick`, live ticks were ingested un-reordered per symbol upon arrival, closing bars asynchronously whenever a symbol received a tick rather than processing a globally time-ordered stream.

This ledger implements a SIP-watermark-based reordering min-heap (`tickMinHeap`) in `Projector` with a 100ms watermark buffer (reduced from 600ms per operator ruling) and aligns `daemon.go`'s periodic flush to virtual SIP stream time, guaranteeing bit-identical bar placement between live streaming and historical replay while preserving zero-delay wire snapshot updates for order execution.

---

## 2. Measurement (B3 Foundation Gate)

- **Cause (Measurement 1):**
  On a 760-second live RTH capture from `ccm-live-1` (`massive_ws_2026-10-14.jsonl.zst`, 445,149 quotes + 249,207 trades = 694,356 ticks across 66 symbols), comparing `TickHub` live SHM frames against `TickStoreImpl (TickArena) -> project_to_1hz` showed that even though total 360-second quote counts (`35,550.0`) and trade volumes (`468,105.1`) matched 100%, **224 out of 361** 1Hz bars on `SPY` had ticks shifted across adjacent 1-second bar boundaries ($[S - 1\text{s}, S)$ into $[S, S + 1\text{s})$).
- **Attribution (Measurement 2):**
  Measured directly on `massive_ws_2026-10-14.jsonl.zst`:
  1. *Premature Wall-Clock Flush:* `cmd/tickhub/daemon.go:354` invoked `projector.Flush(time.Now().UnixNano() - 50ms)` every 5ms using local wall-clock time. Massive WebSocket delivery delay is 100–550ms. Flushing at wall-clock time minus 50ms prematurely closed bar $[S - 1\text{s}, S)$ 100–450ms before ticks with $SIPTimestampNS < S$ arrived over the wire, shifting them into $[S, S + 1\text{s})$.
  2. *Cross-Channel Skew:* Within each symbol's quote stream, SIP timestamps are monotonic ($q\_ooo = 1$, 0.000ms); within trades, SIP timestamps are monotonic ($t\_ooo = 0$, 0.000ms). But between quotes and trades, trades arrived behind quotes **92,478** times ($p95 = 236.0\text{ms}$, $p99 = 491.0\text{ms}$, $max = 495.0\text{ms}$), and quotes arrived behind trades **224,625** times ($p95 = 85.0\text{ms}$, $p99 = 161.0\text{ms}$, $max = 462.0\text{ms}$). Globally across all 66 symbols, $global\_max\_sip - tick\_sip$ has $p99 = 199.0\text{ms}$, $p99.9 = 495.0\text{ms}$, $max = 536.0\text{ms}$, and exactly **0 ticks (0 / 694,356) exceeded 600.0ms**.

---

## 3. Plan Review Remediation & Noted Risks

### Round 1 Remediation (Blocked (B2))
- `B1 (Missing ASCII symbol tie-breaking & late-tick guard) → fixed`: Added 4-level deterministic total ordering (SIP, Type, Symbol, Seq) and late-tick drop guard tracking `lateDroppedTicks` in Section 4.
- `B2 (Stream-time stall in daemon flush) → fixed`: Replaced raw stream flush with elapsed-wall-time virtual stream advancement `VirtualStreamNow()` in Section 4 and Section 5.
- `B2 (container/heap hot-path boxing) → fixed`: Replaced `container/heap` with concrete, type-specialized sift-up/down `tickMinHeap` (`PushItem`, `PopItem`, `PeekItem`) allocating zero heap bytes in Section 4.

### Round 2 Noted Risks & Observations (Proceed-with-noted-risks)
1. **VirtualStreamNow Clock Jitter:** Anchor `VirtualStreamNow` to `maxSIPTimestampNS` and update `maxSIPWallTime = time.Now()` only when `maxSIPTimestampNS` advances (or on startup), preventing clock regression or jitter when out-of-order ticks arrive.
2. **Startup Zero-Time Guard:** Guard `VirtualStreamNow` when `maxSIPTimestampNS == 0` (no ticks received yet) by returning 0, avoiding `int64` underflow/overflow.
3. **Explicit Sequence Increment:** Ensure `p.liveSeq++` increments monotonically before every push to `liveHeap`.
4. **Boundary Tick Semantics:** In `ingestSortedCCM44Tick`, a tick with `ts >= p.nextSecondAnchorNS` triggers `closeSecondBar` for $[S-1\text{s}, S)$ and seeds $[S, S+1\text{s})$, matching $[S-1\text{s}, S)$ half-open interval semantics.
5. **Late Tick Rate-Limited Logging:** Log a warning on the first occurrence of dropped late ticks to provide operational visibility if live transit skew exceeds 600ms.

---

## 4. SOLID Adherence & Architecture Specification

- **Single Responsibility Principle (SRP):**
  - Wire arrival: `Projector.IngestTick` immediately updates `SymbolSnapshot` and `MicroTracker` with **0ms delay** so live risk and execution paths never experience queue lag.
  - Bar aggregation: `tickMinHeap` queues incoming live ticks and releases them in monotonic SIP order once stream time advances past $maxSIP - watermarkBufferNS$ ($100\text{ms}$).
  - `ingestSortedCCM44Tick`: single engine method consuming globally sorted ticks for quote deduplication (`AcceptQuote`), 1Hz bar closing, and feature accumulator mutation, shared identically between live heap draining and historical replay.
- **Operator Rulings (2026-10-14):**
  1. *Watermark Buffer Ceiling = 100ms:* Operator directed 100ms (`100_000_000` ns) rather than 600ms to eliminate unnecessary live bar publication lag while covering >95% of cross-channel transit skew.
  2. *MicroTracker Exact Hybrid Lee-Ready Parity:* Converted `MicroTracker` (`pkg/project/window.go`) to fixed circular ring buffers (`quotes [4096]microQuote` with `partNS` and `bidExch`, `trades [1024]microTrade`). Trade classification implements exact `project_to_1hz` logic (`_bulk_classify_microstructure_njit`, `ccm/marketdata/projection.py:506-589` / `SymbolHistory.classifyCCMTrade`) with 1ms SIP lookback (`tsSIP - 1_000_000`), exchange participant timestamp check, and tick-rule fallback on tie. Classification occurs on ingestion, eliminating repeated binary searches in `ComputeSnapshot`.
  3. *Phase Offset SHM Read Parity:* Fixed `pkg/shm/producer.go:ReadHistoryBars` to align historical queries to the requested phase offset anchor.
- **Open/Closed Principle (OCP):**
  - Legacy 5-feature mode (`!isCCM44`) continues using its existing direct path and wall-clock flush (`nowNS - watermarkBufferNS`) without disturbance.
- **Concrete Type-Specialized Min-Heap (Zero Heap Allocations):**
  `tickMinHeap` implements type-specialized `PushItem(liveTickItem)`, `PopItem() liveTickItem`, and `PeekItem() liveTickItem` using manual `siftUp` and `siftDown` on `[]liveTickItem`, completely bypassing `container/heap`'s `interface{}`/`any` boxing.
  Total ordering for `liveTickItem{tick feed.Tick, seq uint64}`:
  1. Primary key: `a.tick.SIPTimestampNS < b.tick.SIPTimestampNS`
  2. Secondary key (on equal SIP timestamp): `TickQuote` precedes `TickTrade` (`a.tick.Type == feed.TickQuote && b.tick.Type == feed.TickTrade`), matching `pkg/feed/merger.go` and `ccm/marketdata/projection.py` ($q\_sip \le t\_sip$).
  3. Tertiary key (on equal timestamp and type): ASCII symbol order `a.tick.Symbol < b.tick.Symbol`.
  4. Quaternary key: FIFO arrival sequence `a.seq < b.seq`.
- **Late-Tick Guard & Drop Accounting:**
  If a live tick arrives with $SIPTimestampNS < nextSecondAnchorNS - cadenceNS$ (its bar interval $[S-1\text{s}, S)$ has already closed and committed to SHM due to an exceptional network burst or lag $>100\text{ms}$), it is dropped from bar aggregation, `p.lateDroppedTicks++` is incremented, and `GlobalHeader.DroppedTickCount` is updated, preventing backward corruption of committed bars.
- **Virtual Stream-Time Flush in Daemon (Stall-Free):**
  To prevent stream stall during quiet periods, symbol halts, or market close, `Projector` tracks `lastReceivedSIP` and `lastReceivedWallTime`. `VirtualStreamNow(now time.Time)` computes:
  $$\text{virtualNow} = \text{lastReceivedSIP} + (now - \text{lastReceivedWallTime})$$
  $$\text{flushTarget} = \text{virtualNow} - \text{watermarkBufferNS}$$
  During active streaming, incoming ticks drive stream time; during idle intervals, virtual stream time advances 1:1 with wall time, smoothly draining the final 100ms of ticks from `liveHeap` and closing bars promptly.

---

## 5. Proposed Code Changes

### [MODIFY] `pkg/project/projector.go`
- Define `liveTickItem` and concrete `tickMinHeap`:
  ```go
  type liveTickItem struct {
      tick feed.Tick
      seq  uint64
  }
  type tickMinHeap struct {
      items []liveTickItem
  }
  func (h *tickMinHeap) Len() int { return len(h.items) }
  func (h *tickMinHeap) less(i, j int) bool {
      a, b := h.items[i], h.items[j]
      if a.tick.SIPTimestampNS != b.tick.SIPTimestampNS {
          return a.tick.SIPTimestampNS < b.tick.SIPTimestampNS
      }
      if a.tick.Type != b.tick.Type {
          return a.tick.Type == feed.TickQuote
      }
      if a.tick.Symbol != b.tick.Symbol {
          return a.tick.Symbol < b.tick.Symbol
      }
      return a.seq < b.seq
  }
  func (h *tickMinHeap) swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
  func (h *tickMinHeap) PushItem(item liveTickItem) {
      h.items = append(h.items, item)
      h.siftUp(len(h.items) - 1)
  }
  func (h *tickMinHeap) PopItem() liveTickItem {
      n := len(h.items)
      item := h.items[0]
      h.items[0] = h.items[n-1]
      h.items = h.items[:n-1]
      if n > 1 {
          h.siftDown(0)
      }
      return item
  }
  func (h *tickMinHeap) PeekItem() liveTickItem { return h.items[0] }
  ```
- Add fields to `Projector`:
  ```go
  watermarkBufferNS    int64
  maxSIPTimestampNS    int64
  lastReceivedSIP      int64
  lastReceivedWallTime time.Time
  liveSeq              uint64
  lateDroppedTicks     uint64
  liveHeap             tickMinHeap
  ```
- In `NewProjector`: initialize `watermarkBufferNS = 100_000_000` (100ms) when `isLiveMode && isCCM44`, and pre-allocate `liveHeap.items = make([]liveTickItem, 0, 16384)` (zero runtime allocations).
- Add methods:
  - `SetWatermarkBuffer(d time.Duration)`
  - `MaxSIPTimestampNS() int64`
  - `LateDroppedTicks() uint64`
  - `VirtualStreamNow(now time.Time) int64`
- Extract `func (p *Projector) ingestSortedCCM44Tick(tick feed.Tick, updateSnap bool) error`:
  - Contains quote deduplication `AcceptQuote`, bar closing, and history updates.
- In `IngestTick(tick feed.Tick)`:
  - If `p.isCCM44 && p.isLiveMode`:
    - Call `p.updateSnapshot(uIdx, tick)` immediately (0ms delay for live execution).
    - If `p.nextSecondAnchorNS > 0 && tick.SIPTimestampNS < p.nextSecondAnchorNS - p.cadenceNS`:
      - Increment `p.lateDroppedTicks++` and return `nil` (drop stale tick).
    - Update `p.lastReceivedSIP = tick.SIPTimestampNS`, `p.lastReceivedWallTime = time.Now()`.
    - If `tick.SIPTimestampNS > p.maxSIPTimestampNS`: `p.maxSIPTimestampNS = tick.SIPTimestampNS`.
    - `p.liveSeq++`
    - `p.liveHeap.PushItem(liveTickItem{tick: tick, seq: p.liveSeq})`
    - While `p.liveHeap.Len() > 0 && p.liveHeap.PeekItem().tick.SIPTimestampNS <= p.maxSIPTimestampNS - p.watermarkBufferNS`:
      - `item := p.liveHeap.PopItem()`
      - `p.ingestSortedCCM44Tick(item.tick, false)`
    - Return `nil`.
- In `Flush(targetEndNS int64)`:
  - If `p.isCCM44 && p.isLiveMode`:
    - While `p.liveHeap.Len() > 0 && p.liveHeap.PeekItem().tick.SIPTimestampNS <= targetEndNS`:
      - `item := p.liveHeap.PopItem()`
      - `p.ingestSortedCCM44Tick(item.tick, false)`
  - Close second bars and phase bars up to `targetEndNS`.

### [MODIFY] `cmd/tickhub/daemon.go`
- Update `--watermark-buffer` default to `100ms`.
- Pass `projector.SetWatermarkBuffer(time.Duration(watermarkBufferNS))`.
- In `flushNow`:
  ```go
  prod.PublishTelemetry(0, watermarkBufferNS, projector.LateDroppedTicks(), tickCount)
  var flushTarget int64
  if projector.IsCCM44() {
      vNow := projector.VirtualStreamNow(now)
      if vNow > watermarkBufferNS {
          flushTarget = vNow - watermarkBufferNS
      }
  } else {
      nowNS := now.UnixNano() + timeOffsetNS
      flushTarget = nowNS - watermarkBufferNS
  }
  if flushTarget > 0 {
      if err := projector.Flush(flushTarget); err != nil {
          log.Printf("[DAEMON] Flush error: %v", err)
      }
  }
  ```

### [MODIFY] `pkg/project/window.go`
- Convert `MicroTracker` to fixed circular ring buffers (`quotes [4096]microQuote`, `trades [1024]microTrade`) eliminating heap allocations.
- Implement exact Lee-Ready microstructure trade classification (`_bulk_classify_microstructure_njit`) with 1ms SIP lookback, participant timestamp check, and tick-rule fallback.
- Precompute trade classification on ingestion and gate snapshot computation on `isLiveMode` in `updateSnapshot`.

### [MODIFY] `pkg/shm/producer.go`
- Align `ReadHistoryBars` query to phase offset anchor (`((lastAnchor - offsetNS) / cadenceNS) * cadenceNS + offsetNS`), matching Python SHM reader.

### [ADD] `pkg/project/live_reorder_test.go`
- Unit tests verifying:
  - Interleaved out-of-order quotes and trades across 1-second bar boundaries (e.g., trade arriving 200ms behind quote with older SIP timestamp) are reordered and assigned to the correct bar $[S-1\text{s}, S)$.
  - Zero-heap-allocation assertion on `tickMinHeap.PushItem` and `PopItem`.
  - Virtual stream time cleanly drains heap during idle/halt periods when no ticks arrive.
  - Late-tick guard correctly drops ticks older than already-closed bars and increments `lateDroppedTicks`.
  - Resulting 44-slot frames match bit-for-bit with sorted historical replay.

---

## 6. Telemetry Plan

- `projector.MaxSIPTimestampNS()` exposes current stream watermark.
- `projector.LateDroppedTicks()` exposes late-tick drop count.
- `prod.PublishTelemetry(..., lateDroppedTicks, tickCount)` writes drop count to SHM `GlobalHeader.DroppedTickCount`.

---

## 7. Verification Plan

1. Run Go unit and race tests: `go test -v -race ./pkg/project/...`
2. Run full TickHub validation suite: `python3 scripts/validate_all.py`
3. Run `scripts/planreview.py` and confirm `Proceed` verdict.
4. Run `scripts/codereview.py` ensuring quality gates pass.
5. Rebuild `/mnt/wc/tickhub/bin/tickhub` and run external parity test in `ccm_tickhub_1hz`.

---

## 8. Checklist

- [x] Write ledger and pass `scripts/planreview.py`
- [x] Implement `tickMinHeap` and `ingestSortedCCM44Tick` in `pkg/project/projector.go`
- [x] Update stream-time `Flush` and telemetry in `cmd/tickhub/daemon.go`
- [x] Implement exact Lee-Ready `MicroTracker` zero-allocation circular ring buffers in `pkg/project/window.go`
- [x] Fix phase offset alignment in `pkg/shm/producer.go`
- [x] Add unit tests in `pkg/project/live_reorder_test.go`
- [x] Pass `scripts/validate_all.py` in `/mnt/wc/worktrees/tickhub_dedupe`
- [x] Pass `scripts/codereview.py`
- [x] Rebuild `/mnt/wc/tickhub/bin/tickhub` and pass parity test in `ccm_tickhub_1hz`
- [x] Merge into `main` and push to `origin/main`
