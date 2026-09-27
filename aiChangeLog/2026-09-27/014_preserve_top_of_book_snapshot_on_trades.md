# 014 — Preserve Top-of-Book Snapshot State Across Trade and Quote Updates

## Goal & Context
`SymbolSnapshot` (128-byte SeqLock-protected struct at `SnapshotOffset + dir_idx * 128` in `/dev/shm`) exposes the unified, real-time top-of-book state for downstream consumers (e.g. execution engines, order pricing, hyper-flinch guards).

In `pkg/project/projector.go`, `updateSnapshot` had two critical defects:
1. **Quote Wiping on Trades (and Trade Wiping on Quotes)**:
   `updateSnapshot` instantiated a brand-new `&shm.SymbolSnapshot{}` on every incoming tick. When a trade arrived (`tick.Type == feed.TickTrade`), it populated only `LastTradePx` and `LastTradeSz`, leaving `BidPx`, `AskPx`, `Midprice`, and `Spread` at 0.0. Writing this snapshot into shared memory wiped out prevailing top-of-book quotes. Conversely, when a quote arrived, `LastTradePx` and `LastTradeSz` were wiped to 0.0.
2. **Heap Allocation in Hot Path**:
   Instantiating `&shm.SymbolSnapshot{}` on every incoming tick violated the zero-allocation hot path invariant (SSoT Invariant 5).

This change ensures top-of-book state is maintained persistently per symbol with zero allocations in steady-state, seamlessly combining trade and quote updates.

## SOLID Adherence
- **Single Responsibility Principle (SRP)**:
  - `Projector`: Single source of truth for rolling window math, feature aggregation, and top-of-book snapshot maintenance.
  - `Producer`: Responsible solely for shared memory layout, SeqLock write fences, and segment memory mapping.
- **Open/Closed Principle (OCP)**:
  - Preserves existing `shm.SymbolSnapshot` memory layout (128 bytes) and `Producer.WriteSnapshot()` API.
- **Liskov Substitution Principle (LSP)**:
  - Consumers reading `SymbolSnapshot` via C, Go, or Python (`TickHubReader.snapshot()`) continue to observe the exact 128-byte struct layout without change.
- **Interface Segregation Principle (ISP)**:
  - Feeds and consumers remain decoupled; consumers query lock-free SeqLock snapshots directly.
- **Dependency Inversion Principle (DIP)**:
  - Snapshot state is driven monotonically by SIP timestamps and incoming feed events.

## Concurrency & Threading Invariant (Remediates B2)
- **Single-Goroutine Execution Invariant**:
  - `p.snapshots []shm.SymbolSnapshot` is a private, heap-resident staging buffer owned strictly by the `Projector`'s ingestion thread.
  - `SeedState()` is invoked strictly during pre-feed initialization before the tick ingestion loop starts.
  - `updateSnapshot()` is invoked strictly sequentially from `Projector.IngestTick()` on the dedicated feed-consumer goroutine.
  - External readers (e.g. `cmd/tickhub/status.go`, observability `/metrics`, Python dataloaders, C consumers) NEVER access `p.snapshots`. All concurrent readers access the shared memory segment directly via `shm.Producer.Snapshot()` or `readSnapshotSeqLock`, which is fully synchronized by atomic SeqLock store-release / load-acquire fences (`target.SeqLockSeq` odd/even).
  - Hence, `p.snapshots` requires no additional locks or atomics, maintaining zero lock overhead and race-detector clean execution under `-race`.

## Proposed Code Changes

### [MODIFY] `pkg/project/projector.go`
- Add `snapshots []shm.SymbolSnapshot` field to `Projector` struct.
- In `NewProjector()`:
  - Allocate `snapshots: make([]shm.SymbolSnapshot, len(uniqueSymbols))`.
- In `SeedState(symbol string, lastBid, lastAsk, lastPrice float64)`:
  - If `uIdx, ok := p.symToUnique[symbol]; ok`:
    - If `lastBid > 0 && lastAsk > 0`:
      - `p.snapshots[uIdx].BidPx = lastBid`
      - `p.snapshots[uIdx].AskPx = lastAsk`
      - `p.snapshots[uIdx].Midprice = (lastBid + lastAsk) / 2.0`
      - `p.snapshots[uIdx].Spread = lastAsk - lastBid`
    - Else if `lastBid > 0`: `p.snapshots[uIdx].BidPx = lastBid`
    - Else if `lastAsk > 0`: `p.snapshots[uIdx].AskPx = lastAsk`
    - If `lastPrice > 0`:
      - `p.snapshots[uIdx].LastTradePx = lastPrice`
    - Publish seeded snapshot to `p.producer.WriteSnapshot(uIdx, &p.snapshots[uIdx])`.
- In `updateSnapshot(uIdx int, tick feed.Tick)`:
  - Bounds check: `if uIdx < 0 || uIdx >= len(p.snapshots) { return }`.
  - Obtain pointer to preallocated staging snapshot: `snap := &p.snapshots[uIdx]`.
  - Update timestamps:
    - `snap.SIPTimestampNS = tick.SIPTimestampNS`
    - `snap.RecvTimestampNS = tick.RecvTimestampNS` (if `tick.RecvTimestampNS > 0`, else `tick.SIPTimestampNS`)
  - If `tick.Type == feed.TickTrade`:
    - Update `snap.LastTradePx = tick.Price`
    - Update `snap.LastTradeSz = tick.Size`
    - Update `snap.TradeExch = tick.Exch`
    - (Preserves existing `BidPx`, `AskPx`, `BidSz`, `AskSz`, `Midprice`, `Spread`).
  - Else (`tick.Type == feed.TickQuote`):
    - Update `snap.BidPx = tick.BidPx`
    - Update `snap.AskPx = tick.AskPx`
    - Update `snap.BidSz = tick.BidSz`
    - Update `snap.AskSz = tick.AskSz`
    - Update `snap.BidExch = tick.BidExch`
    - Update `snap.AskExch = tick.AskExch`
    - If `tick.BidPx > 0 && tick.AskPx > 0`:
      - `snap.Midprice = (tick.BidPx + tick.AskPx) / 2.0`
      - `snap.Spread = tick.AskPx - tick.BidPx`
    - (Preserves existing `LastTradePx`, `LastTradeSz`, `TradeExch`).
  - Call `p.producer.WriteSnapshot(uIdx, snap)` to publish the unified snapshot atomically to SHM via SeqLock.

### [MODIFY] `pkg/project/projector_test.go`
- Add `TestProjectorSnapshotPersistenceAndZeroAlloc`:
  - Initialize `Projector` with mock producer and 3 symbols (`AAPL`, `MSFT`, `SPY`) testing index boundaries (`0` and `len-1`).
  - Seed initial state with `SeedState("AAPL", 150.00, 150.10, 149.95)`. Verify snapshot contains matching bid, ask, mid, and last_trade.
  - Ingest Quote tick on `AAPL`: bid=150.10, ask=150.20. Verify snapshot has bid=150.10, ask=150.20, mid=150.15, last_trade=149.95.
  - Ingest Trade tick on `AAPL`: px=150.18, sz=100.
  - Automated Regression Assertion: Verify `snap.BidPx > 0 && snap.LastTradePx > 0` (guaranteeing that trade does not wipe bid/ask/mid).
  - Ingest Quote tick on `AAPL`: bid=150.12, ask=150.22. Verify snapshot updates bid/ask/mid and retains last_trade=150.18.
  - Verify zero heap allocations in `updateSnapshot` using `testing.AllocsPerRun`.
  - Ingest tick on boundary symbol `SPY` (index `len-1`) and verify correct snapshot publication.

## Telemetry Plan
- `p.producer.Snapshot(uIdx)` in `cmd/tickhub/status.go` and `/metrics` reports valid non-zero top-of-book quotes even under trade-heavy market activity.
- Verify `TickHubReader.snapshot(sym)` in Python returns valid `bid_px > 0`, `ask_px >= bid_px`, and `midprice > 0`.

## Verification Plan
1. **Plan Review Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/planreview.py aiChangeLog/2026-09-27/014_preserve_top_of_book_snapshot_on_trades.md`
2. **Implementation & Unit Tests**:
   - `go test -v -race ./pkg/project/...`
3. **Full Validation Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/validate_all.py`
4. **Code Review Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/codereview.py aiChangeLog/2026-09-27/014_preserve_top_of_book_snapshot_on_trades.md`
5. **Live VM E2E Verification**:
   - Build tickhub daemon binary, deploy to GCP VM, run `/opt/ccm/scripts/verify_snapshot.py`, verify all 72 symbols have valid quotes and orders place without refusal.

## Checklist
- [ ] Working tree clean prior to review
- [ ] Plan review passed (Round 1 or 2)
- [ ] Implement changes in `pkg/project/projector.go`
- [ ] Unit tests pass with zero allocations
- [ ] Full `validate_all.py` passes cleanly
- [ ] Code review approved
- [ ] Commit
