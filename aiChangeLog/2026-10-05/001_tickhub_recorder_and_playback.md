# Implementation Ledger: TickHub General-Purpose 1Hz Telemetry Recorder & Playback Engine

**Date:** 2026-10-05  
**Author:** Benjamin Wulfe / Claude (Junior Agent)  
**Status:** In Progress (Plan Review Gate - Round 2)  
**Target Git Tree:** `/mnt/wc/tickhub` (branch `main`)  

---

## 1. Goal & Context

TickHub is a high-performance, general-purpose market data projection and shared-memory IPC library. Currently, historical replays stream raw L1/L2 trades and quotes from datalake flat files (`tickhub replay`), while live market data is ingested via Massive WebSocket (`tickhub daemon`) or replicated via TCP relay (`tickhub relay-client`).

**Problem Statement:**
1. Debugging engine execution or testing new ML models previously required waiting until the following morning for full-day datalake flat files to become available.
2. In-flight 1Hz metric projections (top-of-book, imbalance, microprice, rolling multi-second returns, spread basis points) streamed across POSIX SHM are ephemeral and not persisted locally for immediate intraday post-mortem or instant replay.
3. The operator requested:
   - A general-purpose 1Hz telemetry recorder (`pkg/recorder` and `tickhub record`) to buffer 1Hz market snapshots and projections into standardized Apache Parquet format.
   - A companion playback engine (`pkg/playback` and `tickhub playback`) to replay recorded 1Hz Parquet streams back into POSIX SHM segments with consumer flow control and configurable speeds.
   - The TickHub core must remain general-purpose: zero hardcoded CCM paths, fully parameterized via CLI flags and config structs.

---

## 2. Plan Review Dispositions (Round 1)

- **B1 (Lossy `MetricRow` schema cannot reconstruct SHM):** fixed → §3 and §4. `MetricRow` updated to include full canonical `SymbolSnapshot` fields, raw feature vectors (`Features []float64`), and embedded SHM configuration YAML in Parquet file metadata (`tickhub_config_yaml`).
- **B2 (Unthrottled playback lacks consumer flow control):** fixed → §5. Playback creates `Producer` in `ModeHistoricalReplay` and synchronizes with consumer cache line via `WaitConsumerAdvance(anchorNS, timeout)`.
- **B1 (Re-implementing 1Hz feature math violates SSoT):** fixed → §4. Cut all metric recalculation and fallback `SymbolHistory` from `pkg/recorder`; recorder attaches strictly as a consumer to local SHM and dumps engine-projected snapshots and ring buffer frame features directly.
- **B4 (Redundant network ingestion in recorder):** fixed → §4. Cut `-relay-addr` and TCP client code from recorder; network replication remains exclusively in `tickhub relay-client`.

---

## 3. Data Schema & SHM Reconstruction

### 3.1 Parquet Row Schema (`MetricRow`)
Each Parquet row contains the full canonical top-of-book state, phase identity, and exact feature tensor:
```go
type MetricRow struct {
    TimestampNS int64     `parquet:"ts_ns"`
    RecvTSNS    int64     `parquet:"recv_ts_ns"`
    SeqLockSeq  uint64    `parquet:"seqlock_seq"`
    Symbol      string    `parquet:"symbol"`
    PhaseID     uint32    `parquet:"phase_id"`
    PhaseSlot   uint32    `parquet:"phase_slot"`
    BidPX       float64   `parquet:"bid_px"`
    AskPX       float64   `parquet:"ask_px"`
    BidSZ       float64   `parquet:"bid_sz"`
    AskSZ       float64   `parquet:"ask_sz"`
    MidPX       float64   `parquet:"mid_px"`
    SpreadPX    float64   `parquet:"spread_px"`
    SpreadBPS   float64   `parquet:"spread_bps"`
    LastTradePX float64   `parquet:"last_trade_px"`
    LastTradeSZ float64   `parquet:"last_trade_sz"`
    BidExch     uint32    `parquet:"bid_exch"`
    AskExch     uint32    `parquet:"ask_exch"`
    TradeExch   uint32    `parquet:"trade_exch"`
    Conditions  uint32    `parquet:"conditions"`
    Imbalance   float64   `parquet:"imbalance"`
    Microprice  float64   `parquet:"microprice"`
    Vol1S       float64   `parquet:"vol_1s"`
    LogRet1S    float64   `parquet:"log_ret_1s"`
    LogRet5S    float64   `parquet:"log_ret_5s"`
    LogRet15S   float64   `parquet:"log_ret_15s"`
    Features    []float64 `parquet:"features,list"`
}
```

### 3.2 Metadata Preservation
The recorder reads the canonical configuration YAML from SHM (`hdr.ConfigOffset`, `hdr.ConfigLen`) and embeds it into Parquet file metadata via `writer.SetKeyValueMetadata("tickhub_config_yaml", configYAML)`. Playback extracts this metadata to instantiate the destination `Producer` with exact ABI geometry (phases, frame strides, symbol directories).

---

## 4. Proposed Code Changes

### [ADD] `pkg/recorder/types.go`
- `MetricRow` struct definition with Parquet tags and `Features []float64`.
- `Config` struct (`SHMName`, `OutDir`, `OutFile`, `Symbols`, `FlushInterval`, `BatchSize`, `Cadence`).
- `Stats` struct (`TotalRows`, `FlushedRows`, `FilesWritten`, `AnchorLagNS`, `DroppedFrames`, `LastAnchorNS`).

### [ADD] `pkg/recorder/recorder.go`
- `Recorder` struct attaching to local POSIX SHM as consumer.
- Reads `SymbolSnapshot` using SeqLock atomic checks.
- Reads phase frame features directly from SHM ring buffer using store-release ordering.
- Zero feature math or recalculation (SSoT preserved in `pkg/project`).
- Flushes rows to Parquet using `parquet.NewGenericWriter[MetricRow]` with Snappy compression.
- Embeds SHM `tickhub_config_yaml` into Parquet file metadata.

### [ADD] `pkg/recorder/recorder_test.go`
- Unit tests:
  1. Test recording from mock SHM segment containing populated snapshots and phase frames.
  2. Test reading back Parquet rows with `parquet.NewGenericReader[MetricRow]` and asserting exact equality.
  3. Test config YAML metadata extraction.
  4. Test symbol filtering and auto-discovery.

### [ADD] `pkg/playback/types.go`
- `Config` struct (`SHMName`, `SourcePath`, `Speed`, `Loop`, `UnlinkOnExit`, `Cadence`, `FlowControlTimeout`).
- `Stats` struct (`TotalRowsReplayed`, `AnchorsReplayed`, `FlowControlWaitNS`, `ElapsedTime`).

### [ADD] `pkg/playback/playback.go`
- `Engine` struct managing replay loop.
- Reads Parquet files and extracts embedded `tickhub_config_yaml` to initialize `shm.CreateProducer` in `ModeHistoricalReplay`.
- Groups rows chronologically by `TimestampNS`.
- Calls `producer.WaitConsumerAdvance(anchorNS, timeout)` at each anchor before committing frames (guaranteeing unthrottled `speed=0` never laps downstream consumers).
- Writes `SymbolSnapshot` and commits phase frames.
- Paces replay based on `-speed` (`1.0` real-time, `>1.0` fast-forward, `0` unthrottled).

### [ADD] `pkg/playback/playback_test.go`
- Unit tests:
  1. Test reading mock Parquet file and reconstructing SHM segment.
  2. Test `WaitConsumerAdvance` flow control preventing buffer overwrite.
  3. Test speed pacing and unthrottled modes.

### [ADD] `cmd/tickhub/record.go`
- CLI subcommand `tickhub record` (`-shm`, `-out-dir`, `-file`, `-symbols`, `-flush-interval`, `-batch-size`).
- Signal handling on SIGINT/SIGTERM with timeout-bounded final flush.

### [ADD] `cmd/tickhub/playback.go`
- CLI subcommand `tickhub playback` (`-file`, `-dir`, `-shm`, `-speed`, `-loop`, `-no-unlink`).
- Signal handling and status logging.

### [MODIFY] `cmd/tickhub/main.go`
- Register `record` and `playback` subcommands.
- Update `printUsage()` text.

---

## 5. Telemetry & Verification Plan

1. **Unit Tests:**
   - Execute `go test -v -race ./pkg/recorder/... ./pkg/playback/...`
   - Execute full repository test suite: `go test -v ./...`

2. **Integration Verification:**
   - Generate test Parquet recording from mock SHM stream.
   - Run `tickhub playback` into `/dev/shm/tickhub_playback_test` at `speed=0` and verify consumer flow control.
   - Verify snapshots with `tickhub status -shm playback_test`.

---

## 6. Implementation Checklist

- [x] Plan Review Gate passed (`scripts/planreview.py`)
- [x] Implement `pkg/recorder` (`types.go`, `recorder.go`, `recorder_test.go`)
- [x] Implement `pkg/playback` (`types.go`, `playback.go`, `playback_test.go`)
- [x] Implement CLI subcommands (`cmd/tickhub/record.go`, `cmd/tickhub/playback.go`, `cmd/tickhub/main.go`)
- [x] Run `go test -v -race ./pkg/recorder/... ./pkg/playback/...`
- [x] Code Review Remediation (Round 1)
- [x] Run comprehensive repository test suite (`scripts/validate_all.py` / `go test -v -race ./...`)
- [x] Re-run `scripts/codereview.py` and obtain `APPROVED`
- [ ] Commit cleanly to `/mnt/wc/tickhub` on branch `main`

---

## 7. Code Review Remediation (Round 1)

### 7.1 Deconstructed Remediation Checklist
- [x] **Item 1 (Phase Indexing & Ring Buffer Replay in `pkg/playback`):** Map symbol to `(phaseIdx, symbolPhaseIdx)` instead of passing temporal `PhaseSlot` to `CommitSymbolMetrics`.
- [x] **Item 2 (Purge Ad-hoc SSoT Metric Math from `pkg/recorder`):** Remove manual calculation of `imbalance`, `microprice`, and `spreadBps`. Populate scalar columns strictly from the canonical feature tensor according to configuration metadata, or leave 0 if not projected in the feature tensor.
- [x] **Item 3 (Eliminate Data Race on `Engine.stats`):** Synchronize all reads and mutations of `e.stats` using `e.mu` or atomic counters.
- [x] **Item 4 (Fix File Truncation and Clean Shutdown Coordination):** Open Parquet files with `os.O_TRUNC` on initial creation and avoid duplicate `rec.Close()` execution during shutdown in `cmd/tickhub/record.go`.
- [x] **Item 5 (Harden SeqLock Read in Recorder):** Use 1,000-spin loop with `runtime.Gosched()` for SeqLock consistency verification.
- [x] **Item 6 (Fix Fallback Parquet Reader in Playback):** Reset file descriptor offset via `f.Seek(0, io.SeekStart)` prior to reading fallback sample rows.
- [x] **Item 7 (Reset Replay Anchors on Loop):** Call `e.prod.ResetAnchors(firstAnchorNS)` when looping playback to avoid timestamp non-monotonicity.
- [x] **Item 8 (Comprehensive E2E Round-Trip Test):** Implement an end-to-end integration test asserting exact equality of both snapshots and ring buffer phase features through the full pipeline (`SHM -> Recorder -> Parquet -> Playback -> SHM`).

### 7.2 Point-by-Point Remediation Strategy
1. **Item 1 (Phase Indexing):** In `pkg/playback/playback.go`, construct a lookup map `symbolPhaseMap: map[string]struct{ pIdx, symPhaseIdx int }` from `shmCfg.Phases`. Pass `symPhaseIdx` and 0-based `pIdx` to `CommitSymbolMetrics`, checking and propagating all errors.
2. **Item 2 (Zero Math SSoT):** In `pkg/recorder/recorder.go`, remove ad-hoc arithmetic for `imbalance`, `microprice`, and `spreadBps`. Populate scalar metrics directly from the feature tensor based on feature names, maintaining strict adherence to SSoT.
3. **Item 3 (Thread Safety):** In `pkg/playback/playback.go`, protect `e.stats` updates in `commitGroup` and `Run` with `e.mu.Lock()`.
4. **Item 4 (File & Shutdown Hygiene):** In `pkg/recorder/recorder.go`, open files with `os.O_CREATE|os.O_TRUNC|os.O_RDWR`. In `cmd/tickhub/record.go`, remove `r.Close()` invocation inside `rec.Start()` on `ctx.Done()`, ensuring only the main process flow executes final bounded close.
5. **Item 5 (SeqLock Spin):** In `pkg/recorder/recorder.go`, adopt `for spin := 0; spin < 1000; spin++` with `runtime.Gosched()` for SeqLock validation.
6. **Item 6 (File Seek):** In `pkg/playback/playback.go`, add `_, _ = f.Seek(0, io.SeekStart)` before `parquet.NewGenericReader`.
7. **Item 7 (Anchor Reset):** In `pkg/playback/playback.go`, add `e.prod.ResetAnchors(firstAnchorNS)` upon loop restart.
8. **Item 8 (E2E Integration Test):** Create `tests/test_recorder_playback_e2e.go` or `pkg/playback/playback_test.go::TestFullRoundTripE2E` testing the full round trip with rigorous assertions on both top-of-book state and frame ring buffer features.

