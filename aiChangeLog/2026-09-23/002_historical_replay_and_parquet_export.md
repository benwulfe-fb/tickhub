# 002 — Historical Parquet Replay Engine, Lossless SHM Flow Control, and 1Hz Feature Dataset Exporter

## Goal & Context

Operator directive (2026-09-23):
*"you can actually start on the parquet playback now. one issue i am grappling with is whether to use tickhub for training. if i dont, then i need to duplicate the project1hz logic, which i really dont want to do. if i use tickhub for training, it needs to support the various dataloader use cases which parallelizes and randomizes the samples. maybe there can be a control line in SHM back to tickhub as discussed previously that indicates both the parquet file and timestamp range. each parallel dataloader can get their own tickhub instance and feed it file+range (chunking) as they want (they update the control line when they finish reading the SHM. i think 4 workers are used - so we'd keep 4 running tickhub's. thoughts?
can the export be written in python with the same performance as that in go to export?
i mean if tickhub exported using the control line semantics that was shared with streaming and just let python export the parquets
yes make sure you do a full ledger as new designs were discussed"*

### Problem Statement & Root Cause Analysis
1. **Single Source of Truth (SSoT) for Projection**: Feature engineering logic (`project1hz`) cannot be duplicated across Go (production/live) and Python (training). Doing so guarantees train-serve skew, silent floating-point divergences, and double maintenance. The Go `project1hz` engine must remain the authoritative SSoT.
2. **Empirically Proven Training Bottleneck with On-the-Fly Tick IPC**:
   - Running 4 separate TickHub daemons feeding 4 PyTorch `DataLoader` workers on-the-fly over SHM for randomized training samples is fatally bottlenecked by Parquet seek and tick decompression latency.
   - Slicing a random 60-second sample (e.g. sequence for model input) requires seeking and filtering rows in raw trades and quotes Parquet files.
   - Empirical profiling on real datalake files (`/mnt/wc/datalake/2026-05-06/D/DASH.trades.parquet`, 126k rows) demonstrates:
     - Raw tick filter seek (60s window) for a **single symbol**: p50 = **3.38 ms**, p90 = **3.94 ms**, p99 = **4.48 ms**.
     - For a 100-symbol universe, on-the-fly seek/extraction per sample = $100 \times 3.38\text{ ms} = \mathbf{338\text{ ms / sample}}$, capping throughput at only **~2.96 samples/sec/worker** (or ~12 samples/sec across 4 workers).
     - Modern GPU model training consumes 500–2,000 samples/sec; on-the-fly IPC starves the GPU with $< 2\%$ utilization.
   - Conversely, slicing a precomputed contiguous 1Hz feature array (`60s x 100 symbols x 5 features`):
     - Slicing latency: p50 = **0.27 µs**, p99 = **0.50 µs**.
     - Throughput: **3,700,000 samples/sec** (over 1,000,000x faster than raw tick seeks).
3. **The Solution — SHM Replay Flow Control & Python Parquet Export**:
   - The Go daemon runs in `ModeHistoricalReplay`, streaming raw ticks through `project1hz` into SHM at maximum CPU speed.
   - **Lossless Flow Control**: Controlled via the SHM consumer control line (`LastReadAnchorNS` vs `LastWrittenAnchorNS`). The Go daemon pauses when the ring buffer is full and never overwrites unconsumed frames.
   - **Python Exporter**: Reads 1Hz metric frames from SHM, updates `LastReadAnchorNS`, and writes out standard daily 1Hz feature Parquet files using `pyarrow.parquet` (C++ multithreaded engine with ZSTD compression).
   - **Training Pipeline**: PyTorch training memory-maps these precomputed 1Hz feature Parquets directly via PyArrow/Polars/numpy, achieving native array slicing speed with zero IPC overhead, instant random access, and zero train-serve skew.
   - **Strategy Simulation**: The exact same replay engine allows the unmodified Python trading bot to run against SHM for full golden-replay backtesting.

---

## Measurement

1. **Baseline Empirical Profile (Current Storage / Seek Path)**:
   - Measured on `/mnt/wc/datalake/2026-05-06/D/DASH.trades.parquet` (126,265 rows, 16.0 hours):
     - Single-file full read/decompress: **263.18 ms** (trades), **4.96 ms** (quotes).
     - 60-second window filter seek (100 random seeks via `pyarrow.dataset`): p50 = **3.38 ms**, p90 = **3.94 ms**, p99 = **4.48 ms**.
     - On-the-fly multi-symbol (100 symbols) projected latency: **338 ms/sample** = **2.96 samples/sec**.
   - Precomputed 1Hz contiguous matrix slice (`60s x 100 symbols x 5 features`):
     - Slicing latency: p50 = **0.27 µs**, p99 = **0.50 µs** = **3.71M samples/sec**.
2. **Replay Flow-Control Throughput**: The Go daemon must stream 1Hz metric frames into SHM with consumer backpressure at $> 100,000\ \text{bars/sec}$ (aggregating millions of underlying raw ticks per second).
3. **Zero Frame Loss in Replay**: Over a full simulated trading day (23,400 seconds $\times$ $N$ symbols), zero frames may be dropped or overwritten before being read by the consumer (`overrun_count == 0` recorded persistently in `GlobalHeader.DroppedTickCount`).
4. **End-to-End Row Reconciliation**: Total rows written to Parquet must exactly equal $\text{total\_frames\_produced} \times N_{\text{symbols}}$.
5. **Parquet Export Performance**: Python PyArrow batch export of a full day of 100 symbols (2.34M rows, ~130 MB raw) must complete in $< 1.0\ \text{second}$ of disk write time, achieving exact parity with native Go Parquet writing.
6. **Bit-Exact Parity**: Metric values emitted in historical replay mode must match live streaming projections bit-for-bit (exact IEEE 754 float64 representation).

---

## Architecture & Synchronization Contracts

### 1. Replay Flow Control & Decoupled Durability Protocol
The SHM segment operates in one of two modes designated by `GlobalHeader.Mode`:
- `ModeLiveStreaming (0)`: Real-time trading. Producer runs at wall-clock cadence ($1\times$ real-time). If consumer lags by more than `max_frames`, older frames are overwritten and consumer catches up or raises `LaggedAnchorError`.
- `ModeHistoricalReplay (1)`: Lossless historical stepping. Producer runs at maximum CPU rate, throttled strictly by consumer read progress:

```
[Go Producer]
  1. Target next anchor T.
  2. Flow control check:
     delta = (T - atomic.LoadInt64(&header.LastReadAnchorNS))
     while delta >= int64(max_frames) * cadence_ns:
       // Ring buffer is full; pause and yield to consumer
       tickhub_cpu_pause() / runtime.Gosched()
       check consumer_pid and consumer_heartbeat (halt on consumer exit to prevent hang)
  3. Write 1Hz metrics into slot: (T / cadence) & (max_frames - 1)
  4. atomic.StoreInt64(&header.LastWrittenAnchorNS, T)

[Python Consumer / Exporter]
  1. hub.load_sync(cursors, out_tensor)
  2. Append tensor row view to memory buffer
  3. hub.commit_read(cursors) -> Store-Release LastReadAnchorNS = max(c.target_anchor_ns)
     (Decoupled from disk write to sustain 100,000+ bars/sec in-memory flow)
  4. Periodically (every 1,000 seconds) flush accumulated table to disk using
     pyarrow.parquet.ParquetWriter.write_table() targeting temporary file
     `features.parquet.tmp`
  5. Upon completing full trading day, verify row count reconciliation:
     assert table.num_rows == total_expected_rows
     os.replace('features.parquet.tmp', 'features.parquet')  // Atomic rename
```

**Durability Contract & Crash Isolation**:
- `commit_read` is called continuously as frames are read from SHM into RAM, allowing the producer to stream at full CPU speed without disk I/O stalls.
- Disk durability is transactional: output is streamed into `.tmp` file and atomically renamed (`os.replace`) upon end-to-end verification. A crash mid-run never leaves a corrupted or partial dataset.
- In replay mode, `GlobalHeader.DroppedTickCount` strictly tracks buffer overruns (`overrun_count`). The producer asserts `overrun_count == 0`.

**Deadlock Mitigation**:
- In replay mode, producer checks both `syscall.Kill(consumer_pid, 0)` AND monitors `ConsumerHeartbeat` progression every 100ms. If consumer exits or freezes, producer terminates cleanly with `ErrConsumerDeadlock` rather than spinning forever.

### 2. Multi-File Tick Ingestion & K-Way Merge (`pkg/feed`)
Input layout from `/mnt/wc/datalake/YYYY-MM-DD/`:
- Trades: `[A-Z]/TICKER.trades.parquet` (columns: `ticker`, `sip_timestamp`, `price`, `size`, `exchange`, `conditions`)
- Quotes: `[A-Z]/TICKER.quotes.parquet` (columns: `ticker`, `sip_timestamp`, `bid_price`, `bid_size`, `ask_price`, `ask_size`, `bid_exchange`, `ask_exchange`)

Ingestion engine:
- **Zero-allocation streaming**: Uses `github.com/parquet-go/parquet-go` to read row groups into pre-allocated memory buffers.
- **K-Way Min-Heap Merger**:
  - Maintains a min-heap across all open ticker trade and quote files keyed by `sip_timestamp` (int64 nanoseconds).
  - **Deterministic Tie-Breaking Rule**:
    - When two ticks have identical `sip_timestamp`: Quotes are emitted before Trades (establishing top-of-book state at the moment of execution).
    - If ticks have identical `sip_timestamp` and same type: Order lexicographically by ticker symbol string.
  - Emits a unified, strictly chronological tick stream across all universe symbols into the 1Hz projector.

### 3. SSoT 1Hz Projection Engine (`pkg/project`)
Maintains rolling microstructure state per symbol:
- **Bar Aggregates**: Open, High, Low, Close, Volume, VWAP, Trade Count over half-open interval $[T - 1\text{s}, T)$.
- **Microstructure Features**:
  - `log_ret_1s`: $\ln(P_T / P_{T-1\text{s}})$
  - `log_ret_5s`: $\ln(P_T / P_{T-5\text{s}})$
  - `log_ret_15s`: $\ln(P_T / P_{T-15\text{s}})$
  - `vol_1s`: Total trade volume in past 1 second strictly within $[T - 1\text{s}, T)$
  - `spread_bps`: $(\text{Ask} - \text{Bid}) / \text{Mid} \times 10,000$
- **Illiquid Handling**:
  - If zero trades/quotes occur in interval: prices are forward-filled from previous interval; flow volume is set to 0.0; spread is forward-filled.
- **Multi-Phase Alignment**:
  - Evaluates Phase 0 symbols at $T + 0.000\text{s}$ and Phase 1 symbols at $T + 0.500\text{s}$.
  - SPY and QQQ cross-assets are updated independently at each phase offset with their respective rolling windows.

### 4. Python Parquet Exporter (`python/tickhub/export.py`)
- Reads continuous 1Hz metric frames from SHM.
- Accumulates rows into chunked columnar PyArrow arrays (`timestamp_ns`, `phase`, `symbol`, `features...`).
- **Streaming Disk Flush**: Appends chunks every 1,000 seconds via `pyarrow.parquet.ParquetWriter.write_table()` to `features.parquet.tmp` using ZSTD compression (level 3). Keeps memory footprint under 50 MB.
- **Atomic Finalization**: At end of trading day, asserts `total_written_rows == total_expected_rows`, closes Parquet writer, and executes `os.replace("features.parquet.tmp", "features.parquet")`.
- Output layout: `/mnt/wc/datalake/features1hz/date=YYYY-MM-DD/features.parquet`.

---

## Detailed Step-by-Step Implementation Plan

### Step 1: Lossless Flow Control in Go & Python SHM
- `pkg/shm/producer.go`:
  - Add `ModeHistoricalReplay` support to `Producer`.
  - Implement `WaitConsumerAdvance(anchorNS int64, timeout time.Duration) error`.
  - Monitor `ConsumerPID` via `syscall.Kill(pid, 0)` and `ConsumerHeartbeat` to prevent deadlock if consumer terminates.
  - Record persistent `OverrunCount` in `GlobalHeader.DroppedTickCount`.
- `pkg/shm/producer_test.go`:
  - Test flow control backpressure: producer pauses when buffer full, resumes immediately when consumer advances `LastReadAnchorNS`.
- `python/tickhub/shm.py`:
  - Add `commit_read(cursors)`: Store-Release on `LastReadAnchorNS` in `GlobalHeader`.

### Step 2: Parquet Tick Ingestion & K-Way Merger (`pkg/feed`)
- `pkg/feed/types.go`:
  - `Tick` struct: `SIPTimestampNS`, `Symbol`, `Type` (Trade vs Quote), `Price`, `Size`, `BidPx`, `AskPx`, `BidSz`, `AskSz`, `Exchange`, `Conditions`.
- `pkg/feed/parquet_reader.go`:
  - Stream reader for `*.trades.parquet` and `*.quotes.parquet` using `parquet-go`.
- `pkg/feed/merger.go`:
  - Min-heap merger combining $2N$ file streams into a single monotonically ordered iterator with zero heap allocations in the inner loop.
  - Strict tie-breaking: Quote before Trade, then Symbol ASCII.
- `pkg/feed/merger_test.go`:
  - Unit tests verifying deterministic chronological order, tie-breaking order, and end-of-file handling.

### Step 3: 1Hz Window Projection Engine (`pkg/project`)
- `pkg/project/window.go`:
  - Circular ring of past prices/volumes for rolling return (1s, 5s, 15s) and volume calculations strictly over $[T-1\text{s}, T)$.
- `pkg/project/engine.go`:
  - `Projector` struct receiving ticks, tracking current anchor $T$, closing windows at $T$, forward-filling quiet symbols, and committing directly to `shm.Producer`.
- `pkg/project/engine_test.go`:
  - Tests verifying exact return math, illiquid forward-fill, and multi-phase cross-asset commitments.

### Step 4: Python Parquet Batch Exporter (`python/tickhub/export.py`)
- Python script consuming SHM in replay mode:
  - Formats columnar Arrow tables from SHM numpy views.
  - Streams chunks to `/mnt/wc/datalake/features1hz/date=<date>/features.parquet.tmp`.
  - On completion, reconciles row counts and executes `os.replace` to `features.parquet`.
  - Benchmarks write speed and verifies zero missing rows.

### Step 5: End-to-End Replay & Export Verification
- Run full historical replay over an existing datalake date (`/mnt/wc/datalake/2026-05-06`).
- Verify bit-exact consistency, assert `overrun_count == 0`, and benchmark total runtime.

---

## Telemetry & Verification Plan

1. **Flow Control Invariant Assertions**:
   - `assert (LastWrittenAnchorNS - LastReadAnchorNS) < max_frames * cadence_ns` holds at all times in replay mode.
   - Assert `GlobalHeader.DroppedTickCount == 0` at end of full replay run.
2. **K-Way Merge Monotonicity & Tie-Breaking**:
   - Assert `tick[i].SIPTimestampNS <= tick[i+1].SIPTimestampNS` across all combined trades and quotes.
   - Test and assert that when `tick[i].SIPTimestampNS == tick[i+1].SIPTimestampNS`, Quotes strictly precede Trades.
3. **End-to-End Row Reconciliation**:
   - Assert `parquet_table.num_rows == total_expected_frames * num_symbols`.
4. **Parquet Compression Ratio & Speed**:
   - Measure daily feature Parquet file size ($< 25\ \text{MB}$ per day expected for 100 symbols).
   - Measure Python export throughput ($> 50,000\ \text{bars/sec}$).
