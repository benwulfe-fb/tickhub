# TickHub System Architecture & Public API Design

**Status:** Draft / Proposal  
**Author:** Staff Systems & Quantitative Infrastructure Engineer  
**Scope:** Developer Ergonomics, Public API Contracts, Memory Layout (`/dev/shm`), and Synchronization Invariants  

---

## 1. Overview & System Architecture

TickHub is a high-performance Go daemon designed to bridge high-frequency raw market data (live WebSocket feeds from Massive.com or historical tick files from Parquet/flat files) to Python-based quantitative research and trading pipelines. 

TickHub projects irregular, high-rate tick streams into structured, fixed-cadence metric snapshots (e.g., 1 Hz NBBO, VWAP, trade volume, order flow imbalances) and writes them directly into POSIX shared memory (`/dev/shm`). Downstream consumers (e.g., PyTorch deep learning models scoring multi-symbol universes) read contiguous memory blocks with zero runtime IPC, zero serialization overhead, and zero copy (`torch.from_numpy`).

### Architectural Block Diagram

```
 +-----------------------------------+     +-----------------------------------+
 |   Massive.com Live WS Feed        |     |   Historical Tick Files           |
 |   (wss://socket.massive.com/...)  |     |   (Parquet / Flat Tapes)          |
 +-----------------+-----------------+     +-----------------+-----------------+
                   |                                         |
                   +-------------------+---------------------+
                                       |
                                       v
                     +-----------------------------------+
                     |          TickHub Daemon           |
                     |           (`tickhub run`)         |
                     |                                   |
                     |  +-----------------------------+  |
                     |  | Ingestion Arena & Demux     |  |
                     |  | (Zero-Alloc Ring Buffers)   |  |
                     |  +--------------+--------------+  |
                     |                 |                 |
                     |                 v                 |
                     |  +-----------------------------+  |
                     |  | Cadence Projection Engine   |  |
                     |  | (1 Hz Static Metric Blocks) |  |
                     |  +--------------+--------------+  |
                     +-----------------+-----------------+
                                       |
                                       v  POSIX Shared Memory (`mmap`)
    =============================================================================
    POSIX Shared Memory: `/dev/shm/tickhub_<name>`
    -----------------------------------------------------------------------------
    [Global Header]          Magic, status, symbols, features, ring geometry
                             Cache-line isolated `last_written` / `last_read`
    -----------------------------------------------------------------------------
    [Symbol Directory]       Fixed-stride symbol names & index mapping (0..N-1)
    -----------------------------------------------------------------------------
    [Latest Snapshot Table]  Top-of-book per symbol (NBBO, last trade, spread)
                             SeqLock-protected, sub-microsecond access
    -----------------------------------------------------------------------------
    [Cadence Ring Buffer]    Contiguous multi-symbol frames: [max_frames][N][D]
                             Directly zero-copy consumable by NumPy / PyTorch
    =============================================================================
                                       |
                                       v  Direct Pointer / `torch.from_numpy`
                     +-----------------------------------+
                     |   Python Quant / ML Consumer      |
                     |   (Single-Process Batch Inference)|
                     |                                   |
                     |  +-----------------------------+  |
                     |  | `TickHubReader` Client      |  |
                     |  +--------------+--------------+  |
                     |                 |                 |
                     |                 v                 |
                     |  +-----------------------------+  |
                     |  | PyTorch Tensor Batch        |  |
                     |  | `torch.from_numpy(features)`|  |
                     |  | Shape: [N_SYMBOLS, D_FEATS] |  |
                     |  +-----------------------------+  |
                     +-----------------------------------+
```

---

## 2. Public Developer Usage (End-to-End Walkthrough)

### 2.1 CLI Daemon Execution

The daemon lifecycle is managed via standard CLI invocation, configuration files, and standard POSIX process signals (`SIGINT`, `SIGTERM`).

```bash
# Validate configuration and calculate shared memory footprint
tickhub validate --config config.yaml

# Run daemon in foreground (systemd / container entrypoint)
tickhub run --config config.yaml

# Optional flag overrides
tickhub run --config config.yaml --mode replay --verbose
```

#### Process Lifecycle & Signals
- **Startup:** Allocates or re-attaches `/dev/shm/tickhub_<name>`. Formats global header, zeroes ring buffer structures, and transitions status from `BOOTING` to `RUNNING`.
- **`SIGINT` / `SIGTERM`:** Enters graceful drain: sets header status to `HALTING`, finishes current cadence interval commit, and exits cleanly. In replay mode, the SHM segment persists for post-run analysis unless `--unlink-on-exit` is passed.
- **`SIGHUP`:** Re-reads logging levels and metrics configuration flags (symbol universe reconfigurations require restart).

---

### 2.2 Configuration Schema (`config.yaml`)

```yaml
version: 1

# Shared memory segment settings
shm:
  name: "prod_1hz"             # Results in /dev/shm/tickhub_prod_1hz
  max_frames: 1024             # Power of 2 required for fast bitmask indexing
  permissions: 0660            # POSIX octal file permissions
  unlink_on_exit: false        # If true, daemon deletes /dev/shm file on SIGTERM

# Operational mode: live | replay
mode: live

# Upstream data sources
source:
  live:
    provider: "massive"
    url: "wss://socket.massive.com/stocks"
    api_key_env: "MASSIVE_API_KEY"
    reconnect_backoff_ms: 1000
    max_reconnect_backoff_ms: 30000
    queue_capacity: 200000     # Internal frame demux queue
  replay:
    data_dir: "/mnt/wc/datalake/parquet"
    start_time: "2026-09-01T09:30:00.000Z"
    end_time: "2026-09-01T16:00:00.000Z"
    playback_speed: 0.0        # 0.0 = as fast as consumer steps (gated)

# Static Symbol Universe (Order defines row index 0..N-1 in batch tensor)
universe:
  symbols:
    - SPY
    - QQQ
    - AAPL
    - MSFT
    - NVDA
    - AMZN
    - GOOGL
    - META
    - TSLA
    - AMD
    # ... up to 32, 64, or 128 configured symbols

# Projection Cadence & Temporal Grid
cadence:
  interval_ms: 1000            # 1000 ms = 1 Hz projection
  anchor_offset_ns: 0          # Alignment offset from wallclock second boundary
  window_s: 1                  # Aggregation window span per frame

# Metric Selection (Statically compiled features toggled via mask)
metrics:
  core: true                   # last_bid_px, last_ask_px, last_trade_px, midprice, spread
  trade_flow: true             # buy_volume, sell_volume, buy_notional, sell_notional, trade_counts
  quote_flow: true             # bid_quote_count, ask_quote_count, high/low bid/ask
  microstructure: true         # nbbo_quote_rate, nbbo_staleness_ms, dark_trade_primitive
  variance: false              # trade_vol_std_px, trade_vol_skew (optional higher compute)

# Synchronization & Flow Control
execution:
  replay_gated: true           # In replay: block producer if ring buffer full
  overrun_policy: "overwrite"  # In live: "overwrite" advances write, signals lag to reader
  spin_yield: true             # Reader/writer spin strategy (PAUSE vs sched_yield)
```

---

### 2.3 Python Consumer API & Usage Walkthrough

The client is a standalone, dependency-minimal Python package (`pip install tickhub` or single-file module) utilizing Python's built-in `mmap`, `ctypes`, and `numpy`. Zero sockets, zero gRPC, and zero deserialization overhead.

#### Complete End-to-End Consumer Pattern

```python
import time
import torch
import numpy as np
from tickhub import TickHubReader

def run_inference_loop():
    # 1. Attach to shared memory (read-only mapping)
    # Reads /dev/shm/tickhub_prod_1hz without copying
    with TickHubReader(shm_name="prod_1hz", mode="live") as hub:
        print(f"Connected to TickHub SHM segment: {hub.shm_name}")
        print(f"Universe: {hub.num_symbols} symbols: {hub.symbols}")
        print(f"Features: {hub.num_features} columns: {hub.feature_names}")

        # Pre-allocate reusable local buffers for zero-allocation access
        snapshot_buf = hub.new_snapshot_buffer()
        
        # 2. Instant Top-Of-Book Query (SeqLock, Zero Wait)
        ok = hub.snapshot("AAPL", snapshot_buf)
        if ok:
            print(f"AAPL Top of Book: Bid={snapshot_buf.bid_px:.2f} "
                  f"Ask={snapshot_buf.ask_px:.2f} "
                  f"Mid={snapshot_buf.midprice:.2f} "
                  f"SIP_NS={snapshot_buf.sip_timestamp_ns}")

        # 3. Main Model Scoring Loop (Windowed Cadence Frames)
        # In live mode: hub.read_latest() gives most recent completed 1Hz frame
        # In replay mode: hub.read_next() steps sequentially through 1Hz frames
        while hub.is_running():
            # Zero-copy view into SHM ring buffer frame
            # frame.features is a 2D numpy array [N_SYMBOLS, N_FEATURES]
            # memory is borrowed directly from /dev/shm
            frame = hub.read_next(timeout_ms=2000)
            if frame is None:
                continue

            # 4. Zero-Copy PyTorch Tensor Conversion
            # torch.from_numpy shares the underlying memory buffer!
            # Shape: [32, 24], dtype: torch.float64 (or float32)
            tensor_batch = torch.from_numpy(frame.features)

            # Move to device / run forward pass
            # (or pin_memory -> async CUDA transfer)
            with torch.no_grad():
                # predictions = model(tensor_batch)
                pass

            # 5. Acknowledge frame consumption (advances last_read)
            # In replay mode, this unblocks the Go daemon to produce frame N+max_frames
            hub.advance_read(frame.sequence)

if __name__ == "__main__":
    run_inference_loop()
```

---

## 3. Shared Memory Layout & Binary ABI Contract

All structures are laid out with strict byte offsets, natural alignment, and 64-byte cache-line isolation to prevent cross-core false sharing between the Go daemon (writer) and Python process (reader).

### 3.1 Memory Segment Map

```
Offset (Hex)      Offset (Dec)      Section Name              Size
--------------------------------------------------------------------------------
0x00000000        0 B               GlobalHeader              1,024 B (1 KB)
0x00000400        1,024 B           SymbolDirectory           3,072 B (3 KB)
0x00001000        4,096 B           LatestSnapshotTable       16,384 B (16 KB)
0x00005000        20,480 B          Reserved / Page Pad       45,056 B (44 KB)
0x00010000        65,536 B          CadenceRingBuffer         Variable (Page Aligned)
                                    [max_frames * FrameBytes]
```

---

### 3.2 Global Header Specification

The global header resides at offset `0x00000000`. Critical producer and consumer sequence counters reside on isolated 64-byte cache lines.

```
Byte Offset   Type      Field Name          Description
--------------------------------------------------------------------------------
0x0000        uint64    magic               Magic number: 0x5449434B48554231 ("TICKHUB1")
0x0008        uint32    version             ABI schema version: 1
0x000C        uint32    status              0=UNINIT, 1=BOOTING, 2=RUNNING, 3=HALTING, 4=CLOSED
0x0010        uint32    mode                0=LIVE_STREAMING, 1=HISTORICAL_REPLAY
0x0014        uint32    num_symbols         N symbols in universe (e.g. 32)
0x0018        uint32    num_features        D metrics projected per symbol (e.g. 24)
0x001C        uint32    max_frames          Ring capacity (must be power of 2, e.g. 1024)
0x0020        uint64    frame_stride_bytes  Byte size of each CadenceFrame
0x0028        uint64    cadence_interval_ns Interval in nanoseconds (1s = 1,000,000,000)
0x0030        int64     daemon_pid          PID of the Go daemon process
0x0038        int64     heartbeat_ns        Monotonic wall clock ns updated by daemon
--------------------------------------------------------------------------------
0x0040..0x007F (64B)    PRODUCER CACHE LINE (Writer Only)
0x0040        int64     last_written_seq    Monotonically increasing committed frame seq
0x0048        uint64    total_ticks_ingest  Cumulative ticks processed by daemon
0x0050        uint64    overrun_count       Number of times writer overwrote unread frame
0x0058..0x007F [40]byte _pad_producer       Padding to end of 64-byte line
--------------------------------------------------------------------------------
0x0080..0x00BF (64B)    CONSUMER CACHE LINE (Reader Only)
0x0080        int64     last_read_seq       Monotonically increasing consumed frame seq
0x0088        int64     consumer_pid        PID of the Python consumer process
0x0090        int64     consumer_heartbeat  Heartbeat ns updated by Python reader
0x0098..0x00BF [40]byte _pad_consumer       Padding to end of 64-byte line
--------------------------------------------------------------------------------
0x00C0..0x03FF [832]byte _reserved          Reserved for future telemetry/extensions
```

---

### 3.3 Symbol Directory Specification

Resides at offset `0x00000400`. Maps dense symbol index `[0..N-1]` to ASCII symbol names.

- Fixed stride: 16 bytes per symbol entry.
- Max symbols supported in fixed directory: 192 (192 × 16 = 3,072 bytes).
- Each entry:
  - `symbol_name`: 8-byte null-padded ASCII string (e.g., `"SPY\0\0\0\0\0"`).
  - `lot_size`: uint32 (default 100 for equities).
  - `symbol_index`: uint16 (0, 1, ... N-1).
  - `flags`: uint16.

---

### 3.4 Latest Snapshot Table Specification (Top of Book)

Resides at offset `0x00001000`. An array of `SymbolSnapshot` entries indexed by `symbol_index`.
Each entry is exactly **128 bytes** (two 64-byte cache lines), containing a dedicated **SeqLock** sequence counter.

```
Byte Offset   Type      Field Name          Description
--------------------------------------------------------------------------------
0x0000        uint64    seqlock_seq         SeqLock sequence: odd=writing, even=stable
0x0008        int64     sip_timestamp_ns    SIP exchange timestamp (nanoseconds)
0x0010        int64     recv_timestamp_ns   Local arrival timestamp at daemon (ns)
0x0018        float64   bid_px              National Best Bid Price
0x0020        float64   ask_px              National Best Ask Price
0x0028        float64   bid_sz              National Best Bid Size (shares or lots)
0x0030        float64   ask_sz              National Best Ask Size (shares or lots)
0x0038        float64   last_trade_px       Last executed trade price
0x0040        float64   last_trade_sz       Last executed trade size
0x0048        float64   midprice            (bid_px + ask_px) / 2.0
0x0050        float64   spread              ask_px - bid_px
0x0058        uint32    bid_exch            Exchange ID for bid
0x005C        uint32    ask_exch            Exchange ID for ask
0x0060        uint32    trade_exch          Exchange ID for last trade
0x0064        uint32    conditions          Condition flags / TRF flags
0x0068..0x007F [24]byte _pad_snapshot       Padded to exact 128-byte boundary
```

---

### 3.5 Cadence Ring Buffer & Frame Specification

Resides at offset `0x00010000` (64 KB, page-aligned). Contains `max_frames` contiguous slots.

#### Structure of a Single `CadenceFrame`:
```
[FrameHeader: 64 Bytes]
    - sequence: int64
    - start_timestamp_ns: int64
    - end_timestamp_ns: int64
    - num_symbols: uint32
    - num_features: uint32
    - flags: uint32 (e.g. 0x01 = partial_window, 0x02 = dropped_ticks)
    - _pad: [32]byte
[Feature Matrix: N_SYMBOLS * N_FEATURES * 8 Bytes]
    - Contiguous Row-Major 2D array of float64
    - Row i corresponds to symbol i from SymbolDirectory
    - Column j corresponds to feature j
```

#### Tensor Memory Layout:
For $N = 32$ symbols and $D = 24$ features:
- Feature payload per frame = $32 \times 24 \times 8 = 6,144$ bytes.
- Total frame stride = $64 \text{ (header)} + 6,144 \text{ (matrix)} = 6,208$ bytes.
- Total ring buffer size for 1,024 frames = $1,024 \times 6,208 \approx 6.06 \text{ MB}$.
- Directly mapped into PyTorch:
  ```python
  # Shape: (32, 24), Strides: (192, 8)
  frame_tensor = torch.from_numpy(np.ndarray(
      shape=(num_symbols, num_features),
      dtype=np.float64,
      buffer=shm_buf,
      offset=frame_offset + 64
  ))
  ```

---

### 3.6 Binary ABI Implementations (Go & Python)

#### Go Struct Definitions (`internal/shm/layout.go`):
```go
package shm

import "unsafe"

const (
	MagicBytes        = 0x5449434B48554231 // "TICKHUB1"
	HeaderOffset      = 0x00000000
	DirectoryOffset   = 0x00000400
	SnapshotOffset    = 0x00001000
	RingBufferOffset  = 0x00010000
)

type GlobalHeader struct {
	Magic            uint64
	Version          uint32
	Status           uint32
	Mode             uint32
	NumSymbols       uint32
	NumFeatures      uint32
	MaxFrames        uint32
	FrameStrideBytes uint64
	CadenceInterval  uint64
	DaemonPID        int64
	HeartbeatNS      int64

	// Producer Cache Line (Aligned to 64 bytes)
	_pad0            [16]byte
	LastWrittenSeq   int64
	TotalTicksIngest uint64
	OverrunCount     uint64
	_padProducer     [40]byte

	// Consumer Cache Line (Aligned to 64 bytes)
	LastReadSeq      int64
	ConsumerPID      int64
	ConsumerHeartbeat int64
	_padConsumer     [40]byte

	_reserved        [832]byte
}

type SymbolSnapshot struct {
	SeqLockSeq       uint64
	SIPTimestampNS   int64
	RecvTimestampNS  int64
	BidPx            float64
	AskPx            float64
	BidSz            float64
	AskSz            float64
	LastTradePx      float64
	LastTradeSz      float64
	Midprice         float64
	Spread           float64
	BidExch          uint32
	AskExch          uint32
	TradeExch        uint32
	Conditions       uint32
	_pad             [24]byte
}

type FrameHeader struct {
	Sequence         int64
	StartTimestampNS int64
	EndTimestampNS   int64
	NumSymbols       uint32
	NumFeatures      uint32
	Flags            uint32
	_pad             [36]byte
}

// Compile-time struct size verifications
const (
	_ = 1 / (1 - (unsafe.Sizeof(GlobalHeader{}) == 1024))
	_ = 1 / (1 - (unsafe.Sizeof(SymbolSnapshot{}) == 128))
	_ = 1 / (1 - (unsafe.Sizeof(FrameHeader{}) == 64))
)
```

#### Python NumPy / Ctypes Layout (`tickhub/abi.py`):
```python
import ctypes
import numpy as np

class GlobalHeaderStruct(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("magic", ctypes.c_uint64),
        ("version", ctypes.c_uint32),
        ("status", ctypes.c_uint32),
        ("mode", ctypes.c_uint32),
        ("num_symbols", ctypes.c_uint32),
        ("num_features", ctypes.c_uint32),
        ("max_frames", ctypes.c_uint32),
        ("frame_stride_bytes", ctypes.c_uint64),
        ("cadence_interval_ns", ctypes.c_uint64),
        ("daemon_pid", ctypes.c_int64),
        ("heartbeat_ns", ctypes.c_int64),
        ("_pad0", ctypes.c_uint8 * 16),
        ("last_written_seq", ctypes.c_int64),
        ("total_ticks_ingest", ctypes.c_uint64),
        ("overrun_count", ctypes.c_uint64),
        ("_pad_producer", ctypes.c_uint8 * 40),
        ("last_read_seq", ctypes.c_int64),
        ("consumer_pid", ctypes.c_int64),
        ("consumer_heartbeat", ctypes.c_int64),
        ("_pad_consumer", ctypes.c_uint8 * 40),
        ("_reserved", ctypes.c_uint8 * 832),
    ]

class SymbolSnapshotStruct(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("seqlock_seq", ctypes.c_uint64),
        ("sip_timestamp_ns", ctypes.c_int64),
        ("recv_timestamp_ns", ctypes.c_int64),
        ("bid_px", ctypes.c_double),
        ("ask_px", ctypes.c_double),
        ("bid_sz", ctypes.c_double),
        ("ask_sz", ctypes.c_double),
        ("last_trade_px", ctypes.c_double),
        ("last_trade_sz", ctypes.c_double),
        ("midprice", ctypes.c_double),
        ("spread", ctypes.c_double),
        ("bid_exch", ctypes.c_uint32),
        ("ask_exch", ctypes.c_uint32),
        ("trade_exch", ctypes.c_uint32),
        ("conditions", ctypes.c_uint32),
        ("_pad", ctypes.c_uint8 * 24),
    ]

class FrameHeaderStruct(ctypes.Structure):
    _pack_ = 8
    _fields_ = [
        ("sequence", ctypes.c_int64),
        ("start_timestamp_ns", ctypes.c_int64),
        ("end_timestamp_ns", ctypes.c_int64),
        ("num_symbols", ctypes.c_uint32),
        ("num_features", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("_pad", ctypes.c_uint8 * 36),
    ]

assert ctypes.sizeof(GlobalHeaderStruct) == 1024
assert ctypes.sizeof(SymbolSnapshotStruct) == 128
assert ctypes.sizeof(FrameHeaderStruct) == 64
```

---

## 4. Synchronization Protocol & Memory Invariants

### 4.1 SeqLock Protocol for Latest Snapshot Table

Top-of-book reads must never stall incoming ticks, and readers must never observe torn multi-word structs (e.g. `bid_px` from tick $K$ paired with `ask_px` from tick $K+1$).

#### Sequence Diagram: SeqLock Write and Read

```
   Go Daemon (Writer)                       Python Consumer (Reader)
          |                                            |
 [Incoming Tick arrives]                               |
          |                                            |
 1. Atomic Add seq, +1 (Odd = Busy)                    |
    Store-Release                                      |
          |                                            |
 2. Write Fields:                                      |
    - bid_px, ask_px, sizes                            |
    - midprice, spread, timestamps                     |
          |                                            |
 3. Atomic Add seq, +1 (Even = Clean)                  |
    Store-Release                                      |
          |                                            |
          |                                  1. Load seq1 (Load-Acquire)
          |                                  2. If seq1 is ODD -> CPU Pause / Retry
          |                                  3. Copy fields into local struct
          |                                  4. Load seq2 (Load-Acquire)
          |                                  5. If seq1 != seq2 -> Torn read, Retry
          |                                            |
          v                                            v
```

#### Memory Invariants:
- On x86-64, standard stores have release semantics, but a compiler barrier (`runtime.KeepAlive` / Go atomic) prevents instruction reordering across the sequence increment boundary.
- On ARM64 (Apple Silicon / AWS Graviton), explicit Store-Release (`atomic.Store` / release barrier) and Load-Acquire instructions are strictly required.

---

### 4.2 Single-Producer Single-Consumer (SPSC) Cadence Synchronization

#### Sequence Tracking:
- Ring slots are indexed via bitwise AND: `slot_idx = sequence & (max_frames - 1)`.
- `last_written_seq`: Sequence number of the most recently finalized frame.
- `last_read_seq`: Sequence number acknowledged by the Python consumer.

#### Invariants by Mode:

```
Mode: LIVE STREAMING
+-------------------------------------------------------------------------------+
| Invariant: Go daemon NEVER blocks on consumer progress.                       |
| Write Step:                                                                   |
|   1. Write frame header + [N][D] feature matrix into slot_idx.                |
|   2. Memory barrier (Store-Release).                                          |
|   3. Atomic Store `last_written_seq` = new_seq.                               |
| Overrun Detection:                                                            |
|   If (last_written_seq - last_read_seq) >= max_frames:                        |
|     - Reader has fallen behind. Increment `overrun_count`.                    |
|     - Python reader catches up by jumping to `last_written_seq - 1`.          |
+-------------------------------------------------------------------------------+

Mode: HISTORICAL REPLAY
+-------------------------------------------------------------------------------+
| Invariant: Go daemon NEVER overwrites unconsumed data (lossless stepping).    |
| Flow Control:                                                                 |
|   While (target_seq - last_read_seq) >= max_frames:                           |
|     - Daemon spins / yields until Python reader advances `last_read_seq`.     |
|   Once space opens:                                                           |
|     - Write frame payload.                                                    |
|     - Store-Release `last_written_seq` = target_seq.                          |
| Consumer Step:                                                                |
|   - Awaits `last_written_seq >= next_expected_seq`.                           |
|   - Processes tensor batch.                                                   |
|   - Store-Release `last_read_seq` = next_expected_seq.                        |
+-------------------------------------------------------------------------------+
```

---

### 4.3 Edge Cases & Failure Recovery

1. **Zombie Readers & Replay Deadlock:**
   - In replay mode, if the Python consumer crashes without updating `last_read_seq`, the Go daemon could block indefinitely.
   - **Mitigation:** The Go daemon monitors `consumer_pid` and `consumer_heartbeat`. If the consumer process dies (verified via `kill(pid, 0)`), the daemon halts replay with an explicit error rather than hanging.

2. **Illiquid Symbols (Zero Ticks in a 1-Second Window):**
   - If an asset does not trade or quote within an interval:
     - Prices (`last_bid_px`, `last_ask_px`, `last_trade_px`): **Forward-filled** from previous interval PIT state.
     - Flow metrics (`buy_volume`, `sell_volume`, `trade_count`): Zero-filled (`0.0`).
     - Microstructure metrics (`staleness_ms`): Incremented by `interval_ms`.
   - Result: Continuous, non-NaN tensor matrices guaranteed for model stability.

3. **Buffer Wrap Math:**
   - All sequence counters are 64-bit signed integers. At 1 Hz cadence, 64-bit counter overflow requires $2^{63} \text{ seconds} \approx 292 \text{ billion years}$, eliminating sequence wrap bugs.
   - Ring slot indexing always uses bitmask: `slot = uint64(seq) & uint64(max_frames - 1)`.

---

## 5. Open Questions & Roadmap

1. **`tickhub-relay` Network Forwarder:**
   - How should multi-node clusters consume TickHub?
   - Candidate: A lightweight companion binary reading `/dev/shm` and broadcasting frames via raw UDP Multicast or kernel-bypass TCP (e.g. Solarflare OpenOnload).

2. **GPU Direct Memory / CUDA IPC:**
   - Can we map the shared memory buffer directly into GPU memory via CUDA Host Mapped Memory (`cudaHostRegister`) to eliminate host-to-device CPU transfer overheads?

3. **Dynamic Universe Subscription:**
   - Should TickHub support intraday universe updates, or is a fixed pre-allocated static universe strictly required to guarantee zero reallocations in shared memory?
   - Recommendation for v1: Static universe defined in `config.yaml`. Intraday universe changes require restarting the daemon with a regenerated segment.

---

*End of Design Document.*
