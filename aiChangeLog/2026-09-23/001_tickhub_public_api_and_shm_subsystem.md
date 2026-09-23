# 001 — TickHub Public API Design, Temporal Anchors, and Standalone SHM Subsystem

## Goal & Context

Operator directive (2026-09-23):
*"1. watermark must still be implemented inside of tickhub. its used to ensure we have complete data for the 1hz data
2. same with project1hz. - its functionality lives on in tickhub.
main question is how does the engine ensure it has all data from the target anchor and support the phase approach where 30 symbols are in phase 0 and 30 symbols are in phase 500ms... i dont want synchronization between the processes or a shared monitor/mutex/signal. the other concern i have is that the anchor must be load bearing. i do not want bugs to arise because a process grabbed a sequence number and didnt realize it was for the wrong anchor... tickhub publish a singular anchor-publish latency... client engine does a zero-cpu sleep until the expected arrival of the 30 symbols and then loops to scan all symbols not acquired yet in a busy loop until all 30 are collected... on barrier -- implement a proper barrier alternative for python...
important to note - a symbol such as SPY and QQQ may belong to multiple phases. these cross asset symbols must be recomputed with project1hz at each phase independently because they are cross assets...
load_symbol_history should return a cursor to be able to read forward from that point. calling 'get_latest_anchor' allows for missing anchors if there is a hiccup. due to this, lets get rid of get_latest_anchor and rename load_symbol_history to 'begin' (or 'start') which returns a cursor (that has next)... client hold collection of symbol cursors and pass them to 'hub.load()' which does not take a phase, but a collection of cursors along with an out_matrix...
add a 'begin_all' helper that creates cursors for everything and returns the collection -- which is then later filtered by phase to process... cursors may fall behind 'tip' and still work, but be very suboptimal because its not the latest. at the same time, the client must not have any holes in the data... add method called staleness on a cursor, which returns the difference between UTC now and cursor.anchor+L(latency)...
does python have async? load should really be async. everything else can be sync."*

TickHub (`tickhub`) is an independent, greenfield open-source project (`github.com/benwulfe-fb/tickhub`). Its mission is to build a standalone, high-performance Go market data daemon that ingests tick streams, performs windowed 1Hz metric projections with zero allocations in the hot path, and fans out contiguous multi-symbol metric frames directly into POSIX shared memory (`/dev/shm`). 

The primary downstream consumer is an async Python trading engine running multi-phase batch model inference (e.g., scoring Phase 0 at 00.000s and Phase 1 at 00.500s).

This ledger establishes the public developer API contracts, temporal synchronization semantics, and Phase 1 implementation specification for the standalone shared memory subsystem.

---

## Measurement

Because TickHub is an independent greenfield open-source infrastructure tool (not an in-tree refactoring of `ccm/live`), its architectural baseline is measured by its own subsystem delivery benchmarks:

1. **SHM Delivery Overhead**: Time from Go frame commitment in `/dev/shm` to Python memory visibility must be $< 100\ \mu\text{s}$ (vs multi-millisecond inter-process socket/RPC transport).
2. **Async Event-Loop Yield**: In `await hub.load()`, during the $\sim 40\text{ ms}$ network transit window, the event loop must yield 100% of the thread via `await asyncio.sleep()`, allowing concurrent broker WebSocket frames, order cancellations, and fill processing without thread preemption.
3. **Micro-Sweep Efficiency**: The non-blocking sweep across 35 symbols must complete in $< 50\ \mu\text{s}$ when data is published.
4. **Data Continuity**: The fast-drain catch-up protocol must guarantee zero dropped bars during lag recovery, measured by asserting continuous sequential anchor progression ($T, T+1\text{s}, T+2\text{s}$) across all cursors.

---

## Architecture & Synchronization Contracts

### 1. Watermark Buffer & Inclusion Calibration
- The watermark is a **time buffer ($\Delta t$) added to anchor $T$**, not an incoming tick counter that stalls on quiet names.
- The 1-second cadence window $[T - 1\text{s}, T)$ closes unconditionally when $\text{wall\_clock\_time} \ge T + \Delta t$.
- All ticks arriving before $T + \Delta t$ with SIP timestamps in $[T - 1\text{s}, T)$ are aggregated into bar $T$. Ticks arriving after $T + \Delta t$ are recorded as dropped/late.
- TickHub continuously monitors late-tick drop rates and dynamically tunes $\Delta t$ within bounds (`min_buffer_ms` to `max_buffer_ms`) to target configured data completeness thresholds (e.g. 95%, 99%, or 99.9% non-dropped ticks).
- Quiet symbols: when $T + \Delta t$ expires, quiet symbols are closed immediately (prices forward-filled, volume zeroed) and their `symbol_anchor_ns[i]` is committed to $T$.

### 2. Load-Bearing `anchor_ns` Sequence Number
- Every cadence frame in `/dev/shm` is identified by its exact UTC anchor timestamp `anchor_ns` (int64 nanoseconds).
- Sequence invariant: $S = \text{anchor\_ns}$.
- Ring slot mapping:
  $$\text{slot\_index} = \left(\frac{\text{anchor\_ns}}{\text{cadence\_interval\_ns}}\right) \ \& \ (\text{max\_frames} - 1)$$
- Each frame carries `frame.anchor_ns` and per-symbol `symbol_anchor_ns[N]`. A reader verifying slot $k$ checks `frame.anchor_ns == target_anchor_ns`. Cross-anchor misalignment is structurally impossible.

### 3. Multi-Phase Cross-Asset Geometry (SPY, QQQ)
- Symbols present in multiple phases (e.g. SPY, QQQ) are projected over distinct temporal windows (e.g. Phase 0 at 00.000s vs Phase 1 at 00.500s).
- **Dedicated Per-Phase Ring Buffers in SHM**:
  - Phase 0 has an independent ring buffer with contiguous matrix `[max_frames][N_phase0][D]`.
  - Phase 1 has an independent ring buffer with contiguous matrix `[max_frames][N_phase1][D]`.
  - SPY and QQQ have dedicated slots in both buffers. Eliminates cross-phase overwrite collisions while keeping each phase's PyTorch batch tensor 100% contiguous.
  - `LatestSnapshotTable` remains global: 1 entry per unique physical symbol, updated continuously tick-by-tick via SeqLock.

### 4. Published Latency & Async Sleep-Then-Sweep
- Zero inter-process OS primitives (no futex, no mutex, no eventfd, no sockets).
- **Published Latency**: TickHub continuously publishes `anchor_publish_latency_ns` to the SHM header:
  $$\text{anchor\_publish\_latency\_ns} = \text{publish\_wall\_time\_ns} - \text{anchor\_ns}$$
  incorporating watermark buffer $\Delta t$, network transit, demux, projection compute, and clock skew.
- **Async `load`**: In `await hub.load(cursors, out_matrix)`:
  - If cursors are at tip, executes `await asyncio.sleep(remaining_time)`, freeing the event loop for concurrent broker WebSocket I/O and order handling.
  - If any cursor is stale, skips sleep and fast-drains immediately at CPU speed.
  - Sweeps unacquired symbols in the collection without head-of-line blocking. If published latency spikes by $> 50\ \mu\text{s}$, yields the loop via `await asyncio.sleep(0)`.
  - Returns `failed_cursors: list[SymbolCursor]` (empty on success).

### 5. Staleness & Fast-Drain Catch-Up (Zero Data Holes)
- Definition: $\text{staleness\_ns} = \text{now\_utc} - (\text{target\_anchor} + \text{publish\_latency})$.
- `cursor.is_stale`: True when $\text{staleness\_ns} \ge \text{cadence\_ns}$.
- The client filters lagging cursors using native Python list comprehensions:
  ```python
  while stale := [c for c in cursors if c.is_stale]:
      await hub.load(stale, stale_buf, timeout_ms=10.0)
      hub.next(stale)
  ```
  Drains the backlog at CPU speed (< 5 µs per bar) without sleeping, ensuring models maintain continuous, un-corrupted temporal history without missing bars.

### 6. Phase-Aligned Recovery (`rebegin`)
- When an extreme delay (> 17 minutes) triggers `LaggedAnchorError`:
  `c.rebegin()` re-anchors the cursor to the latest anchor on that phase's specific cadence lattice:
  $$\text{latest\_anchor} = \left\lfloor\frac{\text{now} - \text{offset\_ms} - L}{\text{cadence}}\right\rfloor \times \text{cadence} + \text{offset\_ms}$$
  Guarantees Phase 0 cursors re-anchor to `.000s` and Phase 1 cursors re-anchor to `.500s`.

### 7. Hardware Memory Barrier Bridge for Python
- Minimal C shared library `c/tickhub_atomic.c` compiled via GCC to `libtickhub_atomic.so`:
  - `tickhub_atomic_load_acquire_i64(const int64_t* addr)`: calls C11 `atomic_load_explicit(..., memory_order_acquire)`.
  - `tickhub_atomic_thread_fence_acquire()`: emits hardware acquire fence.
  - `tickhub_cpu_pause()`: executes `_mm_pause()` (x86-64) or `isb` (ARM64).
- Loaded via `ctypes.CDLL` in Python `tickhub.shm`.

---

## Public Developer API Contracts

### 1. SSoT `config.yaml`
```yaml
version: 1
shm:
  name: "prod_1hz"
  max_frames: 1024
  permissions: 0660
  unlink_on_exit: false
mode: live
source:
  live:
    provider: "massive"
    url: "wss://socket.massive.com/stocks"
    api_key_env: "MASSIVE_API_KEY"
cadence:
  interval_ms: 1000
  watermark:
    initial_buffer_ms: 50
    target_inclusion_pct: 99.0
    min_buffer_ms: 10
    max_buffer_ms: 250
  phases:
    - id: 0
      name: "phase_0ms"
      offset_ms: 0
      symbols: [SPY, QQQ, AAPL, NVDA]
    - id: 1
      name: "phase_500ms"
      offset_ms: 500
      symbols: [SPY, QQQ, MSFT, AMZN]
metrics:
  core: true
  trade_flow: true
  quote_flow: true
  microstructure: true
```

### 2. CLI Public Interface (Go)
```bash
tickhub validate --config config.yaml
tickhub run --config config.yaml
tickhub inspect --config config.yaml
```

### 3. Python Client API (`TickHubReader`)
```python
class TickHubReader:
    def __init__(self, config_path: str | Path, *, lock_memory: bool = False): ...
    
    # Metadata
    @property
    def phases(self) -> list[str]: ...
    def symbols_for_phase(self, phase: str) -> list[str]: ...
    @property
    def features(self) -> list[str]: ...
    @property
    def cadence_interval_ns(self) -> int: ...
    def is_running(self) -> bool: ...

    # Setup
    def begin_all(self, *, history_steps: int = 0, out_history: Optional[np.ndarray] = None) -> tuple[list[SymbolCursor], int]: ...

    # Stepping & Realtime
    def next(self, cursors: Sequence[SymbolCursor]) -> None: ...
    def snapshot(self, symbol: str, out_buf: Optional[SymbolSnapshot] = None) -> Optional[SymbolSnapshot]: ...

    # ASYNC Load
    async def load(self, cursors: Sequence[SymbolCursor], out_matrix: np.ndarray, *, timeout_ms: float = 100.0) -> list[SymbolCursor]: ...
    def load_sync(self, cursors: Sequence[SymbolCursor], out_matrix: np.ndarray, *, timeout_ms: float = 100.0) -> list[SymbolCursor]: ...


class SymbolCursor:
    symbol: str
    phase: str
    target_anchor_ns: int

    @property
    def staleness_ns(self) -> int: ...
    @property
    def is_stale(self) -> bool: ...
    def next(self) -> int: ...
    def rebegin(self, *, history_steps: int = 0, out_history: Optional[np.ndarray] = None) -> int: ...
```

---

## Phase 1 Implementation Plan: The Standalone SHM Subsystem

Phase 1 strictly establishes the Shared Memory Subsystem as a standalone, tested architectural layer before any feed ingestion or projection logic is written:

```
Step 1: Go Memory Layer (pkg/shm)
├── layout.go         -> Struct sizes (1024B header, 128B snapshot, 64B frame), 64B cache line isolation, compile assertions
├── segment.go        -> POSIX shm_open, ftruncate, mmap, munmap, shm_unlink
└── producer.go       -> API to format SHM, write snapshots via SeqLock, commit frames & symbol anchors, publish latency

Step 2: C Atomic Bridge (c/tickhub_atomic.c)
├── tickhub_atomic.c  -> tickhub_atomic_load_acquire_i64, tickhub_atomic_thread_fence_acquire, tickhub_cpu_pause
└── Makefile          -> gcc -O3 -shared -fPIC to build libtickhub_atomic.so

Step 3: Python Client Layer (python/tickhub)
├── abi.py            -> ctypes mirror of Go structs with exact byte sizes and offsets
└── shm.py            -> TickHubReader: async load(), begin_all(), snapshot(), next(), SymbolCursor

Step 4: Cross-Language Verification & Latency Benchmarks (tests/)
├── test_shm_cross.py -> Go producer writes deterministic pattern; Python verifies bit-exact data across all symbols
└── benchmark_shm.py  -> Measures Linux time.sleep() jitter distribution and sweep loop microsecond overhead
```

---

## Telemetry & Verification Plan

1. **ABI Alignment & Struct Size Parity**:
   - `GlobalHeader`: exactly 1,024 bytes.
   - `SymbolSnapshot`: exactly 128 bytes (dual cache-line aligned).
   - `FrameHeader`: exactly 64 bytes.
   - Compile-time assertion in Go: `_ = 1 / (1 - (unsafe.Sizeof(T{}) == EXPECTED))`.
   - Runtime assertion in Python: `assert ctypes.sizeof(T) == EXPECTED`.

2. **Cross-Language Write/Read Integrity**:
   - Go producer writes multi-symbol cadence frames with varying patterns across both phases.
   - Python consumer sweeps symbols using `libtickhub_atomic.so`, verifying bit-exact float64 matrices.

3. **Latency Benchmarking Harness**:
   - Measure Linux `asyncio.sleep()` / `time.sleep()` jitter distribution across $10,000$ iterations.
   - Measure sweep loop time for 35 symbols: target $< 50\ \mu\text{s}$.
   - Verify fast-drain catch-up drains 10 queued frames in $< 50\ \mu\text{s}$ total without skipping anchors.
