# TickHub System Architecture & Public API Design

**Status:** Final Architectural Specification  
**Author:** Staff Systems & Quantitative Infrastructure Engineer  
**Scope:** Developer Ergonomics, Public API Contracts, Memory Layout (`/dev/shm`), Synchronization Invariants, and Python Barrier Bridge  

---

## 1. Overview & System Architecture

TickHub is a high-performance Go daemon designed to bridge high-frequency raw market data (live WebSocket feeds from Massive.com or historical tick files from Parquet/flat files) to Python-based quantitative research and trading pipelines. 

TickHub projects irregular, high-rate tick streams into structured, fixed-cadence metric snapshots (e.g., 1 Hz NBBO, VWAP, trade volume, order flow imbalances) and writes them directly into POSIX shared memory (`/dev/shm`). Downstream consumers (such as PyTorch deep learning models scoring multi-symbol universes in 500 ms phases) read contiguous memory blocks with zero runtime IPC, zero serialization overhead, zero copy (`torch.from_numpy`), and zero process synchronization primitives (no futex, no mutex, no eventfd).

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
                     |     (`tickhub run --config ...`)  |
                     |                                   |
                     |  +-----------------------------+  |
                     |  | Ingestion Arena & Demux     |  |
                     |  | (Zero-Alloc Ring Buffers)   |  |
                     |  +--------------+--------------+  |
                     |                 |                 |
                     |                 v                 |
                     |  +-----------------------------+  |
                     |  | Cadence Projection Engine   |  |
                     |  | - project1hz kernel (Go)    |  |
                     |  | - Dynamic Watermark Buffer  |  |
                     |  | - Published Latency Monitor |  |
                     |  +--------------+--------------+  |
                     +-----------------+-----------------+
                                       |
                                       v  POSIX Shared Memory (`/dev/shm/tickhub_<name>`)
    =============================================================================
    POSIX Shared Memory Layout (64-Byte Cache-Line Aligned)
    -----------------------------------------------------------------------------
    [Global Header]          Magic, status, num_phases, published latency
                             `anchor_publish_latency_ns` (atomic)
                             `watermark_buffer_ns` (atomic)
    -----------------------------------------------------------------------------
    [Symbol Directory]       Fixed-stride symbol names & index mapping
    -----------------------------------------------------------------------------
    [Latest Snapshot Table]  Top-of-book per UNIQUE symbol (NBBO, last trade)
                             SeqLock-protected, sub-microsecond access
    -----------------------------------------------------------------------------
    [Phase 0 Ring Buffer]    Cadence: 1.0s, Offset: 0 ms
                             Contiguous [max_frames][N_phase0][D] float64
                             Dedicated SPY / QQQ slots for Phase 0
    -----------------------------------------------------------------------------
    [Phase 1 Ring Buffer]    Cadence: 1.0s, Offset: 500 ms
                             Contiguous [max_frames][N_phase1][D] float64
                             Dedicated SPY / QQQ slots re-projected for Phase 1
    =============================================================================
                                       |
                                       v  Async Load (yields to event loop) + Sweep
                     +-----------------------------------+
                     |   Python Quant / ML Consumer      |
                     |   (Single-Process Async Engine)   |
                     |                                   |
                     |  +-----------------------------+  |
                     |  | `TickHubReader` Client      |  |
                     |  | - `begin_all()` cursors     |  |
                     |  | - `await hub.load(cursors)` |  |
                     |  | - C-Level Memory Barrier    |  |
                     |  |   (`libtickhub_atomic.so`)  |  |
                     |  +--------------+--------------+  |
                     |                 |                 |
                     |                 v                 |
                     |  +-----------------------------+  |
                     |  | PyTorch Tensor Batch        |  |
                     |  | `torch.from_numpy(features)`|  |
                     |  | Shape: [N_phase, D] float64 |  |
                     |  +-----------------------------+  |
                     +-----------------------------------+
```

---

## 2. Temporal Semantics: Watermark Buffer, Load-Bearing Anchors, & Staleness

### 2.1 The Dynamic Watermark Buffer ($\Delta t$)

The watermark is **not** an incoming tick counter that waits for quiet names. It is a **time buffer ($\Delta t$) added to the anchor timestamp $T$**:
- The 1-second cadence window $[T - 1\text{s}, T)$ closes unconditionally when:
  $$\text{wall\_clock\_time} \ge T + \Delta t$$
- All ticks arriving before $T + \Delta t$ whose SIP timestamp falls in $[T - 1\text{s}, T)$ are aggregated into bar $T$.
- Any tick arriving *after* $T + \Delta t$ with SIP timestamp $< T$ is recorded as a late/dropped tick.
- **Dynamic Inclusion Calibration**:
  TickHub continuously monitors late-tick drop rates. It dynamically adjusts $\Delta t$ within bounds (`min_buffer_ms` to `max_buffer_ms`) to target configured data completeness thresholds (e.g. 95%, 99%, or 99.9%).
- **Quiet Symbols**:
  Quiet symbols never delay window closure. When $T + \Delta t$ expires, quiet symbols are closed immediately: prices are forward-filled, volume/notionals are zero-filled, and their `symbol_anchor_ns[i]` is committed to $T$.

---

### 2.2 Load-Bearing `anchor_ns` Sequence Number

To prevent race conditions, sequence desynchronization, or stale frame consumption:
- **`anchor_ns` is the foundational sequence identifier.**
- No synthetic sequence integers (`0, 1, 2...`). Every frame slot in the ring buffer is indexed and verified by its exact epoch nanosecond anchor:
  $$\text{slot\_index} = \left(\frac{\text{anchor\_ns}}{\text{cadence\_interval\_ns}}\right) \ \& \ (\text{max\_frames} - 1)$$
- Every frame carries:
  - `frame.anchor_ns`: Global anchor for the frame.
  - `frame.symbol_anchor_ns[i]`: Per-symbol commit anchor timestamp.
- **Invariant**: The consumer verifies `frame.anchor_ns == target_anchor_ns`. Cross-anchor reading is structurally impossible.

---

### 2.3 Staleness Metric & Fast-Drain Catch-Up

When client scoring pauses or takes longer than 1 cadence interval:
- Data is **not lost**; it is preserved in `/dev/shm` (retaining $\sim 17$ minutes across 1,024 frames).
- Recurrent neural networks, causal Transformers, and EMA filters **cannot skip bars**. Dropping intermediate bars creates holes and corrupts model state.

#### Definition of `cursor.staleness_ns`:
$$\text{expected\_publish\_wall\_ns} = \text{cursor.target\_anchor\_ns} + \text{hub.anchor\_publish\_latency\_ns}$$
$$\text{staleness\_ns} = \text{wall\_clock\_now\_ns} - \text{expected\_publish\_wall\_ns}$$

- $\text{staleness\_ns} < \text{cadence\_ns}$: Client is at real-time tip.
- $\text{staleness\_ns} \ge \text{cadence\_ns}$: **Client is $\ge 1$ step behind tip.** Data is already waiting in `/dev/shm`.

#### Fast-Drain Protocol:
When `cursor.is_stale` is True, `await hub.load(cursors, ...)` skips sleep and reads immediately at CPU speed (< 5 µs per bar). The client loops until `not any(c.is_stale for c in cursors)`, catching up in microseconds with zero holes in data.

---

### 2.4 Multi-Phase Cross-Asset Symbols (SPY, QQQ)

Symbols present in multiple phases (e.g. SPY, QQQ) are projected over distinct temporal windows:
- Phase 0: $[T - 1\text{s}, T)$
- Phase 1: $[T - 0.5\text{s}, T + 0.5\text{s})$

**SHM Solution**: Dedicated per-phase cadence ring buffers.
- SPY in Phase 0 sits in Phase 0's contiguous matrix `[N_phase0, D]`.
- SPY in Phase 1 sits in Phase 1's contiguous matrix `[N_phase1, D]`.
- Prevents cross-phase memory overwrite collisions while keeping each phase's PyTorch batch tensor 100% contiguous.

---

## 3. Python Hardware Memory Barrier Bridge (`libtickhub_atomic.so`)

CPython interpreter and standard `ctypes` do not emit CPU memory barrier instructions. To guarantee zero torn reads and strict load-acquire semantics without heavy external dependencies, TickHub ships a minimal C library compiled via GCC:

```c
#include <stdatomic.h>
#include <stdint.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

int64_t tickhub_atomic_load_acquire_i64(const int64_t* addr) {
    return atomic_load_explicit((const _Atomic int64_t*)addr, memory_order_acquire);
}

void tickhub_atomic_thread_fence_acquire(void) {
    atomic_thread_fence(memory_order_acquire);
}

void tickhub_cpu_pause(void) {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ __volatile__("isb" ::: "memory");
#else
    atomic_thread_fence(memory_order_seq_cst);
#endif
}
```

Loaded via `ctypes.CDLL` in Python `tickhub.shm`. Guarantees hardware-level load-acquire barriers and compiler fence prevention.

---

## 4. Public Developer Usage & API Specifications

### 4.1 Single Source of Truth Configuration (`config.yaml`)

Both the Go daemon (`tickhub run`) and the Python client (`TickHubReader`) consume the same declarative YAML:

```yaml
version: 1

shm:
  name: "prod_1hz"             # /dev/shm/tickhub_prod_1hz
  max_frames: 1024             # Power of 2 required
  permissions: 0660
  unlink_on_exit: false

mode: live                     # live | replay

source:
  live:
    provider: "massive"
    url: "wss://socket.massive.com/stocks"
    api_key_env: "MASSIVE_API_KEY"
    reconnect_backoff_ms: 1000
    max_reconnect_backoff_ms: 30000
    queue_capacity: 200000
  replay:
    data_dir: "/mnt/wc/datalake/parquet"
    start_time: "2026-09-01T09:30:00.000Z"
    end_time: "2026-09-01T16:00:00.000Z"
    playback_speed: 0.0

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
      symbols:
        - SPY
        - QQQ
        - AAPL
        - NVDA
    - id: 1
      name: "phase_500ms"
      offset_ms: 500
      symbols:
        - SPY
        - QQQ
        - MSFT
        - AMZN

metrics:
  core: true
  trade_flow: true
  quote_flow: true
  microstructure: true
  variance: false
```

---

### 4.2 CLI Daemon Management (Go)

The Go daemon is managed entirely via the CLI (no public Go library):

```bash
# Validate config and print calculated memory layout
tickhub validate --config config.yaml

# Run daemon in foreground
tickhub run --config config.yaml

# Inspect live SHM state in console
tickhub inspect --config config.yaml
```

---

### 4.3 Python Public API (`tickhub` package)

```python
class TickHubReader:
    def __init__(self, config_path: str | Path, *, lock_memory: bool = False):
        """Attaches to /dev/shm using config.yaml as SSoT."""

    # --- SSoT Metadata ---
    @property
    def phases(self) -> list[str]:
        """Names of configured phases (e.g. ['phase_0ms', 'phase_500ms'])."""

    def symbols_for_phase(self, phase: str) -> list[str]:
        """List of symbols assigned to phase in config."""

    @property
    def features(self) -> list[str]:
        """Ordered list of projected feature names."""

    @property
    def num_features(self) -> int:
        """Total feature dimension D."""

    @property
    def cadence_interval_ns(self) -> int:
        """Cadence duration in nanoseconds (e.g. 1_000_000_000 for 1Hz)."""

    def is_running(self) -> bool:
        """Checks daemon liveness and heartbeat."""

    # --- Sync Initialization & Setup ---
    def begin_all(
        self,
        *,
        history_steps: int = 0,
        out_history: Optional[np.ndarray] = None
    ) -> tuple[list["SymbolCursor"], int]:
        """Initializes cursors for ALL symbols across ALL phases declared in config.
        Returns a plain Python list[SymbolCursor] and number of history steps loaded.
        """

    def next(self, cursors: Sequence["SymbolCursor"]) -> None:
        """Synchronously advances target anchor by cadence interval (+1s) on all cursors."""

    def snapshot(
        self,
        symbol: str,
        out_buf: Optional[SymbolSnapshot] = None
    ) -> Optional[SymbolSnapshot]:
        """Synchronous sub-microsecond top-of-book read via SeqLock."""

    # --- ASYNC Data Loading ---
    async def load(
        self,
        cursors: Sequence["SymbolCursor"],
        out_matrix: np.ndarray,
        *,
        timeout_ms: float = 100.0
    ) -> list["SymbolCursor"]:
        """Asynchronous: Yields to asyncio event loop during network wait, then sweeps.
        
        - If cursors are at tip: executes `await asyncio.sleep(wait_time)`, freeing
          the event loop for concurrent broker WebSocket I/O and order handling.
        - If any cursor is stale: skips sleep and fast-drains immediately at CPU speed.
        - Returns list of SymbolCursor instances that timed out.
        """

    def load_sync(
        self,
        cursors: Sequence["SymbolCursor"],
        out_matrix: np.ndarray,
        *,
        timeout_ms: float = 100.0
    ) -> list["SymbolCursor"]:
        """Synchronous fallback for scripts not running an asyncio event loop."""


class SymbolCursor:
    symbol: str
    phase: str
    target_anchor_ns: int

    @property
    def staleness_ns(self) -> int:
        """(now_utc - (target_anchor + publish_latency)). Delta behind tip in ns."""

    @property
    def staleness_ms(self) -> float:
        """Staleness in milliseconds."""

    @property
    def is_stale(self) -> bool:
        """True if target_anchor is >= 1 cadence step behind live tip."""

    def next(self) -> int:
        """Advances target anchor (+1s). Raises LaggedAnchorError if overwritten."""

    def rebegin(
        self,
        *,
        history_steps: int = 0,
        out_history: Optional[np.ndarray] = None
    ) -> int:
        """Recovery helper: re-anchors to newest phase-aligned anchor and reloads history."""
```

---

### 4.4 End-to-End Client Usage Example (Async Python Engine)

```python
import asyncio
import numpy as np
import torch
from tickhub import TickHubReader, LaggedAnchorError

async def run_trading_engine():
    # Sync attach to /dev/shm via config.yaml
    with TickHubReader("config.yaml", lock_memory=True) as hub:
        history_steps = 180

        # 1. Sync setup: initialize all cursors in one shot -> plain Python list
        all_cursors, loaded = hub.begin_all(history_steps=history_steps)
        print(f"TickHub ready: {len(all_cursors)} cursors ({loaded} history steps).")

        # 2. Partition cursors per phase using native list comprehensions
        phases = [
            (p, [c for c in all_cursors if c.phase == p])
            for p in hub.phases
        ]

        # 3. Pre-allocate batch buffers & zero-copy PyTorch tensors
        phase_data = []
        for p, cursors in phases:
            buf = np.empty((len(cursors), hub.num_features), dtype=np.float64)
            tensor = torch.from_numpy(buf)
            phase_data.append((p, cursors, buf, tensor))

        snap = hub.snapshot("AAPL")

        # 4. Main Multi-Phase Stepping Loop
        while hub.is_running():
            for phase_name, cursors, batch_matrix, batch_tensor in phase_data:
                try:
                    # --- Step A: Fast-drain any individually lagging cursors ---
                    # When stale, await hub.load() does not sleep; runs at CPU speed
                    while stale := [c for c in cursors if c.is_stale]:
                        stale_buf = np.empty((len(stale), hub.num_features), dtype=np.float64)
                        await hub.load(stale, stale_buf, timeout_ms=10.0)
                        hub.next(stale)

                    # --- Step B: Async Real-Time Load at Tip ---
                    # Yields to event loop during the ~40ms network transit window!
                    # Broker order fills, socket frames, and cancels process concurrently.
                    failed = await hub.load(cursors, batch_matrix, timeout_ms=100.0)
                    if failed:
                        print(f"Warning: {len(failed)} symbols timed out in {phase_name}: {[c.symbol for c in failed]}")

                    # --- Step C: Synchronous Model Scoring ---
                    scores = model(batch_tensor)

                    # --- Step D: Synchronous Cursor Advance ---
                    hub.next(cursors)

                    # Instant synchronous top-of-book check
                    hub.snapshot("AAPL", snap)

                except LaggedAnchorError as e:
                    print(f"Lag detected: {e}. Re-anchoring phase {phase_name}...")
                    for c in cursors:
                        c.rebegin(history_steps=history_steps)

if __name__ == "__main__":
    asyncio.run(run_trading_engine())
```

---

## 5. Phase 1 Implementation Plan: The Standalone SHM Subsystem

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
