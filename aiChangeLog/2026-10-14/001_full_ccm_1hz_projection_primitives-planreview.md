# Plan Review — aiChangeLog/2026-10-14/001_full_ccm_1hz_projection_primitives.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-10-14 14:34:54Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

# Plan Review — aiChangeLog/2026-10-14/001_full_ccm_1hz_projection_primitives.md

_Model: Gemini 3.8 Flash · Round: 2 of 2 (Final) · Tool: planreview.py_

---

### 1. Summary of the Proposal

The plan extends `TickHub` from its legacy 5-feature summary mode to the complete 44-slot `ccm` 1Hz projection specification (`nFeat == 44`) across Go (`pkg/feed`, `pkg/project`, `cmd/tickhub`), Python (`python/tickhub/shm.py`), and configuration files. It introduces:
1. Ingestion of participant timestamps, exchange IDs (`BidExch`, `AskExch`, `TradeExch`), and `TrfID` across WebSocket and Parquet readers into `feed.Tick`.
2. Stateful 1Hz bar accumulation and hybrid Lee-Ready classification on `SymbolHistory` in `pkg/project/window.go` with pre-allocated zero-allocation slices (`cap=2048`, reset via `[:0]`), pre-window seeding, and in-place insertion sorting for out-of-order ticks.
3. A 44-slot frame schema in shared memory: Slots 0..35 hold the completed whole-second bar for `[anchor_floor_ns - 1s, anchor_floor_ns)`, while Slots 36..43 hold point-in-time terminal state at phase anchor `K * 1e9 + offset_ns` (eliminating `SymbolSnapshot` reads).
4. Lossless millisecond timestamp encoding in `float64` for Slots 27 and 41, nanosecond bar-relative offsets in Slots 30..33, and reader-side reconstruction in `load_window_1hz`.
5. Updates to configs (`max_frames: 512`), parity validation asserting exact equivalence against `ccm.project_to_1hz`, and `< 15 ms` publish latency SLO verification.

---

### 2. Simplest Sufficient Design

The plan is at the simplest sufficient design. By replacing mutable `SymbolSnapshot` extraction with 8 dedicated terminal slots (`36..43`) embedded directly in each ring-buffer frame, the author eliminated snapshot data races, look-ahead leakage, and timestamp semantic overloading without requiring new synchronization primitives or modifying the core C/Go ABI struct layouts.

---

### 3. Blocking Defects

`None.`

All blocking defects identified in Round 1 have been resolved in the design text:
- **Round 1 B2 & B1 (Terminal row read from unversioned `SymbolSnapshot`):** Resolved. §3 and §4 eliminate `SymbolSnapshot` reads from `load_window_1hz` and embed Slots 36..43 directly into the phase frame.
- **Round 1 B2 (Slot 27 `sip_timestamp` IEEE-754 precision loss):** Resolved. §3 and §4 store integer milliseconds in `float64` and reconstruct via `int64(val) * 1_000_000`, preserving exact nanosecond values for millisecond-truncated quotes.
- **Round 1 B1 (Zero-allocation hot path and within-bar ordering):** Resolved. §3 and §4 specify pre-allocated slice capacities (`2048`), slice reuse (`[:0]`), and an in-place sort triggered only on out-of-order flags.
- **Round 1 Missing Telemetry (< 15 ms SLO):** Resolved. Assertions on `AnchorPublishLatencyNS < 15_000_000` added to both parity test suites (§4, §5, §6).

---

### 4. Non-Blocking Observations

1. In `config/universe64_arm_a_500ms.yaml` (2 phases) and `config/universe64_arm_b_250ms.yaml` (4 phases), `max_frames: 512` retains only 256s and 128s of history; scale `max_frames` to at least 1024 and 2048 if 360s lookbacks are ever queried against multi-phase cadences.
2. In `python/tickhub/shm.py`, verify that `load_window_1hz` indexes historical whole-second bars using the phase stride if executed against multi-phase ring buffers where each phase commits distinct frames.
3. In `tests/test_ccm_1hz_projection_parity.py`, apply floating-point tolerances (`np.testing.assert_allclose(atol=1e-7)`) to `trade_vol_std_px` and `trade_vol_skew` rather than strict bit-equality to account for compiler math differences between Go and Numba.
4. In `pkg/project/window.go`, log a telemetry counter or warning if single-second quote or trade bursts exceed initial capacity `2048` before slice growth.

---

### 5. Methodological & Data-Alignment Concerns

- **Timestamp Encoding:** Storing `sip_timestamp` and `asof_sip_timestamp` as integer milliseconds `float64(ts_ns / 1_000_000)` and reconstructing via `int64(ms) * 1_000_000` is exact and loss-free because tick feeds are millisecond-truncated.
- **Terminal Point-in-Time Alignment:** Populating Slots 36..43 at `phase_anchor_ns` inside the ring buffer guarantees causal point-in-time consistency with zero look-ahead leakage.
- **Half-Open Interval Semantics:** Whole-second bars in Slots 0..35 strictly exclude ticks at `sip_ns >= anchor_floor_ns`, correctly honoring the $[T, T + 1\text{s})$ half-open interval.

---

### 6. Missing Telemetry

`None.`

---

### 7. Verdict

**Verdict: Proceed-with-noted-risks**

All blocking defects from Round 1 have been remediated in the design. The 44-slot frame-versioned architecture achieves the stated 360-second projection goal while maintaining zero-allocation hot paths and ABI backward compatibility. The author may proceed to implementation, recording the multi-phase buffer retention capacity and floating-point parity tolerance observations in the ledger for verification during the post-implementation code audit.
