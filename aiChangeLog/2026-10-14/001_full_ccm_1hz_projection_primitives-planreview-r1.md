# Plan Review — aiChangeLog/2026-10-14/001_full_ccm_1hz_projection_primitives.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-10-14 14:31:07Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

### 1. Summary of the Proposal

The plan extends `TickHub` to compute and publish all 36 `ccm` 1Hz projection primitives (`34` microstructure/bar primitives plus cumulative `quote_idx` and `trade_idx`) in shared memory when `nFeat == 36`, while keeping the legacy 5-feature path when `nFeat == 5`. Specifically, it:
1. Adds participant timestamp, exchange IDs (`BidExch`, `AskExch`, `TradeExch`), and `TrfID` to `feed.Tick`, `massive_ws.go`, and `parquet_reader.go`.
2. Implements stateful 1Hz bar accumulation and bar-close hybrid Lee-Ready classification (`CloseCCMBar`) on `SymbolHistory` in `pkg/project/window.go`, with pre-window state seeding (`IngestPreWindowTick`) in `pkg/project/projector.go` and `cmd/tickhub/worker.go`.
3. Updates `config/live_72.yaml`, `config/universe64_arm_a_500ms.yaml`, and `config/universe64_arm_b_250ms.yaml` to `max_frames: 512` and 36 features.
4. Adds `TickHubReader.load_window_1hz` in `python/tickhub/shm.py` to read 360 whole-second bars from SHM frames, populate the sub-second terminal row `span_s` from `SymbolSnapshot` (repurposing `snap.RecvTimestampNS` as the latest quote SIP timestamp), reconstruct `int64` timestamps, and run window-boundary backfill/forward-fill.

---

### 2. Simplest Sufficient Design

The plan introduces a split-path mechanism where completed whole-second bars (`K * 1e9`) are copied into sub-second phase frames (`K * 1e9 + offset_ns`), while the sub-second terminal state at `s_end_ns > anchor_floor_ns` is read out-of-band from the mutable `SymbolSnapshot` struct (requiring `snap.RecvTimestampNS` to be overloaded with the quote SIP timestamp).

**Cut List:**
1. **Cut `SymbolSnapshot` terminal-row extraction in `TickHubReader.load_window_1hz` (`python/tickhub/shm.py`) and cut repurposing `snap.RecvTimestampNS` (`pkg/project/projector.go`).**
   - *Instead:* When publishing a sub-second phase frame at `K * 1e9 + offset_ns`, write the partial/terminal state up to `K * 1e9 + offset_ns` (`last_bid_px`, `last_ask_px`, `last_bid_sz`, `last_ask_sz`, `last_trade_px`, `sip_timestamp`, `quote_idx`, `trade_idx`) directly into that phase frame (or read the frame committed at `anchor_ns`), while reading the 360 whole-second bars from integer-second frames.
   - *What is lost:* Nothing. Every query at `anchor_ns` becomes a deterministic, frame-versioned read from the ring buffer with zero dependency on live-mutating `SymbolSnapshot` state and zero ABI semantic overloading of `RecvTimestampNS`.

---

### 3. Blocking Defects

- **B2 (Data integrity / look-ahead race) & B1 (Goal-defeating) — Terminal row `span_s` read from unversioned `SymbolSnapshot` after waiting only on `anchor_floor_ns` (§3, §4 `python/tickhub/shm.py`, `pkg/project/projector.go`):**
  - In §4 (`python/tickhub/shm.py`), `load_window_1hz` "waits/spins on demand up to `timeout_ms` if the target whole-second frame `anchor_floor_ns` has not yet been committed, extracts the `span_s` 1Hz bars from SHM, [and] populates the terminal row `span_s` from `SymbolSnapshot` (when `s_end_ns > anchor_floor_ns`)".
  - **Under-read race:** When `s_end_ns > anchor_floor_ns` (e.g., `+250ms`, `+500ms`, `+750ms` phases), waiting only for `anchor_floor_ns` (`K * 1e9`) returns as soon as the integer second is committed, before the producer has processed ticks in `[anchor_floor_ns, s_end_ns)`.
  - **Look-ahead leakage:** `SymbolSnapshot` is a single latest-state struct per symbol that is continuously overwritten as the producer advances (`§3: SymbolSnapshot tracks real-time ticks`). If the producer has advanced past `s_end_ns` when `load_window_1hz` runs, reading `SymbolSnapshot` injects future ticks from `> s_end_ns` into the terminal row `span_s`.
  - **Missing fields in `SymbolSnapshot`:** `SymbolSnapshot` is not modified in §4 and does not contain cumulative `quote_idx` or `trade_idx` (Slots 34..35), so terminal row `span_s` cannot be populated with `quote_idx` and `trade_idx` at `s_end_ns` from `SymbolSnapshot`.
  - **Broken `RecvTimestampNS` contract:** Overwriting `snap.RecvTimestampNS` with the quote SIP timestamp only when `isCCM36` is true (§4 `pkg/project/projector.go`) silently corrupts ingestion-timestamp telemetry in `SymbolSnapshot`.

- **B2 (Data integrity / IEEE-754 precision loss) — Slot 27 `sip_timestamp` stored as `float64(last_quote_sip_ns)` loses integer nanosecond precision (§3, §4 `python/tickhub/shm.py`):**
  - §3 defines Slot 27 as `27: sip_timestamp (float64(last_quote_sip_ns))`, and §4 states that `load_window_1hz` "reconstructs exact `int64` timestamp arrays (`sip_timestamp`, ...)".
  - Unix epoch nanosecond timestamps in 2026 are $\approx 1.78 \times 10^{18}$ ($> 2^{60}$), exceeding IEEE-754 `float64`'s 53-bit mantissa ($9.007 \times 10^{15}$). At $2^{60}$, `float64` ULP is $256\text{ ns}$. Because $10^6 \bmod 256 = 64 \neq 0$, casting millisecond-truncated `int64` nanosecond timestamps to `float64` and back to `int64` corrupts 75% of timestamps by $\pm 64\text{ ns}$ or $\pm 128\text{ ns}$, failing exact `int64` parity with `ccm`.
  - Unlike Slots 30..33 (which §3 stores as `ts_ns - T` offsets in `[0, 1e9)`), Slot 27 can come from a prior bar, so it must be stored as lossless integer milliseconds `float64(last_quote_sip_ns / 1_000_000)` (reconstructed via `int64(val) * 1_000_000`) or an offset from a known epoch anchor.

- **B1 (Goal-defeating) — Zero-allocation hot path & within-bar quote/trade ordering in `SymbolHistory` not specified (§3, §4 `pkg/project/window.go`):**
  - §3 states that "Within each 1-second bar `[T, T + 1s)`, accepted quotes and trades are buffered per unique symbol and classified at bar close (`CloseCCMBar(T + 1s)`), reproducing `_bulk_classify_microstructure_njit`... even when same-millisecond quotes and trades arrive out of order on the live WebSocket wire."
  - The plan does not specify (a) how buffered quotes and trades within `[T, T + 1s)` are sorted/ordered at `CloseCCMBar` to reconcile out-of-order live WebSocket arrival with Parquet order, nor (b) how per-symbol tick buffers in `SymbolHistory` are pre-allocated/reused (`buf[:0]`) so steady-state bar buffering does not heap-allocate or truncate active bursts.

---

### 4. Non-Blocking Observations

1. In `cmd/tickhub/worker.go` (§4), ensure `IngestPreWindowTick` applies the exact same validity filters (`bid_px > 0 && ask_px > 0`, quote dedupe, `price > 0`, millisecond truncation) as in-window ingestion so `quote_idx` and `trade_idx` match full-session counts.
2. In `pkg/feed/parquet_reader.go` (§4), verify that `Exchange` / `BidExchange` / `AskExchange` (`int32`) values outside `[0, 255]` are bounds-checked before indexing `latestV[0..255]`.
3. In `python/tickhub/shm.py` (§4), specify whether `load_window_1hz` advances `LastReadAnchorNS` when used under `ModeHistoricalReplay` so producer flow control (`WaitConsumerAdvance`) does not deadlock.

---

### 5. Methodological & Data-Alignment Concerns

- **Interval Convention Flip (`[T - 1s, T)` vs `[T, T + 1s)`):** Legacy 5-feature mode indexes frames by bar close `[T - 1s, T)`, whereas `isCCM36` mode (§3) buckets whole-second bars on `[T, T + 1s)` where `T = anchor_floor_ns - 1_000_000_000` and closes them at `anchor_floor_ns = T + 1s`. The mathematical interval `[anchor_floor_ns - 1s, anchor_floor_ns)` is still half-open `[T - 1s, T)` relative to the frame's commit anchor `anchor_floor_ns`, so ticks at exact timestamp `anchor_floor_ns` are excluded from the bar closing at `anchor_floor_ns` and included in the next bar. However, reconstructing Slots 30..33 via `T + int64(offset)` requires `python/tickhub/shm.py` and `pkg/project/window.go` to agree unambiguously that `T` is `frame_anchor_floor_ns - 1_000_000_000` (bar start), not `frame_anchor_floor_ns` (bar close).
- **Sub-Second Phase Frame Semantics & Look-Ahead:** As detailed in Blocking Defects (`B2`), reading `SymbolSnapshot` for the sub-second tail `[anchor_floor_ns, s_end_ns)` while waiting only on `anchor_floor_ns` violates point-in-time alignment and risks both look-ahead leakage and missing ticks.
- **53-Bit Float64 Timestamp Truncation:** As detailed in Blocking Defects (`B2`), storing raw epoch nanoseconds in `float64` (Slot 27 `sip_timestamp`) loses precision and breaks exact `int64` timestamp alignment.

---

### 6. Missing Telemetry

- **Publish Latency SLO Verification (`< 15 ms`):** §2 states that "publish latency (`< 15 ms` SLO) [is] verified in §6", but §5 and §6 contain no latency measurement or assertion on `AnchorPublishLatencyNS` during 36-feature projection with bar-close Lee-Ready classification across 64–72 symbols. Add a telemetry readout and assertion on `AnchorPublishLatencyNS` (or bar-close duration) in the parity/validation test.

---

### 7. Verdict

**Verdict: Blocked (B2)**

The plan cannot proceed as written because (1) storing `last_quote_sip_ns` directly as `float64` in Slot 27 corrupts 75% of `int64` nanosecond timestamps due to IEEE-754 53-bit mantissa truncation, and (2) populating the sub-second terminal row `span_s` from the unversioned `SymbolSnapshot` after waiting only on `anchor_floor_ns` introduces a producer/consumer race (under-read or look-ahead leakage), lacks `quote_idx`/`trade_idx` in `SymbolSnapshot`, and corrupts `snap.RecvTimestampNS` semantics. Remediate Slot 27's encoding, store or version sub-second terminal state in the phase frame rather than `SymbolSnapshot`, and specify pre-allocated within-bar tick buffering and ordering rules in `SymbolHistory`.
