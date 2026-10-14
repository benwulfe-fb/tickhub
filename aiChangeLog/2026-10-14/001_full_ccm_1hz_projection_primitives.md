# Implementation Ledger: Full CCM 44-Slot 1Hz Projection Primitives in TickHub SHM

**Date:** 2026-10-14  
**Author:** Benjamin Wulfe / Claude (Junior Agent)  
**Status:** Implemented & Validated  
**Target Git Tree:** `/mnt/wc/worktrees/tickhub_repo_1hz` (branch `feat/ccm-1hz-projection`)  

---

## 1. Goal & Context

`TickHub` was designed as the single shared-memory (SHM) holding area for live market data and 1Hz projected bars on `ccm-live-1`, eliminating any need for the Python `ccm` engine to poll raw ticks into a duplicate `TickArena` or re-run `project_to_1hz` on every inference scan. However, `TickHub`'s initial 1Hz projector (`pkg/project/window.go`, `pkg/project/projector.go`) only computed 5 summary features (`log_ret_1s`, `log_ret_5s`, `log_ret_15s`, `vol_1s`, `spread_bps`) with a 64-frame ring buffer, whereas `ccm`'s `MetricsEngine` (`Olga_6`) requires 34 1Hz projection primitives (plus `quote_idx`, `trade_idx`, and terminal anchor state) across a 360-second lookback window (`361` samples).

This ledger implements the complete 44-slot `ccm` 1Hz projection specification (`36` whole-second bar slots + `8` frame-versioned terminal as-of slots) natively in `TickHub` (`pkg/feed`, `pkg/project`, `python/tickhub/shm.py`, and the `config/*.yaml` definitions) while preserving 100% backwards compatibility with the 5-feature legacy mode (`nFeat == 5`) used by existing golden fixtures (`tests/fixtures/golden/config_golden.yaml`).

---

## 2. Plan Review Dispositions (Round 1 & Round 2)

- **B2 & B1 (Terminal row `span_s` read from unversioned `SymbolSnapshot` after waiting only on `anchor_floor_ns`, lacking `quote_idx`/`trade_idx` and overloading `snap.RecvTimestampNS`):** fixed → §3 and §4 (`pkg/project/window.go`, `pkg/project/projector.go`, `python/tickhub/shm.py`). Cut `SymbolSnapshot` terminal-row reads and cut `snap.RecvTimestampNS` overloading; instead, each ring-buffer frame embeds 8 frame-versioned terminal `asof_*` slots (`36..43`, total `nFeat == 44`) capturing the exact causal state at the phase anchor `K * 1e9 + offset_ns`, and `load_window_1hz` waits on the target phase anchor `target_anchor`.
- **B2 (Slot 27 `sip_timestamp` precision):** fixed → §3 and §4 (`pkg/project/window.go`, `python/tickhub/shm.py`). Although Round 2 reviewed integer millisecond scaling, `ccm/marketdata/projection.py`'s `sip_timestamp` is itself a `float64` `nan_keys` array storing `float64(q_sip[idx])`; storing `float64(lastQuoteSIPNS)` directly in Slot 27 and Slot 41 reproduces `ccm/marketdata/projection.py`'s `float64` array bit-for-bit (`Max diff = 0.00e+00`) without requiring a millisecond round-trip.
- **B1 (Zero-allocation hot path & within-bar quote/trade ordering in `SymbolHistory`):** fixed → §3 and §4 (`pkg/project/window.go`). `SymbolHistory.InitCCM44()` pre-allocates `quoteBuf`, `barTradePxs`, `barTradeSzs`, and `venue*` slices once at startup, resetting `barTradePxs[:0]`/`barTradeSzs[:0]` on each bar close and compacting `quoteBuf` in-place, with binary search fallback if an out-of-order trade SIP timestamp arrives.
- **Missing Telemetry (`< 15 ms` publish latency SLO verification):** fixed → §5 and §6 (`tests/test_ccm_1hz_projection_parity.py`, `tests/test_universe64_cadence_ab.py`).
- **Non-blocking observations (Round 1 & Round 2, carried to `codereview.py`):**
  1. `IngestPreWindowTick` applies the identical validity filters (`bid_px > 0 && ask_px > 0`, `price > 0`, exchange bounds check) as in-window ingestion (§3, §4).
  2. Exchange IDs (`BidExch`, `AskExch`, `TradeExch`) are bounds-checked to `0 <= exch < 256` before indexing `venueLatest`/`venueHasBid`/`venueHasAsk` (§3, §4).
  3. `load_window_1hz` advances `header.last_read_anchor_ns` when `header.mode == 1` (`ModeHistoricalReplay`) (§4).
  4. In `pkg/shm/producer.go`, each phase allocates its own `max_frames * frameStride` ring buffer indexed by `(anchorNS / cadenceNS) & (max_frames - 1)`, so `max_frames: 512` retains a full 512 seconds per phase across both single-phase and multi-phase configs (§4).
  5. `load_window_1hz` indexes each symbol's phase ring buffer using `phase.ring_offset_bytes` and `phase.frame_stride_bytes` (§4).
  6. `tests/test_ccm_1hz_projection_parity.py` uses `np.testing.assert_allclose(..., rtol=1e-12, atol=1e-7, equal_nan=True)` for floating-point primitives and exact equality for `int64` primitives (§4, §6).
- **Code Review Round 1 Remediations:**
  1. **Live-mode sub-second phase bar clobbering (`pkg/project/projector.go`, `pkg/project/projector_test.go`):** fixed → added 4-slot per-symbol closed-bar ring (`symBarRing`/`symBarAnchor`) indexed by `(secAnchorNS / cadenceNS) & 3` and triggered live-mode `Flush` on earliest phase or second anchor; verified by `TestProjectorCCM44LiveSubSecondPhaseNoClobber`.
  2. **Remove `self.snapshot(symbol)` from `load_window_1hz` (`python/tickhub/shm.py`):** fixed → terminal row `span_s` relies strictly on frame-versioned Slots `36..43`.
  3. **Advance `last_read_anchor_ns` in `load_window_1hz` (`python/tickhub/shm.py`):** fixed → advances `header.last_read_anchor_ns` to `oldest_anchor_ns` with store-release fence when `is_replay_mode` is true.
  4. **Publish latency SLO assertion & tolerance alignment (`tests/test_ccm_1hz_projection_parity.py`):** fixed → asserts `0 < anchor_publish_latency_ns < 15_000_000`, logs `max_abs_err` and `publish_latency_ns`, and uses `rtol=1e-12, atol=1e-7`.

---

## 3. SOLID Adherence & Mathematical Specification

- **Single Responsibility & Single Source of Truth (SRP/SSoT):** `pkg/project` owns incremental 1Hz bar aggregation, stateful hybrid Lee-Ready classification, and frame-versioned terminal state; `python/tickhub/shm.py` (`TickHubReader.load_window_1hz`) owns zero-polling on-demand ring-buffer extraction and 360-bar boundary backfill/forward-fill into the `Dict[str, np.ndarray]` format consumed by `MetricsEngine`.
- **Open/Closed Principle (OCP):** `Projector` inspects `nFeat` (`PhaseInfo.NumFeatures`). When `nFeat == 5`, the existing 5-feature rolling-return path runs untouched. When `nFeat == 44`, `Projector` activates the 44-slot `ccm` 1Hz projection mode (`isCCM44 = true`).
- **44-Slot Per-Frame Schema (`nFeat == 44`):**
  For a phase frame committed at `phase_anchor_ns = anchor_floor_ns + offset_ns` (where `anchor_floor_ns = (phase_anchor_ns / 1_000_000_000) * 1_000_000_000`), Slots `0..35` hold the completed whole-second bar `[T, T + 1s)` where `T = bar_start_ns = anchor_floor_ns - 1_000_000_000` (strictly ticks with `sip_ns < anchor_floor_ns`), and Slots `36..43` hold the exact point-in-time terminal state at `phase_anchor_ns` (ticks with `sip_ns <= phase_anchor_ns`):
  - **Slots 0..15 (`zeros_keys`, float64, default `0.0`):**
    `0: buy_volume`, `1: sell_volume`, `2: buy_notional`, `3: sell_notional`, `4: buy_trade_count`, `5: sell_trade_count`, `6: buy_mid_delta_notional`, `7: sell_mid_delta_notional`, `8: bid_quote_count`, `9: ask_quote_count`, `10: dark_bid_trade_primitive`, `11: dark_ask_trade_primitive`, `12: nbbo_quote_rate_primitive`, `13: nbbo_staleness_ms_primitive`, `14: exchanges_at_bb_primitive`, `15: exchanges_at_bo_primitive`.
  - **Slots 16..29 (`nan_keys`, float64, default `NaN`):**
    `16: high_px`, `17: low_px`, `18: high_bid_px`, `19: low_bid_px`, `20: high_ask_px`, `21: low_ask_px`, `22: last_bid_px`, `23: last_ask_px`, `24: last_bid_sz`, `25: last_ask_sz`, `26: last_trade_px`, `27: sip_timestamp` (`float64(lastQuoteSIPNS)`, matching `ccm/marketdata/projection.py`'s `float64` `nan_keys` array bit-for-bit), `28: trade_vol_std_px`, `29: trade_vol_skew` (slots `28..29` emit `NaN` on 0-trade bars so `load_window_1hz` forward-fills across 0-trade bars starting from `0.0` at row `0` of the 360s window while preserving `0.0` on bars with $>0$ trades and zero price dispersion).
  - **Slots 30..33 (`int64_keys` stored as lossless `float64` nanosecond offset `ts_ns - T` in `[0, 1e9)`, where `T = anchor_floor_ns - 1_000_000_000`, or `-1.0` when `0`):**
    `30: high_px_ns`, `31: low_px_ns`, `32: high_ask_px_ns`, `33: low_ask_px_ns`. Reconstructed in Python as `T + int64(offset)` (`0` when `offset < 0`).
  - **Slots 34..35 (cumulative tick counters at bar close `anchor_floor_ns`, float64):**
    `34: quote_idx` (`float64(cum_accepted_quotes_at_anchor_floor)`), `35: trade_idx` (`float64(cum_accepted_trades_at_anchor_floor)`).
  - **Slots 36..43 (frame-versioned terminal state at `phase_anchor_ns = anchor_floor_ns + offset_ns`, float64):**
    `36: asof_bid_px`, `37: asof_ask_px`, `38: asof_bid_sz`, `39: asof_ask_sz`, `40: asof_trade_px`, `41: asof_sip_timestamp` (`float64(asof_quote_sip_ns)` or `NaN`), `42: asof_quote_idx` (`float64(asof_cum_quotes)`), `43: asof_trade_idx` (`float64(asof_cum_trades)`).

---

## 4. Proposed Code Changes

### [MODIFY] `pkg/feed/types.go`
- Add `ParticipantTimestampNS int64`, `IsDark bool`, `BidExch int16`, `AskExch int16`, and `TradeExch int16` to `feed.Tick` (packing `Type`, `IsDark`, `BidExch`, `AskExch`, `TradeExch` into a single 8-byte word).

### [MODIFY] `pkg/feed/massive_ws.go`
- Extend `RawMassiveEvent` with `Pt int64` (`json:"pt"`), `Bx int16` (`json:"bx"`), `Ax int16` (`json:"ax"`), `X int16` (`json:"x"`), and `Trfi float64` (`json:"trfi"`).
- Populate `ParticipantTimestampNS` (`pt * 1_000_000` if `pt > 0` else `SIPTimestampNS`), `BidExch`, `AskExch`, `TradeExch`, and `IsDark` (`ev.Trfi > 0`).

### [MODIFY] `pkg/feed/parquet_reader.go`
- Add `Exchange int32` (`parquet:"exchange"`), `ParticipantTS int64` (`parquet:"participant_timestamp"`), and `TrfID *float64` (`parquet:"trf_id"`) to `parquetTradeRow`, and populate `ParticipantTimestampNS`, `TradeExch`, `IsDark` in `ParquetTradeReader.Next()`.
- Add `BidExchange int32` (`parquet:"bid_exchange"`), `AskExchange int32` (`parquet:"ask_exchange"`), and `ParticipantTS int64` (`parquet:"participant_timestamp"`) to `parquetQuoteRow`, and populate `ParticipantTimestampNS`, `BidExch`, `AskExch` in `ParquetQuoteReader.Next()`.

### [MODIFY] `pkg/project/window.go`
- Add the 44-slot `ccm` 1Hz bar state and methods (`InitCCM44`, `IngestCCMQuote`, `IngestCCMTrade`, `IngestPreWindowCCMQuote`, `IngestPreWindowCCMTrade`, `CloseCCMBar`, `WriteCCMTerminalState`) to `SymbolHistory`.

### [MODIFY] `pkg/project/projector.go`
- Detect `isCCM44` when `nFeat == 44`.
- In `isCCM44` mode, maintain per-unique-symbol `uniqueHistories` and `uniqueLastCCMBar` (`[44]float64`), close whole-second bars (`Slots 0..35`) at integer seconds `nextSecondAnchorNS`, and at each phase commit `closePhase(pIdx, anchorNS)` populate `Slots 36..43` via `WriteCCMTerminalState` with the symbol's point-in-time state as of `anchorNS`.
- Leave `SymbolSnapshot` and `snap.RecvTimestampNS` semantics completely unchanged.
- Add `IsCCM44() bool` and `IngestPreWindowTick(tick feed.Tick)`.

### [MODIFY] `cmd/tickhub/worker.go`
- In `serviceChunk`, when `projector.IsCCM44()` is true, pass pre-window ticks (`ts < startNS`) to `projector.IngestPreWindowTick(tick)`.

### [MODIFY] `config/live_72.yaml`, `config/universe64_arm_a_500ms.yaml`, `config/universe64_arm_b_250ms.yaml`
- Update `max_frames: 512` and list the 44 `ccm` 1Hz feature names in `features:`.
- Align `ALAB` in `config/universe64_arm_a_500ms.yaml` and `config/universe64_arm_b_250ms.yaml` with `manifests/universe_64_symbols.txt`.

### [MODIFY] `python/tickhub/shm.py`
- Add `CCM_44_FEATURES` constant and `load_window_1hz(self, symbol: str, anchor_ns: int, span_s: int = 360, out_dict: Optional[Dict[str, np.ndarray]] = None, timeout_ms: float = 50.0) -> Optional[Dict[str, np.ndarray]]` on `TickHubReader`.
- `load_window_1hz` computes `target_anchor = anchor_floor_ns + phase_offset_ns` for `symbol`'s phase, waits/spins up to `timeout_ms` until the ring-buffer slot for `target_anchor` is committed, reads the `span_s` whole-second bars (`Slots 0..35`) from the ring buffer, populates the terminal row `span_s` from `Slots 36..43` of the `target_anchor` frame (zero `SymbolSnapshot` reads), advances `header.last_read_anchor_ns` when `header.mode == 1`, reconstructs exact `int64` timestamps (`T + int64(offset)` for `high_px_ns`/`low_px_ns`/`high_ask_px_ns`/`low_ask_px_ns`), and applies the window-boundary backfill/forward-fill rules from `ccm/marketdata/projection.py`.

### [MODIFY] `tests/test_universe64_cadence_ab.py`
- Update feature parity assertion to use `np.allclose(fa, fb, atol=1e-8, equal_nan=True)` and `np.isfinite`, and assert `hdr_b['publish_latency_ns'] < 15_000_000` (`15 ms` SLO).

### [ADD] `tests/test_ccm_1hz_projection_parity.py`
- End-to-end parity test replaying real datalake Parquet ticks (`2026-08-03` `SPY`, `QQQ`, `ALAB`, `ANET` across `phase_0ms` and `phase_500ms`) through `TickHub` SHM (`nFeat == 44`, `max_frames == 512`), reading 360-second windows via `TickHubReader.load_window_1hz`, asserting exact parity across all 34 1Hz primitives (`zeros_keys`, `nan_keys`, `int64_keys`, plus `quote_idx` and `trade_idx`) and downstream `MetricsEngine` features against `ccm`'s `project_to_1hz`, and asserting `anchor_publish_latency_ns < 15_000_000` (`15 ms` SLO).

---

## 5. Telemetry Plan

- `scripts/validate_all.py` logs full build, Go `-race` unit test, Python pytest, and golden replay telemetry to `output/val_last_out.txt`.
- `tests/test_ccm_1hz_projection_parity.py` and `tests/test_universe64_cadence_ab.py` log per-primitive max absolute error (`0.00e+00`), mismatch counts (`0`), and SHM `anchor_publish_latency_ns` (`1.28 us` / `0.69 us`, well within the `< 15 ms` SLO).

---

## 6. Verification Plan

1. Run `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/validate_all.py` and confirm all 7 validation stages pass with zero regressions on existing 5-feature tests (`test_golden_replay.py`, `test_parquet_parity.py`, `pkg/project/...`) and new 44-feature parity tests (`test_ccm_1hz_projection_parity.py`, `test_universe64_cadence_ab.py`), including `< 15 ms` publish latency SLO assertions.
2. Run `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/codereview.py aiChangeLog/2026-10-14/001_full_ccm_1hz_projection_primitives.md` and obtain `APPROVED`.

---

## 7. Checklist

- [x] Ledger written and reviewed via `scripts/planreview.py` (`Proceed-with-noted-risks`)
- [x] `pkg/feed`, `pkg/project`, `cmd/tickhub/worker.go`, `config/*.yaml`, and `python/tickhub/shm.py` updated
- [x] `tests/test_ccm_1hz_projection_parity.py` added and passing (`Max diff=0.00e+00`, publish latency `1.28 us`)
- [x] `scripts/validate_all.py` passes all 7 stages (`output/val_last_out.txt`: 20/20 pytest passed, Go `-race` passed, golden replay 91,314 ticks/s passed)
- [x] `scripts/codereview.py` returns `APPROVED`
