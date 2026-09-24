# Plan Review — aiChangeLog/2026-09-23/005_dataloader_control_line_and_parquet_parity_gate.md

_Model: Gemini 3.8 Flash (High) · Round: 3 · Generated: 2026-09-24 01:13:29Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 3/2 · Verdict parsed: **Proceed** · OPERATOR OVERRIDE: Fix B2 POSIX SHM persistent mapping and prevailing price seeding_

---

## 1. Summary of the Proposal

Plan adds a bidirectional SHM control line (`ControlRequest` at offset 0x01C0, `ControlResponse` at offset 0x0200 in `GlobalHeader._reserved`) enabling PyTorch DataLoader workers to command dedicated `tickhub worker` daemons to replay arbitrary `(symbol, date, [start_anchor_ns, end_anchor_ns))` Parquet slices into private SHM segments for zero-copy consumption. Whole feature is gated on a 100% bitwise parity check (`vol_1s`, `log_ret_1s`, `spread_bps`) between TickHub's `project1hz` and legacy `ccm.marketdata.projection.project_to_1hz`. Round 3 revision resolves Round 2 B2 by establishing persistent daemon-owned SHM segments with in-place client crash recovery (`syscall.Kill`), pre-window trade and quote seeding, and explicit ABI padding. Touches 7 files.

---

## 2. Simplest Sufficient Design

Plan is at minimum sufficient footprint (7 files, 1:1 private SHM segments matching PyTorch multi-process DataLoader, no intermediate broker or multiplexing layer). No cut list.

---

## 3. Blocking Defects

`None.`

---

## 4. Non-Blocking Observations

1. `ErrorMsg [24]byte` in `ControlResponse` will truncate detailed OS filesystem paths and Parquet I/O error messages.
2. Ring buffer slot addressing for chunk replay (resetting to slot 0 vs. modulo anchor time) should be explicitly documented in `python/tickhub/shm.py` so downstream tensor slicing is unambiguous.
3. Parquet parity test currently validates only liquid DASH; an illiquid symbol test case should be added post-implementation to verify forward-fill edge cases.
4. `TickHubReader.request_chunk` should initialize `RequestID` strictly greater than existing `ControlResponse.ResponseID` on client attach to avoid matching stale handshake state.
5. Telemetry should track per-chunk replay duration and tick ingestion counts in `ControlResponse` or daemon logs against the 30 ms SLA.

---

## 5. Methodological & Data-Alignment Concerns

- **Warm-Up Seeding**: Seeding `lastBid`, `lastAsk`, and `lastPrice` strictly preceding $T_{\text{start}}$ eliminates cold-start return and spread divergence across non-contiguous training chunks while maintaining causal boundary integrity.
- **Interval Semantics**: Half-open $[T_{\text{start}}, T_{\text{end}})$ producing anchors $T \in (T_{\text{start}}, T_{\text{end}}]$ with frame $T$ covering $[T-1\text{s}, T)$ correctly reflects standard 1Hz projection interval semantics.
- **ABI Parity**: Explicit `_pad uint32` in `ControlResponse` between `ColdStartFrames` and `FirstAnchorNS` guarantees 8-byte alignment across Go and Python ctypes.

---

## 6. Missing Telemetry

`None.`

---

## 7. Verdict

**Verdict: Proceed**

Round 2 B2 IPC deadlock defect is resolved in §Architecture / 3 by eliminating `shm_unlink` on client initialization and client crash recovery. Go daemons retain persistent ownership of `/dev/shm/tickhub_worker_<worker_id>`, while dead client recovery is performed via in-place state reset (`StatusBusy → StatusIdle`) guarded by `syscall.Kill(ConsumerPID, 0)`. Pre-window trade price and BBO warm-up seeding (§Architecture / 4) and explicit ctypes ABI padding (§Architecture / 1) are established in the design. Plan has no remaining blocking defects and is cleared for implementation.
