# Plan Review — aiChangeLog/2026-09-23/005_dataloader_control_line_and_parquet_parity_gate.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 01:11:23Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Blocked (B2)**_

---

# Plan Review — aiChangeLog/2026-09-23/005_dataloader_control_line_and_parquet_parity_gate.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 (final) · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Plan adds bidirectional SHM control line (`ControlRequest` at offset 0x01C0, `ControlResponse` at offset 0x0200 in `GlobalHeader._reserved`) enabling PyTorch DataLoader workers to command private Go `tickhub worker` daemons to replay arbitrary `(symbol, date, [start_anchor_ns, end_anchor_ns))` parquet slices into a private SHM ring buffer for zero-copy consumption. Whole feature gated on 100% bitwise parity check (`vol_1s`, `log_ret_1s`, `spread_bps`) between TickHub's `project1hz` and legacy `ccm.marketdata.projection.project_to_1hz`. Round 2 revision adds private per-worker SHM segments, client PID check (`syscall.Kill`), 5.0s request timeout, BBO warm-up seeding, and tighter execution budgets ($\le 30$ ms). Touches 7 files.

---

## 2. Simplest Sufficient Design

Plan is at or near minimum. 7 files touched: Go/Python layout and ctypes ABI, `tickhub worker` subcommand, Python `request_chunk()` API, parity test, integration test, validation script wiring. 1:1 segment model matches PyTorch multi-process DataLoader without multiplexing overhead. No unnecessary abstractions. No cut list.

---

## 3. Blocking Defects

**B2 loss (Concurrency Deadlock / Broken IPC Rendezvous via `shm_unlink`)**

The claimed remediation for B2 in §Architecture / 3.1 & 3.2 introduces a fatal POSIX shared memory rendezvous disconnect:
- §Architecture / 3.1 specifies: *"On worker initialization, any existing stale segment is unlinked before creating the fresh segment."*
- §Architecture / 3.2 specifies: *"If `ConsumerPID > 0` and process is dead, the daemon resets `StatusBusy → StatusIdle`, clears request state, and unlinks if configured."*
- §Proposed Code Changes / 3 specifies: `tickhub worker` is a persistent daemon process (*"we'd keep 4 running tickhub's"*) that maps the private SHM segment once at startup and enters its poll loop.

Under Linux POSIX shared memory semantics (`shm_unlink`), unlinking an open SHM file removes the name from `/dev/shm`, but existing mappings remain bound to the orphaned anonymous inode. When a PyTorch DataLoader worker restarts or recovers from a crash, unlinking and calling `shm_open(O_CREAT)` allocates a brand new inode at `/dev/shm/tickhub_worker_<worker_id>`. The newly spawned Python client writes `ControlRequest` to the new inode; the persistent Go daemon continues polling the detached old inode. The daemon never receives the request, and the client deadlocks until `timeout_s=5.0` expires. The recovery mechanism designed to survive client crashes guarantees total communication failure on client restart.

**Resolution required:** Eliminate `shm_unlink` on worker initialization and daemon crash handling. The SHM segment must be persistent across client lifetimes, created and owned by the Go daemon. Python clients must attach via `shm_open` without unlinking. On client death detected via `ConsumerPID`, the Go daemon must reset `StatusBusy → StatusIdle` in-place on the shared segment without unlinking the file.

---

## 4. Non-Blocking Observations

1. `ControlResponse` has an implicit 4-byte padding between `ColdStartFrames` (uint32) and `FirstAnchorNS` (int64); add explicit `_pad [4]byte` to prevent ctypes struct layout misalignment.
2. §Architecture / 4 seeds only prevailing BBO quotes before $T_{\text{start}}$, leaving prevailing trade price unseeded and exposing frame 0 `log_ret_1s` to cold-start divergence if a trade occurs at window start.
3. Golden parity gate covers only a single 60s slice of liquid DASH; adding one illiquid symbol with zero-trade intervals is needed to verify forward-fill parity before production deployment.
4. Ring-buffer addressing in chunk replay is unspecified; plan must define whether frame write index resets to slot 0 on each chunk so Python zero-copy slicing is deterministic.
5. `ControlResponse.ErrorMsg` at 24 bytes will truncate common filesystem paths and I/O error strings.

---

## 5. Methodological & Data-Alignment Concerns

- **Trade Price Cold-Start Omission**: §Architecture / 4 seeds `lastBid` and `lastAsk` from `DASH.quotes.parquet` to fix spread cold-start, but fails to seed `lastTrade` from `DASH.trades.parquet`. In randomized chunks where the first bar has a trade or requires return computation against $P_{-1}$, the unseeded prior trade price will yield division-by-zero, infinite returns, or divergence from legacy `project_to_1hz`.
- **Temporal Interval Off-By-One**: §Architecture / 2 specifies $T \in (T_{\text{start}}, T_{\text{end}}]$ with cadence 1s, which produces 60 frames for a 60-second window, whereas §Measurement / 1 evaluates 59 frames. The 1-frame discrepancy between legacy diff returns and TickHub frame emission must be explicitly aligned in `test_parquet_parity.py`.
- **Cross-Language ABI Alignment**: `ControlResponse` field definitions must explicitly account for 8-byte alignment of `int64` in Go and Python ctypes to prevent silent field offset drift.

---

## 6. Missing Telemetry

- `tickhub worker` must log client crash detections and in-place state resets: `[WORKER] Client PID <PID> dead, reset StatusBusy -> StatusIdle on <shm_name>`.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The Round 2 remediation for B2 introduces an active IPC deadlock in §Architecture / 3. Unlinking the private SHM segment on worker initialization (§3.1) and on client crash (§3.2) while maintaining persistent `tickhub worker` daemons severs the POSIX shared memory rendezvous point. The persistent Go daemon remains mapped to the unlinked inode while restarted DataLoader workers write to a newly created inode, causing permanent communication loss and request timeouts. The SHM segment must persist across worker lifecycles with in-place daemon state resets.
