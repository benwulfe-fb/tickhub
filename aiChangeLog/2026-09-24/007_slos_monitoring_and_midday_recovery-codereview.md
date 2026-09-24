# Code Review — aiChangeLog/2026-09-24/007_slos_monitoring_and_midday_recovery.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Verdict (parsed): APPROVED · Generated: 2026-09-24 19:48:08Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

This change formalizes and implements operational SLOs, multi-tier monitoring, and midday restart recovery for TickHub:

- **`pkg/shm/layout.go` & `python/tickhub/abi.py` & `c/tickhub.h`**:
  Replaces `_padProducer [24]byte` in `GlobalHeader` with `BootID uint64` (offset 104), `Generation uint32` (offset 112), `RecoveryMode uint32` (offset 116), and `_padProducer [8]byte` (offset 120), preserving 64-byte producer line alignment. Adds `RecoveryModeColdStart` (0), `RecoveryModeWarmSubCadence` (1), `RecoveryModeWarmResidentGap` (2), and `FlagColdStart` (`0x01`) in `FrameHeader.Flags`. Includes compile-time and runtime offset and size assertions across Go, Python, and C.
- **`pkg/shm/producer.go` & `pkg/shm/segment.go`**:
  Implements `CreateProducerWithRecovery()` to inspect resident `/dev/shm` segments, validating segment size, ABI magic/version, phase count, max frames, total symbols, and per-phase `NumFeatures`. Differentiates sub-cadence downtime ($< 1.0\text{s}$) from resident gap downtime ($\ge 1.0\text{s}$ and $< \text{maxRingNS}$) and cold start. Implements atomic quarantine (`StatusBooting`) followed by atomic metadata publishing (`Generation`, `RecoveryMode`, `DaemonPID`, `HeartbeatNS`, `BootID`). Adds `ReadHistoryBars()`, `SetFrameFlags()`, `Snapshot()`, and `Snapshots()`. Adds nil-checks to `Close()` and exposes `Segment.Size()`.
- **`pkg/project/projector.go` & `pkg/project/window.go`**:
  Adds `SeedHistory()` to reconstruct past bar prices and volumes from historical ring slots backwards via `SeedFromBars()`, preserving exact rolling return queue continuity. Adds `SeedPrevailingPrice()` to seed baseline prices from existing snapshot data during gap recovery, avoiding division-by-zero or `NaN` features. Implements `SetColdStart()` and `IsColdStart()`, asserting `FlagColdStart` on frames for 15 live bars after restart.
- **`cmd/tickhub/daemon.go`**:
  Integrates `CreateProducerWithRecovery()`. Dispatches recovery mode: seeds 60 historical bars across all symbols for sub-cadence recovery (`FlagColdStart = false`), or seeds prevailing snapshot prices and enforces `FlagColdStart = true` for 15s on resident gap recovery. Defers `SetStatus(StatusRunning)` until after history seeding completes. Starts metrics HTTP server with 2s graceful shutdown.
- **`pkg/metrics/server.go` & `pkg/metrics/server_test.go`**:
  HTTP server exposing `/healthz` (200 OK), `/readyz` (evaluates `StatusRunning` and heartbeat freshness $\le 3.0\text{s}$), and `/metrics` (Prometheus text format exposing uptime, status, mode, generation, recovery mode, boot ID, latency, watermark, tick counters, `tickhub_committed_anchors_total`, `tickhub_recovery_downtime_nanoseconds`, and `tickhub_recovery_missed_anchors_total`).
- **`cmd/tickhub/status.go` & `cmd/tickhub/main.go`**:
  Standalone `tickhub status` CLI command rendering an ANSI terminal dashboard at 4Hz. Reads header telemetry and symbol snapshots using SeqLock acquire loads without external dependencies.
- **`python/tickhub/shm.py`**:
  Adds `HeartbeatTimeoutError` when daemon heartbeat age $> 3.0\text{s}$. Adds properties `boot_id`, `generation`, `recovery_mode`, `is_warm_recovered`, and `is_cold_start`. Enhances `reconnect_if_needed()` to handle `StatusBooting` backoff (up to 2.0s), `BootID` changes, and phase-offset-aware cursor realignment. Adds `_auto_realign` support for `LaggedAnchorError` recovery.
- **`tests/producer_helper.go` & `tests/test_midday_recovery.py`**:
  Helper CLI flags for recovery mode, anchor overrides, cold-start flags, and background heartbeats. Integration tests covering sub-cadence recovery math continuity, resident gap recovery with cursor fast-forward, heartbeat timeout detection, and cold-start flag transitions.

---

## 2. Correctness & Concurrency Bugs

No blocking bugs or data races identified in the revised change set. Prior review findings were verified and remediated:

- **[C1 Remediation Verified]** In `cmd/tickhub/daemon.go` (lines 175–205), `prod.SetStatus(shm.StatusRunning)` is deferred until after `SeedHistory()` or `SeedPrevailingPrice()` completes across all symbols and phases. In `python/tickhub/shm.py` (lines 1702–1708), `reconnect_if_needed()` checks `self._header.status == 1` (`STATUS_BOOTING`) and backs off for up to 2.0s before reading `boot_id` and evaluating anchor positions. Python readers do not observe a live new `BootID` while the daemon is actively seeding.
- **[C2 Verified]** In `pkg/project/projector.go` (lines 254–263), `p.producer.SetFrameFlags(pIdx, anchorNS, shm.FlagColdStart)` is called in `closePhase()` strictly after symbol metrics are written and immediately before `p.producer.CommitFrameFinalize(anchorNS)`. Atomic store to `frameHdr.Flags` is committed before the finalize sequence increment.
- **[C3 Verified]** In `pkg/shm/producer.go` (line 1409), `(anchor / cadenceNS) & int64(p.header.MaxFrames - 1)` correctly masks two's complement negative quotients for valid modular slot indexing before casting to `uint32`.
- **[C7 Remediation Verified]** In `pkg/shm/producer.go` (lines 1235–1249), `CreateProducerWithRecovery()` validates `hdr.Phases[pIdx].NumFeatures == numFeatures` across all configured phases before allowing resident recovery. Segments formatted with incompatible feature counts safely fall back to clean cold-start allocation.
- **[C10 Remediation Verified]** In `cmd/tickhub/status.go` (lines 2028–2032), `rawName` is stripped of `/dev/shm/` and `tickhub_` prefixes before calling `shm.AttachSegment(cleanName, true)`. `shmPath()` in `pkg/shm/segment.go` canonicalizes names to `/dev/shm/tickhub_<name>`, resolving segments correctly regardless of prefix.
- **[C11 Remediation Verified]** In `tests/test_midday_recovery.py` (lines 2561–2572), `wait_for_ready()` uses `select.select([proc.stdout], [], [], 0.05)` and checks `proc.poll()`, preventing deadlocks if the helper process terminates prematurely.

---

## 3. Projection Math & Temporal Parity

- **Sub-Cadence Recovery Parity ($< 1.0\text{s}$)**:
  `window.go` `SeedFromBars()` (lines 1054–1066) walks historical bars chronologically backwards from `len(bars) - 1` to `0`. It seeds `prices[s] = px` and back-propagates prices via $P_{t-1} = P_t \cdot \exp(-r_{1\text{s}})$. Volume history is seeded from feature index 3 (`vol_1s`). When subsequent live ticks arrive, lookback intervals ($[T-15\text{s}, T)$, $[T-5\text{s}, T)$, $[T-1\text{s}, T)$) reference populated slots. Verified in `TestProjectorSeedHistoryAndColdStart`: continuous and seeded projectors match $r_{15\text{s}}$ to $< 10^{-9}$ tolerance.
- **Resident Gap Math Protection ($\ge 1.0\text{s}$)**:
  When downtime spans one or more 1Hz boundaries, `daemon.go` calls `SeedPrevailingPrice()` (lines 189–198) from SHM top-of-book snapshots. `window.go` (lines 1070–1094) initializes all history slots with `lastPx` and sets `hasTraded = true`, preventing zero-division or `+Inf`/`NaN` log returns. `FlagColdStart` (`0x01`) is asserted on all frames for 15 seconds (15 live bars), alerting downstream models that rolling features are converging.
- **Half-Open Interval $[T-1\text{s}, T)$ Preserved**:
  Projector windowing logic in `IngestTick()` and `closePhase()` remains untouched for live processing. `phaseNextAnchor` initialization handles post-recovery anchors without look-ahead leakage.

---

## 4. Deviations from the Approved Plan

- **[D1 / M4 Remediation Verified]**: Snapshot prevailing prices are seeded into `Projector` via `SeedPrevailingPrice()` during `RecoveryModeWarmResidentGap` (ledger §4.2 Branch B).
- **[D2 Remediation Verified]**: `daemon.go` seeds 60 historical bars (`ReadHistoryBars(pIdx, sIdx, 60)`), matching ledger §4.2.
- **[D3 Remediation Verified]**: C header (`c/tickhub.h`) mirrors `GlobalHeader` field additions (`boot_id`, `generation`, `recovery_mode`, `_pad_producer[8]`) and frame flag definitions.
- **[D4 Remediation Verified]**: Prometheus exporter in `pkg/metrics/server.go` emits `tickhub_committed_anchors_total`, `tickhub_recovery_downtime_nanoseconds`, and `tickhub_recovery_missed_anchors_total`.
- **[D5 Remediation Verified]**: Telemetry log in `cmd/tickhub/daemon.go` outputs downtime duration and symbol counts matching ledger §7.
- **Operator-Accepted, Tracked**:
  - `window.go` assumes feature index 0 is `log_ret_1s` and index 3 is `vol_1s`. Consistent with default pipeline configuration.
  - Prometheus `/metrics` handler formats responses synchronously. Off critical path, zero impact on 1Hz projection loops.

---

## 5. Systems & Performance Violations

- **Zero-Copy & Zero-Allocation Hot Path**:
  Neither `IngestTick()` nor `closePhase()` introduces dynamic heap allocations in steady state. `featBuffer` is reused.
- **Dual Cache-Line Isolation**:
  `GlobalHeader` fields are strictly partitioned:
  - Producer Cache Line (offset 64–128): `AnchorPublishLatencyNS` (64), `WatermarkBufferNS` (72), `LastWrittenAnchorNS` (80), `DroppedTickCount` (88), `TotalTickCount` (96), `BootID` (104), `Generation` (112), `RecoveryMode` (116), `_padProducer [8]byte` (120).
  - Consumer Cache Line (offset 128–192): starts at offset 128 (`LastReadAnchorNS`).
  Zero false-sharing between producer writes and consumer reads. Verified by static offset assertions in `pkg/shm/layout.go` and runtime assertions in `python/tickhub/abi.py`.
- **SeqLock Integrity**:
  `cmd/tickhub/status.go` validates SeqLock sequence parity (`seq1 % 2 == 0` and `seq1 == seq2`) when inspecting top-of-book snapshots.

---

## 6. Telemetry / Verification Gaps

- **Verification Plan Executed Cleanly**:
  `scripts/validate_all.py` completed all 7 verification tiers green:
  1. C shared library compilation (`libtickhub_atomic.so`).
  2. ABI alignment and struct size checks across Go, Python, and C.
  3. Go build (`cmd/tickhub`, `tests/producer_helper`).
  4. Go unit and race tests (`pkg/metrics`, `pkg/project`, `pkg/relay`, `pkg/shm`).
  5. Python integration test suite (13/13 tests passed, including `test_midday_recovery.py`).
  6. Golden replay bit-identity and performance regression gate (117,553 ticks/s throughput, 19.23ms latency).
  7. CLI smoke execution.
- **[T4 Remediation Verified]**: `tests/test_midday_recovery.py` explicitly asserts `assert reader.is_cold_start is False` after sub-cadence recovery.

---

## 7. Verdict & Remediation

**APPROVED**

The change set satisfies all architectural, concurrency, mathematical, and plan specifications:
- Correct atomic memory ordering across daemon restarts and client re-attachment.
- Exact rolling return parity under sub-cadence recovery with feature warmup protection under gap recovery.
- Strict 64-byte dual cache-line alignment maintained across Go, C, and Python ABIs.
- Comprehensive telemetry endpoints (`/metrics`, `/healthz`, `/readyz`, `tickhub status`).
- Full test suite and validation gates pass cleanly with zero regressions.
