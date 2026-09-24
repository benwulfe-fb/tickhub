# Technical Strategy: Service Level Objectives (SLOs), Monitoring & Dashboard Integration, and Midday Daemon Restart Recovery

**Date**: 2026-09-24  
**Author**: Antigravity  
**Status**: APPROVED (Remediated post Round 2 Plan Review)  
**Corpus**: tickhub  
**Sequential ID**: 007  

---

## 1. Goal & Context

TickHub decouples irregular high-frequency market data tick streams from downstream Python trading engines (`ccm-live` on GCP VM, `ccm-paper` on remote WSL). As TickHub moves toward production live trading, three mission-critical operational pillars must be formalized, engineered, and verified:

1. **Service Level Objectives (SLOs)**:
   - Quantified performance budgets, latency targets, and data integrity guarantees for 1Hz feature projection, shared memory publishing, lock-free snapshot reads, and cross-machine TCP relay replication.
2. **Monitoring & Dashboard Integration**:
   - Multi-tier observability:
     - **SHM Header Telemetry** (< 10ns zero-overhead telemetry for internal processes via cache lines).
     - **Prometheus HTTP Metrics Endpoint** (`/metrics` on `:9090`) exposing ingest rate, queue sizes, per-phase publishing latency histograms, dropped ticks, and relay health.
     - **Health & Readiness Endpoints** (`/healthz`, `/readyz`) for automated systemd / GCP process supervision.
     - **Realtime Status Inspection (`tickhub status`)**: Zero-dependency ANSI live terminal dashboard for operators inspecting tick rates, SeqLock snapshots, 1Hz ring buffer health, and relay clients.
3. **Midday Restart Semantics & Client Resilience**:
   - Formal specification and implementation of daemon midday recovery addressing downtime gaps and rolling feature mathematical integrity:
     - **Sub-Cadence Warm Recovery (< 1.0s downtime)**: Zero missed 1Hz frames. Daemon seamlessly re-attaches, rolling return queues remain continuous, `FlagColdStart = false`.
     - **Resident Re-attach with Gap ($\ge 1.0\text{s}$ downtime)**: Daemon was down across one or more 1Hz boundaries. Previous ring bars are preserved for historical lookups, but rolling return queues are reset and `FlagColdStart = true` is enforced for 15 seconds to prevent calculating corrupted returns across the downtime black hole.
     - **Cold Recovery (SHM Cleared / Stale)**: Daemon re-truncates and formats clean segment, flags `FlagColdStart = true` for 15s.
     - **Client Re-Attachment Protocol**: Downstream clients (`TickHubReader`) detect daemon lifecycle changes (`BootID`, `Generation`), fast-forward across downtime anchor gaps instead of hanging, and observe `FlagColdStart` to inhibit trading until rolling features converge.

---

## 2. Service Level Objectives (SLOs)

| Metric | Target (SLO) | Measurement Method | Failure Action |
| :--- | :--- | :--- | :--- |
| **1Hz Publishing Latency** | $P_{99} \le 15\,\text{ms}$ after bar anchor $T$ | `AnchorPublishLatencyNS` in SHM Producer cache line | Alert via Prometheus, log watermark delay |
| **SeqLock Snapshot Read** | $P_{99} \le 50\,\text{ns}$ | Client benchmarking (`load_acquire_i64` SeqLock loop) | Contention alert |
| **1Hz Feature Matrix Load** | $P_{99} \le 2.0\,\mu\text{s}$ (72 symbols $\times$ 5 features) | Zero-copy `TickHubReader.load_sync` | Diagnostic log on buffer bounce |
| **Local SHM Frame Loss** | **0 frames lost** ($100.000\%$ delivery) | Monotonic anchor check ($T_{k} - T_{k-1} = \text{cadence}$) | Raise `LaggedAnchorError` & fast-forward |
| **Relay Loopback Latency** | $P_{99} \le 500\,\mu\text{s}$ per 1Hz anchor | Relay commit timestamp minus anchor timestamp | TCP buffer tuning, alert on backlog |
| **Relay LAN/WAN Latency** | $P_{99} \le 10\,\text{ms}$ (VM to WSL over VPN/wire) | Client received anchor timestamp vs anchor $T$ | Alert sequence gap, log transmission lag |
| **Relay Frame Delivery** | $99.999\%$ delivery; gaps flagged immediately | Monotonic frame sequence validation in `relay-client` | Immediate reconnect & sequence gap alert |
| **Daemon Midday Warm Recovery** | $T_{\text{recover}} \le 200\,\text{ms}$ to first frame | Daemon restart timestamp to `StatusRunning` | Re-read ring buffer state |
| **Daemon Midday Cold Recovery**| $T_{\text{converge}} \le 15\,\text{s}$ (feature convergence) | `FrameHeader.Flags & FlagColdStart` clear | Alert models that features are in warm-up |
| **Heartbeat Freshness** | Stale threshold: $> 3.0\,\text{s}$ without update | `time.Now().UnixNano() - HeartbeatNS` | Mark `/readyz` 503, downstream pause trading |

---

## 3. SOLID & Architectural Adherence

- **Single Responsibility (SRP)**:
  - `pkg/metrics/server.go`: Responsible solely for HTTP metrics exposure (`/metrics`, `/healthz`, `/readyz`).
  - `pkg/shm/producer.go`: Encapsulates warm-recovery inspection, rolling history extraction from existing ring slots, and generation tagging.
  - `cmd/tickhub/status.go`: Lightweight zero-dependency ANSI live status inspection (`tickhub status`) reading SHM read-only.
  - `python/tickhub/shm.py`: Client-side reconnection, generation change detection, gap skipping, and cursor re-alignment.
- **Open/Closed (OCP)**:
  - Metric exporters (Prometheus, CLI status) consume standard Go telemetry interfaces without modifying projector or SHM hot loops.
- **Single Source of Truth (SSoT)**:
  - All rolling 1Hz mathematical logic remains strictly in `pkg/project/projector.go`. Seeding during warm recovery reuses the exact same history buffer state.
- **Dual Cache-Line Isolation**:
  - Prometheus metrics scraper attaches read-only to `/dev/shm` and samples the Producer cache line (offset 64) via acquire loads, causing zero cache-line bouncing on producer write paths.

---

## 4. Midday Restart & Client Resilience Specification

### 4.1. Lifecycle & Generation Tagging in SHM
To allow clients to unambiguously differentiate between a momentary tick pause and a daemon restart, `GlobalHeader` is enhanced strictly within the Producer Cache Line padding (`_padProducer [24]byte` at offset 104), ensuring zero byte-offset changes for existing fields:
- `BootID uint64`: Offset 104 (0x0068), 8 bytes. Random 64-bit UUID generated on daemon startup.
- `Generation uint32`: Offset 112 (0x0070), 4 bytes. Monotonically incremented each time the SHM segment is formatted or recovered.
- `RecoveryMode uint32`: Offset 116 (0x0074), 4 bytes. `0 = ColdStart`, `1 = WarmSubCadence`, `2 = WarmResidentGap`.
- `_padProducer [8]byte`: Offset 120 (0x0078), 8 bytes. Preserves 64-byte producer line alignment (ending at offset 128).

All lifecycle fields are producer-written and reside on the producer cache line (offset 64-128).

### 4.2. Daemon Midday Recovery Protocol & Atomic Write Ordering
To prevent race conditions with concurrent Python readers during the re-attach window and eliminate feature corruption across downtime gaps:
1. **Probe Existing Segment**:
   - Inspect `/dev/shm/<name>`.
   - If segment exists, size matches, and `Magic == TICKHUB1`:
     - Load `lastAnchor = hdr.LastWrittenAnchorNS`.
     - Calculate downtime: `downtime = time.Now().UnixNano() - lastAnchor`.
     - **Branch A: Sub-Cadence Warm Recovery (`0 < downtime < CadenceInterval`)**:
       - True instantaneous restart (zero missed 1Hz frames).
       - Set `atomic.StoreUint32(&hdr.Status, StatusBooting)`.
       - Read up to 60 previous bars read-only to seed `Projector` (`SeedHistory`).
       - Publish metadata atomically:
         - `atomic.AddUint32(&hdr.Generation, 1)`
         - `atomic.StoreUint32(&hdr.RecoveryMode, 1)` (WarmSubCadence)
         - `atomic.StoreInt64(&hdr.DaemonPID, int64(os.Getpid()))`
         - `atomic.StoreInt64(&hdr.HeartbeatNS, time.Now().UnixNano())`
         - `atomic.StoreUint64(&hdr.BootID, newBootID)`
         - `atomic.StoreUint32(&hdr.Status, StatusRunning)`
       - Projection continues without gap; `FlagColdStart = false`.
     - **Branch B: Resident Re-attach with Gap (`CadenceInterval <= downtime < MaxFrames * CadenceInterval`)**:
       - Daemon was offline for one or more 1Hz anchors.
       - Calculate missed anchors: `missed = downtime / CadenceInterval`.
       - Set `atomic.StoreUint32(&hdr.Status, StatusBooting)`.
       - Read latest bar prices to seed prevailing midprice and last trade, but **do not** stitch rolling return queue across the gap.
       - Configure `Projector.SetColdStart(true)` for 15 seconds (15 contiguous live bars required to re-converge `ret15s`).
       - Publish metadata atomically:
         - `atomic.AddUint32(&hdr.Generation, 1)`
         - `atomic.StoreUint32(&hdr.RecoveryMode, 2)` (WarmResidentGap)
         - `atomic.StoreInt64(&hdr.DaemonPID, int64(os.Getpid()))`
         - `atomic.StoreInt64(&hdr.HeartbeatNS, time.Now().UnixNano())`
         - `atomic.StoreUint64(&hdr.BootID, newBootID)`
         - `atomic.StoreUint32(&hdr.Status, StatusRunning)`
       - Log: `[RECOVERY] Resident segment recovered with downtime gap of %v (%d missed frames). Enforcing FlagColdStart for 15s.`.
     - **Branch C: Cold Recovery (`downtime < 0` or `downtime >= MaxFrames * CadenceInterval`)**:
       - Segment data is too old (ring buffer wrapped). Re-truncate and format clean segment with `RecoveryMode = 0`, `BootID = newBootID`, `Generation = 1`, `FlagColdStart = true` for 15s, `Status = StatusRunning`.
2. **Cold Start Flagging**:
   - When running in cold start or after a gap, `FrameHeader.Flags` sets bit `0x01` (`FlagColdStart`).
   - After 15 seconds of contiguous live bars, clear `FlagColdStart`.
   - Downstream trading engines inspect `flags & 0x01` to determine whether to inhibit trade execution during warmup.

### 4.3. Client-Side Resilience Protocol (`TickHubReader`)
Downstream consumers (`ccm-live` on VM, `ccm-paper` on WSL) execute a continuous liveness and generation monitor:
1. **Liveness & Generation Protocol**:
   - On every `load_sync` or 1-second evaluation:
     - Check `Header.Status`. If `Status == StatusBooting`, sleep 10ms and retry (up to 2000ms) to allow daemon startup/recovery to finalize.
     - Load `Header.BootID` via acquire. Compare with `self._boot_id`.
     - If `BootID` differs:
       - Daemon restart detected!
       - Update `self._boot_id = new_boot_id`.
       - If `Header.RecoveryMode == 1` (WarmSubCadence):
         - Check if `Header.LastWrittenAnchorNS == cursor.anchor + CadenceInterval`.
         - If contiguous: continue seamlessly without resetting cursors.
         - If gap occurred: proceed to gap handler.
       - If `Header.RecoveryMode in (0, 2)` (ColdStart or WarmResidentGap):
         - Fast-forward cursor: `self.rebegin()` or align to latest published anchor.
         - Check `frame.flags & FlagColdStart`. If set: log `[RESILIENCE] Restart recovery active with FlagColdStart=True. Model trade execution inhibited during warmup.`.
2. **Anchor Gap Detection**:
   - Even without `BootID` change, if `Header.LastWrittenAnchorNS > cursor.anchor + CadenceInterval`:
     - Reader detects gap (missed frames).
     - Reader logs `[RESILIENCE] Anchor gap detected: cursor %d -> latest %d (gap %d ms). Fast-forwarding cursor.`.
     - Fast-forwards cursor to `Header.LastWrittenAnchorNS`.
3. **Heartbeat Stall Detection**:
   - If `time.time_ns() - Header.HeartbeatNS > 3_000_000_000` (3.0 seconds):
     - Raise `HeartbeatTimeoutError("TickHub daemon heartbeat stalled > 3.0s")`.
     - In live execution, `ccm-live` pauses new order entry until heartbeat resumes.

---

## 5. Monitoring & Dashboard Architecture

```
                      +---------------------------------------+
                      |       socket.massive.com WebSocket    |
                      +---------------------------------------+
                                          |
                                          v
                      +---------------------------------------+
                      |         tickhub daemon (Go)           |
                      |  - Ingest & Projector (1Hz SSoT)      |
                      |  - Dual Cache-Line SHM Telemetry      |
                      |  - HTTP Server (:9090)                |
                      +---------------------------------------+
                                  /       |       \
           Prometheus Scrape     /        |        \     Zero-Copy Read
          (/metrics, /healthz)  /         |         \
                                v          v          v
                   +---------------+  +----------+  +----------------------+
                   |  Prometheus   |  | /dev/shm |  | `tickhub status` CLI |
                   |  & Grafana    |  | tickhub  |  | Realtime Terminal    |
                   +---------------+  +----------+  +----------------------+
                                           |
                                     TCP Relay (THRL)
                                           |
                                           v
                             +----------------------------+
                             | tickhub relay-client (WSL) |
                             | Replicates into /dev/shm   |
                             +----------------------------+
                                           |
                                           v
                             +----------------------------+
                             | Python ccm-paper (WSL)     |
                             | Reads via TickHubReader    |
                             +----------------------------+
```

### 5.1. Prometheus Metrics Specification (`/metrics` on `:9090`)
- `tickhub_uptime_seconds`: Process uptime.
- `tickhub_ticks_total{type="quote|trade"}`: Counter of raw ticks processed.
- `tickhub_ticks_dropped_total`: Counter of ticks dropped due to buffer overrun or backpressure.
- `tickhub_publish_latency_nanoseconds`: Histogram of time between anchor time $T$ and SHM commit finalize.
- `tickhub_watermark_buffer_nanoseconds`: Current buffer duration between wall clock and market tick time.
- `tickhub_committed_anchors_total`: Total 1Hz frames finalized.
- `tickhub_daemon_status`: Gauge (0: Uninit, 1: Booting, 2: Running, 3: Halting, 4: Closed).
- `tickhub_daemon_recovery_mode`: Gauge (0: Cold, 1: WarmSubCadence, 2: WarmResidentGap).
- `tickhub_daemon_generation`: Current SHM generation sequence.
- `tickhub_recovery_downtime_nanoseconds`: Gauge of duration daemon was down prior to restart.
- `tickhub_recovery_missed_anchors_total`: Counter of 1Hz anchors missed during downtime.
- `tickhub_relay_clients_connected`: Gauge of active TCP relay clients.
- `tickhub_relay_sequence_gaps_total`: Counter of sequence gaps reported by relay clients.

### 5.2. Health Check Endpoints
- `GET /healthz`: Returns HTTP 200 `OK` if daemon process is alive.
- `GET /readyz`: Returns HTTP 200 `READY` only if:
  - WebSocket feed is authenticated and connected.
  - Daemon status is `StatusRunning`.
  - Heartbeat timestamp updated within last 3.0 seconds.
  - Return HTTP 503 `NOT READY` during cold start warmup, booting/recovering, or feed disconnection.

### 5.3. Realtime Terminal Status Dashboard (`tickhub status`)
Command: `tickhub status [--shm tickhub_live] [--watch]`
Zero external dependencies (pure Go stdlib with ANSI escape sequences):
- Inspects `GlobalHeader`, `SymbolDirectoryEntry`, and `SymbolSnapshot` via read-only pointer.
- Prints daemon status, uptime, generation, recovery mode, publish latency, watermark buffer, and top-of-book sample.
- If `--watch` specified, loops every 250ms clearing screen with ANSI `\033[H\033[2J`.

---

## 6. Proposed Code Changes

### 1. `pkg/shm/layout.go` & `pkg/shm/producer.go` [MODIFY]
- Update `GlobalHeader`:
  - Replace `_padProducer [24]byte` with:
    - `BootID uint64` (offset 104)
    - `Generation uint32` (offset 112)
    - `RecoveryMode uint32` (offset 116)
    - `_padProducer [8]byte` (offset 120)
- Add `FlagColdStart uint32 = 0x01` to `FrameHeader.Flags`.
- Update `CreateProducer` to accept existing segment re-attachment if `AllowWarmRecovery` is enabled.
- Implement read-only `ReadHistoryBars(phaseIdx, symbolIdx int, maxBars int) []FrameBar` from existing ring buffer.
- Implement atomic write sequence for recovery publishing (`StatusBooting` -> seed -> metadata -> `StatusRunning`).

### 2. `pkg/project/projector.go` [MODIFY]
- Add `SeedHistory(phaseIdx, symbolIdx int, bars []FrameBar)` allowing projector to pre-populate rolling price state on restart.
- Add `SetColdStart(cold bool)` and set `FlagColdStart` bit in committed `FrameHeader.Flags`.

### 3. `pkg/metrics/server.go` [ADD]
- Implements HTTP server listening on configured address (default `:9090`).
- Handlers:
  - `/metrics`: Prometheus text format exporter sampling SHM header telemetry and internal feed counters.
  - `/healthz`: Liveness check.
  - `/readyz`: Readiness check evaluating feed connectivity, heartbeat age (< 3.0s), and daemon running status.

### 4. `cmd/tickhub/status.go` [ADD]
- Standalone CLI subcommand `tickhub status [--shm <name>] [--watch]`.
- Reads `GlobalHeader` and `SymbolSnapshot` via read-only mmap in a zero-dependency ANSI loop.

### 5. `python/tickhub/shm.py` & `python/tickhub/abi.py` [MODIFY]
- Update Python ABI bindings in `abi.py` to reflect exact field offsets:
  - `boot_id`: `c_uint64` (offset 104)
  - `generation`: `c_uint32` (offset 112)
  - `recovery_mode`: `c_uint32` (offset 116)
  - `_pad_producer`: `c_uint8 * 8` (offset 120)
- Add `boot_id`, `generation`, `is_warm_recovered`, and `is_cold_start` properties to `TickHubReader`.
- Implement `reconnect_if_needed()` in `python/tickhub/shm.py` to handle `StatusBooting` backoff, `BootID` changes, anchor gap fast-forwarding, and heartbeat stall exceptions.

### 6. `tests/test_midday_recovery.py` [ADD]
- Comprehensive integration test for midday restart:
  1. **Sub-Cadence Warm Recovery**:
     - Fast restart (< 1s downtime).
     - Verify new daemon extracts preceding bars, continues with `RecoveryMode = 1`, `FlagColdStart = False`.
     - Assert exact mathematical continuity: `abs(reader.load_sync(61)['ret15s'] - expected_ret15s) < 1e-9`.
  2. **Resident Re-attach with Gap**:
     - Downtime of 5s.
     - Verify new daemon detects gap, sets `RecoveryMode = 2`, asserts `FlagColdStart = True` for 15s.
     - Verify `TickHubReader` detects gap, fast-forwards cursor to latest anchor without hanging, and flags `is_cold_start is True`.
  3. **Cold Start**:
     - Wipe SHM segment.
     - Start daemon without warm recovery.
     - Verify `reader.is_cold_start is True` for frames 1..15, and clears to `False` at frame 16.

---

## 7. Telemetry Plan
- `tickhub daemon`:
  - `[RECOVERY] Probed /dev/shm/%s: Eligible for SUB-CADENCE recovery (downtime: %v). Seeding %d bars across %d symbols.`
  - `[RECOVERY] Probed /dev/shm/%s: Eligible for RESIDENT GAP recovery (downtime: %v, missed ~%d frames). Asserting FlagColdStart for 15s.`
  - `[RECOVERY] Starting in COLD mode. FlagColdStart active for next 15 seconds.`
  - `[HTTP] Telemetry server listening on %s (/metrics, /healthz, /readyz).`
- `tickhub status`:
  - Realtime ANSI status view refreshing at 4Hz.
- `TickHubReader`:
  - `[RESILIENCE] Daemon lifecycle transition detected (Old BootID: 0x%X -> New BootID: 0x%X, Gen: %d, Mode: %d).`
  - `[RESILIENCE] Anchor gap detected (cursor: %d, latest: %d). Fast-forwarding.`

---

## 8. Verification Plan
1. **Unit & Race Tests**:
   - Test Prometheus metric serialization and HTTP handlers (`go test -v -race ./pkg/metrics/...`).
   - Test warm recovery history extraction and projector seeding (`go test -v -race ./pkg/shm/... ./pkg/project/...`).
2. **Integration Verification**:
   - Run `tests/test_midday_recovery.py` verifying warm recovery math continuity, gap skipping, and client transparent re-attachment.
   - Run `scripts/validate_all.py` (ensure ABI struct alignment, Go builds, C builds, Python suite all green).
3. **Code Review**:
   - Run `scripts/codereview.py aiChangeLog/2026-09-24/007_slos_monitoring_and_midday_recovery.md` until `APPROVED`.
4. **Git Commit & Push**:
   - Commit cleanly and push to `origin/main`.

---

## 9. Checklist
- [x] Plan review completed & approved
- [ ] Update ABI layouts (`BootID`, `Generation`, `RecoveryMode`, `FlagColdStart`) across Go and Python
- [ ] Implement warm-recovery ring buffer history reader in `pkg/shm/producer.go` and `pkg/project/projector.go`
- [ ] Implement Prometheus metrics and health endpoints in `pkg/metrics/server.go`
- [ ] Implement zero-dependency status command in `cmd/tickhub/status.go`
- [ ] Enhance `TickHubReader` in `python/tickhub/` with restart detection and re-attachment
- [ ] Implement integration test `tests/test_midday_recovery.py`
- [ ] Run `scripts/validate_all.py` green
- [ ] Run `scripts/codereview.py` APPROVED
- [ ] Commit and push
