# Technical Strategy: Live VM WebSocket Feed, Relay Setup, and Dual-Engine E2E Validation

**File**: `aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md`  
**Author**: Antigravity Pair  
**Date**: 2026-09-24  
**Target Milestone**: Phase 3.5 Production Validation & Cross-Machine Shadow Engine  
**Review Status**: Round 2 (Remediated post B1 / B2)

---

## 1. Goal & Context

The user has authorized taking over the VM (`ccm-live-1` at `100.71.0.9`, Debian 12, GCP `us-east4-b`) post-market-close to test live market data streaming into TickHub, along with a Python client and TickHub Relay connection to WSL.

The high-level objective is an end-to-end demonstration and test where:
1. `tickhub daemon` runs on the VM ingesting live market data from Massive.com WebSocket (`wss://socket.massive.com/stocks`) for the 72 configured symbols from `/mnt/wc/src/manifests/datalake_72_symbols.txt` into `/dev/shm/tickhub_live`.
2. `tickhub relay-server` on the VM streams committed frames and snapshots over TCP (port 9190).
3. `tickhub relay-client` on the WSL workstation connects to `100.71.0.9:9190` via Tailscale and replicates the stream into local WSL `/dev/shm/tickhub_live`.
4. Two engines run concurrently:
   - **Engine A (Prod)**: Runs on the VM, reading local `/dev/shm/tickhub_live` via `TickHubReader`.
   - **Engine B (Paper)**: Runs on WSL, reading replicated `/dev/shm/tickhub_live` via `TickHubReader`.
5. Both engines consume identical 1Hz frames, read atomic SeqLock symbol snapshots, and verify bitwise parity of feature matrices and symbol prices.

### Missing Features & Gaps Remediated

1. **72-Symbol Universe Configuration (`config/live_72.yaml`)**:
   - Single-phase topology (`phase_0ms`, offset `0ms`, all 72 symbols) with `max_frames: 64` (standard ring size matching existing test suites and ABI layout).
2. **Single-Threaded Wall-Clock Ticker in `cmd/tickhub/daemon.go` (B2 Remediation)**:
   - **B2 Fix**: The 250ms wall-clock ticker is serialized directly inside the **existing main `select` loop** in `runDaemon`. No separate goroutine is created.
   - When `case <-ticker.C:` fires:
     - Calls `prod.PublishTelemetry(...)` to keep `HeartbeatNS` updated every $\le 1\,\text{s}$ (SLO-09 compliance during low tick rate).
     - Derives `wallAnchor := (now / cadenceNS) * cadenceNS`. If `wallAnchor > lastAnchor`, calls `projector.Flush(wallAnchor)`.
     - Because tick ingestion (`case tick, ok := <-ticks:`) and ticker events (`case <-ticker.C:`) run sequentially in the single main event loop, all access to `projector` is strictly serialized with zero data races.
3. **B1 Parity Join & Verification Specification**:
   - Both engines output structured JSONL telemetry tagged with `anchor_ns`.
   - The verification script parses both output streams and constructs index maps `vm_by_anchor` and `wsl_by_anchor`.
   - **Join Logic**:
     - Strict inner-join on `anchor_ns`.
     - Any anchor present on VM but absent on WSL is reported as `RELAY_SEQUENCE_DROP` (SLO-05 violation).
     - For every matched anchor:
       - 100% bitwise parity strictly enforced on canonical 1Hz feature matrices (`log_ret_1s`, `log_ret_5s`, `log_ret_15s`, `vol_1s`, `spread_bps`).
       - SeqLock top-of-book snapshots (`BidPx`, `AskPx`, `Midprice`, `LastTradePx`, `Spread`, `LastTradeSz`): In live streaming, `SymbolSnapshot` is an unbuffered instantaneous top-of-book table updated asynchronously by inbound WebSocket ticks. While `relay-server` delta-replicates snapshots and `engine_prod` reads them at the 1Hz boundary, sub-millisecond quote arrivals during read iteration may cause instantaneous top-of-book quotes to advance. Gated on $\ge 99.5\%$ field match and max price delta $\le \$0.50$ (with symbol divergence breakdown and hard failure gate).
     - Per-anchor pipeline replication lag computed as `(wsl_recv_wall_ns - clock_skew_ns) - vm_recv_wall_ns` and asserted $\le 150.0\,\text{ms}$ (SLO-06 compliance for cross-region WAN over Tailscale; baseline ping RTT is ~87ms).
4. **Reference Engine Client (`python/tickhub/engine_client.py`)**:
   - Minimal 1Hz execution loop using `TickHubReader` (reads snapshots, feature matrix, records read duration, emits structured JSON).

---

## 2. SSoT & Architectural Invariants

1. **Single Source of Truth (SSoT)**:
   All rolling features (`log_ret_1s`, `log_ret_5s`, `log_ret_15s`, `vol_1s`, `spread_bps`) and 1Hz half-open interval $[T-1\text{s}, T)$ logic remain strictly in Go `pkg/project/projector.go`. Both Engine A (VM) and Engine B (WSL) read pre-computed arrays from `/dev/shm` without duplicating projection math.
2. **Transparent Client Replication**:
   `TickHubReader` operates on `/dev/shm/tickhub_live` using identical code and ABI bindings on both VM and WSL. The client code is completely oblivious to whether the underlying shared memory is populated by local WebSocket ingestion or by `tickhub relay-client`.
3. **Dual Cache-Line Isolation**:
   Producer Cache Line (offset 64) is written exclusively by the producer (daemon on VM, relay-client on WSL). Consumer Cache Line (offset 128) is written by the engine readers. No cache line bouncing.
4. **SeqLock Atomicity**:
   Symbol snapshots are read with SeqLock version validation (`seq & 1 == 0` and `seq == end_seq`).

---

## 3. SOLID Principles Adherence

- **Single Responsibility Principle (SRP)**:
  - `config/live_72.yaml`: Owns static topology, universe list, and phase offsets.
  - `cmd/tickhub/daemon.go`: Owns WebSocket lifecycle, wall-clock pacing, and signal shutdown.
  - `python/tickhub/engine_client.py`: Implements a minimal 1Hz execution engine cycle (reads snapshots, checks features, logs latency).
  - `scripts/test_live_relay_e2e.py`: Orchestrates deployment, process lifecycles across VM and WSL, captures logs, and asserts parity.
- **Open/Closed Principle (OCP)**:
  - `TickHubReader` remains unchanged; the new engine client reuses its public API (`read_feature_matrix`, `read_snapshot`, `poll_next_frame`).
- **Liskov Substitution Principle (LSP)**:
  - The shared memory layout populated by `relay-client` conforms 100% to the ABI schema of `daemon`, enabling identical consumer behavior.
- **Interface Segregation Principle (ISP)**:
  - Relay protocol packets encapsulate snapshots and frames separately, maintaining independent serialization.
- **Dependency Inversion Principle (DIP)**:
  - Engine clients depend on the abstract `/dev/shm` POSIX interface, not on the network or transport mechanism.

---

## 4. Proposed Code Changes

### 1. `config/live_72.yaml` [ADD]
- Define configuration for live trading with:
  - `name`: `tickhub_live`
  - `max_frames`: `64`
  - `cadence_interval`: `1000000000` (1 second)
  - `features`: `["log_ret_1s", "log_ret_5s", "log_ret_15s", "vol_1s", "spread_bps"]`
  - `unique_symbols`: 72 symbols matching `/mnt/wc/src/manifests/datalake_72_symbols.txt` alphabetically sorted.
  - `phases`:
    - Phase 0: `phase_0ms`, offset `0ms`, all 72 symbols.

### 2. `cmd/tickhub/daemon.go` [MODIFY]
- In `runDaemon`:
  - Introduce `ticker := time.NewTicker(250 * time.Millisecond)` and defer `ticker.Stop()`.
  - In main event loop:
    ```go
    for {
        select {
        case <-ctx.Done():
            ...
        case tick, ok := <-ticks:
            tickCount++
            _ = projector.IngestTick(tick)
        case now := <-ticker.C:
            // Single-threaded serialization on main goroutine: zero data races!
            nowNS := now.UnixNano()
            prod.PublishTelemetry(0, 0, 0, tickCount)
            wallAnchor := (nowNS / cadenceNS) * cadenceNS
            if wallAnchor > lastFlushedAnchor {
                _ = projector.Flush(wallAnchor)
                lastFlushedAnchor = wallAnchor
            }
        }
    }
    ```

### 3. `pkg/shm/producer.go`, `pkg/relay/client.go`, `pkg/relay/server.go` & `cmd/tickhub/relay.go` [MODIFY]
- In `pkg/shm/producer.go`: update `HeartbeatNS` on every frame commit in `CommitFrameFinalizeWithLatency` (which `CommitFrameFinalize` delegates to) ensuring replica and primary SHM segments maintain live heartbeat timestamps during steady-state frame publication.
- In `pkg/relay/client.go`: call `PublishTelemetry` upon frame replication to continuously refresh `HeartbeatNS` and report sequence gap / anchor counters on replica SHM; configure `LatencyBudgetUS` (default 150ms for WAN, 10ms for LAN) to prevent false SLO-violation log spam over WAN.
- In `cmd/tickhub/relay.go`: expose `--latency-budget-ms` flag on `relay-client` passing to `ClientConfig.LatencyBudgetUS`.
- In `pkg/relay/server.go`: broadcast initial snapshot baseline on connect (`isFirstClientAnchor`), then stream updates whenever `SeqLockSeq != lastSentSeq[i]` without filtering on `SIPTimestampNS < nextAnchor`, eliminating dropped snapshot updates across window boundaries during live market data streaming.

### 4. `python/tickhub/shm.py` [MODIFY]
- In `get_latest_phase_anchor`: prioritize `last_written_anchor_ns` over client wall-clock time (`time.time_ns()`), eliminating client clock-skew stalls when consuming cross-machine replicated streams.
- In `load_sync` and `load_async`: only sleep if target anchor is strictly ahead of `last_written_anchor_ns`, and cap sleep duration to 50ms for responsive polling.

### 4. `python/tickhub/engine_client.py` [ADD]
- Standalone reference engine client representing live execution:
  - Flags: `--config <path>`, `--shm <name>`, `--role <prod|paper>`, `--duration <seconds>`, `--output <jsonl>`.
  - Attaches to `/dev/shm/<name>` using `TickHubReader`.
  - Loops on 1Hz anchors, reads all symbol snapshots using SeqLock, reads the feature matrix, records read duration, and prints structured JSON telemetry:
    ```json
    {"role": "prod", "anchor_ns": 1727210000000000000, "local_wall_ns": 1727210000002100000, "symbols": 72, "load_time_us": 142.5, "sample_mid": 575.22, "sample_ret": -0.00012}
    ```

### 5. `scripts/test_live_relay_e2e.py` [ADD]
- End-to-end integration test & validation orchestrator:
  1. Compiles latest `bin/tickhub` with explicit target `CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o bin/tickhub ./cmd/tickhub`.
  2. Pre-flight check verifies `libtickhub_atomic.so` exists.
  3. Copies binary, config, and Python SDK to VM (`100.71.0.9`) via SSH/SCP.
  4. Measures clock skew between WSL and VM using multi-sample median.
  5. Cleans remote SHM and terminates old process groups (`kill -9 -- -PID` and `pkill -9`).
  6. Launches `tickhub daemon` and `tickhub relay-server` on VM.
  7. Launches Engine A (Prod) on VM and Engine B (Paper) on WSL with synchronized durations (20s).
  8. Manages file handles safely with `with open(...) as relay_log:`.
  9. Awaits Engine B completion and polls Engine A PID exit to ensure full telemetry flush before SCP.
  10. **Join & Parity Verification**:
     - Indexes records by `anchor_ns`.
     - Detects and asserts zero `RELAY_SEQUENCE_DROP` violations.
     - Asserts 100% bitwise parity on feature matrices.
     - Asserts snapshot parity with >= 99.5% field match and max price delta <= $0.50 (with symbol divergence breakdown and hard exit `sys.exit(1)`).
     - Asserts steady-state replication lag $\le 150.0\,\text{ms}$ WAN budget with hard exit (`sys.exit(1)`).
  11. Performs process group teardown on remote VM and local host.

---

## 5. Telemetry & Verification Plan

### Automated Test Verification
1. **Unit & Race Tests**:
   - `go test -v -race ./pkg/... ./cmd/...`: verify zero data races with single-threaded select ticker.
2. **Python Test Suite**:
   - `pytest -v tests/`: ensure all 13 existing unit/integration tests pass.
3. **Full Validation Gate**:
   - `python scripts/validate_all.py`: verify all 7 stages pass.
4. **Live Cross-Machine E2E Verification**:
   - Run `python scripts/test_live_relay_e2e.py`: verify live data ingestion, relay replication across Tailscale, and dual-engine consumption with zero divergences.

---

## 6. Checklist
- [x] Clean git working tree verified before opening ledger
- [x] B1 and B2 defects remediated: single-threaded select ticker and anchor_ns inner-join parity verification
- [x] Adversarial plan review (`scripts/planreview.py`) run and APPROVED
- [x] Code changes implemented surgically
- [x] Full validation gate (`scripts/validate_all.py`) passes 7/7 stages
- [x] Live cross-machine E2E test passes on VM and WSL
- [ ] Pre-commit code review (`scripts/codereview.py`) run and APPROVED
- [ ] Committed and pushed to `origin/main`
