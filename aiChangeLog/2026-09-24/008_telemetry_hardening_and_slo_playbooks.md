# Technical Strategy: Telemetry Hardening & SLO Operational Playbooks

**File**: `aiChangeLog/2026-09-24/008_telemetry_hardening_and_slo_playbooks.md`  
**Author**: Antigravity Pair  
**Date**: 2026-09-24  
**Target Milestone**: Operational Hardening & Production Reliability  
**Review Status**: Round 2 (Remediated post B1)

---

## 1. Goal & Context

TickHub provides 1Hz market data projection and POSIX shared memory streaming for downstream quantitative execution models (`ccm-live` on VM, `ccm-paper` on WSL). In Ledger 007, 9 Service Level Objectives (SLOs) were defined along with midday recovery and Prometheus telemetry. 

Audit of the telemetry emission paths identified 4 visibility gaps:
1. **Live Publishing Latency Missing Dynamic Calculation**: `CommitFrameFinalize` in `pkg/shm/producer.go` committed frames without recording actual calculation & commit duration in `AnchorPublishLatencyNS`. The Prometheus metric `tickhub_publish_latency_nanoseconds` remained zero in live operation, and publishing computation delays exceeding the 15ms budget passed silently.
2. **Relay Sequence Gaps Deferred**: `pkg/relay/client.go` tracked `seqGaps` in an atomic counter, but deferred logging until the next 1Hz anchor commit. Instant packet drops over TCP during bursty intervals were not immediately visible in log streams.
3. **Relay Latency Threshold Violations Unlogged**: When TCP replication latency exceeded the 10ms WAN budget (SLO-06), the duration was logged only as informational without an explicit `[SLO-VIOLATION]` tag for log aggregators.
4. **Silent Client Auto-Realign**: In `python/tickhub/shm.py`, when a slow consumer lagged behind the ring buffer capacity and triggered `_auto_realign`, frames were skipped silently without recording an SLO violation in the client logs.

This change closes all 4 telemetry gaps with uniform, structured `[SLO-VIOLATION]` log events and creates operational playbooks for all 9 SLOs in `playbooks/` with concrete triage, mitigation, and recovery procedures.

---

## 2. SLO Catalog & Operational Playbooks

The operational playbooks are organized under `playbooks/`:
- `playbooks/README.md`: Central index, severity triage hierarchy (P0, P1, P2), and Prometheus alert rule reference.
- `playbooks/slo_01_publish_latency.md`: 1Hz Publishing Latency ($P_{99} \le 15\text{ms}$).
- `playbooks/slo_02_seqlock_contention.md`: SeqLock Top-of-Book Snapshot Read ($P_{99} \le 50\text{ns}$).
- `playbooks/slo_03_feature_matrix_load.md`: 1Hz Zero-Copy Matrix Extraction ($P_{99} \le 2.0\mu\text{s}$).
- `playbooks/slo_04_local_shm_frame_loss.md`: Local Shared Memory Frame Continuity (0 frame loss).
- `playbooks/slo_05_relay_sequence_delivery.md`: Relay TCP Sequence Delivery ($99.999\%$ delivery).
- `playbooks/slo_06_relay_latency.md`: Cross-Machine TCP Replication Latency (Loopback $\le 500\mu\text{s}$, WAN $\le 10\text{ms}$).
- `playbooks/slo_07_daemon_midday_recovery.md`: Daemon Midday Warm Recovery ($T_{\text{recover}} \le 200\text{ms}$).
- `playbooks/slo_08_cold_recovery_convergence.md`: Cold Recovery & Feature Convergence ($T_{\text{converge}} \le 15\text{s}$).
- `playbooks/slo_09_heartbeat_staleness.md`: Heartbeat Staleness & Supervisor Eviction ($> 3.0\text{s}$).

---

## 3. SOLID & Architectural Adherence

- **Single Responsibility Principle (SRP)**:
  - `pkg/project/projector.go`: Measures pure phase bar close and SHM commit execution duration (`time.Now().UnixNano() - closeStartNS`) and passes it to the producer.
  - `pkg/shm/producer.go`: Atomically publishes `publishLatencyNS` into the Producer cache line `AnchorPublishLatencyNS`.
  - `pkg/relay/client.go`: Emits immediate log alerts upon packet sequence discontinuity or replication latency over threshold.
  - `python/tickhub/shm.py`: Zero-allocation reader. Emits warnings upon cursor adjustment, lifecycle change, or heartbeat timeout.
- **Single Source of Truth (SSoT)**:
  - `AnchorPublishLatencyNS` measures the exact execution elapsed time of 1Hz feature calculation and SHM commit, captured via `time.Now()` at entry and exit of `closePhase`.
  - Sequence continuity is verified against monotonic packet sequence numbers.
- **Dual Cache-Line Isolation**:
  - `AnchorPublishLatencyNS` resides at offset 64 on the Producer Cache Line. Updating it in `CommitFrameFinalize` stays entirely within the producer's private cache line.

---

## 4. Proposed Code Changes

### 1. `pkg/project/projector.go` & `pkg/shm/producer.go` [MODIFY]
- **B1 Remediation**: Clarify timestamp origin for publishing latency:
  - In `pkg/project/projector.go` `closePhase(pIdx, anchorNS)`:
    - Record wall-clock start time at phase close initiation: `startNS := time.Now().UnixNano()`.
    - Perform feature calculations and `CommitSymbolMetrics`.
    - Calculate execution duration: `publishLatNS := time.Now().UnixNano() - startNS`.
    - Pass `publishLatNS` to `p.producer.CommitFrameFinalizeWithLatency(anchorNS, publishLatNS)`.
  - In `pkg/shm/producer.go`:
    - `CommitFrameFinalizeWithLatency(anchorNS, publishLatNS int64)`:
      - Atomically store `publishLatNS` into `p.header.AnchorPublishLatencyNS`.
      - Atomically store `anchorNS` into `p.header.LastWrittenAnchorNS`.
      - If `publishLatNS > int64(15 * time.Millisecond)`: log structured warning:
        `[SLO-VIOLATION] 1Hz publish latency exceeded budget: %v > 15ms @ anchor %d`.
    - Keep `CommitFrameFinalize(anchorNS)` as backwards-compatible helper defaulting latency to 0 (or measuring internal store duration).

### 2. `pkg/relay/client.go` [MODIFY]
- In `runStream(ctx, conn)`:
  - When `lastPacketSeq > 0 && seq != lastPacketSeq + 1`:
    - Increment `c.seqGaps` by `int64(seq - lastPacketSeq - 1)`.
    - Immediately log structured alert:
      `[SLO-VIOLATION] [RELAY-CLI] Sequence gap detected: expected seq %d, got %d (gap=%d packets)`.
  - In anchor commit handling:
    - If `latencyUS > 10000`: log structured alert:
      `[SLO-VIOLATION] [RELAY-CLI] Replication latency exceeded 10ms budget: %dµs @ anchor %d`.

### 3. `python/tickhub/shm.py` [MODIFY]
- Import `logging` and initialize `logger = logging.getLogger("tickhub.shm")` with `NullHandler`.
- In `reconnect_if_needed`:
  - When `curr_boot != 0 and curr_boot != self._boot_id`:
    - Log: `[RESILIENCE] TickHub daemon restart detected: BootID={curr_boot:#x}, Gen={self._generation}, RecoveryMode={rec_mode}`.
    - If `self.is_cold_start`:
      - Log: `[RESILIENCE] Restart recovery active with FlagColdStart=True. Model trade execution inhibited during warmup.`
- In `_load_into_matrix`:
  - When `target_anchor < oldest_valid and last_written > 0`:
    - If `self._auto_realign`:
      - Log: `[SLO-VIOLATION] [RESILIENCE] Frame lag / buffer overrun detected: symbol=%s, phase=%s, target=%d < oldest_valid=%d (gap=%.1fms). Auto-realigning to %d.`

### 4. `playbooks/` [ADD]
- Add 10 comprehensive markdown playbooks:
  - `playbooks/README.md`
  - `playbooks/slo_01_publish_latency.md`
  - `playbooks/slo_02_seqlock_contention.md`
  - `playbooks/slo_03_feature_matrix_load.md`
  - `playbooks/slo_04_local_shm_frame_loss.md`
  - `playbooks/slo_05_relay_sequence_delivery.md`
  - `playbooks/slo_06_relay_latency.md`
  - `playbooks/slo_07_daemon_midday_recovery.md`
  - `playbooks/slo_08_cold_recovery_convergence.md`
  - `playbooks/slo_09_heartbeat_staleness.md`

---

## 5. Telemetry & Verification Plan

### Automated Test Verification
1. **Unit & Race Tests**:
   - `go test -v -race ./pkg/shm ./pkg/relay ./pkg/metrics ./pkg/project`: verify clean execution with no data races.
2. **Integration Tests**:
   - `pytest -v tests/test_midday_recovery.py`: verify Python reader logging and restart resilience.
   - `pytest -v tests/test_relay_parity.py`: verify relay streaming and sequence handling.
3. **Full Validation Gate**:
   - Run `python scripts/validate_all.py` across all 7 stages.
4. **Smoke Check**:
   - Verify `AnchorPublishLatencyNS` is non-zero after committing frames.

---

## 6. Checklist
- [x] Clean git working tree verified before opening ledger
- [x] B1 timestamp origin defect remediated: `publishLatNS` defined as wall-clock execution duration of `closePhase`
- [x] Adversarial plan review (`scripts/planreview.py`) run and APPROVED
- [x] Code changes implemented surgically
- [x] Operational playbooks written under `playbooks/`
- [x] Full validation gate (`scripts/validate_all.py`) passes 7/7 stages
- [x] Pre-commit code review (`scripts/codereview.py`) run and APPROVED
- [ ] Committed and pushed to `origin/main`
