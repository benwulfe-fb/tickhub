# Playbook: SLO-07 — Daemon Midday Restart Recovery

## 1. SLO Definition & Budget
- **Objective**: On daemon restart during active trading hours, the new daemon process must attach to resident `/dev/shm`, restore symbol state, and resume 1Hz publishing in under 200 milliseconds without losing rolling bar history.
- **Budget**: `WarmRecoveryDuration <= 200ms`, `HistoricalLoss = 0` (zero loss of pre-existing rolling window state).
- **Normal Range**: $40\,\text{ms} - 120\,\text{ms}$ for warm SHM re-attachment and validation.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  tickhub_daemon_warm_recovery_duration_milliseconds > 200
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [RESILIENCE] Warm restart recovery exceeded budget: %d ms > 200 ms
  ```
- **Python / Consumer Telemetry**:
  ```text
  [RESILIENCE] Daemon restart detected (PID change %d -> %d). Re-attaching to resident SHM.
  ```

---

## 3. Severity & Business Impact
- **Severity**: **P1 (Urgent)**
- **Impact**: If warm recovery fails or exceeds 1 second, the engine misses 1Hz bar closures. If historical state in SHM is wiped on restart, rolling features (e.g. 5m, 15m, 1h rolling windows) become uninitialized, requiring a lengthy cold replay from datalake before trading models can execute safely.

---

## 4. Diagnostics & Root Cause Analysis

1. **Verify SHM Resident State Prior to Restart**:
   Check if `/dev/shm/tickhub_live` is intact with matching magic bytes (`0x5449434B48554231`):
   ```bash
   ls -la /dev/shm/tickhub_live
   ```
2. **Check Init Mode Flag**:
   Ensure daemon command line specifies `--attach-shm` or detects existing SHM rather than passing `--force-init` which truncates and overwrites resident memory.
3. **Inspect Process Boot Time**:
   Check what component in daemon startup took $> 200\,\text{ms}$:
   ```bash
   journalctl -u tickhub -n 50 --no-pager
   ```
   Check if symbol directory parsing, YAML configuration loading, or WebSocket pre-connect delayed the SHM attach loop.

---

## 5. Mitigation Procedures

1. **Ensure Safe Daemon Restart via Systemd**:
   Use standard systemd restart to trigger graceful teardown and re-exec:
   ```bash
   systemctl restart tickhub
   ```
2. **Prevent Truncation of Resident SHM**:
   Verify that `pkg/shm/producer.go` checks for existing layout before calling `ftruncate()`:
   ```go
   // Re-attach path must preserve existing ring buffer frames and snapshots
   if info.Size() == expectedSize {
       // Attach warm without zeroing memory
   }
   ```
3. **Verify Downstream Consumer Auto-Realign**:
   Check downstream consumer logs to ensure readers detected PID update (`ProducerPID`) and seamlessly continued polling without requiring consumer process restart.

---

## 6. Verification
1. Run recovery test:
   ```bash
   /mnt/wc/miniconda3/envs/gpu_env/bin/python -m pytest tests/test_resilience.py -k test_daemon_warm_restart
   ```
2. Verify daemon log output:
   `[WARM-RECOVERY] Re-attached to resident SHM in %d ms. Preserved %d rolling frames.`
