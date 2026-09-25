# Playbook: SLO-09 — Heartbeat & Process Staleness

## 1. SLO Definition & Budget
- **Objective**: The TickHub daemon and active consumer processes must update their respective heartbeat timestamps in shared memory at regular sub-second intervals.
- **Budget**: `MaxStaleness <= 3.0s` ($3,000\,\text{ms}$). Alert triggered immediately if `LastHeartbeat > 3.0s`.
- **Normal Range**: Heartbeat updated every $500\,\text{ms} - 1,000\,\text{ms}$.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  (time() - tickhub_daemon_heartbeat_timestamp_seconds) > 3.0
  or
  (time() - tickhub_consumer_heartbeat_timestamp_seconds) > 3.0
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [HEARTBEAT] Daemon heartbeat stale: last seen %d ms ago (budget 3000ms)
  ```
  or
  ```text
  [SLO-VIOLATION] [HEARTBEAT] Consumer (PID %d) heartbeat stale: last seen %d ms ago (budget 3000ms)
  ```
- **CLI Inspection**:
  ```bash
  tickhub status --shm tickhub_live
  ```
  Inspect `Daemon Heartbeat: Xs ago` and `Consumer Heartbeat: Xs ago`.

---

## 3. Severity & Business Impact
- **Severity**: **P0 (Critical / Severe)**
- **Impact**: A stale daemon heartbeat indicates the projection engine has frozen, deadlocked, or crashed without cleaning up SHM. A stale consumer heartbeat in replay mode halts producer advance, or in live mode indicates downstream trading models are hung and not actively processing market ticks.

---

## 4. Diagnostics & Root Cause Analysis

1. **Check Process Existence by PID**:
   Inspect whether daemon or consumer PID recorded in SHM is still alive:
   ```bash
   kill -0 <PID> 2>&1
   ```
   If process does not exist, it terminated abruptly (e.g. `SIGKILL`, OOM killer).
2. **Inspect Process State for D-State or Deadlock**:
   ```bash
   ps aux | grep -E "tickhub|trading-engine"
   ```
   Check if process is in uninterruptible sleep (`D` state, typically waiting on I/O) or CPU lock.
3. **Capture Stack Trace of Stalled Process**:
   ```bash
   # For Go daemon:
   kill -SIGQUIT $(pgrep tickhub)
   # Or via GDB / Delve:
   dlv attach $(pgrep tickhub)
   ```
   Inspect goroutine dump in syslog/journalctl.

---

## 5. Mitigation Procedures

1. **Terminate Deadlocked Process**:
   If process is hung and unresponsive to `SIGTERM`:
   ```bash
   kill -9 $(pgrep tickhub)
   ```
2. **Restart via Systemd Service**:
   ```bash
   systemctl restart tickhub
   ```
   On restart, daemon re-attaches to resident SHM and refreshes daemon heartbeat timestamp within $< 200\,\text{ms}$.
3. **Handle Dead Consumer**:
   If consumer process died without clearing its PID in SHM:
   ```bash
   # Clear stale consumer registration in SHM using tickhub CLI
   tickhub status --reset-consumer --shm tickhub_live
   ```

---

## 6. Verification
1. Inspect `tickhub status`: confirm `Daemon Heartbeat < 1s` and `Consumer Heartbeat < 1s`.
2. Check Prometheus metrics:
   ```bash
   curl -s http://localhost:9090/metrics | grep heartbeat
   ```
