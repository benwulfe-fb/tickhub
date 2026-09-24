# Playbook: SLO-08 — Cold Recovery Convergence

## 1. SLO Definition & Budget
- **Objective**: If `/dev/shm` is unlinked or corrupted during trading hours, a cold-start daemon must replay day-to-date historical ticks from the Parquet datalake to reconstruct rolling feature state in under 300 seconds (5 minutes).
- **Budget**: `ConvergenceDuration <= 300s` ($5.0\,\text{min}$), `FeatureDivergence = 0`.
- **Normal Range**: $45\,\text{s} - 180\,\text{s}$ depending on the time of day (volume of historical ticks to replay).

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  tickhub_cold_replay_duration_seconds > 300
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [REPLAY] Cold recovery convergence exceeded 300s budget: %d s elapsed
  ```
- **CLI Telemetry**:
  Logged during `tickhub replay --mode cold-seed` or daemon cold-start sequence.

---

## 3. Severity & Business Impact
- **Severity**: **P0 (Critical / Severe)**
- **Impact**: While cold recovery is executing, downstream live execution engines cannot trade safely because long-window rolling features (e.g. 15-minute EWMA, 1-hour volume profile) are not yet warmed to full convergence. Delays exceeding 5 minutes leave trading disabled during market volatility.

---

## 4. Diagnostics & Root Cause Analysis

1. **Verify Parquet Datalake Cache Status**:
   Confirm historical tick Parquet files for the current trading date exist in local NVMe cache (`/mnt/wc/data/datalake/`):
   ```bash
   ls -lh /mnt/wc/data/datalake/$(date +%Y-%m-%d)/
   ```
   If files are missing or being fetched over high-latency remote storage, I/O read throughput stalls replay.
2. **Inspect Multi-Threaded Replay Throughput**:
   Ensure historical reader is utilizing all available CPU cores during projection:
   ```bash
   top -b -n 1 -p $(pgrep tickhub)
   ```
3. **Check for Replay Flow-Control Deadlock**:
   Verify replay is running in batch mode rather than pacing against real-time clock or blocked waiting for slow consumer heartbeat:
   ```bash
   journalctl -u tickhub -n 50 --no-pager
   ```

---

## 5. Mitigation Procedures

1. **Ensure Local NVMe Storage for Datalake Replay Cache**:
   Move datalake scratch / daily cache to fast local NVMe instead of NFS or network-attached storage:
   ```bash
   tickhub replay --datalake-dir /mnt/wc/data/datalake --target-shm /dev/shm/tickhub_live
   ```
2. **Run Unthrottled Replay**:
   Ensure `--pace 0` or `--unthrottled` is passed to replay engine so ticks are processed at line-rate ($> 1,000,000\,\text{ticks/sec}$).
3. **Notify Downstream Consumers of Convergence**:
   Downstream models should query `tickhub status` and wait for `State: WARMED / STEADY_STATE` before engaging order entry.

---

## 6. Verification
1. Run cold seed benchmark:
   ```bash
   time tickhub replay --date $(date +%Y-%m-%d) --symbols 72 --dry-run
   ```
2. Confirm convergence finishes within $< 300\,\text{s}$.
3. Verify feature parity between cold replayed snapshot and reference snapshot.
