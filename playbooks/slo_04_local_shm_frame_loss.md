# Playbook: SLO-04 — Local SHM Frame Loss & Buffer Overrun

## 1. SLO Definition & Budget
- **Objective**: Readers must keep pace with the producer ring buffer; no frames must be dropped or skipped due to reader buffer overrun.
- **Budget**: `DroppedFrames = 0`. Zero buffer lag exceeding the 60-second ring buffer capacity. `AutoRealignCount = 0` during active trading hours.
- **Normal Range**: Consumer lags producer by 0 to 1 frame ($0 - 1\,\text{s}$).

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  rate(tickhub_reader_dropped_frames_total[1m]) > 0
  or
  rate(tickhub_reader_auto_realign_total[1m]) > 0
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [RESILIENCE] Frame lag / buffer overrun detected: consumer lag (%d ms) > buffer capacity (60000 ms). Auto-realigning to latest anchor %d. Dropped approx %d frames.
  ```
- **Shared Memory Inspection**:
  ```bash
  tickhub status --shm tickhub_live
  ```
  Check `Dropped Frames: X`.

---

## 3. Severity & Business Impact
- **Severity**: **P0 (Critical / Severe)**
- **Impact**: When consumer lags by $> 60\,\text{s}$, the circular ring buffer wraps around and overwrites unread frames. Consumer is forced to trigger `_auto_realign()`, discarding rolling history. The downstream execution model misses trading bars, potentially leaving stale positions unhedged or calculating corrupt indicators.

---

## 4. Diagnostics & Root Cause Analysis

1. **Check Consumer Process Health**:
   Identify if consumer thread is blocked, crashed, or running slow:
   ```bash
   top -b -n 1 -p $(pgrep -f "ccm-live")
   ```
2. **Inspect Ring Buffer Capacity vs Consumer Read Head**:
   Inspect producer `LastWrittenAnchorNS` vs consumer `LastReadAnchorNS`:
   ```bash
   tickhub status --shm tickhub_live
   ```
   If `Producer Anchor - Consumer Anchor > 60s`, consumer has stalled.
3. **Check for Disk I/O Blockage or Logging Stalls**:
   Check if consumer is blocked writing large debug logs to a slow disk:
   ```bash
   iostat -xz 1 5
   ```

---

## 5. Mitigation Procedures

1. **Immediate Consumer Health Check**:
   If consumer process is unresponsive or deadlock has occurred, kill and restart consumer:
   ```bash
   kill -9 $(pgrep -f "ccm-live")
   systemctl start ccm-live
   ```
   Consumer will attach to resident `/dev/shm/tickhub_live` and immediately align to latest available anchor.
2. **Offload Heavy Async Processing**:
   Ensure downstream Python consumers do not run long ML model inference or synchronous disk writes directly on the SHM polling thread. Use a separate background worker queue for model calculation.
3. **If Producer Frame Rate is Out of Control**:
   Check whether historical replay or a malfunctioning time-source is advancing producer anchors faster than 1Hz real-time:
   ```bash
   journalctl -u tickhub -n 50 --no-pager
   ```

---

## 6. Verification
1. Inspect reader logs: confirm consumer is keeping pace with each 1-second anchor without dropping frames.
2. Verify dropped frame metric is 0:
   ```bash
   curl -s http://localhost:9090/metrics | grep tickhub_reader_dropped_frames_total
   ```
