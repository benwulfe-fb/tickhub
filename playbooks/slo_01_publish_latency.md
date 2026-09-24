# Playbook: SLO-01 — 1Hz Publishing Latency

## 1. SLO Definition & Budget
- **Objective**: 1Hz feature calculation and shared memory commitment must complete within 15 milliseconds of bar closure.
- **Budget**: $P_{99} \le 15.0\,\text{ms}$ ($15,000,000\,\text{ns}$).
- **Normal Range**: $0.2\,\text{ms} - 2.5\,\text{ms}$ across 72 configured symbols.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  tickhub_publish_latency_nanoseconds > 15000000
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] 1Hz publish latency exceeded budget: %v > 15ms @ anchor %d
  ```
- **Terminal Inspection**:
  Run `tickhub status --shm tickhub_live`: check `Pub Latency: Xµs`.

---

## 3. Severity & Business Impact
- **Severity**: **P1 (Urgent)**
- **Impact**: Downstream model inference runs on delayed market observations. If latency exceeds consumer processing intervals, downstream execution models may suffer signal slippage or miss optimal execution fills.

---

## 4. Diagnostics & Root Cause Analysis

1. **Check Live Dashboard**:
   ```bash
   tickhub status --shm tickhub_live
   ```
   Inspect `Watermark Delay` and `Pub Latency`. If `Watermark Delay` is high, delay originates in Massive WebSocket network ingestion, not local projection.
2. **Inspect Process CPU Throttling**:
   ```bash
   top -b -n 1 -p $(pgrep tickhub)
   ```
   Check if daemon thread is hitting 100% CPU on an over-subscribed core.
3. **Verify Heap Allocations**:
   The steady-state projection loop in `pkg/project/projector.go` is designed to be zero-allocation. Check if Go runtime garbage collection cycles are interrupting the hot path:
   ```bash
   GODEBUG=gctrace=1 tickhub daemon --config config/live.yaml
   ```

---

## 5. Mitigation Procedures

1. **Pin Daemon Process to Dedicated Core**:
   ```bash
   taskset -cp 2 $(pgrep tickhub)
   ```
2. **Increase Go Garbage Collector Target**:
   ```bash
   export GOGC=off  # or GOGC=500
   ```
3. **Restart Daemon via Seamless Sub-Cadence Recovery**:
   ```bash
   systemctl restart tickhub
   ```
   TickHub will automatically re-attach to resident `/dev/shm/tickhub_live` within $< 200\,\text{ms}$, seeding rolling history without data gap.

---

## 6. Verification
1. Verify `tickhub status` confirms `Pub Latency < 3000µs`.
2. Confirm Prometheus metric:
   ```bash
   curl -s http://localhost:9090/metrics | grep tickhub_publish_latency_nanoseconds
   ```
