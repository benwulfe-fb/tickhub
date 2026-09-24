# Playbook: SLO-02 — SeqLock Read Contention

## 1. SLO Definition & Budget
- **Objective**: Readers must observe atomic symbol snapshots without excessive spin-lock retry cycles caused by concurrent producer writes.
- **Budget**: $P_{99} \le 1$ retry per snapshot read. Retry count $= 0$ in $\ge 99.9\%$ of snapshot reads across all configured symbols.
- **Normal Range**: 0 retries in steady state. Occasional 1 retry during simultaneous 1Hz phase commit.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  rate(tickhub_reader_seqlock_retries_total[1m]) > 10
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [READER] SeqLock spin exceeded budget: %d retries for symbol %s
  ```
- **Python / Reader Warning**:
  ```text
  [SLO-VIOLATION] Excessive SeqLock retries (> 100) on symbol index %d
  ```

---

## 3. Severity & Business Impact
- **Severity**: **P2 (Elevated)**
- **Impact**: Reading threads spin in tight loops waiting for producer to exit critical section. Can introduce jitter in Python dataloader / execution engine loop. If retries hit maximum threshold (e.g. 100+), reader may observe stale snapshot or raise contention error.

---

## 4. Diagnostics & Root Cause Analysis

1. **Verify Reader / Producer Core Overlap**:
   ```bash
   taskset -cp $(pgrep tickhub)
   taskset -cp $(pgrep -f "python.*ccm")
   ```
   If reader and producer share the same logical hyperthread, producer context-switching out mid-write leaves SeqLock sequence odd (`seq & 1 == 1`), stalling reader until producer is rescheduled.
2. **Inspect SHM Memory Bus Saturation**:
   ```bash
   perf stat -e cache-misses,L1-dcache-load-misses -p $(pgrep tickhub) sleep 2
   ```
3. **Check for Rogue Writer in SHM**:
   Verify only one TickHub producer is attached with write permissions to the snapshot ring buffer:
   ```bash
   lsof /dev/shm/tickhub_live
   ```

---

## 5. Mitigation Procedures

1. **Isolate Producer and Consumer CPU Affinity**:
   Ensure daemon producer runs on an isolated core, while consumer processes run on separate dedicated cores:
   ```bash
   # Pin tickhub daemon to Core 2
   taskset -cp 2 $(pgrep tickhub)
   # Pin execution engine to Cores 4-8
   taskset -cp 4-8 $(pgrep -f "ccm-live")
   ```
2. **Ensure Non-Allocating Read Loop in Python**:
   Verify Python consumers read `SymbolSnapshot` directly via memoryview or ctypes/C-atomic wrapper rather than performing heavy deserialization within the spin window.
3. **If Writer is Stalled Mid-Update**:
   If producer crashed while holding odd sequence lock (`seq & 1 == 1`):
   ```bash
   # Restart daemon to clear corrupted memory or re-initialize SHM
   systemctl restart tickhub
   ```

---

## 6. Verification
1. Inspect reader logs: confirm no `[SLO-VIOLATION] [READER] SeqLock spin` occurrences.
2. Check metrics endpoint:
   ```bash
   curl -s http://localhost:9090/metrics | grep tickhub_reader_seqlock_retries_total
   ```
