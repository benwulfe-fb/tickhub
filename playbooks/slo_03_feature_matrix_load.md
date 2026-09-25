# Playbook: SLO-03 — Feature Matrix Load Duration

## 1. SLO Definition & Budget
- **Objective**: Deserialization of all 72 configured symbols from `/dev/shm` into structured NumPy arrays or PyTorch tensors must complete in under 5.0 milliseconds.
- **Budget**: $P_{99} \le 5.0\,\text{ms}$ ($5,000\,\mu\text{s}$).
- **Normal Range**: $0.5\,\text{ms} - 1.8\,\text{ms}$ on dedicated hardware.

---

## 2. Detection & Alert Triggers
- **Prometheus Metric**:
  ```prometheus
  tickhub_python_matrix_load_seconds{quantile="0.99"} > 0.005
  ```
- **Log Pattern**:
  ```text
  [SLO-VIOLATION] [READER] Feature matrix load exceeded budget: %f ms > 5.0 ms @ anchor %d
  ```
- **Python Telemetry**:
  Logged by `TickHubReader.read_feature_matrix()` if elapsed duration exceeds 5ms.

---

## 3. Severity & Business Impact
- **Severity**: **P2 (Elevated)**
- **Impact**: Feature loading occurs on the critical path of the downstream 1Hz execution loop. Delays eat directly into model inference and order routing budgets, causing late-order submissions relative to market microstructure shifts.

---

## 4. Diagnostics & Root Cause Analysis

1. **Profile Python Reader Hot Path**:
   Identify whether slow load is due to memory allocation, IPC bus contention, or PyTorch tensor construction:
   ```python
   import cProfile
   cProfile.run("reader.read_feature_matrix()")
   ```
2. **Check for Unintended Memory Copy**:
   Verify whether reader uses zero-copy NumPy buffers mapped directly over `/dev/shm` memory, or if an accidental `.copy()` / deepcopy was introduced:
   ```python
   # Buffer base should reference the mmap object
   assert arr.base is not None
   ```
3. **Inspect Garbage Collection Delays**:
   Check if Python GC pauses are inflating the load time:
   ```python
   import gc
   print(gc.get_stats())
   ```

---

## 5. Mitigation Procedures

1. **Pre-allocate Target Tensors**:
   Ensure PyTorch / NumPy arrays are pre-allocated at initialization, copying bytes in-place using `np.copyto()` or direct memoryview slicing rather than constructing new arrays each second.
2. **Disable Generational GC in Execution Loop**:
   ```python
   import gc
   gc.disable()  # Run manual collect only at scheduled market pauses
   ```
3. **Pin Python Worker to Dedicated Isolated Core**:
   ```bash
   taskset -cp 4 $(pgrep -f "trading-engine")
   ```

---

## 6. Verification
1. Run reader benchmark:
   ```bash
   /mnt/wc/miniconda3/envs/gpu_env/bin/python -m pytest tests/test_reader.py -k test_benchmark_matrix_load
   ```
2. Verify load latency remains consistently below 2.0ms over 1,000 consecutive read cycles.
