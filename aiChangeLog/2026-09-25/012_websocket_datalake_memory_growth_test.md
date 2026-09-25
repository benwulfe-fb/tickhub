# 012 — WebSocket Datalake Ingestion Memory Growth & Leak Verification Test

## Goal & Context
Verify whether TickHub daemon exhibits memory growth (heap leaks, buffer accumulation, goroutine leaks, RSS inflation) over sustained ingestion when streaming market data via its WebSocket interface replaying from the datalake.
Expose standard Go runtime memory and process metrics (`go_memstats_heap_alloc_bytes`, `go_memstats_heap_inuse_bytes`, `go_memstats_heap_sys_bytes`, `go_memstats_heap_objects_total`, `go_memstats_num_gc`, `go_goroutines`, `tickhub_process_rss_bytes`) in TickHub's Prometheus `/metrics` endpoint.
Implement an end-to-end memory growth test (`tests/test_websocket_memory_growth.py`) that simulates Massive.com WebSocket protocol, streams hundreds of thousands of ticks from datalake Parquet files into `tickhub daemon`, tracks memory usage and goroutines across dozens of 1Hz projection cycles, and asserts that post-warmup memory remains flat and bounded.

## SOLID Adherence
- **Single Responsibility Principle (SRP)**:
  - `pkg/metrics/server.go`: Exposes operational telemetry and daemon health metrics over HTTP. Adding Go runtime memory stats, goroutines, and process RSS directly serves operational monitoring without bloating business logic.
  - `tests/test_websocket_memory_growth.py`: Dedicated integration test orchestrating the mock WebSocket feed, launching the daemon, sampling memory metrics, and verifying absence of leaks.
- **Open/Closed Principle (OCP)**:
  - Extended `/metrics` Prometheus format without modifying metric reader interfaces or breaking backward compatibility.
- **Liskov Substitution Principle (LSP)**:
  - Standard HTTP endpoints retain existing contracts and error codes.
- **Interface Segregation Principle (ISP)**:
  - Reuses existing `FeedController` and `GlobalHeader` without adding unnecessary coupling.
- **Dependency Inversion Principle (DIP)**:
  - Memory profiling consumes standard library `runtime.MemStats`, `runtime.NumGoroutine()`, and POSIX `/proc/self/statm` without introducing third-party dependencies.

## Proposed Code Changes

### [MODIFY] `pkg/metrics/server.go`
- Add helper `readProcessRSSBytes() uint64` to read resident set size via Linux `/proc/self/statm` (pages * pageSize).
- In `handleMetrics`:
  - Call `runtime.ReadMemStats(&mem)`.
  - Export `go_memstats_heap_alloc_bytes`, `go_memstats_heap_inuse_bytes`, `go_memstats_heap_sys_bytes`, `go_memstats_heap_objects_total`, `go_memstats_num_gc`.
  - Export `go_goroutines` (`runtime.NumGoroutine()`).
  - Export `tickhub_process_rss_bytes`.

### [MODIFY] `pkg/metrics/server_test.go`
- Add assertions in `TestMetricsServerEndpoints` verifying that `go_memstats_heap_alloc_bytes`, `go_memstats_heap_inuse_bytes`, `go_memstats_num_gc`, `go_goroutines`, and `tickhub_process_rss_bytes` are present in `/metrics` output.

### [ADD] `tests/test_websocket_memory_growth.py`
- Mock Massive.com WebSocket server using Python `websockets`:
  - Implements `auth` response: `[{"ev":"status","status":"auth_success"}]`.
  - Implements `subscribe` confirmation.
  - Loads real quotes and trades from datalake Parquet: prefers `/mnt/wc/datalake/2026-09-23` if present, falls back gracefully to `tests/fixtures/golden/datalake/2026-05-06`.
  - Batches events and synchronizes timestamps to advance wall-clock time across 1Hz windows, ensuring projection cycles continuously commit frames to SHM.
- Test runner:
  - Launches `tickhub daemon` with `--ws-endpoint`, `--feed-enabled=true`, `--shm-name tickhub_mem_test`, `--metrics-addr 127.0.0.1:<port>`.
  - Polls `/metrics` every 2 seconds (balancing granularity with minimal stop-the-world overhead).
  - Logs `(elapsed_s, ticks, goroutines, heap_alloc_mb, heap_inuse_mb, heap_objects, num_gc, rss_mb)`.
  - Evaluates post-warmup memory growth:
    - Warmup window: initial 10 seconds for binary loading, mmap, and initial buffer allocation.
    - Post-warmup window: evaluates linear regression slope of `HeapAlloc`, `HeapInuse`, and `RSS`.
    - Asserts that post-warmup heap allocation slope is $< 50\text{ KB/s}$ (bounded by GC oscillation).
    - Asserts that post-warmup goroutine count does not grow (`delta <= 0`).
    - Asserts that post-warmup RSS does not increase by $> 5\%$ relative to the post-warmup baseline.

## Telemetry Plan
- Server `/metrics` exports:
  - `go_memstats_heap_alloc_bytes`
  - `go_memstats_heap_inuse_bytes`
  - `go_memstats_heap_sys_bytes`
  - `go_memstats_heap_objects_total`
  - `go_memstats_num_gc`
  - `go_goroutines`
  - `tickhub_process_rss_bytes`
- Test prints tabular timeline of memory metrics and final leak assessment summary.

## Verification Plan
1. **Plan Review**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/planreview.py aiChangeLog/2026-09-25/012_websocket_datalake_memory_growth_test.md`
2. **Go Unit & Race Tests**:
   - `/mnt/wc/go/bin/go test -v -race ./pkg/metrics`
3. **Memory Test Execution**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u tests/test_websocket_memory_growth.py`
4. **Full Verification Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/validate_all.py`
5. **Code Review Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/codereview.py aiChangeLog/2026-09-25/012_websocket_datalake_memory_growth_test.md`

## Checklist
- [x] No unrequested dependencies added
- [x] Clean tree before edits
- [x] Zero-allocation hot path preserved
- [x] Memory growth rate evaluated and verified flat
- [x] Goroutine count stability verified
- [x] All tests passing
