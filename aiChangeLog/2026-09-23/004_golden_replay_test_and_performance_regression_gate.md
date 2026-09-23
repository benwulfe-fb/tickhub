# 004 — Golden Parquet Replay Test, Bit-Identical SHM Verification, and Performance Regression Gate

## Goal & Context

Operator directive (2026-09-23):
*"i want you to record playback for one of the datalake parquet files through the legacy engine with project1hz to form a golden test. save the input parquet and output 1hz metrics (obtained through shm). make sure the yaml for the test creates a private shm instance and the test should ensure its 100% bit identical to the old metrics. create a ledger for this golden test and make sure its fast and always runs in validate_all. in fact, you should also measure performance and fail on performance regressions."*

### Problem Statement & Scope
1. **Deterministic Golden Certification**: To prevent drift between market data projection and downstream consumers, an immutable, reproducible golden replay test must exist in the repository.
2. **Offline Fixture Isolation**: The test must not depend on external datalake availability or network resources. The input parquet shards (trades & quotes) and output 1Hz metrics must be stored directly in `tests/fixtures/golden/`.
3. **Private SHM Instance**: The test YAML designates an isolated shared memory segment `tickhub_golden_test` to prevent collision with concurrent daemon processes or live streams.
4. **Bit-Identical Invariant**: Metrics read from SHM must match the reference golden metrics with 100% bit-for-bit equality (exact IEEE 754 float64 representation, zero tolerance, via `np.testing.assert_equal`).
5. **Performance Regression Gate**: Replay throughput and extraction latency are measured. If throughput drops below the established baseline ($\ge 50,000$ ticks/sec) or round-trip execution exceeds the strict time budget ($\le 100$ ms), the test and `validate_all.py` fail immediately with diagnostic telemetry.
6. **Fast Execution**: Must execute in $\le 100$ ms so it runs unconditionally on every `validate_all.py` invocation without slowing down development.

---

## Measurement

1. **Input Fixture Geometry**:
   - Source: `/mnt/wc/datalake/2026-05-06/D/` (`DASH.trades.parquet` and `DASH.quotes.parquet`).
   - Window: Market opening session 2026-05-06 09:31:00 to 09:32:00 EST (60 seconds, $[1778074260000000000, 1778074320000000000)$ ns).
   - Cold-Start Isolation: Fixture slice executes cold-start projection over the isolated 60-second window, freezing the exact deterministic output.
   - Ticks: 247 quotes (~14 KB Parquet) + 385 trades (~19 KB Parquet) = **632 ticks total** (~33 KB fixture size).
2. **Empirical Baseline Performance (Workstation Benchmark)**:
   - Go `tickhub replay` ingestion, K-way min-heap merge, and 1Hz projection of 632 ticks:
     - Elapsed time: **5.36 ms** (wall-clock).
     - Throughput: **117,804.8 ticks/sec** (or **~8.5 µs / tick**).
   - Python `TickHubReader` SHM mapping, cursor traversal, and 60-frame matrix load:
     - Extraction time: **0.82 ms**.
     - Per-frame extraction latency: **13.6 µs / frame**.
   - Total round-trip test execution time: **~15–25 ms**.
3. **Regression Gate Thresholds (Authoritative Standards)**:
   - **Throughput Floor**: $\ge 50,000\text{ ticks/sec}$ (measured from Go binary replay telemetry: `tickCount / elapsed.Seconds()`).
   - **Execution Budget Ceiling**: $\le 100\text{ ms}$ (total round-trip wall-clock time encompassing `subprocess.run` replay execution and Python `TickHubReader` SHM extraction).
   - **Bit-Identical Accuracy**: **100.000%** (zero float64 difference across all 60 frames $\times$ 5 features via `np.testing.assert_equal`, plus exact anchor timestamp match via `np.testing.assert_array_equal`).

---

## SOLID & Systems Engineering Adherence
- **Single Responsibility (SRP)**:
  - `cmd/tickhub/main.go`: Add `--no-unlink` flag to skip unlinking on exit so the SHM segment remains resident in `/dev/shm` for post-replay inspection. (No speculative consumer waiting flag).
  - `tests/test_golden_replay.py`: Pytest integration test executing the replay, reading SHM, verifying bit-equality, and checking performance.
  - `scripts/validate_all.py`: Integrates the golden replay check as a first-class validation stage.
- **Single Source of Truth (SSoT)**:
  - 1Hz metric projection math remains strictly in `pkg/project/projector.go`.
- **Deterministic Process Synchronization**:
  - Sequential execution model: `subprocess.run(check=True)` runs `tickhub replay` to process termination (`waitpid`). In Linux tmpfs (`/dev/shm`), all dirty pages are committed in kernel page cache upon process exit, providing a robust synchronization barrier before Python `TickHubReader` attaches.
- **Fail-Fast & Cleanup (Poka-Yoke)**:
  - Fails immediately on any float divergence (`np.testing.assert_equal`).
  - Fails immediately on throughput drop below 50,000 ticks/sec or execution time exceeding 100 ms.
  - Guaranteed cleanup: Pytest fixture / `try...finally` block unlinks `/dev/shm/tickhub_golden_test` unconditionally, preventing resource leaks even on assertion failure or abort.

---

## Proposed Code Changes

### 1. `cmd/tickhub/main.go` [MODIFY]
- Add `--no-unlink` boolean flag (default `false`): if true, sets `shmCfg.UnlinkOnExit = false` so `prod.Close()` leaves the SHM segment resident in `/dev/shm` for consumer analysis.

### 2. `tests/fixtures/golden/` [ADD]
- `tests/fixtures/golden/datalake/2026-05-06/D/DASH.quotes.parquet` (247 quotes).
- `tests/fixtures/golden/datalake/2026-05-06/D/DASH.trades.parquet` (385 trades).
- `tests/fixtures/golden/config_golden.yaml`: config defining private SHM segment `tickhub_golden_test`.
- `tests/fixtures/golden/golden_1hz_metrics.npy`: reference (60, 5) float64 array of `[log_ret_1s, log_ret_5s, log_ret_15s, vol_1s, spread_bps]`.
- `tests/fixtures/golden/golden_anchors.npy`: reference (60,) int64 array of anchor timestamps.

### 3. `tests/test_golden_replay.py` [ADD]
- Pytest test with `try...finally` cleanup verifying:
  1. Replay executes with `--config tests/fixtures/golden/config_golden.yaml --no-unlink` via `subprocess.run(check=True)`.
  2. Parses throughput (ticks/sec) from Go replay stdout.
  3. Maps `/dev/shm/tickhub_golden_test` via `TickHubReader`.
  4. Asserts 100% bit-identical match with `golden_1hz_metrics.npy` (`np.testing.assert_equal`).
  5. Asserts exact anchor timestamp match with `golden_anchors.npy` (`np.testing.assert_array_equal`).
  6. Asserts replay throughput $\ge 50,000$ ticks/sec and total round-trip wall-clock time $\le 100$ ms. Assertion messages print observed values versus thresholds upon failure.
  7. Finalizer unlinks `/dev/shm/tickhub_golden_test`.

### 4. `python/tickhub/shm.py` [MODIFY]
- Update `get_latest_phase_anchor()` to compute latest phase anchor from `last_written_anchor_ns` when `is_replay_mode` is True, enabling `load_symbol_history()` in historical replay.

### 5. `scripts/validate_all.py` [MODIFY]
- Integrate explicit Golden Replay verification stage (Stage 6) into `validate_all.py` pipeline.

---

## Telemetry Plan
- `test_golden_replay.py` prints measured ticks/sec and elapsed milliseconds.
- On gate failure, assertion error messages explicitly report:
  `AssertionError: Replay throughput regression: observed {rate:.1f} ticks/s < floor 50000.0 ticks/s`
  `AssertionError: Execution budget exceeded: elapsed {elapsed_ms:.1f} ms > ceiling 100.0 ms`
- `validate_all.py` logs golden replay execution time and throughput to console and `output/{YYMMDD}_{HHMM}_{PID}_stdout.txt`.

---

## Verification Plan
1. Run `python scripts/planreview.py aiChangeLog/2026-09-23/004_golden_replay_test_and_performance_regression_gate.md`.
2. Implement code changes and fixtures.
3. Run `pytest -v tests/test_golden_replay.py` directly.
4. Run `python scripts/validate_all.py` to confirm all stages pass in $< 5$s overall suite runtime.
5. Run `python scripts/codereview.py aiChangeLog/2026-09-23/004_golden_replay_test_and_performance_regression_gate.md`.
6. Commit and push to `origin/main`.

---

## Checklist
- [x] Plan review completed (Round 1 B1 defects addressed; Round 2 APPROVED / Proceed)
- [x] Create `tests/fixtures/golden/` directory and extract DASH 60s slice
- [x] Record golden 1Hz metrics into `golden_1hz_metrics.npy` and `golden_anchors.npy`
- [x] Add `--no-unlink` flag to `cmd/tickhub/main.go`
- [x] Support replay mode in `python/tickhub/shm.py` `get_latest_phase_anchor`
- [x] Implement `tests/test_golden_replay.py` with bit-identical & throughput assertions
- [x] Integrate into `scripts/validate_all.py`
- [x] Execute `validate_all.py` and verify all tests pass (entire test suite $< 5$s, golden test $\le 100$ms)
- [x] Execute `scripts/codereview.py` (APPROVED by Claude Sonnet 4.6)
- [x] Commit and push
