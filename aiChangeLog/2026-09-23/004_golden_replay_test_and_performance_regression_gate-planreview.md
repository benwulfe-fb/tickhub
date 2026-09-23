# Plan Review — aiChangeLog/2026-09-23/004_golden_replay_test_and_performance_regression_gate.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-23 22:33:13Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

### 1. Summary of the Proposal

Plan creates deterministic golden replay regression test and performance gate:
- Extracts 60-second DASH tick slice (632 ticks, ~33 KB) from 2026-05-06 into `tests/fixtures/golden/`.
- Adds `--no-unlink` CLI flag to `cmd/tickhub/main.go` to retain private segment `/dev/shm/tickhub_golden_test` post-exit.
- Implements `tests/test_golden_replay.py`: runs replay via `subprocess.run(check=True)`, reads SHM via `TickHubReader`, asserts bit-for-bit float64 equality against `golden_1hz_metrics.npy`, asserts exact anchor equality against `golden_anchors.npy`, and enforces performance thresholds ($\ge 50,000$ ticks/s, $\le 100$ ms wall-clock).
- Cleans up SHM segment via pytest fixture `try...finally`.
- Wires test unconditionally into `scripts/validate_all.py` as Stage 6.

---

### 2. Simplest Sufficient Design

Plan is at simplest sufficient design. Dropped speculative `--wait-consumer` flag from Round 1; relies on sequential `waitpid` process boundary where Linux kernel commits dirty tmpfs pages on process termination before Python reader attaches. Scope is minimal: 1 boolean flag, 1 test file, 1 fixture directory, and 1 harness stage. No cut list warranted.

---

### 3. Blocking Defects

None.

*(Prior Round 1 B1 race resolved by sequential execution architecture; prior Round 1 B1 threshold ambiguity resolved to authoritative $\le 100$ ms).*

---

### 4. Non-Blocking Observations

1. Fixture should unlink `/dev/shm/tickhub_golden_test` in test setup as well as teardown to clear stale segments after hard crashes (`SIGKILL`).
2. Regex parsing of Go replay stdout requires strict telemetry formatting in `cmd/tickhub/main.go`; keep output contract stable.
3. `np.testing.assert_equal` treats `NaN == NaN` as true; ensure cold-start projection produces valid floats or explicitly handles NaN representation.
4. Total 100 ms budget includes Python `subprocess` fork/exec overhead; monitor CI runners for virtualization jitter against the 15–25 ms baseline.

---

### 5. Methodological & Data-Alignment Concerns

- **Cold-Start Rolling Horizon**: 60-second slice executes cold-start projection without pre-session history. Rolling window features (`log_ret_5s`, `log_ret_15s`) in the initial 15 frames ($[T_0, T_{15})$) freeze engine-specific zero-fill / unpopulated behavior into the golden `.npy`. This is valid for certifying deterministic engine regression, but any future change to projector warm-up or cold-start semantics will deliberately trip this gate.

---

### 6. Missing Telemetry

None.

---

### 7. Verdict

**Verdict: Proceed**

Round 1 blocking defects are resolved. Speculative consumer synchronization is removed in favor of a clean, synchronous process boundary, the execution ceiling is reconciled across all sections to $\le 100$ ms, and reference anchor assertions are wired into the test. Design is minimal, isolated, and ready for implementation.
