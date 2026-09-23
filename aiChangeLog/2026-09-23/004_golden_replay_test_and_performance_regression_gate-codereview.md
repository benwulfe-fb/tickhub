# Code Review — aiChangeLog/2026-09-23/004_golden_replay_test_and_performance_regression_gate.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-23 22:36:43Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

Five-part change adding golden parquet replay certification and a performance gate:

- **`cmd/tickhub/main.go`**: Adds `--no-unlink` boolean flag; wires `UnlinkOnExit = !*noUnlink` so the private SHM segment `tickhub_golden_test` survives process exit for Python consumer attachment.
- **`python/tickhub/shm.py`**: In `get_latest_phase_anchor()`, if `self.is_replay_mode` is True, computes the latest phase anchor from `last_written_anchor_ns` (floor-aligned to cadence, adjusted for phase offset) rather than from the wall clock, enabling `load_symbol_history()` to work correctly in replay context.
- **`scripts/validate_all.py`**: Renumbers existing stages 1–6 → 1–7; adds `validate_golden_replay()` as Stage 6 that runs `pytest -v -s tests/test_golden_replay.py` in isolation, streams output via `telemetry.log`, and calls `sys.exit(1)` on nonzero returncode; also excludes `test_golden_replay.py` from Stage 5 to prevent double-execution.
- **`tests/fixtures/golden/config_golden.yaml`**: Defines private SHM name `tickhub_golden_test`, 5 features, single symbol `DASH`, single phase `phase_0ms` (offset_ms=0), max_frames=128, 1 Hz cadence.
- **`tests/test_golden_replay.py`**: `autouse` fixture unlinks SHM before and after; main test runs binary via `subprocess.run(..., check=True)`, reads SHM frame-by-frame via `TickHubReader`, asserts bit-identical float64 equality against `.npy`, parses throughput from combined stdout+stderr, asserts ≥ 50,000 ticks/s and ≤ 100 ms round-trip.

---

## 2. Correctness & Concurrency Bugs

**No blocking concurrency bugs.** The sequential `subprocess.run(check=True)` / `waitpid` boundary is the correct synchronization primitive for Linux tmpfs. By the time Python attaches, the Go process has exited and all dirty tmpfs pages are committed. No race exists.

**Minor observations (non-blocking):**

- `test_golden_replay.py:439` — `num_frames` is computed as `int((last_written_anchor_ns - first_anchor_ns) // cadence_ns) + 1`. This is correct for a contiguous replay, but if the Go projector skips a 1 Hz boundary (no tick in that second), `last_written_anchor_ns` may reflect the final _written_ frame, which is still correctly bounded. No overread risk since `max_frames=128 ≥ 60`.
- `test_golden_replay.py:446` — `cursors[0].target_anchor_ns` is read before `load_sync`; this is consistent with the frame-advance protocol (read anchor, then load, then advance), which is correct.
- `shm.py` diff: The replay branch returns early without consuming the `now_ns` / `latency_ns` path, which is correct. The alignment formula `((last_written - offset_ns) // cadence_ns) * cadence_ns + offset_ns` is arithmetically correct for floor-quantizing to cadence boundaries with phase offset. Verified: if `offset_ns == 0` (which it is in the golden config), this reduces to the unambiguous `(last_written // cadence_ns) * cadence_ns`.
- `main.go:138` — `UnlinkOnExit: !*noUnlink` is correct boolean inversion; default (`noUnlink=false`) yields `UnlinkOnExit=true` (existing behavior preserved); passing `--no-unlink` yields `UnlinkOnExit=false` (segment retained). No regression to live paths.

---

## 3. Projection Math & Temporal Parity

- **SSoT preserved**: No projection math appears in the test or fixtures. All 1Hz math remains in `pkg/project/projector.go` (unmodified). The `.npy` golden files are pre-recorded outputs, not recomputed in the test — the test only asserts byte equality against them.
- **Cold-start acknowledged**: Plan review §5 explicitly records that the 60-second window executes cold-start without pre-session history, so `log_ret_5s` / `log_ret_15s` zero-fill behavior in initial frames is deliberately frozen into the golden. Correct as designed.
- **Half-open interval**: Not touched by this change. YAML does not specify time bounds; those are encoded in the fixture parquet shards, which are binary and not reviewable here but are stated to cover `[1778074260000000000, 1778074320000000000)` per the ledger.
- **No look-ahead or tie-breaking issues**: The test is a pure replay-and-compare. No new projection logic is added.
- **Feature list**: `config_golden.yaml` features `[log_ret_1s, log_ret_5s, log_ret_15s, vol_1s, spread_bps]` — 5 features, matching the `(60, 5)` golden array shape and plan §1.

---

## 4. Deviations from the Approved Plan

All plan items are implemented. No deviations found:

| Plan Item | Status |
|---|---|
| `--no-unlink` flag in `main.go` | ✅ Implemented exactly as specified |
| `get_latest_phase_anchor` replay branch in `shm.py` | ✅ Implemented |
| `tests/fixtures/golden/config_golden.yaml` | ✅ Present, private SHM name `tickhub_golden_test` |
| Fixture parquet + `.npy` files | ✅ Present (binary) |
| `test_golden_replay.py` bit-identical + perf gates | ✅ Implemented |
| Stage 6 in `validate_all.py`, stage renumbering | ✅ Correct (1–6 → 1–7, golden = stage 6) |
| Stage 5 excludes `test_golden_replay.py` | ✅ `--ignore=tests/test_golden_replay.py` present |
| Plan review §4.1: pre-test SHM unlink | ✅ `clean_shm` fixture unlinks before `yield` |
| Plan review §4.3: NaN handling note | Acknowledged (not a code change; `np.testing.assert_equal` NaN-equality is engine behavior, documented in plan review) |

---

## 5. Systems & Performance Violations

No violations found:

- **Zero-allocation hot path**: Not applicable; the golden test is a one-shot integration test, not a hot path. No allocation constraints apply.
- **Cache-line false sharing**: Not applicable to this change set (no new SHM struct layout introduced).
- **Heap escapes**: Python subprocess / numpy usage is appropriate for a test harness.
- **Bare unhandled errors**: `subprocess.run(..., check=True)` raises `CalledProcessError` on nonzero exit — correct. `os.unlink` in cleanup is guarded by `OSError` catch — correct. `np.load` failure would propagate as an assertion error through pytest — correct.
- **`validate_golden_replay`** runs pytest via subprocess with `capture_output=True` and streams output via `telemetry.log`; calls `sys.exit(1)` explicitly on failure. Consistent with the existing `validate_python_tests` pattern.

---

## 6. Telemetry / Verification Gaps

- **Regex robustness**: `test_golden_replay.py:472` regex `r"Completed replay of \d+ ticks in .*?\(([\\d\\.]+)\s+ticks/sec\)"` requires the Go binary to emit exactly that format. Plan review §4.2 flags this as a known dependency; the output contract must remain stable. The regex itself is well-formed and will hard-fail (`assert match is not None`) with full stdout/stderr dump on format drift — adequate diagnostic.
- **Telemetry print lines** (`[GOLDEN TEST TELEMETRY]`) are emitted only with `-s` flag in `validate_all.py` (`pytest -v -s`), which is correct since `validate_golden_replay` passes `-s`. Stage 5 does not pass `-s`; the golden stage does — intentional and correct.
- **No gap**: the plan's telemetry requirements (throughput and elapsed ms printed on every run, assertion messages on gate failure) are all implemented.

---

## 7. Verdict & Remediation

**APPROVED.** All plan items are implemented correctly. The sequential synchronization model is sound, the boolean flag inversion is correct, the `shm.py` replay anchor formula is arithmetically verified, the fixture configuration matches the 5-feature / 1-symbol / 60-frame / private-SHM design, the pre/post SHM cleanup fixture addresses plan review §4.1, and the performance gates match the authoritative thresholds (≥ 50,000 ticks/s, ≤ 100 ms). No blocking defects found.
