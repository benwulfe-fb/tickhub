# Plan Review — aiChangeLog/2026-09-23/004_golden_replay_test_and_performance_regression_gate.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-23 22:32:18Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

## 1. Summary of the Proposal

Record a 60-second DASH tick slice (632 ticks) through the existing Go `tickhub replay` → SHM → Python `TickHubReader` pipeline, freeze the output as `.npy` golden files, and add a pytest test that replays, reads SHM, and asserts bit-identical output plus a throughput floor (≥50 k ticks/sec) and wall-clock ceiling (150 ms). Two new CLI flags (`--no-unlink`, `--wait-consumer`) are added to `cmd/tickhub/main.go` to control SHM lifetime during the test. The test is wired unconditionally into `validate_all.py`.

---

## 2. Simplest Sufficient Design

The goal is: *golden replay → SHM read → bit-identical check + throughput gate, runs in validate_all, fixture-only.*

The minimum is exactly: fixture files + pytest file + validate_all wiring + the two CLI flags. The plan is at that minimum. No cut list warranted; proceed.

---

## 3. Blocking Defects

**B1 — Race between `--no-unlink` and consumer read (goal-defeating)**

The plan adds `--wait-consumer` to pause the producer until `LastReadAnchorNS >= LastWrittenAnchorNS` before exiting, but **Section 3 (`tests/test_golden_replay.py`)** describes the test invoking the binary with only `--no-unlink`, with no mention of `--wait-consumer`. The test then opens SHM and reads 60 frames after the subprocess returns.

If the binary exits without `--wait-consumer`, the test's safety guarantee is: the segment is not unlinked (good), but there is **no synchronisation barrier ensuring the final SHM flush is complete before the Python reader begins traversal**. A fast workstation completes in 5.36 ms; on a loaded CI box the kernel scheduler may not have flushed all writes before the Python `mmap` view is established. The result is a flaky bit-identity failure — or worse, a silently wrong golden that certifies corrupt state.

The plan describes `--wait-consumer` but does not state it is used in the test invocation. If the intent is for the producer to exit *then* the test reads (not concurrently), the correct flag combination is `--no-unlink` **plus** some explicit producer-exit synchronisation (either `--wait-consumer` with a dummy reader touch, or a simpler `os.waitpid` + memory barrier). The plan must specify which model is used and enforce it in the test invocation shown.

*Citation*: "Add `--wait-consumer` flag: if true, pauses after final flush until `LastReadAnchorNS >= LastWrittenAnchorNS` before exiting." (§ Proposed Code Changes / 1) vs. "Replay executes with `--config … --no-unlink`" (§ 3 / step 1) — `--wait-consumer` is absent from the test invocation.

**B1 — Threshold inconsistency (goal-defeating, measurement-vs-gate mismatch)**

§ Goal & Context / item 6 states the execution budget is `< 100 ms`. § Measurement / item 3 (Regression Gate Thresholds) states the gate is `< 150 ms`. The checklist (§ Checklist) references `< 5 s` for `validate_all`. Three different numbers for the same constraint. The test code will implement exactly one; the plan does not resolve which is authoritative. If the test implements 150 ms but the stated goal is 100 ms, the gate is 50% looser than specified — which may pass regressions the operator intended to catch.

*Citation*: "Fast Execution: Must execute in $< 100$ ms" (§ Goal & Context / 6) vs. "Maximum allowable replay & extraction wall-clock time: **150 ms**" (§ Measurement / 3).

---

## 4. Non-Blocking Observations

1. `golden_anchors.npy` is listed as a fixture but not referenced in the `test_golden_replay.py` verification steps (§ 3 steps 1–5) — it may be recorded and never asserted.
2. `.npy` files committed to git will bloat the repo on every re-recording; LFS or an explicit policy note is absent.
3. `--wait-consumer` semantics depend on `LastReadAnchorNS` being written by the Python reader during the test — if the test reads *after* producer exit, that field is never updated, making the flag useless in the described workflow regardless of whether it is added.
4. The plan does not state where `TickHubReader` cleans up on test abort (SIGKILL, OOM); SHM leaks are possible without a pytest fixture finalizer or `atexit`.
5. Throughput is measured from the Go binary's own elapsed time (§ Measurement / 2), not from the test's `subprocess.run` wall-clock — the plan should clarify which clock the gate reads.

---

## 5. Methodological & Data-Alignment Concerns

The window is stated as `[1778074260000000000, 1778074320000000000)` ns — a clean half-open interval. No concern there. However:

**Look-ahead leakage risk**: The golden `.npy` is recorded once from the live engine and then frozen. If the projector uses `log_ret_5s` or `log_ret_15s` referencing bars prior to 09:31:00 (i.e., pre-session state) that were available on 2026-05-06 but will not be present in the 60-second fixture slice, the fixture-only replay will produce different values than the full-session replay — and the golden will certify the *wrong* (fixture-truncated) answer. The plan gives no indication that the fixture includes any warm-up pre-session ticks or that the projector's rolling state is initialised to zero at session open. This is not fatal if the engine is known to hard-reset at session open, but the plan should state that assumption explicitly.

---

## 6. Missing Telemetry

The test must emit (and `validate_all` must log) the **actual gate values at failure time** — ticks/sec observed and ms elapsed — not just pass/fail. The plan mentions printing these (§ Telemetry Plan) but does not specify they appear in the failure message when the gate trips. Without them, a CI failure gives no signal for triage.

---

## 7. Verdict

**Verdict: Blocked (B1)**

Two goal-defeating defects must be resolved before implementation. First, the test invocation (`--no-unlink` only) provides no synchronisation barrier guaranteeing the producer's final SHM flush is visible to the Python reader — making the bit-identical assertion inherently racy. Second, the execution-budget threshold is stated as three different values (100 ms / 150 ms / 5 s) across the same document; the test will implement exactly one, silently overriding the operator directive. Both are one-line fixes to the plan: (1) mandate `--no-unlink --wait-consumer` in the test invocation and describe the dummy-reader handshake, (2) resolve the threshold to a single authoritative value and delete the others.
