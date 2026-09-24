# Plan Review — aiChangeLog/2026-09-24/007_slos_monitoring_and_midday_recovery.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 19:20:10Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Three coupled pillars added to a production-bound 1Hz SHM engine: (1) quantified SLOs for publish latency, SeqLock read, relay delivery, and heartbeat freshness; (2) a Prometheus+health-endpoint HTTP server, a read-only terminal TUI (`tickhub top`), and SHM-sampled telemetry — all observation-only and off the hot path; (3) a daemon midday restart protocol that either warm-recovers from a still-resident ring buffer (seeding projector history from up to 60 prior frames) or cold-starts with a `FlagColdStart` bit suppressing downstream trade execution for up to 15 s. The Python `TickHubReader` is extended to detect `BootID` changes, transparently remap SHM, and re-align cursors. Six files are modified or created; ABI structs are extended in Go, C, and Python.

---

## 2. Simplest Sufficient Design

The three pillars are genuinely coupled: the SLOs are only enforceable if the telemetry exists, and warm recovery depends on the generation/BootID ABI extension that the client resilience protocol also depends on. The core change bundle is coherent and tight.

**One material over-reach:** `cmd/tickhub/top.go` (the TUI) is entirely independent of the goal. The stated goal (§1) is "formalize, engineer, and verify" the three pillars. `tickhub top` is operator convenience; it adds a new `cmd/` entrypoint with its own curses dependency, contributes nothing to the SLOs, Prometheus delivery, or restart safety, and can be deferred without losing anything the goal requires.

**Cut list:**

| Cut | File | Lost |
|---|---|---|
| Terminal TUI | `cmd/tickhub/top.go` | Operator convenience dashboard; not needed to prove SLOs or recovery |

Everything else (ABI extension, recovery logic, metrics server, Python resilience, integration test) is load-bearing for the stated goal.

---

## 3. Blocking Defects

**B2 — SHM warm-recovery race: concurrent Python reader during daemon re-attach window**

§4.2 step 1 describes warm recovery as: attach to existing segment → read up to 60 frames to seed history → then `Generation++`, update `BootID`, set `Status = StatusRunning`. The plan is silent on what the Python `TickHubReader` sees during the interval between daemon attach and the `BootID` write. During that window: the old segment is live, `BootID` still carries the dead daemon's value, but the Go daemon is now also writing to it (advancing `Generation`, possibly beginning projector seeding writes into ring slots). A Python consumer mid-`load_sync` will observe SHM being mutated by the new daemon before the `BootID` sentinel has flipped — it will not yet know a restart occurred, will not run `reconnect_if_needed()`, and may read a partially-overwritten frame. Because the recovery path writes `Generation++` and `BootID` non-atomically with respect to the SeqLock protocol (the plan does not say these fields are covered by the producer-side SeqLock), a concurrent reader can load a torn `GlobalHeader`. The plan's own liveness protocol (§4.3) is predicated on `BootID` being the unambiguous restart sentinel, but that sentinel is only reliable if it is the **last** field written atomically under the existing SeqLock discipline — the plan does not establish this ordering guarantee anywhere in §4.1 or §6/change 1.

**Fix required before implementation:** §4.2 must specify the exact write ordering for `GlobalHeader` fields during warm recovery, and §4.1 / change 1 must confirm that `BootID`/`Generation`/`RecoveryMode` writes are covered by the existing producer-side SeqLock (or equivalent atomic publication barrier), so that a racing Python reader either sees the full old state or the full new state, never a mix.

---

## 4. Non-Blocking Observations

1. §2 SLO table: "0 frames lost (100.000% delivery)" is a SLO that cannot be met over a TCP relay with no retransmit layer; the relay SLO and the frame-loss SLO should be scoped separately.
2. §5.2 `/readyz` says heartbeat must be fresh within 2 s, but §2 SLO table says stale threshold is 3 s — pick one number.
3. §4.2 warm-recovery eligibility check compares `LastWrittenAnchorNS` against `(MaxFrames - 2) * CadenceInterval`; if `MaxFrames` is large this window is generous, but if a clock jump or feed outage occurs the daemon could incorrectly classify stale data as warm-eligible.
4. `pkg/shm/recovery.go` is listed in §3 (SOLID) but not in §6 (code changes) — it should appear in the change list or the §3 reference should be removed.
5. The integration test (§6 change 6) kills the daemon but does not specify whether the SHM segment is kept resident between kill and restart — the test must explicitly control this to exercise both warm and cold paths.

---

## 5. Methodological & Data-Alignment Concerns

**ABI parity:** Adding `BootID uint64` + `Generation uint32` + `RecoveryMode uint32` to `GlobalHeader` changes the struct layout. §6 change 5 correctly flags Python ABI (`abi.py`) update, but the checklist (§9) references "C" alignment update. If any C consumer reads `GlobalHeader` (e.g. a C relay client or the `tickhub top` TUI via ctypes), the C struct must be updated in lockstep. The plan lists this in the checklist but not as an explicit file in §6 — the C header file (`include/tickhub.h` or equivalent) is missing from the change list.

**SHM field alignment:** `GlobalHeader` changes add `uint64 + uint32 + uint32` = 16 bytes. Alignment is fine if these are appended after existing 8-byte-aligned fields, but the plan does not state where in the struct these fields are inserted. Inserting them in the middle of an existing layout would silently corrupt all existing field offsets for C and Python readers that use fixed offsets.

---

## 6. Missing Telemetry

To know the warm recovery path actually worked (and not silently fell back to cold): the integration test must assert that `ret15s` values at frame 61 are mathematically continuous with frame 60 (i.e., the projector's rolling queue was correctly seeded, not zeroed). The test plan in §6 change 6 step 5 mentions "no numerical discontinuity" but does not specify the assertion — this should be a concrete numeric bound (e.g., `abs(ret15s[61] - expected_from_seed) < epsilon`), not a qualitative pass.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The warm-recovery window creates a concurrent-access hazard: the new daemon begins mutating the live SHM segment before the `BootID` sentinel is atomically published, violating the plan's own guarantee that `BootID` is the unambiguous restart signal to Python consumers. In a live-trading system where `ccm-live` pauses order entry on heartbeat stall detection, a torn `GlobalHeader` read during the re-attach window could cause the reader to either miss the restart signal entirely (trading on stale/overwritten data) or misread partially-written state without triggering `reconnect_if_needed()`. The plan must specify the atomic write ordering of `GlobalHeader` lifecycle fields under the SeqLock protocol before implementation proceeds. The TUI (`cmd/tickhub/top.go`) should also be deferred — it is the one clean cut that reduces scope without losing anything the goal requires.
