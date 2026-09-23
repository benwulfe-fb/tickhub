# Plan Review — aiChangeLog/2026-09-23/001_tickhub_public_api_and_shm_subsystem.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-23 16:57:56Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: B3 halt 1/2 (UNCOUNTED — round budget unchanged, still 1/2) · Verdict parsed: **Blocked (B3)**_

---

## Review: `aiChangeLog/2026-09-23/001_tickhub_public_api_and_shm_subsystem.md`

---

### 1. Summary of the Proposal

Replace the Python orchestrator's inline tick-drain / watermark / 1Hz-projection with a Go daemon (TickHub) that writes cadence frames into POSIX shared memory (`/dev/shm`). The client engine (Python) reads frames without any OS synchronization primitive: it sleeps until an empirically-derived expected-publish timestamp, then busy-sweeps 30 symbols checking `symbol_anchor_ns[s] == target_anchor_ns` per symbol. Anchor identity is the timestamp itself (not a separate counter), making cross-anchor reads structurally detectable. A thin C library (`libtickhub_atomic.so`) provides acquire-fence loads and `_mm_pause` to Python via `ctypes`. Phase 1 is scoped to the SHM subsystem only (no feed ingestion, no projection logic), with a cross-language test suite and latency benchmarks.

---

### 2. Simplest Sufficient Design

The stated Phase 1 goal is: *"establish the Shared Memory Subsystem as a standalone, tested architectural layer."* The plan is appropriately scoped to that. The components (Go `pkg/shm`, C atomic bridge, Python `tickhub/shm.py`, cross-language test) are all load-bearing for the stated deliverable. No obvious component can be removed without losing either correctness (the C barrier bridge is necessary given the CPython `ctypes` limitation the plan correctly identifies) or testability.

**The plan is at or near the minimum for its Phase 1 scope.**

One structural note for the record: `abi.py` and `shm.py` are listed as separate files. This is fine; `abi.py` holding ctypes struct definitions separately from reader logic is a clean separation. Not a cut candidate.

---

### 3. Blocking Defects

**B3 — Unmeasured root-cause attribution.**

The plan's design is justified by this claim (§ "Goal & Context"): *"This consumes 15–40 ms of the tick-scan latency budget on the orchestrator thread, creating latency spikes, GC pauses, and GIL contention."*

This is a quantitative root-cause attribution — specifically, that the Python orchestrator tick-scan path is where 15–40 ms is spent, and that the cause is that work (watermark + project_to_1hz + feature build + scoring) running on the orchestrator thread.

The changelog presents no profiling data, no flame graph, no `cProfile`/`py-spy` trace, and no wall-clock measurement establishing:

1. **The magnitude**: that 15–40 ms is actually observed in production (vs. estimated or anecdotal).
2. **The attribution**: that this cost lives on the orchestrator thread rather than in, e.g., the model forward pass, GPU transfer, or numpy allocations in `LiveFeatureBuilder` / `LiveScorer` — none of which move to TickHub.

The design being specified in Phase 1 is the SHM transport layer. That layer can only recover latency that is attributable to the *data-delivery and projection* portion of the orchestrator thread. If the 15–40 ms is dominated by `LiveFeatureBuilder` or `LiveScorer` (which are explicitly described as remaining in Python), the SHM subsystem recovers nothing material, and the architectural bet is unfalsifiable from the changelog text.

**Measurement required before implementation:**
- Run `py-spy record` against `ccm/live/` under live or replay feed for ≥ 1 trading session.
- Produce a flamegraph that isolates: (a) WebSocket drain + TickArena update, (b) watermark check, (c) `project_to_1hz`, (d) `LiveFeatureBuilder`, (e) `LiveScorer`.
- Confirm that (a)–(c) account for a material fraction of the 15–40 ms, and that (d)–(e) do not dominate.

Without this, the plan is building a sophisticated IPC subsystem to fix a bottleneck that may not be where the plan claims it is.

**Verdict: `Blocked (B3)`**

---

### 4. Non-Blocking Observations

1. The sleep-then-sweep protocol assumes `anchor_publish_latency_ns` is stable across sessions; a single latency spike (e.g., GC in Go, kernel preemption) that exceeds the sleep budget causes the sweep to start too early and busy-spin for the full spike duration — this degrades gracefully but is undocumented.
2. Dynamic $\Delta t$ adjustment (§ 1, "dynamically adjusts $\Delta t$") is described without bounds; an unbounded watermark growth would silently delay all phase consumers without any alerting threshold specified.
3. The C library `Makefile` is listed but no mention of a build-system integration (CMake, `go generate`, Bazel) is made — the `.so` must exist before the Python tests run; build ordering is implicit.
4. `max_frames` must be a power-of-two for the bitwise-AND slot mapping to be correct (§ 2, slot formula); the plan does not state this constraint or enforce it.
5. The `sched_yield` fallback in the sweep loop (§ 3) is described alongside `time.sleep(0.00005)` as if interchangeable; on Linux with `SCHED_OTHER`, `sched_yield` does not guarantee a 50 µs yield and may return immediately, defeating the intent.

---

### 5. Methodological & Data-Alignment Concerns

N/A

---

### 6. Missing Telemetry

Per the B3 block, the prerequisite measurement is a production flamegraph of `ccm/live/` attributing orchestrator-thread latency to each component. This is required to know whether implementing this plan would actually move the metric it claims to target.

Post-implementation (if the B3 measurement clears): the plan already specifies sweep latency and sleep-jitter benchmarks (§ Verification). The one operationally critical addition is a live drop-rate counter for late ticks arriving after $T + \Delta t$ — the plan mentions TickHub monitors this internally but does not require it be visible to an operator in real time, which is the only signal that would prompt a $\Delta t$ increase in production.

---

### 7. Verdict

**Verdict: Blocked (B3)**

The plan builds a non-trivial cross-language IPC layer to eliminate 15–40 ms of orchestrator-thread latency, but that number is asserted without profiling data and its attribution to the data-ingestion / projection path (rather than to `LiveFeatureBuilder` or `LiveScorer`, which remain in Python) is unestablished. If the bottleneck is downstream of where TickHub intervenes, Phase 1 delivers correct infrastructure that moves no production metric. Take a single `py-spy` flamegraph under live feed, confirm the attribution, then this review clears.
