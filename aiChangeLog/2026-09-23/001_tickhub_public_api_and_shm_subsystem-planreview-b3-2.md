# Plan Review — aiChangeLog/2026-09-23/001_tickhub_public_api_and_shm_subsystem.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-23 18:22:56Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: B3 halt 2/2 (UNCOUNTED — round budget unchanged, still 1/2) · Verdict parsed: **Blocked (B3)**_

---

# Plan Review — `aiChangeLog/2026-09-23/001_tickhub_public_api_and_shm_subsystem.md`

_Model: Claude Sonnet 4.6 (Thinking) · Round: 2 · Generated: 2026-09-23T11:22:08-07:00_

---

## 1. Summary of the Proposal

Greenfield Go daemon (`tickhub`) that ingests tick streams, applies a time-buffered watermark, projects 1Hz cadence bars, and writes contiguous per-phase multi-symbol metric frames into POSIX SHM (`/dev/shm`). Python client reads via a C atomic bridge (`libtickhub_atomic.so`), async-sleeps until the expected publish moment, then busy-sweeps unacquired symbols. Phase 1 scope: SHM subsystem only — Go `pkg/shm`, C atomic bridge, Python `abi.py` / `shm.py`, cross-language test + latency benchmark. Feed ingestion and projection logic are deferred.

Prior B3 block (missing profiling attribution for the 15–40 ms claim) is disposed by the current changelog: TickHub is reframed as an independent greenfield open-source project with its own subsystem delivery benchmarks, not an in-tree refactoring of `ccm/live`. The architectural baseline is therefore defined by the four measurements in the "Measurement" section, not by profiling `ccm/live`.

---

## 2. Simplest Sufficient Design

Phase 1's stated goal is "establish the Shared Memory Subsystem as a standalone, tested architectural layer." The four components (Go `pkg/shm`, C atomic bridge, Python `abi.py`/`shm.py`, cross-language test + benchmark) are all load-bearing for that goal. The public API section (§ "Public Developer API Contracts") is larger — full `TickHubReader` class, `SymbolCursor`, `begin_all`, `staleness`, `rebegin`, `load_sync`, `snapshot`, multi-phase config YAML, CLI interface — but is documented as contracts, not implemented in Phase 1. The implementation plan itself (§ "Phase 1 Implementation Plan") is scoped to `pkg/shm`, `c/`, `python/tickhub/{abi,shm}.py`, and `tests/`. The plan is at or near the minimum for Phase 1 scope.

---

## 3. Blocking Defects

**B3 — Prior block partially cleared; one quantitative attribution remains unestablished.**

The prior B3 was the 15–40 ms orchestrator-thread claim. The changelog disposes this correctly: TickHub is a greenfield independent project; its baseline is its own subsystem benchmarks, not a profile of `ccm/live`. That disposal holds — do not re-raise.

However, the "Measurement" section (§ "Measurement") introduces its own quantitative claim that functions as a design justification:

> **Async Event-Loop Yield**: In `await hub.load()`, during the **~40 ms network transit window**, the event loop must yield 100% of the thread via `await asyncio.sleep()`.

"~40 ms network transit window" is asserted as a known, measured quantity that justifies a specific architectural decision: the async sleep-then-sweep design (§ 4, "Async `load`") exists precisely to exploit this window. The 40 ms figure determines whether `asyncio.sleep(remaining_time)` actually frees meaningful time for broker WebSocket I/O, or whether the window is so short that `await asyncio.sleep(0)` cooperative-yield is sufficient. If the true transit window is 5 ms, the async architecture still works but its benefit shrinks dramatically; if it is 80 ms, the sleep is too conservative.

The changelog does not cite a measurement source for this 40 ms. It does not appear in the prior review (the prior review's 15–40 ms was orchestrator CPU cost, not network transit), so this is not a re-raised finding. It is a new, independent quantitative attribution embedded in the remediated text.

**Measurement required:**
- Instrument the existing live feed (or the Massive WebSocket source referenced in `config.yaml`) to record the distribution of `SIP_timestamp → local_arrival_time` latency across a representative trading session.
- Confirm the p50/p95/p99 figures bracket ~40 ms. If they do, the sleep-then-sweep design is correctly sized and Measurement #2 is satisfied. If not, `initial_buffer_ms: 50` and the sleep duration in `await hub.load()` are miscalibrated before a single line of Go is written.

This is not the same finding as the prior B3 (which was about CPU attribution inside `ccm/live`). It is a network-layer latency attribution that the changelog itself introduces in the Measurement section as a design foundation.

**Verdict: Blocked (B3)**

---

## 4. Non-Blocking Observations

1. `max_frames: 1024` in the config example (§ "SSoT `config.yaml`") but the power-of-two constraint for the bitwise-AND slot formula (§ 2) is not stated as a validation rule in `tickhub validate` — prior observation, still not closed in the design text.
2. `rebegin` threshold of "> 17 minutes" (§ 6) is hardcoded in the design; no config knob is specified, nor is it derived from any stated invariant.
3. `load_sync` is in the public API class (§ "Python Client API") but absent from the Phase 1 implementation plan — its scope boundary (Phase 1 vs. later) is unspecified.
4. The `failed_cursors` return type from `await hub.load()` (§ 4) documents timeout behavior but the changelog does not specify what the caller must do with failed cursors — whether `next()` still advances them or whether they are stuck at the same `target_anchor_ns`.
5. Build ordering dependency between `libtickhub_atomic.so` and Python tests is implicit; no build integration (CMake, `go generate`, or a top-level `Makefile`) is specified in the Phase 1 plan.

---

## 5. Methodological & Data-Alignment Concerns

N/A

---

## 6. Missing Telemetry

The ~40 ms network transit figure (§ "Measurement", item 2) must be established by instrument before implementation, as stated in §3. This is the prerequisite measurement, not a post-implementation metric.

Post-implementation: the plan's own Verification section already covers ABI alignment, cross-language write/read integrity, sleep jitter, and sweep latency. The one operational gap noted in the prior review — a live drop-rate counter visible to an operator in real time — is referenced in § 1 ("TickHub continuously monitors late-tick drop rates") but the plan does not specify how this is surfaced (log line, SHM header field, Prometheus metric). Needed to know in production when `max_buffer_ms` must be raised.

---

## 7. Verdict

**Verdict: Blocked (B3)**

The prior B3 block is correctly and fully disposed: TickHub's baseline is its own subsystem benchmarks, not a profile of `ccm/live`, and that reframing is coherent. However, the remediated "Measurement" section introduces a new quantitative claim — "~40 ms network transit window" — as the explicit justification for the async sleep-then-sweep design. This figure is asserted, not measured, and it is architecturally load-bearing: it calibrates `initial_buffer_ms`, the sleep duration in `await hub.load()`, and the decision to use a full cooperative sleep rather than a simple yield. A one-session latency histogram from the Massive WebSocket feed (or any representative replay) confirming the p50/p95 bracket around 40 ms closes this immediately. Take the measurement first; the design is otherwise sound.
