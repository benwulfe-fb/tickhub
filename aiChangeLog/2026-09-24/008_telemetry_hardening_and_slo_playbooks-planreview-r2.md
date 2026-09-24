# Plan Review — aiChangeLog/2026-09-24/008_telemetry_hardening_and_slo_playbooks.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 20:03:20Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

### 1. Summary of the Proposal

Four telemetry fixes closing visibility gaps across Go producer, Go relay client, and Python SHM reader, plus 10 operational playbooks:
1. `pkg/project/projector.go` & `pkg/shm/producer.go`: Measure wall-clock elapsed execution time in `closePhase` (`publishLatNS`), commit to `AnchorPublishLatencyNS`, log `[SLO-VIOLATION]` if >15ms.
2. `pkg/relay/client.go`: Immediately log `[SLO-VIOLATION]` on packet sequence discontinuity; log `[SLO-VIOLATION]` if TCP replication latency exceeds 10ms WAN budget.
3. `python/tickhub/shm.py`: Initialize module logger with `NullHandler`; emit `[RESILIENCE]` on daemon restart / cold start; emit `[SLO-VIOLATION]` on buffer overrun triggering `_auto_realign`.
4. `playbooks/`: Add central README index plus 9 operational playbooks with triage, diagnostics, and recovery procedures for SLO-01 through SLO-09.

---

### 2. Simplest Sufficient Design

Plan is at minimum for stated 4 visibility gaps and 9 operational playbooks. No unnecessary abstractions, state, or dependencies introduced.

---

### 3. Blocking Defects

None.

---

### 4. Non-Blocking Observations

1. In `pkg/relay/client.go`, check `seq > lastPacketSeq + 1` rather than `seq != lastPacketSeq + 1` to prevent negative gap math on reconnect/retransmit.
2. In `pkg/relay/client.go`, replication latency check hardcodes 10ms WAN budget; consider configurable threshold if loopback deployments require 500µs alerting.
3. In `pkg/shm/producer.go`, ensure atomic write order guarantees `AnchorPublishLatencyNS` is visible before or alongside `LastWrittenAnchorNS`.
4. In `python/tickhub/shm.py`, ensure calling execution models attach handlers so `NullHandler` does not drop warning events silently.

---

### 5. Methodological & Data-Alignment Concerns

- **1Hz Projection Semantics**: Measuring wall-clock delta inside `closePhase` decouples calculation latency from bar-logical timestamp $T$, resolving B1 defect from Round 1.
- **Cache-Line Isolation**: Writing `AnchorPublishLatencyNS` at offset 64 remains confined to producer cache line; zero impact on consumer SeqLock read paths.
- **ABI Parity**: Field layout and offsets in SHM header remain identical across Go, C, and Python.

---

### 6. Missing Telemetry

- Unit test verifying non-zero `AnchorPublishLatencyNS` and Prometheus metric update upon synthetic phase close.
- Integration test with injected packet sequence gap verifying immediate `[SLO-VIOLATION]` log emission.

---

### 7. Verdict

**Verdict: Proceed**

Round 1 B1 timestamp origin defect is remediated: `publishLatNS` measures exact wall-clock elapsed duration of `closePhase`, correctly isolating publish latency from bar-logical timestamp $T$. Changes are surgical, address all 4 identified visibility gaps directly, preserve dual cache-line isolation and zero-copy reader guarantees, and add required operational playbooks without bloat. Proceed to implementation.
