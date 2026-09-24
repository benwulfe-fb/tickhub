# Plan Review — aiChangeLog/2026-09-24/008_telemetry_hardening_and_slo_playbooks.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 20:02:26Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

## Adversarial Plan Review: `008_telemetry_hardening_and_slo_playbooks.md`

---

### 1. Summary of the Proposal

Three targeted instrumentation fixes to close telemetry blindspots, plus 10 markdown operational playbooks:

1. **`pkg/shm/producer.go`** — `CommitFrameFinalize` computes wall-clock publish latency in live mode and writes it to `AnchorPublishLatencyNS`; logs `[SLO-VIOLATION]` if >15ms.
2. **`pkg/relay/client.go`** — `runStream` immediately logs sequence gaps rather than deferring to the next anchor commit; also logs relay replication latency violations >10ms.
3. **`python/tickhub/shm.py`** — adds `logging`, emits `[SLO-VIOLATION]` on `_auto_realign` and `[RESILIENCE]` events on daemon restart detection.
4. **`playbooks/`** — 10 markdown files (1 README + 9 SLO playbooks) with triage, diagnostics, and recovery procedures.

---

### 2. Simplest Sufficient Design

The stated goal is: *close 3 telemetry gaps with structured log events*. The playbooks are explicitly scoped as a co-deliverable in the goal statement (§1 final sentence), so they are in-scope.

The three code changes are each small, localized, and directly address the named gaps. The plan is at or near minimum for the code changes.

**One specific cut**: The relay latency violation log (`§4, Change 2`, second bullet in anchor commit handling — `latencyUS > 10000`) addresses SLO-06 but is **not listed as one of the 3 critical visibility gaps** in §1. Gap 2 is specifically "Relay Sequence Gaps Deferred." The latency log is an addition beyond the stated scope. It should be removed or the stated gap list should be updated to name it explicitly. Minor scope creep, but worth naming.

Everything else is at minimum.

---

### 3. Blocking Defects

**B1 — Publish latency calculation uses wall clock vs. anchor timestamp origin.**

§4, Change 1: `latency := time.Now().UnixNano() - anchorNS`

The plan states that `anchorNS` is "the frame's anchor timestamp $T$" (§3, SSoT bullet). In a 1Hz projection engine, `anchorNS` is typically the **market data bar timestamp** — the logical time of the projected interval — not the wall-clock time the frame was assembled. If `anchorNS` is a market timestamp (e.g., a Unix nanosecond derived from exchange feed timestamps or a synthetic 1Hz grid), then `time.Now().UnixNano() - anchorNS` is not publishing latency; it is the age of the bar, which includes market data latency, feed latency, and projection computation time stacked together. The plan never establishes that `anchorNS` is wall-clock-at-frame-start rather than bar-logical-time.

The fix is trivially different if this assumption is wrong: you'd record a `commitStartNS := time.Now().UnixNano()` at entry to `CommitFrameFinalize` and compute `latency := time.Now().UnixNano() - commitStartNS`. But if `anchorNS` is already wall-clock-at-frame-start as intended, the plan should say so explicitly, because the SLO budget (15ms) only makes sense if the reference point is "when the frame began assembly," not "what bar time this frame represents."

**This is goal-defeating (B1)** if `anchorNS` is a bar logical timestamp: the metric would be computed against the wrong reference, the SLO check would be permanently wrong, and the stated gap ("publishing delays exceeding the 15ms budget passed silently") would remain undetected — it would just be replaced by a different silent wrong metric.

The plan must state what `anchorNS` represents and confirm it is wall-clock-at-frame-start, or it must change the latency formula to capture a wall-clock entry time.

---

### 4. Non-Blocking Observations

1. `seqGaps` atomic is incremented in both the new immediate log path and presumably the deferred anchor path — verify no double-count per gap event.
2. The relay latency log (§4 Change 2, second bullet) references `latencyUS > 10000` — confirm units: 10,000µs = 10ms matches SLO-06 WAN budget but not loopback (500µs); a single threshold covers both only if the loopback path has a separate check.
3. Python `logger = logging.getLogger("tickhub.shm")` at module level is fine, but downstream consumers must configure a handler or the `[SLO-VIOLATION]` logs will be silently discarded by the default NullHandler.
4. Playbook count mismatch: §2 lists 10 files (1 README + 9 SLO .md files = 10), §1 says "all 9 SLOs" — consistent, but worth a quick count pass before commit.
5. The checklist (§6) gates on `scripts/planreview.py` APPROVED before code changes — ensure this review fulfills or substitutes that gate.

---

### 5. Methodological & Data-Alignment Concerns

The B1 defect above is the crux: **1Hz projection semantics require knowing whether `anchorNS` is bar-logical-time or wall-clock-at-assembly-start**. In a projection engine running a half-open interval $[T-1\text{s}, T)$, the anchor timestamp $T$ is almost certainly the bar's logical endpoint, not a wall-clock capture time. If so, `time.Now() - anchorNS` at commit time yields bar age (potentially >1s into the past under normal operation), not publish latency. The 15ms threshold would fire continuously and meaninglessly. This is the concrete alignment concern.

---

### 6. Missing Telemetry

To know the change worked:
- A test or log sample confirming `AnchorPublishLatencyNS` is non-zero in live mode after the fix (the plan notes it was previously always zero — a post-fix smoke log showing a non-zero value is the minimum acceptance signal).
- A confirmation that the relay sequence gap log fires immediately on a synthetic injected gap, not only at the next anchor boundary (the stated behavioral fix).

---

### 7. Verdict

**Verdict: Blocked (B1)**

The publish latency formula `time.Now().UnixNano() - anchorNS` is the mechanical heart of gap fix #1 (the primary motivation for this change), and it is correct only if `anchorNS` is a wall-clock capture from frame assembly start. In a 1Hz projection engine the anchor is most likely a bar logical timestamp, which would make the formula measure bar age rather than publishing latency — permanently wrong against a 15ms budget, and the original gap would remain silently unclosed. The plan must establish what `anchorNS` represents and either confirm the formula or replace it with an explicit wall-clock entry capture before implementation proceeds.
