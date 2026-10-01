# Plan Review — aiChangeLog/2026-09-30/001_decouple_tickhub_ingestion_loop.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-10-01 04:16:14Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: B3 halt 1/2 (UNCOUNTED — round budget unchanged, still 1/2) · Verdict parsed: **Blocked (B3)**_

---

## Plan Review: `aiChangeLog/2026-09-30/001_decouple_tickhub_ingestion_loop.md`

---

### 1. Summary of the Proposal

During a live incident on 2026-09-30, publish latency spiked 530–1195ms and downstream watermark lag hit 1489.9ms. The plan attributes this to three concurrent bugs in `cmd/tickhub/daemon.go`:

1. Tick ingestion and the 5ms flush timer share one `select` loop — high-volume bursts starve the timer.
2. The wall anchor computation (`wallAnchor = (nowNS / cadenceNS) * cadenceNS`) only ticks at integer-second boundaries, so Phase 1's 500ms-offset anchor is never flushed until the next full second — a ~500ms latency floor.
3. `Flush()` is called with `wallAnchor` instead of `nowNS - watermarkBufferNS`, losing the watermark subtraction.

Proposed fix: (a) Move tick ingestion into a dedicated goroutine, communicating via the existing channel; (b) add `sync.Mutex` to `Projector` to serialize `IngestTick` vs `Flush`; (c) fix the flush call to `projector.Flush(nowNS - watermarkBufferNS)`.

---

### 2. Simplest Sufficient Design

The three root causes are independent bugs in `daemon.go`'s main loop (bugs 1 and 3) and one arithmetic fix (bug 2). The mutex on `Projector` is only needed *because* of bug 1 (the goroutine split). Without the goroutine split, bug 2 (phase offset) and bug 3 (missing watermark subtraction) are single-line fixes in the existing serial loop with zero new concurrency surface.

**The plan is materially larger than its minimum for bugs 2 and 3.** The goroutine split + mutex is only strictly required if the serial loop cannot drain ticks fast enough *even with* the timer-starvation-free design. That premise is untested (see B3 below).

If the goroutine split is retained (treating timer starvation as independently justified), then the plan is near-minimum. The mutex scope listed under `§3 / pkg/project/projector.go` is broad — it includes read-only accessors like `IsColdStart()`, `CommittedFrames()`, `AnchorRange()` — but this is a code-audit-level concern, not a structural one.

**No cut list is required if B3 is resolved in favor of keeping the goroutine split.** If B3 is NOT established, the cut is: remove the goroutine, remove the mutex entirely, and fix only the two arithmetic bugs in the existing loop — approximately 5 lines changed vs. ~40.

---

### 3. Blocking Defects

**B3 — Unmeasured attribution for the goroutine split.**

The plan's *structural* decision — splitting ingestion into a dedicated goroutine and adding a `sync.Mutex` to `Projector` — is justified exclusively by "Single-Thread Event Loop Contention" (§1 Attribution point 1): tick-channel cases continuously won the `select`, starving the 5ms flush timer.

This is a quantitative attribution claim ("high-volume tick bursts continuously matched the tick channel case, *starving and jittering* the 5ms flush timer"). The plan presents:

- **The effect (measured):** watermark lag 1489.9ms, publish latency 530–1195ms at `daemon.go:328`. ✓
- **The attribution (not measured):** that timer starvation — not bugs 2 and 3 alone — accounts for a material portion of that latency.

Bugs 2 and 3 are *also* present and *independently* sufficient to produce the observed latency:
- Bug 2 alone: Phase 1 anchors flush up to ~995ms late, fully consistent with the 1489.9ms watermark lag.
- Bug 3 alone: missing watermark subtraction means `Flush` targets the raw wall anchor rather than `nowNS - watermarkBufferNS`, which could account for hundreds of ms of additional lag.

The plan does **not** present a measurement showing that, after fixing bugs 2 and 3, timer starvation remains a measurable problem. Without that measurement, the goroutine split and its accompanying `sync.Mutex` rest on an unfalsifiable premise: the design choice that multiplies the concurrency surface of `Projector` cannot be adjudicated from the text.

**Required measurement before proceeding with the goroutine split:** instrument the existing serial loop (after applying only the bug-2 and bug-3 arithmetic fixes) with a histogram of `ticker.C` actual-vs-scheduled fire times and `IngestTick` call duration during a replay of the incident volume. If starvation is confirmed (e.g., >1ms timer jitter at p99), the goroutine split is warranted; if not, only the two arithmetic fixes are needed.

`Blocked (B3)`

---

### 4. Non-Blocking Observations

1. The mutex is acquired in `IsColdStart()` (a getter) — if it's called on the hot path, this adds contention with no semantic requirement; a `sync/atomic.Bool` would suffice.
2. The ingest goroutine logs every `IngestTick` error individually; at high tick rates this could itself saturate I/O and re-introduce timer interference.
3. `ingestDone` channel close-wait in the main loop is not shown handling the case where `feedMgr.Ticks()` is never closed on `ctx.Done()` — potential goroutine leak if the channel isn't closed by the feed manager.
4. Bug 3 (missing `- watermarkBufferNS`) should be verified against the existing `WatermarkBufferNS` field in the producer cache line (§GEMINI.md ABI invariant) to confirm the value is live and correct at flush time.
5. No mention of whether existing `go test -race ./...` passes on the *current* (pre-change) codebase — a failing baseline makes the race detector gate meaningless.

---

### 5. Methodological & Data-Alignment Concerns

**Half-open interval / phase semantics:** The fix in bug 2 changes the flush target to `nowNS - watermarkBufferNS`. The plan must verify this expression still satisfies the `[T−1s, T)` half-open invariant across all phase offsets (0ms and 500ms), i.e., that `phaseNextAnchor[pIdx] <= nowNS - watermarkBufferNS` correctly gates both phases within the 5ms cadence without prematurely or doubly flushing a phase. This is not demonstrated in the plan text.

**Concurrency / quote-before-trade tie-breaking:** With a concurrent ingest goroutine and a mutex-gated `Flush`, quote-before-trade tie-breaking correctness depends entirely on the mutex fully serializing the two goroutines. This holds *only if* the ingest goroutine never batches or reorders ticks before calling `IngestTick`. The plan does not state whether the channel delivers ticks in strict arrival order, or whether `feedMgr` can buffer and reorder.

---

### 6. Missing Telemetry

One metric the plan must add to know the change worked: **per-phase flush jitter** — the delta between `phaseNextAnchor[pIdx]` and the actual `nowNS` at the moment `Flush` commits that phase, reported as a histogram (p50/p99/max) in the existing telemetry path. Without it, there is no way to confirm bugs 2 and 3 are resolved in production, nor to detect regression.

---

### 7. Verdict

**Verdict: Blocked (B3)**

The goroutine split and `sync.Mutex` addition — the structural heart of this plan — are justified by a timer-starvation attribution that the text does not measure. Bugs 2 (phase-offset arithmetic) and bug 3 (missing watermark subtraction) are each independently sufficient to explain the full observed latency and are trivially fixed in the existing serial loop. The plan must first apply those two fixes and measure whether starvation remains before deciding whether the additional goroutine + mutex complexity is warranted. Proceeding as written introduces unnecessary concurrency surface in `Projector` on an unfalsifiable premise.
