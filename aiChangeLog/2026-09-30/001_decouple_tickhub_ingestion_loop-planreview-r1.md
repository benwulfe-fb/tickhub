# Plan Review — aiChangeLog/2026-09-30/001_decouple_tickhub_ingestion_loop.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-10-01 04:17:24Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Three bugs caused the 2026-09-30 latency incident in `cmd/tickhub/daemon.go`:
1. `wallAnchor := (nowNS / cadenceNS) * cadenceNS` — integer-second stride skips Phase 1 (500ms-offset) anchors until the next full second (~500ms floor).
2. `projector.Flush(wallAnchor)` omits `- watermarkBufferNS`, bypassing the 50ms watermark buffer.
3. The `select` loop gives equal priority to tick ingestion and the 5ms flush timer.

The revised plan (responding to Round 1's B3) drops the goroutine split and mutex entirely. Instead it:
- Replaces `wallAnchor` with `flushTarget := nowNS - watermarkBufferNS` (fixes bugs 1 and 2 together).
- Wraps the main `select` with a prioritized outer `select` that drains `ticker.C` and `ctx.Done()` before entering the tick-ingest case (fixes bug 3).
- Zero new goroutines, zero new state, ~30 lines changed in `daemon.go` only.

---

## 2. Simplest Sufficient Design

Plan is at or near the minimum. The three bugs are fixed in a single function with no new files, no new abstractions, and no new types. The prioritized-select idiom is the canonical Go pattern for this exact problem and adds no persistent state.

---

## 3. Blocking Defects

**B2 — Prioritized select creates a starvation inversion: tick channel can now be starved by timer.**

The proposed loop (`daemon.go:320–343`) is:

```go
// outer select
select {
case <-ctx.Done():   …return
case now := <-ticker.C:  flushNow(now); continue   // ← drains ALL pending timer ticks
default:
}

// inner select (only reached when ticker.C was empty)
select {
case <-ctx.Done():   …
case tick := <-feedMgr.Ticks():   …
case now := <-ticker.C:   flushNow(now)
}
```

The outer `select` has a `default`, so it is non-blocking. It checks `ticker.C` first, then falls through to the inner `select` when the timer is not ready — which is correct in the normal case.

The problem is the `continue` on `ticker.C` in the outer select. Each time `ticker.C` fires the loop restarts from the top, performs the outer select again, and — if another tick has already arrived in the 5ms window — fires again before ever touching `feedMgr.Ticks()`. At a 5ms cadence under a 200 Hz-plus burst (which is the incident scenario), `ticker.C` can accumulate a backlog. Each outer-select iteration drains exactly one timer event, loops back, and — since the timer channel likely still has events buffered — prioritizes another `flushNow` call over any tick ingestion. This cannot deadlock, but it can starve tick ingestion for multiple timer cycles, delaying `IngestTick` calls and causing ticks to buffer in `feedMgr.Ticks()` beyond the watermark window — the reverse of the original bug and equally latency-inducing.

**Minimum fix inside the plan's own design:** drain all pending timer ticks in the outer select before entering the inner select, or use a `for { select { case <-ticker.C: …; default: break } }` drain loop so the inner select always runs after at most one flush. Alternatively, replace the prioritized-select idiom with a single `select` that calls `flushNow` in both the `ticker.C` arm and after every `IngestTick` call (check-after-ingest pattern), which is starvation-free in both directions.

`Blocked (B2)`

---

## 4. Non-Blocking Observations

1. `flushNow` logs via `log.Printf` on every flush error at 5ms cadence; at high error rates this is I/O on the hot path.
2. `prod.PublishTelemetry(0, watermarkBufferNS, 0, tickCount)` inside `flushNow` emits `AnchorPublishLatencyNS=0` — verify whether `0` is the correct sentinel or a latency-measurement gap.
3. Round 1 observation 5 (baseline `go test -race` status pre-change) remains un-dispositioned in §6.
4. Round 1 methodological note on half-open interval correctness of `flushTarget = nowNS - watermarkBufferNS` across both phase offsets is un-dispositioned — warrants a comment in code asserting `phaseNextAnchor[pIdx] <= flushTarget` precondition.
5. Round 1 observation 4 (`WatermarkBufferNS` liveness at flush time) is un-dispositioned; confirm the field is written before the loop enters.

---

## 5. Methodological & Data-Alignment Concerns

**Half-open interval / phase semantics:** The combined fix (`flushTarget = nowNS - watermarkBufferNS`) is correct for bug 2. For bug 1 (phase offset), correctness relies on `Projector.Flush` internally using `phaseNextAnchor[pIdx] <= targetEndNS` to gate each phase independently. The plan states this is the existing behavior but does not reproduce the predicate. If `Flush` internally also computes its own anchor from `targetEndNS` using integer arithmetic (e.g., `(targetEndNS / cadenceNS) * cadenceNS`), bug 1 would survive the fix. The plan should confirm `Flush`'s internal gate is phase-aware (does not re-derive an integer-stride anchor) — this is a single-line verification, not a structural objection.

---

## 6. Missing Telemetry

**Per-phase flush jitter histogram** (raised in Round 1 §6, un-dispositioned in §6 of the changelog): the delta between `phaseNextAnchor[pIdx]` and actual `nowNS` at `Flush` commit time, at p50/p99/max. Without it there is no production signal to confirm bugs 1 and 2 are resolved or to detect regression. This is the one metric that would definitively close the incident.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The prioritized-select idiom as written introduces a starvation inversion: the outer `select` + `continue` pattern drains `ticker.C` one event per loop iteration and restarts the outer check before serving any tick ingestion. Under burst conditions where `ticker.C` accumulates backlog, tick ingestion can be deferred for multiple flush cycles — the mirror image of bug 3 and equally capable of causing downstream lag. The fix is small (drain the timer channel fully before entering the inner select, or adopt a check-after-ingest pattern), but it must be stated in the plan before implementation.
