# Plan Review — aiChangeLog/2026-09-30/001_decouple_tickhub_ingestion_loop.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-10-01 04:18:11Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

## 1. Summary of the Proposal

Plan resolves 2026-09-30 latency incident in `cmd/tickhub/daemon.go:320-357` via two targeted changes in daemon event loop:
1. Replaces integer-second stride `wallAnchor := (nowNS / cadenceNS) * cadenceNS` with continuous `flushTarget := nowNS - watermarkBufferNS`. Allows `projector.Flush` to independently trigger Phase 0 (0ms) and Phase 1 (500ms) anchor completions without 500ms latency floor, while applying configured 50ms watermark buffer.
2. Replaces Round 1 prioritized-select loop with check-after-ingest pattern: inside `case tick := <-feedMgr.Ticks():`, loop runs non-blocking `select` on `ticker.C` to service pending 5ms timer flush immediately after tick ingestion. When tick channel idles, main `select` blocks and services `case now := <-ticker.C:`.

Retains single-threaded `Projector` architecture without goroutines, channels, or mutexes.

---

## 2. Simplest Sufficient Design

Plan is at minimum sufficient design: fixes phase stride truncation, watermark buffer subtraction, and timer starvation within single event loop in ~30 lines in `cmd/tickhub/daemon.go`.

---

## 3. Blocking Defects

None.

*(Round 1 B2 starvation inversion verified resolved: outer prioritized select with `continue` removed; check-after-ingest drains `ticker.C` non-blockingly after tick ingestion without loop restart, guaranteeing bidirectional starvation freedom between tick ingestion and timer flushes.)*

---

## 4. Non-Blocking Observations

1. `flushNow` logs errors synchronously via `log.Printf` on hot path if `projector.Flush` fails.
2. `prod.PublishTelemetry(0, watermarkBufferNS, 0, tickCount)` passes literal `0` for latency and anchor; ensure sentinel values match reader expectations.
3. In check-after-ingest, `now` passed to `flushNow(now)` reflects `ticker.C` fire time rather than fresh `time.Now()`, introducing minor timestamp jitter bounded by tick processing duration.
4. Periodic 5-second diagnostic logging remains coupled to `flushNow` execution frequency.

---

## 5. Methodological & Data-Alignment Concerns

N/A. `flushTarget := nowNS - watermarkBufferNS` preserves half-open interval $[T-1\text{s}, T)$ semantics across independent phases; single-threaded execution preserves quote-before-trade tie-breaking and ASCII symbol ordering invariants.

---

## 6. Missing Telemetry

Per-phase flush jitter metric tracking delta between `phaseNextAnchor[pIdx]` and `nowNS` at `Flush` commit time to empirically verify elimination of Phase 1 500ms latency floor in production.

---

## 7. Verdict

**Verdict: Proceed**

The revision successfully eliminates the Round 1 B2 starvation inversion by adopting the check-after-ingest pattern, ensuring bidirectional progress between tick ingestion and the 5ms timer flush. The continuous `flushTarget` correctly resolves both the Phase 1 500ms stride truncation and watermark buffer subtraction without altering `Projector` invariants or introducing concurrency overhead. The plan is minimal, verifiable, and ready for implementation.
