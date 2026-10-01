# Code Review — aiChangeLog/2026-09-30/001_decouple_tickhub_ingestion_loop.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-10-01 04:20:09Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

`cmd/tickhub/daemon.go` lines 313–357 are replaced. The old code:
- Maintained `cadenceNS` (integer-second stride) and `lastFlushedAnchor`
- Computed `wallAnchor := (nowNS / cadenceNS) * cadenceNS` — truncating to integer seconds
- Guarded flush with `wallAnchor > lastFlushedAnchor`
- Used a single flat `select` where `ticker.C` and `feedMgr.Ticks()` competed equally, allowing tick bursts to defer the timer indefinitely

The new code:
- Introduces `flushNow(now time.Time)` closure capturing `timeOffsetNS`, `watermarkBufferNS`, `tickCount`, `projector`, `feedMgr`, `startTime`, `lastReport`
- Computes `flushTarget := nowNS - watermarkBufferNS` — continuous, no integer stride truncation, watermark buffer applied correctly
- Calls `projector.Flush(flushTarget)` unconditionally on each timer fire (projector internally checks `phaseNextAnchor[pIdx] <= targetEndNS`)
- Removes `cadenceNS` and `lastFlushedAnchor` variables (now dead after the change)
- Implements check-after-ingest: inside `case tick := <-feedMgr.Ticks():`, after `IngestTick`, executes a non-blocking inner `select { case now := <-ticker.C: flushNow(now); default: }` to drain any pending timer immediately
- Falls through to `case now := <-ticker.C: flushNow(now)` in the outer select when no tick arrives

## 2. Correctness & Concurrency Bugs

**No bugs found.**

- The entire loop is single-threaded; `flushNow` is a closure called only from this goroutine. No data races on `tickCount`, `lastReport`, `projector`, `prod`.
- `lastReport` captured by closure is a local variable in `runDaemon`, mutated only within `flushNow` — consistent.
- `ticker.C` is a buffered channel (capacity 1). The non-blocking inner drain is correct: at most one pending tick can exist; draining once per ingested tick is sufficient. Under sustained tick bursts where multiple 5ms intervals have elapsed between ticker drains, at most one timer event is buffered — this is correct Go ticker behavior (excess ticks are dropped). No accumulated backlog.
- `ctx.Done()` arm remains in outer select only; inner select has no `ctx.Done()` arm. This is correct: shutdown is not triggered inside the tick arm, so a single in-flight tick completes before shutdown is noticed on the next outer loop iteration. No concern.
- `projector.Flush(flushTarget)` error is now logged (`log.Printf`), whereas the old code silently discarded it (`_ = projector.Flush(...)`). This is strictly an improvement, not a regression.

## 3. Projection Math & Temporal Parity

**No violations.**

- `flushTarget := nowNS - watermarkBufferNS`: continuous, applies the 50ms buffer correctly. Projector receives `targetEndNS` and internally evaluates `phaseNextAnchor[pIdx] <= targetEndNS`. Phase 0 (0ms offset) and Phase 1 (500ms offset) both become eligible independently whenever their respective `phaseNextAnchor` crosses the watermark-buffered wall time. The 500ms latency floor from integer-second stride truncation is eliminated.
- Half-open interval `[T-1s, T)` semantics: `Flush` logic in `projector.go` is not touched; invariant preserved.
- Illiquid forward-fill, quote-before-trade tie-breaking, ASCII symbol ordering: all reside in `projector.go` which is unchanged. SSoT not violated.
- `timeOffsetNS` applied consistently (`nowNS := now.UnixNano() + timeOffsetNS`) — same as original code.

## 4. Deviations from the Approved Plan

**None.**

The ledger (§3) and plan review (Round 2 verdict) specify exactly:
1. Replace `wallAnchor := (nowNS / cadenceNS) * cadenceNS` with `flushTarget := nowNS - watermarkBufferNS` — ✓ done.
2. Replace outer prioritized-select with check-after-ingest non-blocking inner select — ✓ done.
3. Retain single-threaded contract, zero goroutines, zero mutexes — ✓ done.

The `lastFlushedAnchor` guard (`wallAnchor > lastFlushedAnchor`) is removed. This is correct and expected: `projector.Flush` is now idempotent across repeated calls with a monotonically increasing `flushTarget` because `phaseNextAnchor` advances internally. No deduplication guard is required in the caller.

## 5. Systems & Performance Violations

**None.**

- No heap allocations introduced: `flushNow` is a closure defined once before the loop; it captures local variables by reference. The closure itself does not escape to heap in steady-state (Go compiler inlines single-site closures called in the same goroutine scope without interface boxing). No new allocations per iteration.
- `flushTarget` is a stack `int64`. No boxing.
- `prod.PublishTelemetry(0, watermarkBufferNS, 0, tickCount)` passes sentinel `0` for latency/anchor args — consistent with plan review observation 2 (accepted residual; existing contract).
- Cache-line dual isolation (`GlobalHeader` producer line at offset 64, consumer line at offset 128): this change does not touch `pkg/shm`, `c/`, or Python ABI. No cache-line false sharing introduced.
- `feedMgr.FeedStatus()` called at most once per 5-second reporting interval inside `flushNow` — not on the hot path per iteration.
- `log.Printf` on Flush error: on the hot path only in failure cases; acceptable.

## 6. Telemetry / Verification Gaps

**Minor gap (non-blocking, consistent with plan review):**

- Plan review §6 noted a missing per-phase flush jitter metric (delta between `phaseNextAnchor[pIdx]` and `nowNS` at commit time). The implementation does not add this metric. This was flagged as a non-blocking observation in the plan review; it is not a remediation requirement for the code review gate.
- `prod.PublishTelemetry(0, watermarkBufferNS, 0, tickCount)` passes `0` for `AnchorPublishLatencyNS` and `LastWrittenAnchorNS`. These sentinel zeros are consistent with the pre-existing call pattern and do not break downstream readers that interpret zero as "not yet committed."

## 7. Verdict & Remediation

The implementation faithfully executes the approved plan with no correctness defects, no concurrency issues, no projection math violations, no ABI/shm concerns, and no deviations from the ledger or plan review. The change is minimal, surgical, and strictly improves the error-handling posture (Flush errors now logged vs. silently dropped).

**APPROVED — no remediation required.**
