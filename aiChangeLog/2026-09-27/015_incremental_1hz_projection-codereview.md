# Code Review — aiChangeLog/2026-09-27/015_incremental_1hz_projection.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-28 04:16:06Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

Four files modified, one added:

- **`pkg/project/window.go`**: Adds `spreadBps float64` field to `SymbolHistory`. Adds `recalcSpread()` helper that uses `h.lastPrice` (with 100.0 fallback when `<= 0`) as denominator, matching the old `CloseBar` denominator exactly. Calls `recalcSpread()` from `UpdateQuote`, `UpdateTrade` (price-valid branch only), and `SeedFromBars`/`SeedPrevailingPrice` seeders. In `CloseBar`, replaces the live spread computation with `h.spreadBps` read (with a `<= 0` guard fallback). Adds three `if px == pN { 0.0 } else { math.Log(...) }` fast-path branches for the three log-return features.

- **`pkg/shm/producer.go`**: Adds `CommitPhaseFrame(phaseIdx, anchorNS, features)`. Validates phase index, gates on `WaitConsumerAdvance` for phase 0 (replay mode). Computes slot via `(anchorNS / cadenceNS) & (MaxFrames-1)` and frame offset. Writes FrameHeader metadata (not AnchorNS) in Stage 1, bulk-copies feature floats via `unsafe.Slice` + `copy` in Stage 2, stores per-symbol anchors atomically in Stage 3, stores `FrameHeader.AnchorNS` atomically last in Stage 4. Does **not** call `CommitFrameFinalizeWithLatency` itself — that remains the caller's responsibility.

- **`pkg/project/projector.go`**: Adds `phaseFeatBuffers [][]float64` pre-allocated in `NewProjector()` (one slice per phase, `nSymbols × nFeat` floats). `NewProjector` reads `NumFeatures` from SHM header when available, falls back to 5. `closePhase` now populates `phaseFeatBuffers[pIdx]` in-loop then calls `CommitPhaseFrame` once, replacing 72 per-symbol `CommitSymbolMetrics` calls.

- **`cmd/tickhub/daemon.go`**: Adds `--watermark-buffer` flag (default `"50ms"`, parses to int64 ns, fatal on parse error). Reduces ticker from 250ms to 5ms. Passes `watermarkBufferNS` to `prod.PublishTelemetry(0, watermarkBufferNS, 0, tickCount)`.

- **`pkg/project/projector_test.go`**: Adds `TestProjectorBulkCommitAndZeroAlloc` — verifies sym0 return `ln(105/100)`, sym1 return `0.0`, all 72 symbol anchors, and `testing.AllocsPerRun(1000, ...)` == 0 for `closePhase`.

- **`pkg/shm/producer_test.go`**: Adds `TestCommitPhaseFrameBulk` — verifies FrameHeader fields, per-symbol anchors, and feature values; then runs a parallel `CommitSymbolMetrics` write and compares header/anchor/feature values field-by-field.

- **`pkg/project/parity_test.go`** (new): `TestBitwiseParityIncrementalVsPriorCloseBar` (5,000 randomized bars, bit-exact IEEE-754 comparison against `PriorReferenceSymbolHistory`), `TestBitwiseParitySpecificEdgeCases` (4 edge cases), `TestBitwiseParitySHMFrameBulkVsSerial` (72-symbol raw-byte comparison of bulk vs serial writes), `TestBitwiseParityProjectorEndToEnd` (multi-phase frame header/anchor validation).

---

## 2. Correctness & Concurrency Bugs

**No blocking defects found.** The following are observations only:

### 2a. `recalcSpread()` denominator parity — VERIFIED CORRECT
The ledger states `recalcSpread()` uses `h.lastPrice`. In the diff, `recalcSpread()` reads `px := h.lastPrice` and applies a `100.0` fallback when `px <= 0`, which is the same 100.0 fallback used in `CloseBar`'s `px` initialization. The formula `((lastAsk - lastBid) / px) * 10000.0` matches the old `CloseBar` spread formula with the same `px`. Bit-for-bit parity holds.

### 2b. `recalcSpread()` called in `CloseBar` when `h.spreadBps <= 0` — corner case handled
The `spreadBps <= 0` guard in `CloseBar` re-invokes `recalcSpread()` as a late safety net. The condition can only fire if `spreadBps` is 0.0 (zero-value struct field at cold start before any tick) or if `recalcSpread` itself wrote 0.0, which cannot happen since the minimum is 1.0. This guard is a correct and safe defense-in-depth without introducing divergence.

### 2c. `CommitPhaseFrame` — `WaitConsumerAdvance` conditioned only on `phaseIdx == 0`
The ledger says "In replay mode (phaseIdx == 0), enforces flow control." The implementation checks `if phaseIdx == 0` without additionally checking `ModeHistoricalReplay`. This matches the existing `CommitSymbolMetrics` pattern (which also applies flow control on phase 0 unconditionally), so no regression.

### 2d. `featOffset` arithmetic — VERIFIED CORRECT
`featOffset = frameOffset + 64 + nSym*8`. With `nSym = 72`, anchor array = 576 bytes. Total header + anchors = 640 bytes. The plan review confirmed this is 64-byte cache-line aligned and 8-byte float64 aligned. The `unsafe.Slice` slices `totalFloats = nSym * nFeat` float64 values starting at `featOffset`. No buffer overrun: the `len(features) >= int(totalFloats)` guard in Stage 2 protects against under-sized input.

### 2e. `CommitFrameFinalizeWithLatency` not called inside `CommitPhaseFrame` — CORRECT
The ledger requires Stage 5 (`CommitFrameFinalizeWithLatency`) to be called by `closePhase` after `CommitPhaseFrame` returns. `closePhase` already calls `p.producer.CommitFrameFinalizeWithLatency(anchorNS, publishLatNS)` at lines following the new `CommitPhaseFrame` call (the existing cold-start/latency block is preserved). Correct separation of concerns.

### 2f. `curAnchor` captured by value in `AllocsPerRun` closure — correct
The zero-alloc test increments `curAnchor` inside the closure with `curAnchor += 1_000_000_000`. Since `curAnchor` is a stack `int64` captured by reference, this is correct and does not cause heap escapes.

---

## 3. Projection Math & Temporal Parity

**All 1Hz invariants preserved:**

- **Half-open interval** `[T-1s, T)`: `frameHdr.StartTimestampNS = anchorNS - cadenceNS`, `EndTimestampNS = anchorNS`. Unchanged from `CommitSymbolMetrics`. ✓

- **Illiquid forward-fill**: `CloseBar` uses `h.lastPrice` which persists from last known state when no trades arrive. `recalcSpread()` similarly reuses `h.lastBid`/`h.lastAsk`. No change to this semantics. ✓

- **Quote-before-trade tie-breaking**: not changed — still governed by the event loop's tick ordering. ✓

- **Log-return fast path**: `math.Log(1.0) == 0.0` in IEEE 754, so `if px == p1 { 0.0 }` is bitwise identical. Confirmed by `PriorReferenceSymbolHistory` reference test. ✓

- **Slot indexing modulo `MaxFrames`**: `slot = (anchorNS / cadenceNS) & (MaxFrames-1)` — identical to `CommitSymbolMetrics` slot computation. ✓

- **Feature ordering** `[r1, r5, r15, vol, spreadBps]` at `base = sIdx * 5`: matches downstream consumer layout (symbol-major, 5 features/symbol). ✓

- **`TestBitwiseParitySHMFrameBulkVsSerial`** does a raw `bytes.Equal` comparison of the entire frame stride for 32 bars × 72 symbols. This is a comprehensive parity gate. ✓

---

## 4. Deviations from the Approved Plan

No material deviations found:

| Plan Item | Implemented? | Notes |
|---|---|---|
| `spreadBps` field + `recalcSpread()` | ✓ | Exact per spec |
| `recalcSpread()` in `UpdateQuote`, `UpdateTrade`, seeders | ✓ | All 4 call sites |
| Log-return fast-paths (`px==pN → 0.0`) | ✓ | All 3 returns |
| `phaseFeatBuffers` pre-allocated in `NewProjector` | ✓ | Per-phase, `nSym*nFeat` |
| `CommitPhaseFrame` 5-stage ordering | ✓ | Stages 1–4 in producer, Stage 5 in projector |
| `closePhase` populates buffer, calls `CommitPhaseFrame` | ✓ | Loop separated from commit |
| `--watermark-buffer` flag, default 50ms | ✓ | Fatal on parse error |
| Ticker 250ms → 5ms | ✓ | |
| `PublishTelemetry(0, watermarkBufferNS, 0, tickCount)` | ✓ | |
| `TestProjectorBulkCommitAndZeroAlloc` | ✓ | AllocsPerRun asserted |
| `TestCommitPhaseFrameBulk` parity with `CommitSymbolMetrics` | ✓ | Field-by-field comparison |
| `TestBitwiseParityIncrementalVsPriorCloseBar` 5,000 bars | ✓ | Bit-exact 5-feature |
| `TestBitwiseParitySpecificEdgeCases` 4 cases | ✓ | All 4 named cases |
| `TestBitwiseParitySHMFrameBulkVsSerial` | ✓ | `bytes.Equal` raw frame |
| `TestBitwiseParityProjectorEndToEnd` | ✓ | Multi-phase, anchors validated |

One minor deviation from plan language: `TestCommitPhaseFrameBulk` does not perform a full raw `bytes.Equal` bulk-vs-serial frame comparison — it compares header fields and feature values individually rather than as raw memory. This is functionally sufficient (all addressable fields are verified) and is covered by `TestBitwiseParitySHMFrameBulkVsSerial` in `parity_test.go`. Not a blocking issue.

---

## 5. Systems & Performance Violations

None found.

- **Zero-allocation hot path**: `phaseFeatBuffers[pIdx]` is pre-allocated; `featBuf := p.phaseFeatBuffers[pIdx]` is a slice header copy (stack only). `CommitPhaseFrame` uses `unsafe.Slice` over existing SHM memory; no heap allocations inside. `testing.AllocsPerRun(1000, ...)` enforces this at test time. ✓

- **Cache-line isolation**: `CommitPhaseFrame` writes only to phase ring buffer memory (frame data). No writes to `GlobalHeader` producer cache line (offset 64) or consumer cache line (offset 128) inside Stage 1–4. Stage 5 (`CommitFrameFinalizeWithLatency`) writes `LastWrittenAnchorNS` at offset 128 + `AnchorPublishLatencyNS` at offset 64 — same as before, no regression. ✓

- **`unsafe.Slice` alignment**: `featOffset = frameOffset + 64 + nSym*8`. With `nSym=72`, `featOffset` is `640` bytes into the frame ring. Both 64 and 640 are multiples of 8, so `float64` alignment is satisfied on all target architectures. ✓

- **Ticker frequency 5ms**: 200 ticks/second overhead is minimal; the `wallAnchor > lastFlushedAnchor` gate ensures 199 of 200 ticks exit in O(1). Acknowledged in ledger and plan review observation #5. ✓

- **`recalcSpread()` on every tick**: O(1) arithmetic; no allocation. Plan review observation #1 flags this as a tradeoff; headroom for bursts is the operator's concern, not a code defect. ✓

---

## 6. Telemetry / Verification Gaps

- **Pre-deploy baseline capture on `ccm-live-1`** and **A/B benchmark** (`run_ab_benchmark.sh`) are live-environment steps outside the scope of this code review; they remain in the Verification Plan checklist as post-commit gates. No gap in the code itself.
- **`WatermarkBufferNS` SHM observability** (Prometheus `tickhub_watermark_buffer_seconds`): `PublishTelemetry(0, watermarkBufferNS, ...)` correctly wires the value into `WatermarkBufferNS` in `GlobalHeader`. The Prometheus scrape of this field is handled by existing telemetry infrastructure, not part of this diff. No gap introduced.
- `TestCommitPhaseFrameBulk` verifies `hdr.AnchorNS == anchor` (Stage 4 release) only *after* `CommitFrameFinalize`, so it does not explicitly verify that Stage 4 occurs before Stage 5. However, `CommitPhaseFrame` unconditionally executes `atomic.StoreInt64(&frameHdr.AnchorNS, anchorNS)` before returning, and the caller invokes `CommitFrameFinalize` only after `CommitPhaseFrame` returns. The ordering is structurally enforced, not just by test. ✓

---

## 7. Verdict & Remediation

**APPROVED.** No blocking defects. All five invariants (half-open interval, illiquid forward-fill, quote-before-trade, zero-allocation hot path, dual cache-line isolation) are preserved. Bitwise parity is enforced by comprehensive test suite. Memory ordering is structurally correct (Stage 1–4 sequential single-goroutine writes with atomic release stores in the required order). Plan deviations are absent. The change is ready for commit.
