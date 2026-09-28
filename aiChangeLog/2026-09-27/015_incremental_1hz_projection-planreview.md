# Plan Review — aiChangeLog/2026-09-27/015_incremental_1hz_projection.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-28 04:08:18Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

## 1. Summary of the Proposal

Plan optimizes 1Hz `closePhase` latency across 72 symbols via four changes:
- `pkg/project/window.go`: Cache `spreadBps` on tick arrival (`UpdateQuote`, `UpdateTrade`, seeders); fast-path zero-movement return calculations (`px == pN -> 0.0`) avoiding `math.Log(1.0)`.
- `pkg/shm/producer.go`: Add `CommitPhaseFrame` replacing 72 sequential `CommitSymbolMetrics` calls with single bulk commit using strict store-release ordering (metadata $\to$ feature slice copy $\to$ symbol anchors $\to$ `FrameHeader.AnchorNS` $\to$ `GlobalHeader.LastWrittenAnchorNS`).
- `pkg/project/projector.go`: Pre-allocate `phaseFeatBuffers` in `NewProjector()` for zero-allocation hot path; dispatch bulk frames to `CommitPhaseFrame`.
- `cmd/tickhub/daemon.go`: Reduce flush ticker from 250ms to 5ms for prompt frame boundary closure; wire `--watermark-buffer` flag into telemetry.

Target outcome: `closePhase` execution drops from ~2,300 ns to 351 ns (quiet) / 1,121 ns (50% active), zero heap allocations, 100% bitwise parity preserved.

---

## 2. Simplest Sufficient Design

The plan directly targets the two profiled bottlenecks accounting for 90.91% CPU (`math.archLog` 54.55%, `CommitSymbolMetrics` 36.36%). The bulk commit and fast-path return logic introduce no unnecessary abstractions or state. The design is at the minimum necessary complexity to solve the stated problem.

---

## 3. Blocking Defects

None.

*(Round 1 B2 Remediation Verification)*: The prior B2 torn-read defect is verified resolved. The store-release pipeline in `CommitPhaseFrame` explicitly stages writes such that `FrameHeader.AnchorNS` is committed via `atomic.StoreInt64` in Stage 4 *after* Stage 2 vectorized feature `copy()` and Stage 3 per-symbol anchor release stores. Consumers polling either symbol anchors or `FrameHeader.AnchorNS` cannot observe partially written feature memory.

---

## 4. Non-Blocking Observations

1. Calling `recalcSpread()` on every quote/trade arrival trades ingestion CPU for boundary latency; verify tick ingestion headroom during peak bursts.
2. Contiguous `features` slice layout in `phaseFeatBuffers` assumes symbol-major ordering (`sIdx * nFeat + fIdx`) matches downstream consumer layout.
3. Live speedup under active market churn will track the measured 2.05× (1,121 ns) rather than 6.54× (351 ns) quiet baseline.
4. Cold-start bar with uninitialized quotes relies on `h.spreadBps = 1.0` fallback matching `CloseBar` behavior.
5. Intermediate 5ms ticker ticks evaluate `wallAnchor > lastFlushedAnchor` safely inside single-goroutine event loop.

---

## 5. Methodological & Data-Alignment Concerns

- **SHM Layout & Alignment**: `FrameHeader` (64 bytes) + Symbol Anchors ($72 \times 8 = 576$ bytes) = 640 bytes. Features array offset 640 is an exact multiple of 64 bytes (cache-line aligned) and 8 bytes (`float64` aligned).
- **Projection Invariants**: Half-open interval $[T-1\text{s}, T)$, illiquid forward-fill, quote-before-trade, and modulo-32 slot indexing remain strictly preserved.
- **Cross-Language ABI Parity**: Binary layout matches C bridge and Python `TickHubReader` without struct changes; release store sequence preserves visibility contract across languages.

---

## 6. Missing Telemetry

None.

---

## 7. Verdict

**Verdict: Proceed**

The plan resolves the prior Round 1 B2 memory ordering defect by enforcing strict multi-stage release stores (`features` $\to$ symbol anchors $\to$ `FrameHeader.AnchorNS` $\to$ `GlobalHeader`). Quantitative attribution is reconciled against empirical pprof profiles, zero-allocation constraints are preserved, and ABI layout parity is intact. The plan is sound and ready for implementation.
