# Plan Review — aiChangeLog/2026-09-27/015_incremental_1hz_projection.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-28 04:07:00Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Three coordinated optimizations to `closePhase` latency:

1. **Incremental spread pre-computation + log fast-paths** (`window.go`): Cache `spreadBps` on tick arrival; skip `math.Log` when price is unchanged (returning `0.0` directly).
2. **Bulk SHM publication** (`producer.go`): Replace 72 serial `CommitSymbolMetrics` calls with a single `CommitPhaseFrame` — one `FrameHeader` write, one vectorized `copy()` for all features, then sequential atomic anchor stores.
3. **5ms watermark ticker** (`daemon.go`): Replace 250ms ticker to close frames promptly during quiet periods. Wire `--watermark-buffer` flag into telemetry.

Claimed result: `closePhase` latency drops from ~2,300 ns to ~351 ns (6.54×), validated by parity tests and a live A/B benchmark on `ccm-live-1`.

---

## 2. Simplest Sufficient Design

The goal is reducing `closePhase` latency, grounded in two root causes (90.91% of CPU): `math.Log` calls and 72 redundant `CommitSymbolMetrics` calls.

The plan is **near minimum** for what it claims. Each of the three components maps directly to a measured cost center. One tightening observation: the 5ms ticker + `--watermark-buffer` flag is a fourth change not attributed to any measured cost in the profile (the profile shows `closePhase` internals, not ticker latency). It is not strictly required to achieve the stated goal. However, it is small (daemon.go wiring only) and its correctness argument is self-contained. Not a blocking cut.

---

## 3. Blocking Defects

**B2 — SHM corruption risk in `CommitPhaseFrame` atomic ordering**

The plan describes `CommitPhaseFrame` as:
1. Write `FrameHeader` once (including `AnchorNS`).
2. `copy()` all features into SHM.
3. Write atomic release stores for symbol anchors sequentially.

The existing contract of `CommitSymbolMetrics` (which this replaces) presumably uses symbol anchor stores as the *visibility gate* for each symbol's features — a consumer polling symbol anchor `sIdx` uses the anchor's store-release as the signal that `features[sIdx*nFeat : (sIdx+1)*nFeat]` is ready. Under `CommitPhaseFrame`, the `FrameHeader.AnchorNS` is written **before** any symbol anchor. A concurrent C or Python consumer that polls `FrameHeader.AnchorNS` (rather than per-symbol anchors) to detect a new frame could observe the header and begin reading feature memory while the `copy()` is still in progress. The plan does not state whether downstream consumers gate on `FrameHeader.AnchorNS` or per-symbol anchors, nor does it establish that the `copy()` + sequential anchor writes are the *sole* visibility mechanism. If any consumer uses `FrameHeader.AnchorNS` as the read gate, the bulk copy creates a window of torn reads. The plan must either: (a) move `FrameHeader.AnchorNS` write to *after* the feature copy and all anchor stores, or (b) explicitly state and verify that no consumer gates on `FrameHeader.AnchorNS`.

**Class: B2 (shared memory corruption / torn read race).**

---

## 4. Non-Blocking Observations

1. The 76.6× speedup on `CloseBar` math (1,532 ns → 20 ns) is implausible if *any* symbol moves price — in a live feed, most 72 symbols will have non-zero returns most seconds; the 20 ns figure likely reflects the all-unchanged-price best case, not a representative workload.
2. `CommitPhaseFrame` retains `CommitFrameFinalizeWithLatency` as a separate call after the bulk commit — its interaction with the new ordering (header written first) should be explicitly stated in the plan.
3. The 5ms ticker fires 200 times/second; the plan's gate `if wallAnchor > lastFlushedAnchor` correctly suppresses 199 of 200 ticks, but the overhead arithmetic should confirm `lastFlushedAnchor` update is atomic-safe across the event loop.
4. `recalcSpread()` fallback `else { h.spreadBps = 1.0 }` differs from what the existing `CloseBar` uses when uninitialized — bitwise parity claim needs explicit test coverage for the uninitialized-price path.
5. The `--watermark-buffer` flag default of `"50ms"` is not reconciled against the existing hardcoded `WatermarkBufferNS` value used in the current binary; a silent default change could shift live behavior before the A/B baseline is captured.

---

## 5. Methodological & Data-Alignment Concerns

**ABI offset arithmetic in `CommitPhaseFrame`**: The plan states symbol anchors at `frameOffset + 64 + sIdx*8` and features at `frameOffset + 64 + nSym*8 + featIdx*8`. The GEMINI.md ABI spec confirms `FrameHeader` = 64 bytes, symbol anchors = `NumSymbols * 8` bytes. This is consistent. The `SymbolDirectoryEntry` (16 bytes) clarification — that it lives in the global header, not the frame — is correctly noted.

**Half-open interval / forward-fill / quote-before-trade**: No change to interval semantics. `CloseBar` slot indexing is unchanged. No concern.

**Cross-language ABI parity**: The vectorized `copy()` writes raw `float64` bytes; no endianness or padding concern for same-host C/Python consumers. Python `TickHubReader` opened `O_RDWR` for historical replay flow control is not affected by this change.

---

## 6. Missing Telemetry

The plan needs one additional metric to know the B2 fix (if applied) is correct in production: **a consumer-side torn-read counter** — if the Python or C reader ever observes a symbol anchor that is set but its corresponding feature slice contains values from a prior frame (detectable by cross-checking `FrameHeader.AnchorNS` vs. symbol anchor values), it should increment a counter. Without this, a rare torn-read race in production is silent.

---

## 7. Verdict

**Verdict: Blocked (B2)**

`CommitPhaseFrame` writes `FrameHeader.AnchorNS` first, then bulk-copies features, then writes per-symbol anchors. If any downstream consumer (C bridge, Python reader) uses `FrameHeader.AnchorNS` as the frame-ready signal — which is architecturally natural — it can race into partially-written feature memory. The plan does not establish the consumer's actual read-gate protocol, nor does it move the `AnchorNS` store to after the copy. Resolve by either (a) writing `FrameHeader.AnchorNS` last (after all anchor stores), or (b) auditing and explicitly documenting that no consumer polls `FrameHeader.AnchorNS` as a visibility gate. Once resolved, the rest of the plan is sound.
