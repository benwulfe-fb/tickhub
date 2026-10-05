# Plan Review — aiChangeLog/2026-10-05/001_tickhub_recorder_and_playback.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-10-05 23:34:19Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

## 1. Summary of the Proposal

Plan adds 1Hz telemetry recorder ([`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder), [`cmd/tickhub/record.go`](file:///mnt/wc/tickhub/cmd/tickhub/record.go)) and playback engine ([`pkg/playback`](file:///mnt/wc/tickhub/pkg/playback), [`cmd/tickhub/playback.go`](file:///mnt/wc/tickhub/cmd/tickhub/playback.go)). Recorder attaches to local POSIX SHM as consumer, captures canonical [`SymbolSnapshot`](file:///mnt/wc/tickhub/pkg/shm/types.go) via SeqLock atomic checks and ring buffer phase features via store-release ordering, embeds SHM configuration YAML into Parquet metadata, and flushes Snappy-compressed Parquet files ([`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go)). Playback reads Parquet, recovers configuration YAML to initialize [`shm.CreateProducer`](file:///mnt/wc/tickhub/pkg/shm) in `ModeHistoricalReplay`, writes snapshots/frames, and paces stream via `-speed` with consumer flow control synchronization via `WaitConsumerAdvance`.

---

## 2. Simplest Sufficient Design

Plan is at minimum sufficient scope after Round 1 cuts: redundant TCP relay ingestion dropped, secondary metric math / fallback history dropped, canonical snapshot + raw feature tensor preserved, and embedded SHM configuration metadata handles ABI layout reconstruction.

---

## 3. Blocking Defects

None.

---

## 4. Non-Blocking Observations

1. Dynamic slice allocations for `Features []float64` in [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) cause heap churn; use buffer pooling or pre-allocated row batches in [`pkg/recorder/recorder.go`](file:///mnt/wc/tickhub/pkg/recorder/recorder.go).
2. Verify scalar metric fields (`SpreadBPS`, `Imbalance`, `Microprice`, `LogRet*`) against 128-byte [`SymbolSnapshot`](file:///mnt/wc/tickhub/pkg/shm/types.go) ABI to prevent zero-filling fields absent from top-of-book snapshot.
3. Validate timeout handling in [`pkg/playback/playback.go`](file:///mnt/wc/tickhub/pkg/playback/playback.go) when downstream consumer crashes or stalls on `WaitConsumerAdvance`.
4. Ensure multi-symbol rows in [`pkg/playback/playback.go`](file:///mnt/wc/tickhub/pkg/playback/playback.go) group strictly by `TimestampNS` before committing frame headers.
5. Signal handler in [`cmd/tickhub/record.go`](file:///mnt/wc/tickhub/cmd/tickhub/record.go) should use context timeout to prevent hanging on blocked disk flush during shutdown.

---

## 5. Methodological & Data-Alignment Concerns

- **Dual Cache-Line Isolation:** Replay producer interacts with consumer cache line (offset 128) strictly via `WaitConsumerAdvance(anchorNS, timeout)` read-check, preserving isolation from producer cache line (offset 64).
- **1Hz Projection SSoT:** All bespoke metric recalculations eliminated from recorder; engine output captured verbatim.
- **Cross-Language ABI Parity:** Embedding `tickhub_config_yaml` in Parquet metadata ensures playback instantiates identical phase layouts, frame strides, and directory offsets across Go, C, and Python.

---

## 6. Missing Telemetry

None. Plan instruments `AnchorLagNS`, `DroppedFrames`, `FlowControlWaitNS`, and row counters in §4.

---

## 7. Verdict

**Verdict: Proceed-with-noted-risks**

Round 1 blocking defects resolved cleanly: redundant TCP networking and duplicate metric math removed, lossy schema replaced with canonical snapshot state plus raw feature tensors and embedded configuration metadata, and playback buffer overruns eliminated via `WaitConsumerAdvance` flow control. Author can proceed to implementation, addressing heap allocation efficiency and snapshot schema alignment during implementation and code audit.
