# Plan Review — aiChangeLog/2026-10-05/001_tickhub_recorder_and_playback.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-10-05 23:33:24Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

## 1. Summary of the Proposal

Plan introduces 1Hz telemetry recorder ([`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder), [`cmd/tickhub/record.go`](file:///mnt/wc/tickhub/cmd/tickhub/record.go)) and playback engine ([`pkg/playback`](file:///mnt/wc/tickhub/pkg/playback), [`cmd/tickhub/playback.go`](file:///mnt/wc/tickhub/cmd/tickhub/playback.go)). Recorder reads 1Hz snapshots from POSIX SHM or TCP relay (`-relay-addr`), calculates/formats metrics into flat Parquet schema ([`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) via `github.com/parquet-go/parquet-go`), and flushes Snappy-compressed files to disk. Playback reads Parquet, reconstructs 1Hz anchor frames, acts as SHM producer ([`shm.CreateProducer`](file:///mnt/wc/tickhub/pkg/shm)), and publishes snapshots and frame features into SHM with speed throttling or unthrottled mode (`-speed 0`).

---

## 2. Simplest Sufficient Design

Stated goal: persist in-flight 1Hz projections to Parquet for post-mortem analysis and replay them to POSIX SHM for ML model testing.

Smallest sufficient design:
1. Recorder attaches to local POSIX SHM as consumer. At each anchor commit, dump canonical [`SymbolSnapshot`](file:///mnt/wc/tickhub/pkg/shm/types.go) records and frame feature tensors directly to Parquet. Zero metric recalculation.
2. Playback reads serialized records, formats SHM producer, and streams frames respecting existing flow control ([`WaitConsumerAdvance`](file:///mnt/wc/tickhub/pkg/shm)).
3. Use existing `tickhub relay-client` to populate local SHM if recording from remote relay.

**Cut List:**
- **Cut TCP relay ingestion (`-relay-addr`) from [`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder):** [`cmd/tickhub/relay_client.go`](file:///mnt/wc/tickhub/cmd/tickhub/relay_client.go) already replicates network streams to local SHM. Redundant network client code in recorder adds zero capability and violates SRP.
- **Cut metric recalculation (`spread_bps`, `imbalance`, `microprice`) and fallback rolling [`project.SymbolHistory`](file:///mnt/wc/tickhub/pkg/project/projector.go) from [`pkg/recorder/recorder.go`](file:///mnt/wc/tickhub/pkg/recorder/recorder.go):** [`pkg/project/projector.go`](file:///mnt/wc/tickhub/pkg/project/projector.go) is SSoT. Recorder must dump engine output, not invent secondary calculation path.
- **Cut ad-hoc [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) as playback source:** Lossy 18-column flat schema cannot reconstruct canonical SHM segment state.

---

## 3. Blocking Defects

### B1 (Goal-Defeating): Lossy [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) Schema Cannot Reconstruct SHM for ML Model Testing
- **Grounding:** Plan Section 1 states goal is "testing new ML models" by replaying 1Hz streams "back into POSIX SHM segments". Section 3 defines [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) containing only 18 scalar fields (`Mid`, `Bid`, `Ask`, `Spread`, `SpreadBPS`, `BidSZ`, `AskSZ`, `Imbalance`, `Microprice`, `LastTradePX`, `LastTradeSZ`, `Vol1S`, `LogRet1S`, `LogRet5S`, `LogRet15S`, `Seq`). Section 4 states [`pkg/playback/playback.go`](file:///mnt/wc/tickhub/pkg/playback/playback.go) "writes `SymbolSnapshot` records with SeqLock, and commits frame features."
- **Failure:** TickHub SHM ABI defines 128-byte [`SymbolSnapshot`](file:///mnt/wc/tickhub/pkg/shm/types.go), 64-byte [`FrameHeader`](file:///mnt/wc/tickhub/pkg/shm/types.go), 32-byte [`PhaseInfo`](file:///mnt/wc/tickhub/pkg/shm/types.go), and rolling phase feature matrices consumed by downstream Python ML dataloaders (`python/tickhub/`). [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) discards snapshot metadata, phase descriptors, and arbitrary feature tensors, preserving only 5 hardcoded return/spread metrics. Playback cannot reconstruct canonical SHM frames or feature tensors from [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go). Downstream ML models reading SHM will read empty, corrupted, or fabricated feature buffers. Stated goal defeated.

### B2 (Loss / Concurrency): Unthrottled Playback Lacks Consumer Flow Control
- **Grounding:** Plan Section 1 & Section 4 define playback speed `-speed 0` (unthrottled) and `-speed > 1.0` writing to SHM via [`shm.CreateProducer`](file:///mnt/wc/tickhub/pkg/shm).
- **Failure:** TickHub SHM architecture requires consumer flow control during historical replay (`GEMINI.md` Invariant 3: `ModeHistoricalReplay` uses `WaitConsumerAdvance` on consumer cache-line offset 128 `LastReadAnchorNS` to prevent producer from lapping consumer). Plan defines only sleep-based pacing on `-speed` and zero coordination with `LastReadAnchorNS`. At `speed=0`, playback loops at disk/CPU speed, immediately lapping attached Python/C consumers, overwriting unread ring buffer frames, and corrupting SHM reads.

### B1 (Goal-Defeating / Invariant Violation): Re-Implementing 1Hz Feature Math Violates SSoT
- **Grounding:** Plan Section 3 defines bespoke formulas for `spread_bps`, `imbalance`, `microprice`. Section 4 specifies [`pkg/recorder/recorder.go`](file:///mnt/wc/tickhub/pkg/recorder/recorder.go) has "fallback to rolling `project.SymbolHistory`" and [`pkg/recorder/recorder_test.go`](file:///mnt/wc/tickhub/pkg/recorder/recorder_test.go) tests "metric calculation correctness (imbalance, microprice, spread_bps)".
- **Failure:** Violates SSoT (`GEMINI.md` Invariant 1: all 1Hz feature calculation and rolling window math live *strictly* in [`pkg/project/projector.go`](file:///mnt/wc/tickhub/pkg/project/projector.go); downstream consumers *never* duplicate math). Recalculating metrics and rolling returns inside [`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder) duplicates logic, risks divergence on tie-breaks/half-open intervals, produces inconsistent data from canonical engine projections, and leaves fallback history unprimed on mid-stream attachment.

### B4 (Unnecessary Complexity): Redundant Network Ingestion in Recorder
- **Grounding:** Plan Section 2 & Section 4 add TCP relay endpoint ingestion (`-relay-addr`) and data source abstraction to [`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder).
- **Failure:** [`cmd/tickhub/relay_client.go`](file:///mnt/wc/tickhub/cmd/tickhub/relay_client.go) and `pkg/relay` already receive network streams and write to local POSIX SHM. Duplicating TCP network client logic into [`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder) adds unnecessary networking abstractions, connection lifecycle management, and error handling to a component whose single responsibility is disk serialization.

---

## 4. Non-Blocking Observations

1. Profile `github.com/parquet-go/parquet-go` heap allocations during batch writes against zero-allocation hot-path rules.
2. Parquet row ordering across multi-symbol anchors must be strictly chronological by `ts_ns` to avoid reader re-sorting.
3. Signal handling on SIGINT/SIGTERM in [`cmd/tickhub/record.go`](file:///mnt/wc/tickhub/cmd/tickhub/record.go) needs timeout bounds so blocked disk flush cannot hang shutdown.
4. Dynamic symbol discovery in [`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder) must handle mid-day symbol directory additions without dropping frames.
5. Snappy compression level and row group size should be exposed in `Config` for memory-constrained replay environments.

---

## 5. Methodological & Data-Alignment Concerns

- **SHM Dual Cache-Line Isolation:** Plan omits consumer cache-line (offset 128) handling. Replay producer must synchronize with `LastReadAnchorNS` to support flow-controlled replay.
- **1Hz Projection Semantics:** Recalculating metrics inside recorder bypasses engine tie-breaking (quote-before-trade, ASCII symbol order) and illiquid forward-fill semantics established in [`pkg/project/projector.go`](file:///mnt/wc/tickhub/pkg/project/projector.go).
- **Cross-Language ABI Parity:** [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) does not mirror C/Python ABI struct layouts ([`SymbolSnapshot`](file:///mnt/wc/tickhub/pkg/shm/types.go) 128 bytes, [`FrameHeader`](file:///mnt/wc/tickhub/pkg/shm/types.go) 64 bytes). Downstream consumers expecting standard SHM layout cannot consume playback streams generated from this truncated schema.

---

## 6. Missing Telemetry

- **Flush lag:** Delta between SHM `LastWrittenAnchorNS` and recorder's last flushed anchor timestamp (`AnchorLagNS`).
- **Dropped anchor frames:** Counter tracking frames dropped when Parquet serialization or disk I/O blocks buffer flush.
- **Consumer flow control wait:** Nanoseconds spent by playback engine blocked on `WaitConsumerAdvance`.

---

## 7. Verdict

**Verdict: Blocked (B1)**

Plan fails on core goal: [`MetricRow`](file:///mnt/wc/tickhub/pkg/recorder/types.go) is a lossy, hand-rolled 18-column table that discards full [`SymbolSnapshot`](file:///mnt/wc/tickhub/pkg/shm/types.go) fields, phase definitions, and feature matrices, making true SHM playback for downstream ML models impossible. In addition, plan violates SSoT by re-implementing 1Hz metric formulas in [`pkg/recorder`](file:///mnt/wc/tickhub/pkg/recorder), lacks consumer flow control (`WaitConsumerAdvance`) in playback causing silent buffer overwrites at `speed=0`, and introduces redundant TCP relay networking. Must redesign to dump and replay canonical SHM frame/snapshot layouts directly.
