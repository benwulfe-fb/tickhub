# Code Review — aiChangeLog/2026-09-24/006_massive_ws_feed_and_tickhub_relay.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Verdict (parsed): APPROVED · Generated: 2026-09-24 19:09:17Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

Change set delivers live WebSocket market data ingestion and cross-machine TCP shared memory replication for `tickhub`:
- `pkg/feed/massive_ws.go`: WebSocket client connecting to `wss://socket.massive.com/stocks`. Handles authentication (`action="auth"`), explicit universe formatting (`FormatSubscription`), and JSON decoding (`ParseMassiveEvents`) mapping `ev=Q` (NBBO) and `ev=T` (trades) into `feed.Tick`. SIP timestamps in milliseconds multiplied by $1,000,000$ (`NSPerMS`). Reconnection uses capped exponential backoff with random jitter.
- `pkg/relay/protocol.go`: Binary wire protocol (`THRL`, magic `0x5448524C`, version 1). Defines packet encoding and decoding for `MsgTypeHandshake` (`0x01`), `MsgTypeSnapshot` (`0x02`), `MsgTypeFrameSlot` (`0x03`), and `MsgTypeAnchor` (`0x04`). `WritePacket` writes stack-allocated 13-byte header followed by payload.
- `pkg/relay/server.go`: Attaches to source SHM segment via `shm.AttachSegment`. Implements causal streaming: on client connection, transmits initial baseline snapshots for all populated symbols (`isFirstClientAnchor`), then streams per-anchor updates ($[nextAnchor - \text{cadenceNS}, nextAnchor)$), followed by raw ring-buffer slot bytes (`MsgTypeFrameSlot`) for each phase, culminating in `MsgTypeAnchor`. Uses bounded 1,000-spin SeqLock read (`readSnapshotSeqLock`) with best-effort copy fallback.
- `pkg/relay/client.go`: Connects to `RelayServer` via TCP. Decodes handshake, instantiates replica SHM producer (`shm.CreateProducer`), memcopies raw frame slots (`WriteRawSlot`), applies SeqLock snapshots (`WriteSnapshot`), and commits anchors (`CommitFrameFinalize`). Implements reconnection loop with backoff, tracking sequence gaps (`seqGaps`) and reconnect counts (`reconnects`).
- `pkg/shm/producer.go`: Added `WriteRawSlot` (validating phase index, `slotIdx < MaxFrames`, and `len(rawBytes) == stride`), `ReadRawSlot` (returning defensive copy), and `Segment()` accessor.
- `cmd/tickhub/`: Added `daemon`, `relay-server`, and `relay-client` CLI subcommands; added `--shm-name` override to `replay`.
- `tests/test_relay_parity.py`: End-to-end integration test verifying 100.000% bitwise parity on 1Hz metric frames and snapshots between primary and replica SHM segments against golden reference, verifying throughput within $\le 500\,\mu\text{s/bar}$.

---

## 2. Correctness & Concurrency Bugs

All Round 2 defects verified resolved in current diff; no new blocking defects identified.

### Verification of Prior Remediations
1. **B1 (`Close()` / `readPump` channel panic race resolved)**:
   - `pkg/feed/massive_ws.go:804`: `wg sync.WaitGroup` added to struct.
   - `pkg/feed/massive_ws.go:904–908`: `Start()` increments `c.wg.Add(1)` and wraps `readPump(ctx)` in goroutine with `defer c.wg.Done()`.
   - `pkg/feed/massive_ws.go:913–923`: Channel closure moved exclusively to `readPump` defer block (`c.chanOnce.Do(func() { close(c.tickChan) })`).
   - `pkg/feed/massive_ws.go:1015–1031`: `Close()` closes `c.stopChan`, closes network connection, and awaits `c.wg.Wait()` before exiting. Eliminates concurrent send on closed channel.
2. **B2 (Snapshot baseline causal filter resolved)**:
   - `pkg/relay/server.go:2193, 2219–2231`: Session-scoped boolean `isFirstClientAnchor` tracked per connection. On initial anchor, server transmits all snapshots where `scratchSnap.SeqLockSeq > 0`, ensuring quiescent symbols receive top-of-book baseline state immediately upon replica connection. Subsequent anchors correctly enforce $[nextAnchor - \text{cadenceNS}, nextAnchor)$ window filter.
3. **B3 (Unbounded spin in `readSnapshotSeqLock` resolved)**:
   - `pkg/relay/server.go:2323–2339`: Spin loop bounded to `const maxSpins = 1000` iterations with `runtime.Gosched()`. If contention persists or producer crashed mid-write with odd sequence, loop falls through to best-effort copy without hanging CPU.
4. **Bounds Enforcement on Raw Slot Operations**:
   - `pkg/shm/producer.go:2368–2377`: `WriteRawSlot` enforces `phaseIdx` bounds, `slotIdx < p.header.MaxFrames`, and exact byte slice stride length `uintptr(len(rawBytes)) == stride`.
   - `pkg/shm/producer.go:2386–2397`: `ReadRawSlot` enforces identical bounds and returns heap copy (`make([]byte, stride)`) rather than direct alias to mapped memory.
5. **Framing & Payload Bounds**:
   - `pkg/relay/protocol.go:1714–1716`: `ReadPacket` validates `totalLen` between 9 bytes and 16MB ceiling, rejecting malformed length prefixes.
   - `pkg/relay/protocol.go:1588–1591`: `DecodeHandshake` verifies payload length matches expected dynamic layout size before decoding slice offsets.

---

## 3. Projection Math & Temporal Parity

- **Feature Definition SSoT**: Maintained. Relay bypasses feature re-projection on replica machine; server memcopies pre-calculated byte slices from ring buffer slots (`MsgTypeFrameSlot`), and client writes directly via `WriteRawSlot`. WSL paper engine and VM live engine read identical dual-cache-line layout.
- **Temporal Causal Sequencing**: Strict contract verified: for anchor $T$, server transmits snapshots for $[T - \text{cadenceNS}, T)$, followed by raw frame slots for each configured phase, followed by `MsgTypeAnchor`. Client writes all snapshots and frame slots before calling `CommitFrameFinalize(T)`. Downstream consumers reading replica SHM never observe torn composite frames.
- **Timestamp Scaling**: Massive.com WebSocket millisecond timestamps (`t`) multiplied by `NSPerMS` (`1_000_000`), matching nanosecond resolution across datalake parquet fixtures and `ccm.marketdata.projection`.
- **Bitwise Parity Verification**: `tests/test_relay_parity.py:2579–2593` verifies `actual_repl_metrics == actual_prim_metrics` (0.00000000 maximum difference) and bit-identical parity against golden numpy arrays across all 72 symbols and 5 metric features.

---

## 4. Deviations from the Approved Plan

- **D1 (Client Reconnection Implemented — Resolved)**: `pkg/relay/client.go:1248–1302` implements reconnection loop with backoff in `ConnectAndReplicate`, properly incrementing `c.reconnects`.
- **D2 (Static ABI Version — Operator-Accepted, Tracked)**: `RelayVersion = 1` remains static integer constant in `pkg/relay/protocol.go:1507`. Accepted in plan review §4.3; schema compatibility validated at handshake via `MaxFrames`, `TotalSymbols`, `FrameStrideBytes`, and `RingOffsetBytes`.
- **D3 (Live Feed Snapshot Skew — Operator-Accepted, Tracked)**: In live continuous streaming, primary snapshot slots may update during transmission of anchor $T$. Documented and accepted in plan review §5; 1Hz ring buffer frames remain immutable and bit-identical.

---

## 5. Systems & Performance Violations

- **P1 (Packet Header Allocation — Resolved)**: `pkg/relay/protocol.go:1691`: `WritePacket` formats 13-byte wire framing into stack-allocated array (`var header [13]byte`), eliminating heap allocation on packet transmission hot path.
- **Buffer Flush Management**: `pkg/relay/server.go:2191, 2254`: Network writes buffered through 64KB `bufio.Writer` and flushed once per anchor, minimizing syscall overhead.
- **Scratch Buffer Re-use**: `pkg/relay/server.go:2192, 2314`: `scratchSnap` pre-allocated once outside loop and passed by pointer into `readSnapshotSeqLock`.
- **Minor Observation (Non-Blocking)**: `pkg/relay/protocol.go:1627`: `EncodeSnapshot` allocates 130-byte slice per updated snapshot. At 72 symbols/sec, throughput footprint ($\approx 9\,\text{KB/s}$) is negligible for Go GC; could reuse pre-allocated packet scratch buffer in future optimization.

---

## 6. Telemetry / Verification Gaps

- **T1 (Sequence Gaps and Reconnection Metrics — Resolved)**: `pkg/relay/client.go:1392–1393`: Telemetry line `[RELAY-CLI]` now reports `gaps` and `reconnects` alongside latency and snapshot counts.
- **T2 (Live Baseline Snapshots — Verified)**: `pkg/relay/server.go:2219` tested via unit tests; initial snapshot emission verified for all active symbols upon client connection.
- **End-to-End Parity Gate**: `tests/test_relay_parity.py` asserts latency $\le 500\,\mu\text{s/bar}$, zero metric difference, and bit-identical snapshot fields across all symbols.

---

## 7. Verdict & Remediation

APPROVED

The implementation resolves all blocking defects from prior review cycles. Concurrency races in `MassiveWSClient` shutdown are closed via proper `sync.WaitGroup` coordination, snapshot synchronization over the relay respects causal order and baseline initialization, bounds checks protect shared memory slot memcopies, and bitwise parity against golden references is verified at 100.000% fidelity. Code is ready for merge.
