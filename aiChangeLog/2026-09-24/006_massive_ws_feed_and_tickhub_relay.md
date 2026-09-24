# 006 — Massive.com WebSocket Live Feed & Cross-Machine TickHub Relay with Bitwise Parity Gate

## Goal & Context

Operator directive (2026-09-24):
> *"so my goal is to deploy onto the VM and be able to run the engine without its own marketdata connection and rely on tickhub. and this should be a performance advantage with a gain to overall tick latency as the marketdata handling can be done in a different process and be multithreaded. the second part of this is the tickhub-relay where I can run a paper engine on the wsl box connecting to the same data and I am able to compare A vs B engines on different boxes with the same marketdata (albeit slightly more latency on the wsl box and using ibkr paper instead of live). this will let me run on real money on ccm-live without having to enter a paper test mode before releasing a new version. I can run a new version in parallel to validate first.*
>
> *question. the snapshot design using SeqLock will work remotely on wsl just as well as local on VM? i want to make sure the exact same implementation runs on the client and the client does not know whether its connecting to a local tickhub or a tickhub relay.*
> *we should subscribe on the symbols (72 I think) from massive as configured. not say on all symbols which would kill the box. should be near identical to subscription on current python engine. so its a good reference implementation.*
> *We need a good test to ensure bitwise identical results of this path versus the python project1hz, so we should be able to test with offline datalake until marketclose when you can take over the VM (4:05 EST) and test with realtime marketdata. do not interfere with marketdata connection during regular hours.*
> *So you can build option a now, but not test it. you should be able to test a tickhub-relay using offline datalake as a data source. proceed with that"*

### Review Dispositions (Round 1)
- **B2 (Snapshot / Frame Ordering Race)** → **fixed**: Mandated strict per-anchor sequencing contract in §Architecture / 2: for each anchor $T$, all `MsgSnapshot` updates strictly precede `MsgFrameSlot`, and `CommitFrameFinalize(T)` is invoked ONLY after all snapshots and frame slots for anchor $T$ are written to the replica SHM. Added per-anchor snapshot telemetry in §Telemetry.
- **B4 (Raw Ring Slot Memcopy)** → **fixed**: Replaced float64 feature matrix re-serialization with raw SHM frame slot memcopy in §Architecture / 2. `MsgFrameSlot` carries the exact byte slice of the ring buffer slot (`PhaseIdx`, `SlotIdx`, `AnchorNS`, `RawSlotBytes`), eliminating feature dimension parsing and re-projection logic.
- **Simplest Sufficient Design (Cut Speculative Heartbeat)** → **fixed**: Cut `MsgHeartbeat` from the wire protocol as recommended; TCP connection close provides clean, non-speculative peer disconnect detection.
- **Massive.com Timestamp Units** → **fixed**: Documented that Massive.com WebSocket `t` is SIP epoch milliseconds; multiplied by $1,000,000$ to nanoseconds matching `ccm/live/massive_feed.py`.

---

## Measurement & Regression Gate Thresholds

1. **Relay Re-Projection Bitwise Parity**:
   - Metric frames (`vol_1s`, `log_ret_1s`, `spread_bps`): **100.000%** bit-identical match between Primary SHM and Relay Replica SHM.
   - Parity vs legacy `ccm.marketdata.projection.project_to_1hz`: **100.000%** bit-identical match across all frames.
2. **SeqLock Snapshot Fidelity**:
   - SeqLock reads from Relay Replica SHM return identical `BidPx`, `AskPx`, `BidSz`, `AskSz`, `LastTradePx`, `LastTradeSz`, and `Midprice`.
   - SeqLock sequence consistency check: zero torn reads observed over $\ge 1,000$ consecutive concurrent snapshot accesses.
3. **Relay Network Latency & Throughput**:
   - Relay packet serialization + TCP loopback transit + SHM replication latency: **$\le 500$ microseconds** per 1Hz bar.
   - Throughput floor: $\ge 50,000$ ticks/sec replayed without frame loss or ring buffer wrap-around.

---

## SOLID & Systems Engineering Adherence

- **Single Responsibility (SRP)**:
  - `pkg/feed/massive_ws.go`: Responsible solely for connecting to `wss://socket.massive.com/stocks`, authenticating, subscribing to configured symbols, and deserializing JSON frames into `feed.Tick`.
  - `pkg/relay/protocol.go`: Defines the compact binary wire framing (`THRL`), message types (`MsgHandshake`, `MsgSnapshot`, `MsgFrameSlot`, `MsgAnchorCommit`), and encoding/decoding.
  - `pkg/relay/server.go`: Streams committed SHM frames and snapshots over TCP with per-anchor causal ordering.
  - `pkg/relay/client.go`: Receives binary TCP stream, performs raw memcopies into replica ring buffer, writes SeqLock snapshots, and commits frame anchors.
  - `cmd/tickhub/daemon.go`: Standalone live daemon orchestrating WebSocket ingestion into SHM.
  - `cmd/tickhub/relay.go`: CLI subcommands `relay-server` and `relay-client`.
- **Open/Closed (OCP)**:
  - Decoupling relay streaming from the ingestion source allows `relay-server` to stream from ANY TickHub SHM segment (whether populated by `replay`, `worker`, or live `daemon`).
- **Single Source of Truth (SSoT) & Zero-Recomputation**:
  - All rolling projection math lives solely in `pkg/project/projector.go`. The relay transmits exact pre-computed frame slot bytes; the relay client does not re-compute features or duplicate math.

---

## Architecture & Synchronization Contracts

### 1. SeqLock Transparency on Remote WSL Box
The client trading engine (`ccm-live` on VM or `ccm-paper` on WSL) interacts strictly with POSIX shared memory:
- **VM**: `Massive WS -> tickhub daemon -> /dev/shm/tickhub_live -> Engine A (Live)`
- **WSL**: `tickhub-relay client -> /dev/shm/tickhub_live -> Engine B (Paper)`

Because `tickhub-relay client` runs as a native Linux process inside WSL:
1. It creates and maps `/dev/shm/tickhub_live` via `shm.CreateProducer`.
2. When receiving `MsgSnapshot`, it executes `prod.WriteSnapshot(symbolIdx, snap)`:
   - Advances `SeqLockSeq` to odd.
   - Copies snapshot fields with store-release ordering.
   - Advances `SeqLockSeq` to even.
3. When receiving `MsgFrameSlot`, it writes the raw slot bytes directly into the replica ring buffer slot.
4. When receiving `MsgAnchorCommit(anchorNS)`, it advances `LastWrittenAnchorNS`.
5. The paper trading engine on WSL uses the standard `TickHubReader` with zero modifications:
   - Lock-free SeqLock reads in < 50ns.
   - 1Hz numpy feature arrays zero-copy in < 1µs.
   - Client is 100% agnostic to whether producer is local daemon or relay client.

### 2. Binary Relay Wire Protocol (`THRL`) & Causal Ordering (B2 & B4 Resolution)
Framing format over TCP:
```
+------------------+------------------+-------------------+----------------------+
| Length (4B BE)   | MsgType (1B)     | Sequence (8B BE)  | Payload (Variable)   |
+------------------+------------------+-------------------+----------------------+
```
Message Types:
- `0x01` `MsgHandshake`: Server sends ABI version, cadence, total symbols, phases config (offset_ms, frame_stride, ring_offset) to verify schema alignment before streaming.
- `0x02` `MsgSnapshot`: `SymbolIdx uint16` + `SymbolSnapshot` (128 bytes). Total: 130 bytes.
- `0x03` `MsgFrameSlot`: `PhaseIdx uint8` + `SlotIdx uint32` + `AnchorNS int64` + `RawSlotBytes []byte` (fixed size: `frame_stride` bytes).
- `0x04` `MsgAnchorCommit`: `AnchorNS int64` + `NumSnapshots uint32`.

**Causal Sequencing Contract**:
For every anchor $T$:
1. Server emits all `MsgSnapshot` updates for ticks occurring in $[T - 1\text{s}, T)$.
2. Server emits `MsgFrameSlot` for each phase covering anchor $T$.
3. Server emits `MsgAnchorCommit(anchorNS=T, num_snapshots=N)`.
4. Client applies snapshots via `prod.WriteSnapshot()`, writes raw frame slots via memcopy, validates that $N$ snapshots were applied, and ONLY then calls `prod.CommitFrameFinalize(T)`.
5. Downstream engine reading at anchor $T$ is guaranteed to see fully converged snapshots and matching 1Hz features without torn composite reads.

### 3. Massive.com WebSocket Client Guardrails
- **Endpoint**: `wss://socket.massive.com/stocks`
- **Auth**: `{"action":"auth","params":"<API_KEY>"}`
- **Subscription**: `{"action":"subscribe","params":"Q.SYM1,T.SYM1,Q.SYM2,T.SYM2,..."}` formatted from `config.yaml` `unique_symbols` (e.g. 72 symbols). Never wildcard `Q.*, T.*`.
- **Parsing**:
  - `ev="Q"` -> NBBO tick (`BidPx`, `AskPx`, `BidSz`, `AskSz`, `SIPTimestampNS = t * 1_000_000` where `t` is SIP epoch ms).
  - `ev="T"` -> Trade tick (`Price`, `Size`, `SIPTimestampNS = t * 1_000_000`).
- **Market Hours Safeguard**: Do NOT connect live to `socket.massive.com` during regular market hours (until 4:05 PM EST / 1:05 PM PST). Build now, test against mock WebSocket server.

---

## Proposed Code Changes

### 1. `pkg/feed/massive_ws.go` [ADD]
- Connects to `wss://socket.massive.com/stocks`.
- Formats explicit symbol subscription string (`Q.SYM,T.SYM`).
- High-performance JSON parser for `ev="Q"` and `ev="T"`, mapping into `feed.Tick`.
- Exponential backoff reconnection.

### 2. `pkg/relay/protocol.go`, `pkg/relay/server.go`, `pkg/relay/client.go` [ADD]
- Protocol definitions: `THRL` header, `MsgHandshake`, `MsgSnapshot`, `MsgFrameSlot`, `MsgAnchorCommit`.
- `RelayServer`: Attaches to source SHM segment, streams updates over TCP with per-anchor causal ordering.
- `RelayClient`: Connects to `RelayServer`, creates replica SHM segment, writes snapshots and raw frame slot bytes, commits anchors.

### 3. `cmd/tickhub/main.go`, `cmd/tickhub/daemon.go`, `cmd/tickhub/relay.go` [ADD / MODIFY]
- Add `daemon` subcommand for live streaming mode.
- Add `relay-server` and `relay-client` subcommands.

### 4. `tests/test_relay_parity.py` [ADD]
- End-to-end integration test:
  1. Replays golden fixture into `/dev/shm/tickhub_relay_primary`.
  2. Runs `relay-server` on loopback TCP port.
  3. Runs `relay-client` replicating into `/dev/shm/tickhub_relay_replica`.
  4. Reads frames and snapshots from replica via `TickHubReader`.
  5. Asserts 100% bitwise parity on frames and snapshots against primary and legacy `project_to_1hz`.

### 5. `scripts/validate_all.py` [MODIFY]
- Wire `test_relay_parity.py` into stage 5 of the validation pipeline.

---

## Telemetry Plan
- `tickhub daemon`: `[DAEMON] Authenticated to Massive WS. Subscribed to N symbols. Streaming to /dev/shm/<name>`.
- `tickhub relay-server`: `[RELAY-SRV] Streaming anchor <T> to client (N snapshots, P frame slots)`.
- `tickhub relay-client`: `[RELAY-CLI] Replicated anchor <T> to /dev/shm/<name> (N snapshots, latency <X>µs)`.
- `test_relay_parity.py`: `[RELAY TELEMETRY] Primary vs Replica: 0.00000000 difference across all frames. Snapshots bit-identical`.

---

## Verification Plan
1. Run `python scripts/planreview.py aiChangeLog/2026-09-24/006_massive_ws_feed_and_tickhub_relay.md`.
2. Implement `pkg/feed/massive_ws.go` with mock WebSocket server unit tests (`go test -v ./pkg/feed/...`).
3. Implement `pkg/relay/` binary protocol, server, and client with unit tests (`go test -v ./pkg/relay/...`).
4. Implement CLI commands `daemon`, `relay-server`, `relay-client`.
5. Implement `tests/test_relay_parity.py` testing replay -> primary SHM -> relay TCP -> replica SHM -> Python `TickHubReader`.
6. Run `python -u scripts/validate_all.py` (ensure all stages pass).
7. Run `python -u scripts/codereview.py aiChangeLog/2026-09-24/006_massive_ws_feed_and_tickhub_relay.md` until APPROVED.
8. Commit and push to `origin/main`.

---

## Checklist
- [x] Plan review completed & approved
- [x] Implement `pkg/feed/massive_ws.go` and unit tests
- [x] Implement `pkg/relay/` protocol, server, client, and unit tests
- [x] Implement `cmd/tickhub/daemon.go` and `cmd/tickhub/relay.go`
- [x] Implement `tests/test_relay_parity.py` with 100% bitwise parity gate
- [x] Update `scripts/validate_all.py`
- [x] Run `scripts/validate_all.py` green
- [x] Run `scripts/codereview.py` APPROVED
- [ ] Commit and push
