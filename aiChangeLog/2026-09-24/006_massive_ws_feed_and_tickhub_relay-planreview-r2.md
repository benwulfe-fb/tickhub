# Plan Review — aiChangeLog/2026-09-24/006_massive_ws_feed_and_tickhub_relay.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 18:48:22Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

# Plan Review — aiChangeLog/2026-09-24/006_massive_ws_feed_and_tickhub_relay.md

## 1. Summary of the Proposal

Plan delivers two capabilities for `tickhub`:
1. **Option A (Massive WS Daemon)**: Go WebSocket client (`pkg/feed/massive_ws.go`) connecting to `wss://socket.massive.com/stocks`. Authenticates, subscribes explicitly to configured 72-symbol universe, parses `ev=Q/T` JSON payloads into `feed.Tick`, ingests into primary `/dev/shm/tickhub_live`. Exposed via `tickhub daemon`.
2. **Option B (Cross-Machine Relay)**: TCP relay service (`pkg/relay/`) with compact binary wire protocol `THRL`. `relay-server` reads committed frame slots and SeqLock snapshots from primary SHM; `relay-client` replicates them into local WSL SHM segment. Downstream paper engine uses existing `TickHubReader` unchanged.
3. **Parity Gate**: End-to-end integration test (`tests/test_relay_parity.py`) gated at 100.000% bitwise parity on 1Hz metric frames and SeqLock snapshot fidelity against primary SHM and legacy `project_to_1hz`.

---

## 2. Simplest Sufficient Design

Plan is at minimum sufficient scope following Round 1 remediation:
- **Speculative Complexity Cut**: `MsgHeartbeat` eliminated; TCP disconnect provides native peer drop detection.
- **Raw Ring Slot Memcopy**: Replaced float64 matrix re-serialization with raw ring buffer slot byte copying (`MsgFrameSlot`). Eliminates feature decoding and re-projection logic in client.
- **No Excess Abstraction**: Surface limited strictly to feed client, wire protocol, server/client relay, CLI subcommands, and parity validation test.

No further cuts required.

---

## 3. Blocking Defects

None.

---

## 4. Non-Blocking Observations

1. In §Architecture / 2, `num_snapshots=N` in `MsgAnchorCommit` must represent transmitted symbol snapshots ($N \le 72$), not individual raw ticks, since SHM maintains only current state per symbol.
2. During live VM streaming, ticks arriving immediately after anchor $T$ can update primary snapshot slots while `relay-server` transmits; replica snapshots represent point-in-time reads at anchor sync.
3. `MsgHandshake` ABI version should derive from compile-time hash of layout constants rather than a manually bumped integer.
4. `pkg/feed/massive_ws.go` exponential backoff must specify maximum retry cap and jitter to avoid thundering-herd reconnect storms.
5. `scripts/validate_all.py` stage 5 must ensure deterministic unlinking of `/dev/shm/tickhub_relay_*` segments on failure.

---

## 5. Methodological & Data-Alignment Concerns

- **Snapshot Synchronization vs 1Hz Anchor**: In offline datalake replay, primary SHM snapshots freeze at anchor $T$, ensuring 100.000% bitwise parity. In live trading, ticks arriving in $[T, T + \Delta t)$ modify primary snapshot slots while `relay-server` reads them. Projected 1Hz metric frames remain bit-identical (immutable in ring buffer), but live quote snapshots may exhibit minor millisecond-level timing drift across the VM/WSL boundary.
- **Cross-Language ABI Parity**: Raw slot memcopy (`RawSlotBytes`) preserves identical C/Go/Python memory layout of the dual cache-line ring buffer. Validating `frame_stride` and `ring_offset` during `MsgHandshake` guarantees WSL Python `TickHubReader` reads identical struct offsets.

---

## 6. Missing Telemetry

- Reconnection counter and sequence gap metric in `tickhub relay-client` to detect TCP stream stalls or packet drop in flight.

---

## 7. Verdict

**Verdict: Proceed-with-noted-risks**

The plan resolves prior blocking defects: the causal sequencing contract between snapshots and frame commits is formally specified in the `THRL` wire contract (closing B2), float64 matrix re-serialization is replaced with direct ring-slot memcopy (closing B4), and speculative heartbeats were pruned. The remaining risks — live snapshot timing skew during continuous market feed ingestion and ABI hash validation — are non-blocking operational concerns properly captured in the ledger for post-implementation audit and guarded by `test_relay_parity.py`. Proceed to implementation.
