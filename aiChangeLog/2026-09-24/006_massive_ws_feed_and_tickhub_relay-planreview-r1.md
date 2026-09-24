# Plan Review — aiChangeLog/2026-09-24/006_massive_ws_feed_and_tickhub_relay.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 18:46:16Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Two independent deliverables bundled in one change:

**Option A — Massive.com WS Daemon**: A new Go WebSocket client (`pkg/feed/massive_ws.go`) that connects to `wss://socket.massive.com/stocks`, authenticates, subscribes to the configured 72-symbol universe, parses `ev=Q/T` JSON frames into `feed.Tick`, and feeds a local SHM segment via the existing `tickhub` pipeline. New `daemon` CLI subcommand wires it up.

**Option B — Cross-Machine Relay**: A new TCP relay (`pkg/relay/`) that streams pre-computed SHM frames and SeqLock snapshots from the VM (`relay-server`) to a WSL box (`relay-client`). The relay client populates a local `/dev/shm/tickhub_live` segment identically to the daemon, so the paper engine on WSL sees the same `TickHubReader` interface with zero code changes. A new binary protocol `THRL` carries `MsgHandshake`, `MsgSnapshot`, `MsgFrame`, `MsgHeartbeat`. Parity is gated at 100% bit-identical match between primary and replica SHM, validated against the offline datalake replay.

---

## 2. Simplest Sufficient Design

The stated goal is:
1. Engine on VM draws from tickhub SHM instead of its own WS connection.
2. Paper engine on WSL draws from a relay-replicated local SHM.
3. Bitwise parity proof between the two paths and legacy `project_to_1hz`.

The plan is **close to minimum** for Option B. The relay protocol, server, client, and parity test are all load-bearing. One area worth scrutiny:

**Potential cut — `MsgFrame` relay of the full float64 feature matrix**: The changelog states *"All projection math remains strictly in `pkg/project/projector.go`. The relay transmits pre-computed frames; no math is duplicated on the receiving end."* This is correct for the WSL consumer. However, the plan must also transmit `MsgSnapshot` (raw tick fields) so the WSL consumer can serve SeqLock reads to the paper engine. If the paper engine only needs the 1Hz features and not live quote streaming, `MsgSnapshot` could be dropped. But the operator directive explicitly requires the paper engine to use the same `TickHubReader` interface with SeqLock snapshot reads, so both message types are necessary. No cut available here.

**Actual cut available — `MsgHeartbeat`**: The plan lists `MsgHeartbeat` with a `TimestampNS` field and logs it in telemetry, but the operator directive contains no liveness or watchdog requirement. The relay client can detect connection loss via TCP close. The heartbeat is speculative complexity. **Cut `MsgHeartbeat` from the protocol.** The only thing lost: sub-second dead-peer detection when the TCP session stays half-open. The plan's latency SLA (≤500µs per 1Hz bar) makes this detectable by absence of `MsgFrame` within 2 seconds anyway.

**No other cuts identified.** The rest of the surface area — protocol definition, server, client, daemon, parity test — is directly required by the stated goal.

---

## 3. Blocking Defects

**B2 — Relay client SHM write ordering for `MsgFrame` is incomplete.**

Protocol section (Architecture §1, bullet 3): *"When receiving `MsgFrame`, it executes `prod.CommitSymbolMetrics(...)` and `prod.CommitFrameFinalize(anchorNS)`."*

The relay server is attached to an already-live SHM segment populated by the primary. The primary's `CommitSymbolMetrics` writes are interleaved with SeqLock snapshot writes across the 72-symbol window during a 1Hz cycle. The relay streams `MsgSnapshot` and `MsgFrame` as discrete TCP messages with no defined ordering guarantee between them within a given anchor. If `MsgFrame` arrives at the relay client before all 72 `MsgSnapshot` messages for that anchor, `CommitFrameFinalize` is called on the replica SHM with stale or absent snapshot data. The paper engine reading that frame will see a committed feature frame paired with unconverged snapshots — a torn composite read that the SeqLock alone cannot prevent because the inconsistency is between two separate SHM regions (snapshot slots vs. frame ring), not within a single SeqLock-protected slot.

The plan does not specify a sequencing contract: it neither mandates that all `MsgSnapshot`s for anchor T precede `MsgFrame` for anchor T, nor provides a per-anchor snapshot count acknowledgment on the client side before calling `CommitFrameFinalize`. The parity test (`test_relay_parity.py`) reads after the fact and would likely not observe this race on a quiet loopback replay, but the condition is real under any concurrent snapshot update during relay.

**Fix required**: The protocol must either (a) mandate that `relay-server` transmits all `MsgSnapshot`s for anchor T before `MsgFrame` T and the client enforces this ordering before calling `CommitFrameFinalize`, or (b) the `MsgFrame` payload must include an embedded snapshot count and the client must buffer until it is satisfied. This must be stated in `protocol.go` specification before implementation.

---

**B4 — `MsgFrame` carries a variable-length float64 feature matrix that re-introduces re-projection on the relay path, contradicting the plan's own SRP claim.**

`MsgFrame` definition (Architecture §2): *"`AnchorNS int64` + `PhaseIdx uint8` + `NumSymbols uint16` + `NumFeatures uint16` + float64 feature matrix."*

The relay server reads pre-computed frames from the primary SHM ring buffer and re-serializes them into `MsgFrame`. The relay client then deserializes and calls `prod.CommitSymbolMetrics(...)` to write them into the replica ring buffer. This means the relay client is performing a **feature-matrix write path** — it must know the buffer layout, the phase index semantics, and the symbol ordering to correctly invoke `CommitSymbolMetrics`. This is not a no-math passthrough; it is a partial re-implementation of the producer write protocol in the relay client.

The simpler design: the relay server copies the **raw SHM frame ring bytes** directly (a fixed-size memcopy of the committed ring slot) and the relay client writes those bytes directly into the corresponding replica ring slot. No `NumSymbols`, `NumFeatures`, no float64 deserialization, no `CommitSymbolMetrics` call. This is a strict subset of the current design, is layout-transparent, and eliminates a category of ABI mismatch bugs between server-side and client-side feature dimension assumptions.

If the existing `pkg/shm` ring buffer layout is fixed-size and position-stable (which the plan implies by calling it "dual cache-line layout"), raw slot copy is both simpler and less fragile. The plan should adopt this or explicitly justify why `CommitSymbolMetrics` must be called rather than a raw memcopy.

---

## 4. Non-Blocking Observations

1. The `MsgHandshake` ABI version field is correct in concept but the plan does not specify what `ABI version` encodes — if it is just a monotonic integer, a schema change that doesn't bump it will silently pass the handshake and corrupt the replica.
2. The 500µs relay latency SLA (Measurement §3) is stated but no baseline measurement of the existing datalake-replay-to-SHM path latency is provided; if that path already consumes >400µs, the SLA is unachievable before implementation begins.
3. `SIPTimestampNS = t * 1_000_000` (Architecture §3) — if Massive's `t` field is milliseconds-since-epoch, this is correct; if it is Unix seconds (float), the formula is wrong by 1000×. The plan does not cite what Massive documents `t` as.
4. `scripts/validate_all.py` is marked `[MODIFY]` but no description of what is added beyond wiring `test_relay_parity.py` — the modification scope is underspecified relative to the level of detail elsewhere.
5. The `MsgHeartbeat` (identified as cuttable in §2) has no consumer behavior specified — the plan does not state what `relay-client` does on heartbeat timeout, making it a dead protocol element as written.

---

## 5. Methodological & Data-Alignment Concerns

**SeqLock snapshot ordering vs. `CommitFrameFinalize`**: Covered as B2 above; restated here as the primary cross-language ABI concern. The Go relay client calling `CommitFrameFinalize` before all snapshots for that anchor have been written to the replica SHM violates the same half-open interval `[T-1s, T)` contract that `project_to_1hz` enforces. A `MsgFrame` represents the projection over `[T-1s, T)`. If the relay client commits that frame before the final `MsgSnapshot` for that window is written, downstream readers can observe a frame whose snapshot fields reflect T-2s state, breaking bitwise parity.

**Quote-before-trade ordering**: The relay protocol does not distinguish ordering of `MsgSnapshot` messages for the same symbol within an anchor. If the server streams them in arrival order (not sorted by `SIPTimestampNS`), the replica snapshot for a symbol may be written in a different order than on the primary — irrelevant for final parity if only the last snapshot per anchor is what matters, but the plan should state this explicitly.

---

## 6. Missing Telemetry

**Per-anchor snapshot count at relay client**: The relay client must log (or assert) how many `MsgSnapshot` messages it received for each anchor before calling `CommitFrameFinalize`. Without this counter, the B2 race described above is invisible in production logs — frames will be committed silently with incomplete snapshots and the parity test on the loopback replay may not catch it.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The relay client's `CommitFrameFinalize` invocation has no specified ordering contract relative to the `MsgSnapshot` stream for the same anchor. On a quiet loopback replay this race is latent and will likely not be caught by `test_relay_parity.py`, but under any realistic concurrent snapshot update cadence it produces torn composites in the replica SHM that violate both the SeqLock transparency invariant and the 100% bitwise parity gate. This must be resolved in the protocol spec — specifically, an explicit sequencing rule in `protocol.go` — before any implementation begins. The B4 finding (raw ring-slot copy vs. `CommitSymbolMetrics` re-invocation) should be adjudicated at the same time, as the resolution of B2 may make B4's simpler design the obvious path.
