# Plan Review — aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 20:26:49Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Four changes to wire up a live end-to-end test across two machines:

1. **`config/live_72.yaml`** — production config for 72 symbols, two phases (0ms + 500ms), `max_frames=256`.
2. **`cmd/tickhub/daemon.go`** — add a 250ms wall-clock ticker to (a) keep `HeartbeatNS` alive during tick droughts and (b) drive `projector.Flush(wallClockAnchor)` at 1Hz boundaries when no ticks arrive.
3. **`python/tickhub/engine_client.py`** — reference engine that reads `/dev/shm`, loops on 1Hz anchors, emits JSONL telemetry.
4. **`scripts/test_live_relay_e2e.py`** — orchestrator that SSHs to VM, copies the binary and config, launches daemon + relay-server + Engine A on VM, relay-client + Engine B on WSL, collects 20 s of telemetry, and asserts anchor match, price parity, and relay latency ≤ 10 ms.

The two stated-but-not-new components (relay-server, relay-client, `TickHubReader`) are described as already existing.

---

## 2. Simplest Sufficient Design

The goal is: *demonstrate live data → relay → dual-engine parity in one 20-second run.*

What that strictly requires:
- The 72-symbol config (gap #1 — genuinely needed).
- The wall-clock ticker fix in `daemon.go` (gap #2 — genuinely needed to avoid stale heartbeat killing the run before 20 s elapse).
- A way to read both engines' telemetry and diff them.

**The plan is materially larger than needed.** Specific cut candidates:

| Item | What to cut | What is lost |
|---|---|---|
| `engine_client.py` as a *new standalone file* | Inline the 20-second read loop directly in `test_live_relay_e2e.py` | One fewer file; no public API lost — `TickHubReader` already exists |
| Two-phase config (`phase_0ms` + `phase_500ms`, all 72 symbols in both) | Single-phase config for the E2E test | Phase parity is not tested by the E2E parity assertion; 500ms phase exists only for production scheduling |
| `max_frames: 256` (plan §4 item 1) | Needs justification or should match existing examples | If existing relay protocol assumes a different ring size, a silent mismatch corrupts the ring without error |

The two-phase config is the clearest over-reach: the parity assertion (§4 item 4, "snapshot midprices, bids, asks, and volume match") does not mention phases. If phase scheduling matters to correctness, the plan does not explain *why* both phases must appear in the live_72 config for the E2E test to pass. This is B4 territory — raised below.

---

## 3. Blocking Defects

### B1 — Parity check measures latency, not parity

The plan's parity gate (§4, item 4, "Parity check") asserts:

> "snapshot midprices, bids, asks, and volume match between VM and WSL"

Engine A and Engine B run on different clocks and collect telemetry asynchronously. The orchestrator collects "20 seconds of telemetry" but the plan nowhere describes **how anchor-alignment is established before the comparison**. The only mechanism stated is "Anchor match: identical sequence of anchors received by both engines" — but this is a separate gate from the parity check and the plan does not say that parity is computed *only on anchor-matched rows*.

Under the relay path (VM → Tailscale → WSL), Engine B lags Engine A by relay latency (≤ 10 ms per SLO-06). If both engines emit JSONL at wall-clock time and the orchestrator zips the two streams naively, a single skipped anchor on either side shifts every subsequent row, producing false parity failures or — worse — false passes if two adjacent anchors happen to carry the same price. The plan does not describe the join key or the join logic. Without it, the parity gate cannot be trusted, and the stated goal ("verify bitwise parity") is not achieved. **This is B1.**

**Required fix**: The plan must specify the join key (anchor_ns), that the join is an inner-join on anchor_ns before comparing fields, and that unmatched anchors on either side are flagged as divergences, not silently dropped.

### B2 — `projector.Flush(wallClockAnchor)` called concurrently with tick path

§4 item 2 adds a 250ms ticker goroutine that calls `projector.Flush(currentWallAnchor)` whenever the wall clock crosses a 1Hz boundary. The existing tick path (§1 gap description) already calls into the projector via `case tick, ok := <-ticks`. The plan does not state whether `projector.Flush` is goroutine-safe, nor does it introduce any synchronization (mutex, channel serialization) between the ticker goroutine and the tick-processing goroutine. If `Flush` and tick ingestion touch shared projector state concurrently, this is a data race — directly contradicting the "zero races" requirement of `go test -race ./pkg/...` (§5 item 1). In a production SHM engine, a race on projector state can corrupt committed frames visible to all readers. **This is B2.**

**Required fix**: The plan must either (a) serialize the ticker through the same select loop as the tick channel (no new goroutine), or (b) explicitly state that `projector.Flush` is internally synchronized and cite where that invariant is established.

---

## 4. Non-Blocking Observations

1. `max_frames: 256` in `live_72.yaml` is not justified; if relay-client pre-allocates the ring based on config, a mismatch between VM and WSL config values silently corrupts the ring.
2. `scripts/test_live_relay_e2e.py` cross-compiles the binary on WSL and SCPs it to the VM (Debian 12 / linux/amd64); the plan does not assert GOOS/GOARCH targets, which will silently break if the WSL host is arm64.
3. 20-second collection window at post-market close may yield all-forward-fill frames (no live ticks), making the "sample_ret" field in telemetry always zero and the parity check trivially vacuous.
4. The two-phase config (§4 item 1) with 72 symbols in *both* phases doubles the SHM write surface without the plan explaining what Phase 1 (500ms) contributes to the E2E test.
5. Checklist item "Adversarial plan review approved" is listed as unchecked (§6) — this review satisfies that gate; ensure it is marked before implementation begins.

---

## 5. Methodological & Data-Alignment Concerns

**Flush timing and half-open interval semantics**: The plan computes `currentWallAnchor = (time.Now().UnixNano() / cadenceNS) * cadenceNS` inside the 250ms ticker. If the ticker fires at T=999ms and a real tick arrives at T=1001ms, both the ticker and the tick handler could attempt to flush the same anchor window. The plan does not state how `lastFlushedAnchor` is updated atomically relative to the tick path — if the tick handler also advances `lastFlushedAnchor`, a concurrent ticker write races on that variable (separate from the projector race above). The half-open interval $[T-1s, T)$ could be double-flushed, producing a second committed frame for the same anchor with forward-filled values overwriting the real tick values — a look-ahead-free violation if the real tick was late but valid.

---

## 6. Missing Telemetry

The plan must measure **relay replication lag per anchor** (not just assert ≤ 10 ms in aggregate) to know whether the parity check's assumption of anchor alignment holds. Without per-anchor relay lag, a slow Tailscale burst could cause Engine B to skip anchors silently. One additional log field — `relay_lag_ns` from Engine B computed as `(local_wall_clock_ns - anchor_ns)` at read time — is sufficient and is not currently specified.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The wall-clock ticker goroutine calls `projector.Flush` concurrently with the tick-processing path but the plan introduces no synchronization and makes no claim that `projector.Flush` is goroutine-safe. In a shared-memory engine this is a data-corruption risk, not merely a test failure. Resolve by serializing the ticker event through the existing select loop before implementing the rest. Once B2 is resolved, address the B1 parity join-key underspecification before the E2E script is coded, or the verification gate it creates will be untrustworthy.
