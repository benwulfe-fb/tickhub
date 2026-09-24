# Plan Review — aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 20:28:02Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

## 1. Summary of the Proposal

Four changes implement live market data ingestion, cross-machine replication, and dual-engine validation:

1. **`config/live_72.yaml`**: 72-symbol universe, single phase (`phase_0ms`, offset `0ms`), standard ring size `max_frames: 64`.
2. **`cmd/tickhub/daemon.go`**: 250ms wall-clock ticker added to main `select` loop in `runDaemon`. Keeps `HeartbeatNS` updated ($\le 1\,\text{s}$) and drives `projector.Flush(wallAnchor)` during tick drought.
3. **`python/tickhub/engine_client.py`**: Standalone 1Hz reader client reading SeqLock snapshots and feature matrices, outputting structured JSONL telemetry tagged with `anchor_ns`.
4. **`scripts/test_live_relay_e2e.py`**: Integration orchestrator. Compiles amd64 binary, SCPs binary and config to VM, launches daemon + relay-server + Engine A on VM, relay-client + Engine B on WSL, collects 20s telemetry, inner-joins on `anchor_ns`, asserts bitwise snapshot/feature parity and relay lag $\le 10\,\text{ms}$.

---

## 2. Simplest Sufficient Design

Plan is near minimum.

Prior scope bloat cut: two-phase topology reduced to single-phase `phase_0ms`, and ring size reduced from 256 to standard 64 frames. Dedicated `engine_client.py` justified over inlining: executes identically on remote VM over SSH and local WSL without escaping multi-line Python strings over SSH.

---

## 3. Blocking Defects

Prior review defects closed:
- Prior B1 (`anchor_ns` join key / parity verification): Closed. §1.3 and §4.4 specify strict inner-join on `anchor_ns`, report sequence drops as `RELAY_SEQUENCE_DROP`, assert bitwise identity across 72 symbol snapshots and feature matrices, and measure replication lag $\le 10.0\,\text{ms}$.
- Prior B2 (`projector.Flush` concurrency race): Closed. §1.2 and §4.2 serialize ticker inside existing main `select` loop alongside `case tick, ok := <-ticks:` on single main goroutine. Zero data races.

None.

---

## 4. Non-Blocking Observations

1. `scripts/test_live_relay_e2e.py` SCPs binary and config, but must also ensure `python/tickhub/engine_client.py` and Python runtime dependencies exist on VM before launching Engine A.
2. Post-market-close execution may yield zero live trades; parity check will validate forward-fill frame replication rather than dynamic trade price ingestion.
3. `daemon.go` ignores `!ok` when `ticks` channel closes in `case tick, ok := <-ticks:`; handle channel closure to prevent busy loop on shutdown.
4. Verify `projector.Flush` handles duplicate anchor invocations idempotently if tick-driven ingestion independently flushes anchor boundaries.
5. Remote process cleanup in `test_live_relay_e2e.py` must signal process groups (`kill -- -PID`) to prevent orphaned daemon or relay processes on VM.

---

## 5. Methodological & Data-Alignment Concerns

Single-threaded `select` loop ensures interval $[T-1\text{s}, T)$ logic in `pkg/project/projector.go` executes sequentially. `wallAnchor := (nowNS / cadenceNS) * cadenceNS` with `wallAnchor > lastFlushedAnchor` check guarantees strictly non-decreasing anchor flushes on wall clock. If tick path also triggers flushes internally, ensure `projector.Flush` enforces idempotent no-op when anchor $\le$ internal `lastFlushedAnchor`.

---

## 6. Missing Telemetry

None. Telemetry specifies `anchor_ns`, `local_wall_ns`, load durations, sample prices/returns, and per-anchor replication lag (`wsl_recv_wall_ns - anchor_ns`).

---

## 7. Verdict

**Verdict: Proceed-with-noted-risks**

Prior blocking defects B1 and B2 successfully remediated: parity verification specifies strict `anchor_ns` inner-join with drop detection, and ticker concurrency race eliminated via single-threaded event loop. Scope trimmed to single-phase 64-frame layout. Noted risks (VM script copying, channel closure handling, remote process group teardown) are implementation details for post-implementation code audit. Author may proceed to implementation.
