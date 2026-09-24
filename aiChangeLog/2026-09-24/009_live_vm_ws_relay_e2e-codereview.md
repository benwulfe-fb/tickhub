# Code Review — aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md

_Model: Gemini 3.8 Flash (High) · Round: 3 · Verdict (parsed): APPROVED · Generated: 2026-09-24 21:20:53Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

1. **Summary of the Implemented Change**

Change enables live WebSocket market data ingestion on GCP VM (`ccm-live-1`), cross-machine TCP relay replication across Tailscale WAN to WSL workstation, and dual-engine 1Hz validation.
- [`cmd/tickhub/daemon.go`](file:///mnt/wc/src/tickhub/cmd/tickhub/daemon.go): Calls [`projector.SetStartAnchor`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L83) at initialization. Adds 250ms `time.Ticker` in single-threaded event loop. Ticker calls [`prod.PublishTelemetry`](file:///mnt/wc/src/tickhub/pkg/shm/producer.go#L503) every 250ms (refreshes `HeartbeatNS` $\le 1\,\text{s}$ during tick drought). Flushes [`projector.Flush`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L300) when `wallAnchor > lastFlushedAnchor`.
- [`cmd/tickhub/relay.go`](file:///mnt/wc/src/tickhub/cmd/tickhub/relay.go): Adds `--latency-budget-ms` flag on `relay-client` (default 150ms for WAN).
- [`pkg/relay/client.go`](file:///mnt/wc/src/tickhub/pkg/relay/client.go): Parameterizes `LatencyBudgetUS` (defaults to 150ms). Calls [`producer.PublishTelemetry`](file:///mnt/wc/src/tickhub/pkg/shm/producer.go#L503) on frame replication to refresh replica `HeartbeatNS` and publish sequence gap/anchor counters.
- [`pkg/relay/server.go`](file:///mnt/wc/src/tickhub/pkg/relay/server.go): Removes `SIPTimestampNS` window filter. Emits baseline snapshot on connect (`isFirstClientAnchor`), then streams deltas via `SeqLockSeq != lastSentSeq[i]`.
- [`pkg/shm/producer.go`](file:///mnt/wc/src/tickhub/pkg/shm/producer.go): Updates `HeartbeatNS` via `time.Now().UnixNano()` inside [`CommitFrameFinalizeWithLatency`](file:///mnt/wc/src/tickhub/pkg/shm/producer.go#L479) on every frame commit.
- [`python/tickhub/shm.py`](file:///mnt/wc/src/tickhub/python/tickhub/shm.py): Prioritizes `last_written_anchor_ns` in [`get_latest_phase_anchor`](file:///mnt/wc/src/tickhub/python/tickhub/shm.py#L412) when $> 0$, eliminating client clock skew stalls. Only sleeps in [`load_sync`](file:///mnt/wc/src/tickhub/python/tickhub/shm.py#L567) and [`load_async`](file:///mnt/wc/src/tickhub/python/tickhub/shm.py#L545) when `min_target_ns > last_written`, capping sleep to 50ms.
- [`python/tickhub/engine_client.py`](file:///mnt/wc/src/tickhub/python/tickhub/engine_client.py): Reference 1Hz execution client reading snapshots with SeqLock and feature matrices from SHM. Catches `(KeyError, RuntimeError)` exceptions during SeqLock contention. Outputs structured JSONL telemetry.
- [`scripts/test_live_relay_e2e.py`](file:///mnt/wc/src/tickhub/scripts/test_live_relay_e2e.py): Cross-machine E2E test orchestrator. Compiles amd64 binary with CGO, verifies `libtickhub_atomic.so`, syncs to VM, measures clock skew via persistent SSH ping, runs VM daemon + relay server + Engine A (Prod), runs WSL relay client + Engine B (Paper), inner-joins on `anchor_ns`, verifies contiguous anchors (zero sequence drops), 100% bitwise parity on feature matrices, snapshot parity ($\ge 99.5\%$ field match, $\le \$0.50$ max delta with symbol breakdown), and asserts P99 pipeline replication lag $\le 150\,\text{ms}$ WAN budget.

2. **Correctness & Concurrency Bugs**

- Prior review remediation verified:
  - [`python/tickhub/engine_client.py:689-690`](file:///mnt/wc/src/tickhub/python/tickhub/engine_client.py#L689-L690): Catches `(KeyError, RuntimeError)` on [`reader.snapshot(sym)`](file:///mnt/wc/src/tickhub/python/tickhub/shm.py#L474). SeqLock read contention timeout will not crash client.
  - [`scripts/test_live_relay_e2e.py:951`](file:///mnt/wc/src/tickhub/scripts/test_live_relay_e2e.py#L951): Hardcoded Python path replaced with `sys.executable`.
  - [`scripts/test_live_relay_e2e.py:891,1019`](file:///mnt/wc/src/tickhub/scripts/test_live_relay_e2e.py#L891): Added `pkill -9 -f '[e]ngine_client.py'` to cleanup routines. Prevents remote Python engine orphans.
  - [`pkg/relay/client.go:462-464,479-482`](file:///mnt/wc/src/tickhub/pkg/relay/client.go#L462-L464): Latency threshold configurable via `LatencyBudgetUS` (default 150ms). False `[SLO-VIOLATION]` log spam eliminated.
- Single-threaded event loop in [`cmd/tickhub/daemon.go:170-205`](file:///mnt/wc/src/tickhub/cmd/tickhub/daemon.go#L170-L205): `case tick, ok := <-ticks:` and `case now := <-ticker.C:` run sequentially on main goroutine. Access to `projector` serialized. Zero data races.
- Channel closure: base code handles `if !ok { prod.SetStatus(shm.StatusClosed); return }`. No busy loop on shutdown.
- SHM layout: ABI sizes and cache-line alignments verified by [`scripts/validate_all.py`](file:///mnt/wc/src/tickhub/scripts/validate_all.py) stage 2. Zero ABI slips.

3. **Projection Math & Temporal Parity**

- In [`pkg/project/projector.go:176-236`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L176-L236), [`IngestTick`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L176) closes phase at target $T$ before processing ticks with timestamp $\ge T$. Half-open interval $[T-1\text{s}, T)$ strictly preserved.
- Wall-clock flush: `wallAnchor := (nowNS / cadenceNS) * cadenceNS` with `wallAnchor > lastFlushedAnchor` invokes [`Flush(wallAnchor)`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L300) only after wall clock crosses anchor. If tick ingestion already closed window, [`Flush`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L300) is idempotent no-op because `phaseNextAnchor[pIdx] > wallAnchor`.
- Illiquid forward-fill: ticks absent -> [`Flush`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L300) triggers [`closePhase`](file:///mnt/wc/src/tickhub/pkg/project/projector.go#L238), calling [`hist.CloseBar(slot)`](file:///mnt/wc/src/tickhub/pkg/project/history.go). Rolling arrays forward-filled correctly.
- Top-of-book snapshot parity: `SymbolSnapshot` is unbuffered instantaneous table updated by live WebSocket ticks. Quote delta between VM reader and WSL reader during live trading is inherent to asynchronous quote arrival. Code enforces $\ge 99.5\%$ field match and $\le \$0.50$ max price delta.
- Feature matrix parity: 100% bitwise parity asserted across all 72 symbols for `log_ret_1s`, `log_ret_5s`, `log_ret_15s`, `vol_1s`, `spread_bps`. Feature definition SSoT maintained in Go `projector.go`.

4. **Deviations from the Approved Plan**

- Prior blocking deviations closed:
  - D1 (replication lag assertion): Remediation verified in [`scripts/test_live_relay_e2e.py:1133-1157`](file:///mnt/wc/src/tickhub/scripts/test_live_relay_e2e.py#L1133-L1157). Pipeline replication lag computed as `((w_wall - closest_skew) - p_row["local_wall_ns"]) / 1e6` for every joined anchor, asserting P99 $\le 150.0\,\text{ms}$ with hard `sys.exit(1)`.
  - D2 (snapshot parity threshold discrepancy): Remediation verified in [`aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md`](file:///mnt/wc/src/tickhub/aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md). Ledger §1.3 and §4.10 reconciled with code to reflect unbuffered top-of-book nature, specifying $\ge 99.5\%$ field match and $\le \$0.50$ max delta.
  - D3 (WAN latency threshold): Configurable latency budget implemented in [`pkg/relay/client.go`](file:///mnt/wc/src/tickhub/pkg/relay/client.go) and [`cmd/tickhub/relay.go`](file:///mnt/wc/src/tickhub/cmd/tickhub/relay.go).
- Residual risk operator-accepted:
  - Producer commit path VDSO `time.Now().UnixNano()` call in [`pkg/shm/producer.go:482`](file:///mnt/wc/src/tickhub/pkg/shm/producer.go#L482) is 1Hz cadence only, operator accepted.
- No unremediated deviations remain.

5. **Systems & Performance Violations**

- Hot-path allocations: `readSnapshotSeqLock` uses pre-allocated stack snapshot buffer `scratchSnap` in [`pkg/relay/server.go:172`](file:///mnt/wc/src/tickhub/pkg/relay/server.go#L172). Zero heap allocations in streaming loop.
- Dual cache-line isolation preserved: Producer writes at offset 64 (`ProducerCacheLine`), consumer writes at offset 128 (`ConsumerCacheLine`). No cache line bouncing.
- CPU utilization: [`python/tickhub/shm.py:562,581`](file:///mnt/wc/src/tickhub/python/tickhub/shm.py#L562) caps polling sleep to 50ms when waiting for next anchor. Avoids 1-second blocking stalls while avoiding busy-wait spinning.
- Rate reporting in [`cmd/tickhub/daemon.go:189-197`](file:///mnt/wc/src/tickhub/cmd/tickhub/daemon.go#L189-L197) throttled to 5-second intervals via `ticker.C`.

6. **Telemetry / Verification Gaps**

- None. Full verification pipeline active:
  - `go test -v -race ./pkg/... ./cmd/...`: Passes with zero race warnings.
  - [`scripts/validate_all.py`](file:///mnt/wc/src/tickhub/scripts/validate_all.py): Passes all 7 stages including ABI alignment, unit tests, 13 pytest suites, and golden replay performance benchmark (118,952 ticks/s, 19ms round-trip latency).
  - Cross-machine E2E script records per-anchor JSONL telemetry from both Prod (VM) and Paper (WSL), validates contiguous 1Hz anchors, bitwise feature matrix identity, snapshot tolerances, and P99 WAN replication latency against SLO-06.

7. **Verdict & Remediation**

APPROVED. All prior blocking defects (D1, D2, D3, C1, C2, C3) remediated. Code is mathematically sound, free of concurrency races, satisfies SLO requirements, matches ledger specifications, and passes complete verification suite. Ready to commit and push.
