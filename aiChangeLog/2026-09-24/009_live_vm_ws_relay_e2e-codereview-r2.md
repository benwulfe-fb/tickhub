# Code Review — aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Verdict (parsed): DENIED · Generated: 2026-09-24 21:09:36Z · Tool: codereview.py (Antigravity `agy`)_

---

DENIED

1. **Summary of the Implemented Change**

Change implements live WebSocket ingestion on GCP VM (`ccm-live-1`), cross-machine TCP relay replication across Tailscale WAN to WSL, and dual-engine 1Hz validation.
- `cmd/tickhub/daemon.go`: Sets `projector.SetStartAnchor(time.Now().UnixNano())` before loop. Adds 250ms `time.Ticker` in single-threaded `select` loop calling `prod.PublishTelemetry(0, 0, 0, tickCount)`, deriving `wallAnchor = (nowNS / cadenceNS) * cadenceNS`, flushing `projector.Flush(wallAnchor)` on anchor advance. Rate reporting restructured inside `ticker.C` case.
- `pkg/relay/client.go`: Calls `c.producer.PublishTelemetry(latencyUS*1000, 0, uint64(c.seqGaps.Load()), uint64(c.anchorsCount.Load()))` on frame replication.
- `pkg/relay/server.go`: Removes `SIPTimestampNS` window filtering; sends baseline snapshots on connect (`isFirstClientAnchor`), then streams deltas where `scratchSnap.SeqLockSeq > 0 && scratchSnap.SeqLockSeq != lastSentSeq[i]`.
- `pkg/shm/producer.go`: Updates `HeartbeatNS` via `atomic.StoreInt64(&p.header.HeartbeatNS, time.Now().UnixNano())` in `CommitFrameFinalizeWithLatency` (called by `CommitFrameFinalize`).
- `python/tickhub/shm.py`: `get_latest_phase_anchor` uses `last_written_anchor_ns` when $> 0$. `load`/`load_sync` skips sleep if `min_target_ns <= last_written`, caps sleep to 50ms (`min(sleep_sec, 0.05)`).
- `python/tickhub/engine_client.py` (new): 1Hz reference engine client. Attaches to SHM with 15s retry loop. In 1Hz loop calls `load_sync`, reads SeqLock snapshots and features, logs JSONL with `anchor_ns`, `local_wall_ns`.
- `scripts/test_live_relay_e2e.py` (new): Cross-machine E2E test. Compiles amd64 binary, SCPs binary/configs/SDK to VM, measures SSH ping RTT and clock skew, launches daemon + relay server + Engine A (20s) on VM, launches relay client + Engine B (20s) on WSL, syncs exit via `tail --pid`, fetches logs, inner-joins on `anchor_ns`, verifies sequence continuity, feature matrix parity, snapshot parity (99.5% match, $0.50 max delta), checks SSH ping RTT against 150ms budget.

2. **Correctness & Concurrency Bugs**

- `python/tickhub/engine_client.py:91-103`: Unhandled `RuntimeError` on SeqLock contention timeout. `reader.snapshot(sym)` in `python/tickhub/shm.py:530` raises `RuntimeError("SeqLock read contention timeout for symbol '{symbol}'")` after 100 retry iterations. `engine_client.py` catches only `KeyError`. Under writer contention on live data, unhandled `RuntimeError` crashes engine client process. Catch `(KeyError, RuntimeError)`.
- `scripts/test_live_relay_e2e.py:207`: Hardcoded Python interpreter path `python_bin = "/mnt/wc/miniconda3/envs/gpu_env/bin/python"`. Breaks if executed in alternative virtualenv or system Python. Use `sys.executable`.
- `scripts/test_live_relay_e2e.py:228-234`: Fragile stdout streaming. `while engine_b_proc.poll() is None:` terminates immediately upon process exit, dropping unread lines buffered in `engine_b_proc.stdout`. If `engine_b_proc` hangs before output, `readline()` blocks with no timeout.
- `scripts/test_live_relay_e2e.py:147,257`: Incomplete remote process cleanup for Python engine. Teardown runs `kill -9 $pid` for PIDs in `/tmp/engine_prod.pid` and `killall -9 tickhub_bin`. If Python process spawned subprocesses or PID file missing, `engine_client.py` orphans on VM. Add `pkill -9 -f engine_client.py`.
- Prior review C2 disposition (`cmd/tickhub/daemon.go:169`): Verified handled in base code (`if !ok { prod.SetStatus(shm.StatusClosed); return }`). Prior C2 was false positive from diff context.
- Prior review C4 disposition (`pkg/shm/producer.go:487`): Verified handled. `CommitFrameFinalize` delegates to `CommitFrameFinalizeWithLatency(anchorNS, 0)`, which updates `HeartbeatNS`.

3. **Projection Math & Temporal Parity**

- Single-threaded event loop in `daemon.go` correctly sequences tick ingestion and ticker flushes. Zero data races.
- `wallAnchor := (nowNS / cadenceNS) * cadenceNS` with `wallAnchor > lastFlushedAnchor` flushes anchor $T$ only after wall clock crosses $T$. In `projector.go`, `IngestTick` closes phase at $T$ before processing ticks with $ts \ge T$, preserving half-open interval $[T-1\text{s}, T)$. Forward-fill on tick drought operates via `hist.CloseBar(slot)`.
- `SymbolSnapshot` in TickHub SHM (`pkg/shm/layout.go:135`) is single unbuffered top-of-book table at `SnapshotOffset + i*128`, not anchor-versioned ring buffer. Asynchronous reading by Engine A on VM and Relay Server on VM races with live inbound ticks. 99.5% match and $0.50 max delta in `test_live_relay_e2e.py` reflects this architectural limitation, but conflicts with ledger assertions (see Section 4).
- Feature matrix parity strictly asserts exact equality on `features` (`float64` lists). Passes 100% bitwise parity across joined anchors.

4. **Deviations from the Approved Plan**

- **D1 (Unremediated Prior Finding D5 / T2): Replication lag calculation omitted and replaced with SSH ping** [BLOCKING].
  Ledger §1.3 lines 49-50 and §4.10 line 157 explicitly state:
  `Per-anchor replication lag computed as (wsl_recv_wall_ns - clock_skew_ns) - vm_recv_wall_ns and asserted <= 150.0ms`.
  `Asserts steady-state replication lag <= 150.0ms WAN budget with hard exit (sys.exit(1))`.
  In `scripts/test_live_relay_e2e.py:365-373`, script calculates `one_way_transit_ms = wan_rtt_ms / 2` from pre-test SSH date ping and tests that against `WAN_BUDGET_MS = 150.0`. Formula `(wsl_recv_wall_ns - clock_skew_ns) - vm_recv_wall_ns` never computed. Pipeline replication lag of committed frames across WAN completely unverified.
- **D2 (Unremediated Prior Finding M4 / T1): 100% bitwise snapshot parity claimed in ledger, relaxed in code** [BLOCKING].
  Ledger §1.3 line 47 and §4.10 line 156 claim:
  `Bitwise identity verified for top-of-book SeqLock snapshots... across all 72 symbols`.
  `Asserts 100% bitwise snapshot parity with hard exit (sys.exit(1))`.
  In `scripts/test_live_relay_e2e.py:359-363`, test allows 0.5% divergence (`match_pct >= 99.5`) and up to $0.50 price delta. Discrepancy between ledger contract and implementation.
- **D3: `pkg/relay/client.go:206` hardcoded 10ms threshold contradicts 150ms WAN budget**.
  Ledger specifies 150ms WAN budget for Tailscale cross-region replication (baseline ping RTT ~87ms, one-way transit ~44ms). `client.go:206` hardcodes `if latencyUS > 10000` (10ms). Logs `[SLO-VIOLATION]` every second on WAN.

5. **Systems & Performance Violations**

- `pkg/relay/client.go:206-209`: Spams `[SLO-VIOLATION]` log every second because 10ms threshold is exceeded by normal 44ms WAN transit.
- `python/tickhub/engine_client.py:90-128`: Allocates 72 Python dicts per second for snapshots plus 72x5 matrix conversion. Acceptable for test client, produces ~100KB JSONL per second.
- `pkg/shm/producer.go:482`: VDSO `time.Now().UnixNano()` on 1Hz commit path. Operator accepted.

6. **Telemetry / Verification Gaps**

- **T1: True pipeline replication latency unmeasured**.
  `test_live_relay_e2e.py` records `local_wall_ns` in both engines and measures `clock_skew_ns`, but never computes `lag_ms = ((paper_wall_ns - clock_skew_ns) - prod_wall_ns) / 1e6` for joined anchors. Test passes even if relay replication stalls or lags by seconds, provided initial SSH ping was $< 300\text{ms}$.
- **T2: Snapshot divergence metrics not aggregated by symbol**.
  When snapshot divergence occurs within 99.5% tolerance, script does not report which symbols or fields diverged, obscuring whether divergences are quote flickers or stale symbols.

7. **Verdict & Remediation**

DENIED. Prior finding D5/T2 unremediated (per-anchor replication lag assertion replaced with SSH ping check), ledger contract contradicts code on snapshot parity threshold, and unhandled `RuntimeError` in `engine_client.py`.

Remediation list:
1. `scripts/test_live_relay_e2e.py`: Implement per-anchor replication lag assertion specified in Ledger §1.3: for each anchor in `common_anchors`, calculate `lag_ms = ((paper_by_anchor[a]["local_wall_ns"] - clock_skew_ns) - prod_by_anchor[a]["local_wall_ns"]) / 1e6`. Assert `max(lag_ms) <= 150.0` (or `p99 <= 150.0`) with `sys.exit(1)`. Retain `wan_rtt_ms / 2` as diagnostic network log only.
2. `aiChangeLog/2026-09-24/009_live_vm_ws_relay_e2e.md`: Reconcile ledger text with snapshot implementation: document that `SymbolSnapshot` is unbuffered live top-of-book, update §1.3 and §4.10 to state $\ge 99.5\%$ field match and $\le \$0.50$ delta tolerance, and remove inaccurate claims of "100% bitwise snapshot parity".
3. `python/tickhub/engine_client.py:92-103`: Catch `(KeyError, RuntimeError)` around `reader.snapshot(sym)` to prevent unhandled crash on SeqLock contention timeout.
4. `scripts/test_live_relay_e2e.py:207`: Replace hardcoded `/mnt/wc/miniconda3/envs/gpu_env/bin/python` with `sys.executable`.
5. `scripts/test_live_relay_e2e.py:147,257`: Add `pkill -9 -f engine_client.py 2>/dev/null || true` to remote VM cleanup commands.
6. `pkg/relay/client.go:206`: Parameterize or relax hardcoded 10ms threshold in `latencyUS > 10000` to prevent false `[SLO-VIOLATION]` log spamming on every frame over WAN.
