# Plan Review — aiChangeLog/2026-09-23/005_dataloader_control_line_and_parquet_parity_gate.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 01:09:23Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B2)**_

---

## 1. Summary of the Proposal

Add a bidirectional SHM control line inside `GlobalHeader._reserved` (two 64-byte cache lines at offsets 448 and 512) so that PyTorch DataLoader workers can command dedicated TickHub instances to replay arbitrary `(symbol, date, [start_ns, end_ns])` chunks into a private ring buffer, which workers then consume zero-copy. Gate the entire feature on a bitwise parquet parity test asserting 100% IEEE 754 float64 match between TickHub's `project1hz` and the legacy `ccm.marketdata.projection.project_to_1hz` across volume, log returns, and spread. Seven files touched (two new test files, one new Go subcommand, four modifications).

---

## 2. Simplest Sufficient Design

The stated goal is two things: (a) parquet parity gate, (b) SHM control line for chunked replay. Both are real requirements from the operator directive. The plan is close to minimum for (b) — four parallel workers over SHM with a single request/response slot per segment is the natural fit. No extraneous abstraction is introduced.

**One structural question worth naming:** the control line embeds a single `(ControlRequest, ControlResponse)` pair per SHM segment. With 4 workers each owning a *private* SHM segment ("each parallel dataloader can get their own tickhub instance"), this is correct and sufficient — no multiplexing is needed. The plan is consistent with the operator's stated 4-worker model.

Plan is at or near minimum. No cut list.

---

## 3. Blocking Defects

**B2 — Spin-poll without backoff or timeout on the Go side is a liveness hazard.**

The protocol handshake (Architecture §2) has the daemon detect `RequestID > LastHandledRequestID` by polling. The Python `request_chunk()` ("polls with `cpu_pause()` for daemon response, `timeout_s=5.0`") has a Python-side timeout. The plan states **no equivalent timeout on the Go daemon side**. If the Python client dies or the SHM segment is reused after a crash mid-request, the daemon has no specified exit condition: the `Status = StatusBusy` it wrote will never be cleared by anyone, and the next request from a new client will correctly increment `RequestID` — but only if the Python side is still alive to do so. The real hazard: if a DataLoader worker process is killed mid-replay, the Go worker is left with `Status = StatusBusy` and the next worker that maps the same private SHM segment (after OS recycle) may see a stale `ResponseID == RequestID` and consume garbage frames. The plan specifies `ConsumerHeartbeat` exists in the Consumer Cache Line (offset 0x0080) but does not specify that the Go worker uses it to detect dead clients and reset `StatusBusy`. This is an **undefined recovery path for the most likely failure mode in a multi-process training loop** (worker process crashes are routine in PyTorch DataLoader).

**Resolution required:** Specify that the Go worker monitors `ConsumerHeartbeat` and transitions `StatusBusy → StatusIdle` (or equivalent reset) if heartbeat is stale for a configurable threshold, OR document that the private SHM segment is fully torn down and re-created on worker restart, making stale state unreachable.

---

## 4. Non-Blocking Observations

1. The parity measurement covers only DASH 2026-05-06, one 60-second window. Spreading to at least one illiquid symbol and one cross-midnight window would harden the gate before merge.
2. `ControlRequest.Date` is a string or integer — its encoding and byte width are unspecified in the changelog, leaving a silent ABI misalignment risk between Go and ctypes.
3. The verification plan (§Verification, step 6) runs the parity test *after* the control-line integration test, but parity is the stated gate for the *entire* feature; reversing steps 5 and 6 reduces the window where broken projection math sits in a green CI state.
4. `ErrorMsg` in `ControlResponse` at 64 bytes total leaves very little room for a useful error string once `RespID`, `Status`, and `NumFramesWritten` are packed in — silent truncation of error context is likely.
5. The plan states `< 15 ms` round-trip for 632 ticks but sets the regression gate at `≤ 100 ms` — a 6× slack margin that may hide real regressions; tightening the gate to e.g. `≤ 30 ms` would catch performance regressions sooner.

---

## 5. Methodological & Data-Alignment Concerns

**Spread cold-start exception is a silent parity hole.** The plan acknowledges that the first 6 frames default to `1.0 bps` in TickHub "due to cold-start quote absence" and excludes them from the bitwise parity assertion (53 of 59 frames tested). This is a *design divergence*, not just a test convenience. If a DataLoader chunk happens to start at a point where TickHub has no prior quote state (which is guaranteed for every chunk boundary during training, since each chunk replay starts fresh), *every chunk's leading frames will forward-fill to 1.0 bps* rather than the true prevailing spread. The parity test hides this by exempting the cold-start frames — but training will silently receive corrupted spread values at every chunk boundary. This is **not addressed anywhere in the plan** and is a correctness defect under the SSoT goal. It may belong in B1 (plan cannot achieve SSoT for spread without a fix), but since the plan does not claim spread SSoT under cold-start conditions and explicitly carves out those frames, I flag it here as the highest-priority methodological concern: **the chunk boundary cold-start spread corruption is unmitigated and will affect every non-contiguous training sample.**

**Half-open interval semantics:** the plan states `[start_anchor_ns, end_anchor_ns)` implicitly (consistent with standard tick window conventions) but never explicitly documents whether `EndAnchorNS` is inclusive or exclusive. At frame boundaries this is a ±1 bar ambiguity that could cause the final bar to be dropped or double-counted. Must be nailed in the ABI doc.

---

## 6. Missing Telemetry

The plan needs one thing it currently lacks: **a per-chunk cold-start frame count reported in `ControlResponse`** (or in worker logs) so that the consumer knows how many leading spread values are synthetic. Without it, the Python training code has no way to mask or weight those frames correctly. This is the measurement needed to know whether the feature is working correctly (or must be rolled back) in the context of the spread SSoT claim.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The Go worker daemon has no specified mechanism to detect or recover from a dead Python client that leaves `Status = StatusBusy` in the control line. In a PyTorch DataLoader context, worker process crashes are a normal operating condition (OOM kills, SIGKILL from the trainer, dataset errors). A stale `StatusBusy` on a private SHM segment that gets re-mapped by a new worker will produce either a livelock (new client increments `RequestID` but daemon is still servicing or blocked on the prior run) or silent consumption of stale frame data. The `ConsumerHeartbeat` field exists in the layout but the plan never specifies the daemon reads it. This must be resolved — either via heartbeat-based reset or guaranteed SHM teardown on worker restart — before implementation.
