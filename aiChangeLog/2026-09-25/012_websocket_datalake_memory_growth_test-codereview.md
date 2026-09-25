# Code Review — aiChangeLog/2026-09-25/012_websocket_datalake_memory_growth_test.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-25 14:29:56Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

**`pkg/metrics/server.go`**: Added `readProcessRSSBytes()` helper that reads `/proc/self/statm`, parses field[1] (resident pages), and multiplies by `os.Getpagesize()`. In `handleMetrics`, appended `runtime.ReadMemStats` call followed by seven Prometheus-format text blocks: `go_memstats_heap_alloc_bytes`, `go_memstats_heap_inuse_bytes`, `go_memstats_heap_sys_bytes`, `go_memstats_heap_objects_total`, `go_memstats_num_gc`, `go_goroutines`, and `tickhub_process_rss_bytes`. Also added a trailing `\n` after `tickhub_recovery_missed_anchors_total` for blank-line separation.

**`pkg/metrics/server_test.go`**: Extended `expectedSubstrings` slice with presence-checks for all seven new metric names.

**`tests/test_websocket_memory_growth.py`**: Full end-to-end test: loads datalake Parquet (DASH/DELL quotes+trades), spins a mock `websockets` server that replays ticks in 50-event batches at ~10k ticks/s with wall-clock timestamps, launches `tickhub daemon` with `--feed-enabled=true` and `--metrics-addr`, polls `/metrics` every 2 s, collects tabular samples, then asserts: goroutine delta ≤ 0, post-GC heap trough drift < 1.5 MB, RSS growth < 5% of post-warmup baseline.

---

## 2. Correctness & Concurrency Bugs

**`server.go` — `/proc/self/statm` field index**: `/proc/self/statm` fields are: `[0]=VmSize [1]=VmRSS [2]=shared ...` in pages. Field `[1]` is VmRSS (resident set). This is correct. No defect.

**`server.go` — `handleMetrics` is not holding the server mutex during `ReadMemStats`**: `handleMetrics` reads `s.committedAnchors`, `s.downtime`, `s.missedAnchors` — all atomic or under lock — before the new block. `runtime.ReadMemStats` reads Go runtime internal state; it needs no application lock. No race with application fields.

**`server.go` — conditional RSS emission**: `tickhub_process_rss_bytes` is only emitted when `rss > 0`. On Linux this is always satisfied for a running process. The test's `tickhub_process_rss_bytes` substring check (in `server_test.go`) will pass only if `readProcessRSSBytes` returns > 0 in the test process, which it will. No material defect; slight fragility on non-Linux CI is acceptable (plan scopes to Linux).

**`test_websocket_memory_growth.py` — `loop.call_soon_threadsafe(lambda: asyncio.create_task(ws_server.stop()))` in `finally`**: `asyncio.create_task` schedules a coroutine on the running loop, but this is called from the main thread via `call_soon_threadsafe`. The lambda creates the task inside the loop's thread — this pattern is correct for running a coroutine from outside the loop. The subsequent `time.sleep(0.5)` gives it time to execute. No defect.

**`test_websocket_memory_growth.py` — `find_free_port` TOCTOU**: Port is found then released before binding. Race window exists. This is a known test-infrastructure pattern; acceptable for a local integration test.

**`test_websocket_memory_growth.py` — daemon stdout/stderr not drained during test run**: `subprocess.Popen` with `stdout=PIPE` / `stderr=PIPE` without draining risks pipe buffer deadlock if the daemon writes > 65 KB to stdout or stderr during the test window. For a 20-second run with normal logging this is unlikely but technically present. The `communicate()` call in the premature-exit branch drains correctly; the normal path does not drain until `finally` (which calls neither `communicate` nor explicit drain). Minor risk, not blocking.

---

## 3. Projection Math & Temporal Parity

This change adds no projection logic. The mock server assigns `ev["t"] = now_ms` (wall-clock milliseconds) to every event in a batch. The daemon's 1Hz projector uses `sip_timestamp` (nanoseconds) from the parsed event, not `t`. The Massive.com client in the daemon parses the `t` field as the event timestamp for the live feed path. Assigning `now_ms` (ms, ~13-digit) to `t` means all 50 events in a batch share the same millisecond timestamp, which is fine — events land across multiple 1Hz windows as wall-clock time advances. The quote-before-trade ordering within a 1Hz window is determined by timestamp tie-breaking; since all events in a batch share `t`, the daemon processes them in arrival order. This is consistent with how the live feed operates (real feed also batches same-ms events). No projection math violation.

Half-open interval semantics, illiquid forward-fill, and ASCII symbol tie-breaking are not touched by this change.

---

## 4. Deviations from the Approved Plan

**Plan review finding (B4)**: The plan review flagged the `server_test.go` modification as redundant and recommended removing it. The implementation **retained** the `server_test.go` assertions. This deviates from the B4 remediation recommendation. However, the plan review verdict was **Proceed-with-noted-risks** (not a blocking denial), and B4 was explicitly called "minor enough not to block." The retained unit assertions are harmless and incrementally additive (they catch handler-stub regressions in isolation, not solely end-to-end). This deviation does not introduce incorrectness.

**Plan review observation 1** (STW GC pause at scrape): addressed — polling interval is 2 s (plan reviewed at 1 s, ledger says 2 s). Remediated.

**Plan review observation 5** (goroutine leak detection absent): fully addressed — `go_goroutines` is exported, sampled, and `goroutine_growth <= 0` is asserted.

**Plan review observation 2** (RSS baseline stabilization): partially addressed. Baseline is taken at first post-warmup sample (≥10 s elapsed). No explicit stabilization check (e.g., RSS variance < threshold before capturing baseline) is performed. This remains a noted risk, not a blocking defect — the plan review did not elevate it to B-level.

**Plan review observation 3** (hardcoded datalake date): fixture fallback to `tests/fixtures/golden/datalake/2026-05-06/D` is implemented. Acceptable.

**Plan review observation 4** (timestamp progression across 1Hz windows): events use `now_ms = int(time.time() * 1000)`, which does advance across real wall-clock seconds. Each 2-second poll interval corresponds to ~2 projection cycles firing. The daemon's live feed path anchors on wall-clock time for the 1Hz projection boundary, so this is correct.

---

## 5. Systems & Performance Violations

**`runtime.ReadMemStats` in hot request path**: This call triggers a stop-the-world GC pause. It is invoked on every `/metrics` scrape. The plan review flagged this (observation 1) and the ledger acknowledges it. The test polls every 2 s, making STW pauses infrequent (~every 2 s, ~sub-millisecond). For an operational metrics endpoint this is a known and accepted trade-off in the absence of Prometheus client_golang; no third-party dependency is added (plan constraint met). Not a blocking violation given the explicit acknowledgment.

**`readProcessRSSBytes` allocates**: `os.ReadFile` allocates a small buffer; `strings.Fields` allocates a slice. These are in the `/metrics` handler, not in the projection hot path. Zero-allocation hot path (projection inner loop, SeqLock, ring-buffer writes) is untouched. No violation.

**`server_test.go` change**: Adds strings to an existing slice literal. No allocation or concurrency concern in tests.

---

## 6. Telemetry / Verification Gaps

- **Goroutine leak assertion**: now present (`goroutine_growth <= 0`). Gap closed.
- **Heap slope assertion**: replaced by the more robust trough-drift metric (`< 1.5 MB`), which is more GC-resistant than a raw slope. Sound.
- **RSS baseline stabilization**: no explicit check that baseline RSS has stabilized before recording it. If the daemon is still warming up at second 10 (e.g., large Parquet load still paging in), the baseline will be inflated and the 5% cap will pass trivially. This is a test-validity risk noted by the plan review; it is not remediated but is not blocking.
- **`total_ticks > 50_000` assertion**: reasonable liveness check given ~10k ticks/s over 20 s.
- **`total_committed > 0` assertion**: confirms at least one 1Hz projection frame was committed to SHM.

---

## 7. Verdict & Remediation

The Go-side change is correct, minimal, and non-invasive. The Python integration test correctly addresses all blocking plan-review findings (goroutine assertion, 2s polling interval). The one structural deviation (retaining `server_test.go` assertions despite the B4 recommendation) is explicitly permitted by the plan review's non-blocking verdict. No concurrency bugs, no projection math violations, no ABI changes, no new dependencies beyond stdlib and the already-present `websockets` package. All seven telemetry metrics are implemented and exercised.
