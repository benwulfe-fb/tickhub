# Code Review — aiChangeLog/2026-09-27/013_flatfile_ws_server_and_time_offset.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): APPROVED · Generated: 2026-09-27 19:22:18Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. Summary of the Implemented Change

Four artifacts delivered:

**`cmd/tickhub/daemon.go` (modified):** Adds `--time-offset` flag and `TICKHUB_TIME_OFFSET_NS` env var fallback. Parses via `time.ParseDuration` first; falls back to integer with a heuristic (`|ns| < 1e11` → treat as seconds, multiply by 1e9; else treat as raw nanoseconds). Shifts both `SetStartAnchor` call (`time.Now().UnixNano() + timeOffsetNS`) and cadence-ticker `nowNS` by the same offset before the floor-division anchor calculation. Logs the offset and effective virtual timestamp.

**`scripts/filter_massive_flatfile.py` (new):** Streaming gzip merge of trades and quotes CSVs. Two generators yield `(ts_ns, tie_breaker, record_str)` tuples — quotes with tie-breaker `0`, trades with `1`. Merged via `heapq.merge` with `key=lambda item: (item[0], item[1])`. Writes unified 9-column `.csv.gz` with header. O(1) memory; periodic progress logging.

**`scripts/flatfile_ws_server.py` (new):** Async WebSocket server. Handles `auth` → `subscribe` handshake. At subscribe completion captures `stream_start_wall = time.time()` and `virtual_start_ns = int((stream_start_wall + offset_s) * 1e9)`. Scans file sequentially to first record ≥ `virtual_start_ns`, then resets both `stream_start_wall` and `virtual_start_ns` to the actual first record's timestamp. Elapsed-time pacing: `target_wall_time = stream_start_wall + (event_virtual_offset_s / speed)`. Batches up to 50 events or flushes before each sleep. Emits quote fields `bp/bs/ap/as`, trade fields `p/s`, timestamp as `t_ns // 1_000_000`. Periodic telemetry every 10 s.

**`tests/test_flatfile_ws_server.py` (new):** `unittest.IsolatedAsyncioTestCase`. Builds synthetic trades/quotes CSV.GZ with controlled tie at `base_ts + 1e9`. Tests filter output (5 lines, quote-before-trade at tie timestamp, GOOG pruned). Tests WS server at 10x speed by driving `websockets.serve` directly with `handle_connection`, collects 4 events, asserts ordering and timing `< 0.6 s`.

---

## 2. Correctness & Concurrency Bugs

**Minor / acceptable for offline harness:**

- `flatfile_ws_server.py:635` — outer `while line:` loop uses `line = f.readline()` at bottom (`line 699`). The `break` at line 626 exits the fast-forward `for line in f` loop, leaving `line` bound to the first in-window record. The `while line:` body processes that record first (correct), then calls `f.readline()` at the end. This correctly transitions from the iterator-based fast-forward to the readline-based streaming loop without double-consuming or skipping the first valid record. ✓

- `flatfile_ws_server.py:599` — `batch_flush_interval` is defined but never consulted; batching is driven solely by `len(batch) >= 50` and by the `sleep_duration > 0.0005` flush. The variable is dead code. Benign for correctness; slightly misleading.

- `flatfile_ws_server.py:662` — Quote field order in dict: `bp/bs/ap/as`. Plan (line 75) specifies `bp/ap/bs/as` (bid-price, ask-price, bid-size, ask-size). Actual: `"bp":…, "bs":…, "ap":…, "as":…`. Go consumer `pkg/feed/massive_ws.go` maps by JSON key name, not positional order, so this is correct regardless of dict insertion order. ✓

- `flatfile_ws_server.py:690–692` — Telemetry uses `t_ns` which may be from a skipped (wrong-symbol) record in the current iteration because the `t_ns = int(parts[2])` assignment at line 643 is inside the `if target_symbols is None or sym in target_symbols:` branch. If the current line is filtered out, `t_ns` retains the last emitted value — this is acceptable approximation for a log line in an offline harness.

- `filter_massive_flatfile.py:416` — `heapq.merge(q_gen, t_gen, key=…)` merges two pre-sorted generators. The key correctly encodes `(ts, 0)` for quotes and `(ts, 1)` for trades. Plan review observation #3 (add `symbol` as a third key element for ASCII tie-breaking across parallel symbols on matching nanoseconds) was flagged as non-blocking. The implementation does not add the third element. Intra-type same-nanosecond ordering is not guaranteed deterministic across generators, but plan review explicitly accepted this as non-blocking. ✓ (non-blocking)

- `tests/test_flatfile_ws_server.py:855` — `port=0` passed to `FlatFileWSServer.__init__` but the server object's `run()` is never called; the test creates its own `websockets.serve` listener at ephemeral port. The `server` object's `port` field is unused. Harmless.

- `tests/test_flatfile_ws_server.py:879` — `asyncio.wait_for(ws.recv(), timeout=2.0)` called in a `while len(received_events) < 4` loop. If the server sends all 4 events in a single `json.dumps(batch)` call, the loop exits after one `recv()`. If split across messages, multiple recvs occur. Either path is correct given the `extend` accumulation.

---

## 3. Projection Math & Temporal Parity

**1Hz cadence anchor (daemon.go):**
```go
nowNS := now.UnixNano() + timeOffsetNS          // virtual wall clock
wallAnchor := (nowNS / cadenceNS) * cadenceNS   // floor division, same as baseline
```
The floor-division anchor boundary calculation is unchanged. Shifting `nowNS` uniformly means the 1 Hz cadence fires at virtual second boundaries aligned with `virtual_start_ns`, preserving the half-open window `[T-1s, T)` semantics with respect to ingested ticks (which carry their own `sip_timestamp_ns` and are compared against the anchor by the projector unchanged). ✓

**SetStartAnchor:** `time.Now().UnixNano() + timeOffsetNS` — initializes the anchor to the virtual epoch. Tick ingestion comparison against the anchor uses tick `sip_timestamp_ns` directly; this is consistent with virtual time. ✓

**Quote-before-trade tie-breaking in filter:** `(ts, 0)` for quotes, `(ts, 1)` for trades. `heapq.merge` on sorted generators with this key places quotes before trades at equal nanosecond timestamps. Matches SSoT rule. ✓

**Projector and SHM untouched:** no modifications to `pkg/project/projector.go`, `pkg/shm`, or any ABI struct. All 1Hz math, SeqLock, ring-buffer, and illiquid-forward-fill logic is unmodified. ✓

**No look-ahead leakage:** server fast-forwards by dropping rows with `t_ns < virtual_start_ns` then resets baseline to first retained record. No future data is passed to the projector before its virtual time arrives. ✓

---

## 4. Deviations from the Approved Plan

- **Dead variable `batch_flush_interval`** (server line 599): ledger describes "10ms boundary" batching. The 10 ms boundary is referenced only in this variable which is never used. Batching logic flushes on `len(batch) >= 50` and on any sleep trigger. The effective behavior approximates the intent without the explicit 10 ms wall-clock window check. Acceptable deviation; ledger language was "up to 50 events or 10ms boundary" — the implemented flush-on-sleep achieves equivalent burst grouping in practice.

- **Plan review observation #3 unaddressed** (symbol as third heapq key element): acknowledged as non-blocking in plan review; implementation omits it. Within-nanosecond same-type ordering is non-deterministic but functionally harmless for this use case.

- **Plan review observation #5 unaddressed** (`AnchorPublishLatencyNS` against virtual vs raw `time.Now()`): plan review flagged that telemetry `AnchorPublishLatencyNS` may log false multi-hour lag. The daemon diff does not touch `PublishTelemetry` call arguments (line 288 of the diff: `prod.PublishTelemetry(0, 0, 0, tickCount)`). `AnchorPublishLatencyNS` is passed as `0` explicitly in the existing call — the field is not computed from `time.Now()` at this call site. No false lag is introduced. ✓ (observation moot)

- **`--time-offset` integer heuristic** (plan review observation #1): flag now documented in help string as "integer nanoseconds". The heuristic `|ns| < 1e11` (100 seconds) treats small integers as seconds, large integers as nanoseconds. For a typical weekend replay offset of ~`-181800s` (-50.5 h), the absolute value `181800` < `1e11` is `true`, so it would be multiplied by 1e9 → `-181800000000000 ns` (correct). For raw nanosecond inputs like `-653880000000000` (> 1e11 in magnitude), treated as nanoseconds (correct). The boundary is adequate for this use case.

---

## 5. Systems & Performance Violations

- **Zero-allocation hot path:** Time offset is parsed once at daemon startup. The cadence ticker loop modification adds only integer arithmetic (`now.UnixNano() + timeOffsetNS`) — no allocations. ✓

- **Cache-line dual isolation:** No SHM struct modifications. Producer and consumer cache lines at offsets 64 and 128 are unmodified. `PublishTelemetry` signature unchanged. ✓

- **Python scripts:** Not on the zero-allocation hot path. Acceptable GC behavior for an offline harness.

- **`batch_flush_interval` dead variable:** cosmetic; no performance impact.

---

## 6. Telemetry / Verification Gaps

All planned telemetry lines are present:
- `[WS-SERVER] Client connected from {addr}` ✓
- `[WS-SERVER] Fast-forwarded to virtual_start_ns={ts} (dropped {N} rows in {dt:.2f}s)` ✓
- `[WS-SERVER] Replaying at {speed}x real-time (offset: {offset_s}s, virtual_start: {v_start_iso})` ✓ (extended but correct)
- Periodic `emitted/rate/lag_ms/current_vtime` every 10 s ✓
- `[DAEMON] Configured time offset: {offset} (effective virtual now: {timestamp})` ✓

Test coverage:
- Filter ordering and universe pruning: asserts quote-before-trade at tie, GOOG exclusion, record count ✓
- WS server handshake, pacing, ordering, field integrity: asserts `auth_success`, `success`, event ordering (`Q/AAPL`, `T/AAPL`, `Q/MSFT`, `T/MSFT`), elapsed `< 0.6 s` at 10x ✓

Gap (non-blocking): pacing at `speed=5.0` is not separately tested as the plan specified; only `speed=10.0` is exercised. The pacing formula is the same regardless of speed multiplier, so the gap is trivial.

---

## 7. Verdict & Remediation

The implementation faithfully executes the approved plan. The daemon time-offset logic is correct, non-allocating, and preserves 1Hz projection semantics. The filter merge correctly enforces quote-before-trade ordering. The WS server fast-forward and pacing are mechanically sound. The test covers the critical correctness properties. All blocking defects from plan review are absent. Non-blocking plan-review observations are either moot, non-blocking by prior designation, or harmlessly omitted.

No blocking defects found.
