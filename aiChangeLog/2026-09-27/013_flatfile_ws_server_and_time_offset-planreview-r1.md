# Plan Review — aiChangeLog/2026-09-27/013_flatfile_ws_server_and_time_offset.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-27 19:16:46Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

## 1. Summary of the Proposal

Three new artifacts + one Go change to enable weekend testing against recorded market data without live feeds:

1. **`filter_massive_flatfile.py`** — filters raw Massive CSV.GZ trade/quote files to the 72-symbol universe, merges both streams into a single chronologically sorted CSV.GZ with a 7-column schema.
2. **`flatfile_ws_server.py`** — WebSocket server that speaks the Massive wire protocol (auth/subscribe handshake, Q/T JSON arrays), replays the filtered file at 1x real-time, fast-forwarding to "virtual now" on connect.
3. **`tests/test_flatfile_ws_server.py`** — integration test covering filter output, server handshake, and timestamp alignment.
4. **`cmd/tickhub/daemon.go` `--time-offset`** — shifts `SetStartAnchor()` and the cadence ticker's `nowNS` by `timeOffsetNS` so TickHub's 1Hz windows align with the virtual playback timeline.

---

## 2. Simplest Sufficient Design

The stated goal is "end-to-end testing over the weekend without live markets." The filter utility and WS server are both necessary. The daemon time-offset is necessary to align projection anchors.

One material over-complexity: **two redundant time-offset input methods** — `--time-offset-s` (seconds delta) AND `--target-time` (absolute timestamp string) in `flatfile_ws_server.py`, plus `--time-offset` (Go duration string) AND `TICKHUB_TIME_OFFSET_NS` AND `TICKHUB_TIME_OFFSET_SEC` in the daemon. That is three env var / flag surfaces for one concept. For a one-weekend testing tool, one input path each is sufficient. The duplication is not blocking by itself, but see B4 below.

Otherwise the plan is close to minimum.

---

## 3. Blocking Defects

**B4 — Unnecessary complexity in time-offset interface surface**

The daemon adds *three* independent ways to specify the same offset (`--time-offset` flag, `TICKHUB_TIME_OFFSET_NS`, `TICKHUB_TIME_OFFSET_SEC`), and the WS server adds two (`--time-offset-s`, `--target-time`). The plan nowhere justifies why multiple input forms are required simultaneously. For a weekend replay harness with exactly one operator, a single canonical input (e.g., `--time-offset-s` on the server, `TICKHUB_TIME_OFFSET_NS` or `--time-offset` on the daemon — pick one each) is sufficient and eliminates the parsing ambiguity of "which env var wins when both are set." The plan does not resolve precedence between `TICKHUB_TIME_OFFSET_NS` and `TICKHUB_TIME_OFFSET_SEC`, which is a latent correctness bug in the daemon modification. This is unnecessary structural complexity the stated goal does not require.

**Cut list:** Remove `TICKHUB_TIME_OFFSET_SEC` (keep only `TICKHUB_TIME_OFFSET_NS` or the flag), and remove `--target-time` from the WS server (a caller can compute the delta themselves). Nothing is lost for the stated goal.

---

**B1 — Sleep formula is wrong; playback will not achieve 1x real-time**

In the `flatfile_ws_server.py` design (Proposed Code Changes → ADD `flatfile_ws_server.py`):

> `sleep_duration = (t_ns - offset_ns) / 1e9 - time.time()`

Here `t_ns` is the event's virtual nanosecond timestamp, and `offset_ns` is presumably `offset_s * 1e9`. This computes:

```
sleep = (t_ns - offset_ns) / 1e9 - time.time()
      = t_ns/1e9 - offset_s - time.time()
      = virtual_event_time_s - offset_s - wall_time_s
      = real_event_time_s - wall_time_s
```

That is `real_source_timestamp - current_wall_clock` — correct only if the server is run at the exact moment the original data was recorded, which it never is. The correct formula is:

```
sleep = (t_ns / 1e9 - virtual_start_ns / 1e9) / speed - (time.time() - stream_start_wall)
```

i.e., sleep until the elapsed playback time equals the elapsed real time. As written, `sleep_duration` will be a large negative number for any historical data, causing all rows to be emitted instantly in a burst — the server will not replay at 1x real-time, defeating the plan's stated goal of "uncompressed 1x real-time" playback and the downstream TickHub integration test.

---

## 4. Non-Blocking Observations

1. The filtered schema (`event_type,symbol,timestamp_ns,val1,val2,val3`) uses anonymous `val1–val4` column names; field-name documentation would help future debuggers.
2. The `--speed` multiplier on the WS server is unused by the sleep formula as written — even after fixing the formula, speed must divide elapsed playback time.
3. The test script verifies "timestamps aligned to virtual time" but the plan gives no tolerance / assertion threshold; the test may be vacuous.
4. `filter_massive_flatfile.py` streams both files simultaneously for merge-sort — the plan doesn't note that this requires both files fit in memory or uses a proper merge iterator; large files may OOM.
5. The Go `--time-offset` flag is parsed as "string, e.g. `-50h30m` or integer seconds" — two formats with no stated precedence rule is a parsing ambiguity.

---

## 5. Methodological & Data-Alignment Concerns

**Quote-before-trade tie-breaking:** The filter utility's sort key is described as "nanosecond `sip_timestamp`, breaking ties with quote-before-trade" — this matches the TickHub invariant. ✓

**Half-open interval / 1Hz semantics:** The daemon change shifts `nowNS` uniformly before the cadence calculation `(nowNS / cadenceNS) * cadenceNS`. This preserves the floor-division anchor alignment, so the half-open window $[T-1\text{s}, T)$ shifts correctly with the offset. ✓

**SHM / ABI / zero-alloc:** Neither the filter script nor the WS server touches SHM. The daemon modification only changes the `int64` passed to `SetStartAnchor()` and the local `nowNS` variable — no struct layout changes. N/A for ABI parity; zero-alloc hot path is unaffected. ✓

---

## 6. Missing Telemetry

The WS server logs rows dropped on fast-forward but does not log **rows emitted** or **cumulative lag** (wall time minus scheduled emit time). Without a running lag counter, there is no way to confirm the server is sustaining 1x pace or has fallen behind — and given the sleep formula bug, this will be the failure mode. Add a periodic `[WS-SERVER] emitted={N} lag_ms={lag:.1f}` log line (e.g., every 10 s of playback).

---

## 7. Verdict

**Verdict: Blocked (B1)**

The sleep formula in `flatfile_ws_server.py` subtracts the raw offset from the event nanosecond timestamp and compares to `time.time()`, which produces a permanently negative sleep for any historical replay file. All events will drain instantly rather than at 1x real-time. This directly defeats the plan's stated goal of "uncompressed 1x real-time" playback and invalidates the downstream integration test. Fix the formula to track elapsed playback time relative to stream-start before implementation. The B4 over-complexity in the offset interface surface should be resolved in the same pass.
