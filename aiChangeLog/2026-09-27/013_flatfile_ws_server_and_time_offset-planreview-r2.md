# Plan Review — aiChangeLog/2026-09-27/013_flatfile_ws_server_and_time_offset.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-27 19:17:45Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

# Plan Review — aiChangeLog/2026-09-27/013_flatfile_ws_server_and_time_offset.md

_Model: Gemini 3.8 Flash · Round: 2 of 2 · Tool: planreview.py_

---

## 1. Summary of the Proposal

Plan creates weekend offline testing harness for TickHub and downstream engine using recorded Massive.com SIP flat files:

1. **`scripts/filter_massive_flatfile.py`** — Streams raw `trades_v1` and `quotes_v1` `.csv.gz` archives in $O(1)$ heap memory. Filters to 72 symbols in `config/live_72.yaml`. Merges streams with `heapq.merge` key `(sip_timestamp_ns, 0 if quote else 1)` into unified 7-column sorted `.csv.gz`.
2. **`scripts/flatfile_ws_server.py`** — WebSocket server emulating Massive auth/subscribe handshake. Fast-forwards to virtual start time ($T_{\text{wall}} + \Delta$). Paces uncompressed playback via elapsed wall-clock formula `target_wall_time = stream_start_wall + (event_virtual_offset_s / speed)`. Batches bursts into JSON arrays matching `RawMassiveEvent`.
3. **`cmd/tickhub/daemon.go`** — Adds `--time-offset` flag and `TICKHUB_TIME_OFFSET_NS` env var. Shifts `effectiveStartNS` and cadence loop `nowNS` by `timeOffsetNS`. Maintains floor-division 1Hz anchor calculation.
4. **`tests/test_flatfile_ws_server.py`** — Integration test verifying handshake within 1.0s, pre-virtual-start drop, pacing within $\pm 50\text{ms}$ tolerance at 1x/5x, and field integrity.

---

## 2. Simplest Sufficient Design

Plan is at minimum sufficient scope for stated goal.

Round 1 complexity cuts adopted: `--target-time` removed from WS server; `TICKHUB_TIME_OFFSET_SEC` removed from daemon; precedence between flag and env var defined explicitly. Four artifacts (one filter script, one WS server, one daemon flag hook, one integration test) match minimum operational requirements for live-like weekend replay.

No further cuts required.

---

## 3. Blocking Defects

None.

---

## 4. Non-Blocking Observations

1. In `cmd/tickhub/daemon.go`, unitless integer parsing fallback for `--time-offset` is ambiguous between seconds and nanoseconds; require explicit duration suffix (e.g. `-165600s`) or document fallback unit as nanoseconds.
2. In `scripts/flatfile_ws_server.py`, timestamp emitted as `t_ns // 1_000_000` (epoch ms); ensure `pkg/feed/massive_ws.go` unpacks ms without precision loss.
3. In `scripts/filter_massive_flatfile.py`, add `symbol` as third element in `heapq.merge` key tuple to guarantee deterministic ASCII tie-breaking across parallel symbols on matching nanoseconds.
4. Sequential line-by-line gzip scan during fast-forward on client connect may cause initial multi-second delay on full-day SIP files; acceptable for offline harness.
5. Ensure daemon telemetry reporting `AnchorPublishLatencyNS` calculates against virtual `nowNS` rather than raw host `time.Now()` to avoid logging false multi-hour publish lag during historical replay.

---

## 5. Methodological & Data-Alignment Concerns

- **1Hz Projection Semantics**: Cadence loop calculates `nowNS := now.UnixNano() + timeOffsetNS` before `(nowNS / cadenceNS) * cadenceNS`. Anchor boundary alignment and half-open window $[T-1\text{s}, T)$ semantics remain strictly invariant.
- **Quote-Before-Trade Tie-Breaking**: `heapq.merge` key `(sip_timestamp_ns, 0 if quote else 1)` sorts quote (0) before trade (1) on identical timestamps.
- **SHM Dual Cache-Line Isolation & ABI Parity**: Untouched. No struct modifications or layout shifts in C, Go, or Python ABI layers.
- **Zero-Allocation Hot Path**: Time offset parsed once at startup; cadence ticker modification contains zero heap allocations.

---

## 6. Missing Telemetry

None. Periodic lag logging (`emitted`, `rate`, `lag_ms`, `current_vtime`) and daemon virtual anchor logging added as requested.

---

## 7. Verdict

**Verdict: Proceed**

Round 1 blocking defects remediated. Pacing formula in `scripts/flatfile_ws_server.py` now correctly tracks elapsed wall-clock time relative to stream start with speed scaling. Interface surface pruned to canonical `--time-offset-s` on server and `--time-offset` on daemon. Test assertion tolerances and telemetry specified. Plan is approved for implementation.
