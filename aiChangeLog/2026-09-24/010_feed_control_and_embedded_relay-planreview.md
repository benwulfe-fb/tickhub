# Plan Review — aiChangeLog/2026-09-24/010_feed_control_and_embedded_relay.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 22:14:50Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

### 1. Summary of the Proposal

Plan enables safe production cutover on GCP VM `ccm-live-1` alongside legacy engine under Massive.com single-session WebSocket limit:
- Default standby mode (`--feed-enabled=false`): daemon boots, initializes `/dev/shm/tickhub_live`, pulses 250ms `HeartbeatNS`, serves metrics, but holds WebSocket closed.
- Dynamic feed control without daemon restart: HTTP endpoints `GET/POST /control/feed` on metrics server and CLI subcommands `tickhub feed status|enable|disable`.
- Telemetry clobber guard: `pkg/shm/producer.go:PublishTelemetry` updates `AnchorPublishLatencyNS` only when `publishLatencyNS > 0`, preventing 250ms heartbeats from wiping frame publish latency.
- Embedded relay removed per Round 1 review; standalone `tickhub relay-server` remains isolated.

---

### 2. Simplest Sufficient Design

Plan is now at minimum required surface. Scope creep from Round 1 (embedded relay) excised cleanly. Remaining components — boot flag, feed manager lifecycle, HTTP control endpoint, CLI wrapper, and latency guard — map directly to zero-downtime cutover and session safety. No structural cuts needed.

---

### 3. Blocking Defects

None.

---

### 4. Non-Blocking Observations

1. Event loop in `cmd/tickhub/daemon.go` must set tick channel reference to `nil` while disabled to avoid busy-spin on closed channel.
2. `FeedManager` requires internal mutex or state machine to serialize concurrent `enable`/`disable` calls and avoid duplicate dials.
3. Query-param dispatch (`?action=enable`) works, but path-based routing (`/control/feed/enable`) avoids query string parsing quirks.
4. Default control port (`127.0.0.1:9090`) in `cmd/tickhub/feed_ctl.go` should share constant with default metrics bind address.
5. Guard `publishLatencyNS > 0` drops true zero-nanosecond measurements, acceptable under OS clock resolution.

---

### 5. Methodological & Data-Alignment Concerns

N/A

---

### 6. Missing Telemetry

None.

---

### 7. Verdict

**Verdict: Proceed**

Plan cleanly resolves Round 1 B4 defect by dropping embedded relay, preserving process-level crash isolation between ingestion and relay delivery. Proposed feed manager, CLI control interface, and telemetry clobber guard are surgical and directly solve Massive.com session collision constraint. Verification plan in §5 sufficiently covers standby state, transition sequence, and heartbeat continuity. Proceed to implementation.
