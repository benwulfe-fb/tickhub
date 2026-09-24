# Plan Review — aiChangeLog/2026-09-24/010_feed_control_and_embedded_relay.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 22:14:13Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Simplify-first**_

---

## Adversarial Review: `aiChangeLog/2026-09-24/010_feed_control_and_embedded_relay.md`

---

### 1. Summary of the Proposal

On the production GCP VM `ccm-live-1`, TickHub must coexist with a legacy trading engine that shares the same Massive.com WebSocket API key. Simultaneous connections would cause session flapping. The plan addresses this with four changes:

1. **Standby mode** (`--feed-enabled=false`): daemon starts, writes SHM, heartbeats, serves relay and Prometheus — but holds the WebSocket closed.
2. **Live feed control** via HTTP (`GET/POST /control/feed`) and CLI subcommands (`tickhub feed status|enable|disable`) so the operator can flip the feed without restarting the daemon.
3. **Embedded relay**: `--relay-addr` collapses the separate `relay-server` process into the daemon goroutine.
4. **Clobber guard**: `PublishTelemetry` skips writing `AnchorPublishLatencyNS` when `publishLatencyNS == 0` so heartbeat ticks don't zero the last measured frame latency.

---

### 2. Simplest Sufficient Design

The **core problem** is: don't hold the Massive.com WebSocket while the legacy engine is live, then hand over ownership at operator discretion. The minimal machine for that is:

- A `--feed-enabled` boot flag on the daemon.
- A single HTTP endpoint (or even a signal handler / flag file) to toggle the feed at runtime.

**The embedded relay (§1.3, §4 `--relay-addr`) is not required by the stated goal.** The stated justification is "eliminates unnecessary operational surface" — but the plan also says the relay runs on the *active* SHM segment, and its existence predates this ledger. Nothing in the cutover scenario demands that the two processes merge *before* cutover is safe. This is scope creep bundled into a latency-sensitive operational change.

**Cut list:**

| Component | Remove | What is lost |
|---|---|---|
| `--relay-addr` flag, embedded `relay.Server` goroutine, §4 `[MODIFY] daemon.go` relay block | Yes | Slightly fewer process-management steps at cutover; the separate relay process remains. The stated goal (safe coexistence + live feed toggle) is fully preserved. |
| `cmd/tickhub/feed_ctl.go` + `main.go` `feed` subcommand (CLI wrapper) | Optional | Operator must `curl` the control endpoint directly instead of `tickhub feed enable`. Saves one file and a dispatch entry; acceptable for an internal operator tool if curl is in runbooks. Not a hard cut, but worth questioning. |

The clobber guard (§4 `producer.go`) and standby semantics are small and clearly motivated. Keep them.

---

### 3. Blocking Defects

**B4 — Unnecessary new component: embedded relay**

The plan bundles the relay server collapse into a feed-control ledger. The stated goal (§1, first paragraph) is exclusively about coexistence and WebSocket session safety. The embedded relay justification ("adds unnecessary operational surface," §1 final bullet) is a convenience argument, not a safety argument. Merging them adds a non-trivial goroutine supervision path inside the daemon (`relay.Server` lifecycle management, error propagation, restart policy on relay accept failures) that is orthogonal to feed-control correctness and introduces a new daemon crash surface. If the relay goroutine panics or blocks, the daemon — now the sole SHM writer — goes with it. A standalone relay process would crash independently. The plan provides no relay goroutine failure isolation or restart policy, and the verification plan (§5) tests neither relay failure injection nor daemon-relay interaction under feed toggle. This is a materially simpler design that achieves the same stated goal: implement standby mode and feed toggle now; defer relay embedding to its own ledger where relay failure modes can be properly specified and tested.

**Grounds:** §1 "running `tickhub daemon` and `tickhub relay-server` as two separate processes adds unnecessary operational surface" — but the plan's own stated goal is session safety at cutover, not process count. §5 verification plan has no relay failure test. §4 `daemon.go` change adds relay lifecycle without specifying restart/supervision policy.

---

### 4. Non-Blocking Observations

1. §4 `FeedController` interface has `error` in `FeedStatus()` return — a pure status query that returns an error suggests the manager can be in a broken-indeterminate state; clarify what error means here.
2. §5 smoke test uses `--metrics-addr :9099` and `--control-addr 127.0.0.1:9099` — confirm the control and metrics server share the same port (otherwise the smoke test is wrong as written).
3. `POST /control/feed?action=enable` with a query param for action rather than distinct URL paths is mildly non-idiomatic REST; not a defect but worth noting for CLI parity.
4. `--feed-enabled` default `true` means operators who forget the flag on restart will immediately re-establish the Massive.com session — the safe default for the stated scenario would be `false`.
5. The clobber guard (`publishLatencyNS > 0`) silently drops a zero latency measurement if one ever legitimately occurs; the guard is correct for the heartbeat case but the comment should note this exclusion explicitly.

---

### 5. Methodological & Data-Alignment Concerns

N/A — this ledger touches no projection math, SHM layout, ABI boundaries, or interval semantics. The `AnchorPublishLatencyNS` clobber guard keeps the field on Producer Cache Line (offset 64) per §2.2; no alignment change is introduced.

---

### 6. Missing Telemetry

The verification plan (§5) has no assertion that `HeartbeatNS` is still being updated at 250ms cadence while the feed is disabled, and no assertion that relay clients connected *before* a feed disable remain connected afterward. Both are required to know that standby mode actually works as specified in §1.1 and §2.3. Add these two assertions to the smoke test before the `feed enable` step.

---

### 7. Verdict

**Verdict: Simplify-first**

The feed-control and clobber-guard changes are tight and correctly motivated. The embedded relay is not required by the stated goal, introduces daemon-crash risk from relay goroutine failures without any supervision/restart policy, and its verification is absent from the test plan. Remove it from this ledger; open a dedicated relay-embedding ledger where failure isolation can be specified. The remaining plan — standby mode, HTTP control endpoint, CLI subcommands, clobber guard — is then a clean, reviewable unit and should proceed.
