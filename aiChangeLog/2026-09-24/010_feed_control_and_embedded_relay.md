# Technical Strategy Ledger: Dynamic Feed Control and Safe Prod Standby Mode

- **Date**: 2026-09-24
- **Feature**: Dynamic Feed Control and Safe Standby Mode (Remediated per Plan Review Round 1)
- **Status**: Proposed
- **Author**: Antigravity / Pair Programming
- **Affected Systems**: `cmd/tickhub/daemon.go`, `cmd/tickhub/feed_ctl.go`, `cmd/tickhub/main.go`, `pkg/metrics/server.go`, `pkg/shm/producer.go`

---

## 1. Goal & Context

When TickHub runs on the production GCP VM (`ccm-live-1`), it must coexist safely with the legacy production trading engine until the operator explicitly flips the switch to cut over to TickHub.
Massive.com enforces strict limits on concurrent WebSocket sessions per API key. If TickHub and the legacy engine connect simultaneously with the same credentials, connection flapping or session drops could disrupt production trading.

Per Plan Review Round 1 (Simplify-First), embedded relay server is decoupled and removed from this ledger to preserve process crash isolation. The standalone `tickhub relay-server` continues reading SHM independently.

This ledger implements:
1. **Safe Standby Mode (`--feed-enabled=false` default)**:
   - `tickhub daemon` initializes `/dev/shm/tickhub_live`, runs the 250ms heartbeat ticker, hosts Prometheus metrics, and allows external relay servers/readers to attach — while holding the WebSocket connection strictly closed.
2. **Dynamic Feed Control (No Daemon Restart)**:
   - Control endpoints on the daemon's internal HTTP metrics server (port 9090):
     - `GET /control/feed`: returns current feed status (`ENABLED` or `DISABLED`), ingested ticks, and connection state.
     - `POST /control/feed?action=enable`: dynamically launches the Massive.com WebSocket client inside the running daemon.
     - `POST /control/feed?action=disable`: cleanly terminates the WebSocket client, returning the daemon to standby mode while keeping `/dev/shm` and heartbeats alive.
   - CLI management subcommands:
     - `tickhub feed status [--control-addr 127.0.0.1:9090]`
     - `tickhub feed enable [--control-addr 127.0.0.1:9090]`
     - `tickhub feed disable [--control-addr 127.0.0.1:9090]`
3. **Latency Telemetry Clobber Guard**:
   - In `pkg/shm/producer.go:PublishTelemetry`, only update `AnchorPublishLatencyNS` if `publishLatencyNS > 0`. Prevents 250ms heartbeat ticks from zeroing out the last measured frame publish latency.

---

## 2. SSoT & Architectural Invariants

1. **Single Source of Truth**:
   The Go daemon remains the sole writer to `/dev/shm/tickhub_live`. Feeding ticks into `projector.IngestTick` occurs exclusively through the daemon event loop.
2. **Dual Cache-Line Isolation**:
   `AnchorPublishLatencyNS` resides on Producer Cache Line (offset 64). Updating it stays strictly on the producer cache line.
3. **Graceful Standby Semantics**:
   When the feed is disabled, the daemon continues publishing 250ms heartbeats (`HeartbeatNS`) and advancing time if needed, keeping downstream readers aware that the daemon is alive in standby mode.
4. **Zero Engine Code in TickHub**:
   The `tickhub` repository remains strictly focused on high-performance ingestion, 1Hz projection, shared memory, and cross-machine relay. No application-level CCM strategy or broker logic is introduced into `tickhub`.

---

## 3. SOLID Principles Adherence

- **Single Responsibility Principle (SRP)**:
  `FeedManager` encapsulates runtime lifecycle of the Massive WebSocket client (connect, disconnect, status) without cluttering the 1Hz projection loop.
- **Open/Closed Principle (OCP)**:
  Metrics HTTP server handles `/metrics` and delegates `/control/feed` to the `FeedController`.
- **Interface Segregation Principle (ISP)**:
  Feed control interface exposes only `FeedStatus()`, `EnableFeed()`, and `DisableFeed()`.

---

## 4. Proposed Code Changes

### [ADD] `cmd/tickhub/feed_ctl.go`
- Implements `runFeedCtl(args []string)` handling `status`, `enable`, `disable`.
- Connects to daemon HTTP control port (default `127.0.0.1:9090`) and executes action, printing human-readable and scriptable status.

### [MODIFY] `cmd/tickhub/main.go`
- Registers `feed` subcommand in CLI dispatch (`status`, `enable`, `disable`).

### [MODIFY] `pkg/metrics/server.go`
- Adds `FeedController` interface:
  ```go
  type FeedController interface {
      FeedStatus() (enabled bool, ticks uint64)
      EnableFeed(ctx context.Context) error
      DisableFeed(ctx context.Context) error
  }
  ```
- Exposes `/control/feed` handler supporting GET (status) and POST (enable/disable).

### [MODIFY] `cmd/tickhub/daemon.go`
- Adds `--feed-enabled` boolean flag (default `false` for safe standby on prod; can be passed `true` or enabled via CLI).
- Implements `FeedManager` wrapping `feed.MassiveWSClient` with thread-safe start/stop lifecycle.
- In single-threaded event loop, reads from dynamic tick channel.

### [MODIFY] `pkg/shm/producer.go`
- In `PublishTelemetry`: guard `if publishLatencyNS > 0` before storing into `p.header.AnchorPublishLatencyNS`.

---

## 5. Telemetry & Verification Plan

### Automated Verification
1. **Unit & Race Tests**:
   - `go test -v -race ./pkg/... ./cmd/...`: verify feed manager synchronization, start/stop transitions, and metrics server control endpoints with zero data races.
2. **Full Validation Gate**:
   - `python scripts/validate_all.py`: all 7 stages must pass green.
3. **CLI & Standby Smoke Test**:
   - Run `tickhub daemon --feed-enabled=false --metrics-addr :9099`.
   - Assert `HeartbeatNS` advances every 250ms in SHM while feed is disabled.
   - Run `tickhub feed status --control-addr 127.0.0.1:9099` -> assert `DISABLED`.
   - Run `tickhub feed enable --control-addr 127.0.0.1:9099` -> assert transitions to `ENABLED`.
   - Run `tickhub feed disable --control-addr 127.0.0.1:9099` -> assert transitions to `DISABLED`.
   - Assert `HeartbeatNS` continues advancing without interruption.

---

## 6. Checklist
- [x] Clean git working tree verified before opening ledger
- [x] B4 defect remediated: embedded relay removed per Plan Review Round 1
- [ ] Adversarial plan review (`scripts/planreview.py`) run and APPROVED
- [ ] Surgical implementation of feed controller, CLI, and latency clobber guard
- [ ] Full validation gate (`scripts/validate_all.py`) green
- [ ] Adversarial code review (`scripts/codereview.py`) run and APPROVED
- [ ] Git commit and push
