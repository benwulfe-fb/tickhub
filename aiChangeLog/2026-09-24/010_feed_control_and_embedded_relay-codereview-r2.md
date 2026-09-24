# Code Review — aiChangeLog/2026-09-24/010_feed_control_and_embedded_relay.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Verdict (parsed): DENIED · Generated: 2026-09-24 22:22:13Z · Tool: codereview.py (Antigravity `agy`)_

---

DENIED

1. **Summary of the Implemented Change**

Change adds dynamic feed lifecycle control and safe standby mode to `tickhub`:
- `cmd/tickhub/daemon.go`: Introduces `FeedManager` wrapping `feed.MassiveWSClient` with `sync.Mutex`, `atomic.Uint64` tick counter, and `sync.WaitGroup`. Adds `--feed-enabled` (default `false`) and `--api-key-file` flags. Event loop consumes `feedMgr.Ticks()`.
- `cmd/tickhub/constants.go`: New file defining shared `DefaultMetricsPort`, `DefaultMetricsBind`, and `DefaultControlAddr`.
- `cmd/tickhub/feed_ctl.go`: CLI tool handling `tickhub feed <status|enable|disable>`. Queries daemon HTTP control endpoints.
- `pkg/metrics/server.go`: Adds `FeedController` interface and `GET/POST /control/feed` handler.
- `pkg/shm/producer.go`: Clobber guard in `PublishTelemetry` (`publishLatencyNS > 0`) prevents 250ms standby heartbeats from overwriting anchor publish latency.
- `pkg/metrics/server_test.go`: Synchronizes `mockFeedController` with `sync.Mutex`; tests `/control/feed` endpoint.
- `cmd/tickhub/feed_mgr_test.go`: Tests `FeedManager` lifecycle, mock WS server enable/disable re-enable cycles, and standby `HeartbeatNS` progression.

---

2. **Correctness & Concurrency Bugs**

### B1 — Premature feed teardown via HTTP request context cancellation (`pkg/metrics/server.go:784`, `cmd/tickhub/daemon.go:486`)

Critical defect breaking dynamic feed activation:
```go
// pkg/metrics/server.go:784
case "enable":
    err = fc.EnableFeed(r.Context())
```
```go
// cmd/tickhub/daemon.go:486-487
client := feed.NewMassiveWSClient(fm.endpoint, fm.apiKey, fm.symbols, 65536)
clientCtx, clientCancel := context.WithCancel(ctx)
if err := client.Start(clientCtx); err != nil {
```
`r.Context()` scopes strictly to incoming HTTP request. When `handleControlFeed` sends response and returns, `net/http` cancels `r.Context()`.
Consequences:
1. `clientCtx` cancelled immediately after HTTP 200 returned to `tickhub feed enable`.
2. `readPump` in `pkg/feed/massive_ws.go:230` catches `<-ctx.Done()`, exits, runs `c.conn.Close()`, and closes `c.tickChan`.
3. Bridge goroutine in `cmd/tickhub/daemon.go:503` catches `<-cCtx.Done()` and exits.
4. `fm.enabled` remains `true`.
5. WebSocket connection severed; daemon permanently stranded reporting `ENABLED` while ingestion is dead.

### B2 — State machine race and deadlock across concurrent `DisableFeed`/`EnableFeed` (`cmd/tickhub/daemon.go:524–541`)

```go
func (fm *FeedManager) DisableFeed(ctx context.Context) error {
	fm.mu.Lock()
	if !fm.enabled {
		fm.mu.Unlock()
		return nil
	}
	if fm.clientCancel != nil {
		fm.clientCancel()
		fm.clientCancel = nil
	}
	client := fm.client
	fm.client = nil
	fm.enabled = false
	fm.mu.Unlock()

	if client != nil {
		client.Close()
	}
	fm.wg.Wait()
	log.Printf("[FEED] Massive WS feed DISABLED. Daemon running in standby mode.")
	return nil
}
```
`fm.mu` unlocked before `client.Close()` and `fm.wg.Wait()`.
If concurrent `EnableFeed` executes immediately after `fm.mu.Unlock()`:
1. `EnableFeed` acquires `fm.mu`, sees `fm.enabled == false`, creates new client, calls `fm.wg.Add(1)`, starts new goroutine, and sets `fm.enabled = true`.
2. `DisableFeed` resumes and enters `fm.wg.Wait()`. WaitGroup counter increased by new goroutine; `DisableFeed` blocks waiting on new session goroutine, hanging HTTP response.
3. Concurrent `Add` and `Wait` across zero-counter transitions can trigger runtime panic (`panic: sync: WaitGroup misuse: Add called concurrently with Wait`).
Note: Bridge goroutine never touches `fm.mu`. Dropping mutex before `fm.wg.Wait()` unnecessary and unsafe.

---

3. **Projection Math & Temporal Parity**

No projection math changes. `IngestTick` path intact. Half-open interval $[T-1\text{s}, T)$, quote-before-trade sorting, and feature schemas untouched. Latency clobber guard (`publishLatencyNS > 0` in `pkg/shm/producer.go:924`) verified.

---

4. **Deviations from the Approved Plan**

- **Remediated from Round 1**:
  - D3 (shared constants): Implemented via `cmd/tickhub/constants.go`.
  - V1 (re-enable test): Implemented via `TestFeedManagerReEnableCycle`.
  - V2 (mock concurrency): Added `sync.Mutex` in `pkg/metrics/server_test.go`.
  - V3 (heartbeat test): Added `TestStandbyHeartbeatContinuity`.
  - B4 (duplicate close): Removed duplicate `feedMgr.Close()` in `daemon.go`.
  - B5 (JSON headers): Unified in `handleControlFeed`.
- **New Regression**:
  - Remediating B3 by passing `ctx` to `context.WithCancel(ctx)` in `EnableFeed` caused fatal B1 bug because `server.go` passes transient `r.Context()`.

---

5. **Systems & Performance Violations**

- P1: Channel bridging (`MassiveWSClient.Ticks()` -> `FeedManager.tickCh`) maintains two 65536 buffered allocations. Acceptable off hot path.
- P2: `handleControlFeed` re-acquires `fm.mu` in `FeedStatus()` immediately after `EnableFeed`/`DisableFeed`. Minor TOCTOU in response payload under concurrent HTTP requests.

---

6. **Telemetry / Verification Gaps**

- V1: `TestFeedManagerReEnableCycle` passes single 3-second context across entire test. Fails to simulate transient context cancellation (e.g., HTTP request lifecycle), missing B1 bug.

---

7. **Verdict & Remediation**

**DENIED**

Remediations required:

1. **Decouple feed runtime lifecycle from HTTP request context (`pkg/metrics/server.go`, `cmd/tickhub/daemon.go`)**:
   Pass root application context (from `signal.NotifyContext`) to `NewFeedManager(rootCtx, ...)`. In `EnableFeed`, derive `clientCtx` from `fm.rootCtx` (`context.WithCancel(fm.rootCtx)`), using passed `ctx` only for connect/handshake timeout if necessary. Alternatively, in `server.go`, pass `context.Background()` to `fc.EnableFeed()`.
2. **Serialize feed state transitions (`cmd/tickhub/daemon.go`)**:
   Keep `fm.mu` locked through `fm.wg.Wait()` in `DisableFeed` (since bridge goroutine does not acquire `fm.mu`), or introduce explicit state (`stateDisabled`, `stateEnabling`, `stateEnabled`, `stateDisabling`) to block overlapping `EnableFeed` while teardown runs.
3. **Add regression test for request context cancellation (`cmd/tickhub/feed_mgr_test.go`)**:
   Test `EnableFeed(cancelledCtx)` where `cancelledCtx` cancelled immediately after `EnableFeed` returns; assert `FeedManager.Ticks()` continues streaming from mock server.
