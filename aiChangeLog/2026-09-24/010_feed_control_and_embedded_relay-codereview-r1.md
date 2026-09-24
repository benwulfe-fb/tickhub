# Code Review — aiChangeLog/2026-09-24/010_feed_control_and_embedded_relay.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Verdict (parsed): DENIED · Generated: 2026-09-24 22:18:30Z · Tool: codereview.py (Antigravity `agy`)_

---

DENIED

## 1. Summary of the Implemented Change

`daemon.go`: introduces `FeedManager` struct that wraps `feed.MassiveWSClient` with a `sync.Mutex` + `atomic.Uint64` tick counter. Adds `--feed-enabled` (default `false`) and `--api-key-file` flags. At boot, if `--feed-enabled=true`, calls `feedMgr.EnableFeed`; otherwise enters standby. Event loop reads from `feedMgr.Ticks()` (persistent channel). `DisableFeed` calls `clientCancel()` + `client.Close()`, sets `enabled=false`. A background goroutine per `EnableFeed` drains the underlying WS client channel into `fm.tickCh`.

`pkg/metrics/server.go`: adds `FeedController` interface, `feedCtrl`/`feedMu` (RWMutex) fields to `Server`, `SetFeedController`, and `handleControlFeed` handler supporting `GET /control/feed` (status) and `POST /control/feed?action=enable|disable`.

`cmd/tickhub/feed_ctl.go`: CLI glue; issues HTTP GET/POST to daemon control port. Defaults `127.0.0.1:9090`.

`pkg/shm/producer.go`: guards `AnchorPublishLatencyNS` store with `publishLatencyNS > 0`.

`pkg/metrics/server_test.go`: adds `TestControlFeedEndpoint` exercising the new handler.

`cmd/tickhub/feed_mgr_test.go`: exercises `FeedManager` lifecycle with empty API key.

---

## 2. Correctness & Concurrency Bugs

### B1 — Busy-spin / receive-from-nil NOT applied: plan review observation #1 unresolved (`daemon.go:424`)

Plan review observation 1 explicitly states:

> Event loop in `cmd/tickhub/daemon.go` must set tick channel reference to `nil` while disabled to avoid busy-spin on closed channel.

The event loop selects directly on `feedMgr.Ticks()`:

```go
case tick := <-feedMgr.Ticks():
```

`Ticks()` returns `fm.tickCh`, which is a **single, permanent buffered channel created at `NewFeedManager` and never replaced**. When the feed is disabled, the background goroutine exits (context cancelled), so no new ticks are written. The channel stays open and non-nil; `select` will block on it correctly in normal operation. **However**, the original plan review concern was about the event loop blocking on a nil-able reference after disable, and the fix in Go terms is `case tick := <-ticksCh` where `ticksCh` is a local variable set to `nil` when disabled. The implementation instead keeps a permanent shared channel — **this is acceptable in isolation**, but it means:

- If `Enable` is called a second time (re-enable), a **new background goroutine** is launched and starts writing to the **same `fm.tickCh`**. The old goroutine may still be draining the old `client.Ticks()` channel (context cancelled but goroutine may not have exited yet before the new one starts). There is a **race window** where two goroutines write to `fm.tickCh` concurrently — that is safe (channel is goroutine-safe) but the old goroutine may deliver stale ticks from the prior session into the new session's ingestion pipeline.

### B2 — Re-enable goroutine leak / stale-tick contamination (`daemon.go:278–296`)

On `EnableFeed` called a second time after `DisableFeed`:
- `clientCancel()` was called, which signals `cCtx.Done()` in the prior goroutine.
- But `EnableFeed` creates a **new** `clientCtx` from `context.Background()`, not from any outer context. There is no synchronization (`WaitGroup`, channel close) ensuring the old goroutine has exited before the new one starts.
- The old goroutine could still be alive (blocked on `ticks` channel from the old client) and send one last tick to `fm.tickCh` after the new feed is live, injecting a tick from the old session into the new session.
- **Net result**: tick contamination and an unpredictable number of goroutines alive simultaneously across repeated enable/disable cycles — a goroutine leak under repeated cycling.

### B3 — `EnableFeed` ignores the passed `ctx` argument entirely (`daemon.go:267`)

```go
clientCtx, clientCancel := context.WithCancel(context.Background())
```

The `ctx` parameter passed into `EnableFeed` is ignored. The daemon passes the signal context `ctx` here:

```go
if err := feedMgr.EnableFeed(ctx); err != nil {
```

But the internal client context is derived from `context.Background()`, so **SIGTERM will not propagate to the feed client through the context chain**. The daemon's `defer feedMgr.Close()` and the explicit `feedMgr.Close()` in the `ctx.Done` case do call `DisableFeed`, which cancels the internal context — but this is an unnecessary decoupling that violates the principle of context propagation and creates a subtle race: the signal handler fires, `cancel()` fires, the event loop reaches `case <-ctx.Done()`, calls `feedMgr.Close()`, which calls `DisableFeed`. But `defer feedMgr.Close()` also fires after `return`. `DisableFeed` is idempotent via the `enabled` guard — acceptable but redundant and slightly confusing.

The deeper issue: the ignored `ctx` means future callers who pass a deadline-bound context expecting it to bound the connection attempt (e.g., in a test or controlled environment) will not get cancellation propagation.

### B4 — Double call to `feedMgr.Close()` at shutdown (`daemon.go:415, 362`)

```go
defer feedMgr.Close()           // line ~362
...
case <-ctx.Done():
    feedMgr.Close()             // line ~415
    prod.SetStatus(shm.StatusClosed)
    return
```

`DisableFeed` is idempotent due to the `enabled` guard, so this is not a correctness bug per se. But `feedMgr.Close()` acquires `fm.mu`; calling it in both the deferred path and the explicit path means the deferred `Close()` runs on an already-disabled manager. Benign, but noisy and unnecessary. Minor.

### B5 — `handleControlFeed`: `w.Header().Set` before `w.WriteHeader` in the error path for `nil` controller (`server.go:540–542`)

```go
if fc == nil {
    http.Error(w, `{"error":"feed controller not registered"}`, http.StatusNotImplemented)
    return
}
w.Header().Set("Content-Type", "application/json")
```

`http.Error` internally sets `Content-Type: text/plain; charset=utf-8` and calls `WriteHeader`. The JSON content-type set below is never reached for the nil case, but the error body is a raw JSON string served as `text/plain`. Clients parsing `Content-Type: application/json` will fail. Minor but inconsistent.

### B6 — `Ticks()` method exposes write-end of channel without synchronization (`daemon.go:252–254`)

```go
func (fm *FeedManager) Ticks() <-chan feed.Tick {
    return fm.tickCh
}
```

`fm.tickCh` is set once at construction and never replaced — so no data race here. This is fine.

### B7 — `tickCount` in `FeedManager` is never reset between enable/disable cycles (`daemon.go:288`)

`fm.tickCount.Add(1)` accumulates across all sessions. `FeedStatus()` returns this value as `ticks`. After a disable+re-enable, the returned tick count will not reflect "ticks in current session" but rather cumulative since daemon start. This is a UX confusion but not a correctness bug for the system. The plan does not specify reset semantics, so this is a non-blocking observation — noted only.

---

## 3. Projection Math & Temporal Parity

No projection math changes in this diff. The `IngestTick` call site is unchanged; the same tick values flow to the same projector. The `PublishTelemetry` change (latency clobber guard) is correct — it prevents overwriting a valid latency measurement with `0` from heartbeat ticks. No look-ahead leakage, symbol ordering, or forward-fill logic is touched. No concerns.

---

## 4. Deviations from the Approved Plan

### D1 — Plan Review Observation #1 NOT remediated (critical)

Plan review round 2, observation 1:

> Event loop in `cmd/tickhub/daemon.go` must set tick channel reference to `nil` while disabled to avoid busy-spin on closed channel.

The implementation does not nil the local tick reference on disable. Instead, it uses a permanent shared channel. While this avoids a literal busy-spin (channel blocks when empty), it does **not** remediate the reviewer's explicit concern about the event loop's behavior on disable. Moreover, the re-enable goroutine leak (B2) is a direct consequence of this architectural choice. The observation was non-blocking in the plan review, but the implementation introduced a new correctness defect (B2) by not following the guidance.

### D2 — Plan Review Observation #2 NOT remediated

Plan review round 2, observation 2:

> `FeedManager` requires internal mutex or state machine to serialize concurrent `enable`/`disable` calls and avoid duplicate dials.

A `sync.Mutex` is present and guards `EnableFeed`/`DisableFeed`. This IS remediated for the serialization requirement. However, the goroutine lifecycle (B2 — old goroutine not awaited before new one starts) means concurrent correctness is incomplete. The mutex prevents concurrent `Enable` calls, but a sequential `Disable → Enable` cycle can still have the old goroutine alive and writing to `fm.tickCh`. The `sync.Mutex` alone does not constitute a safe state machine for lifecycle management.

### D3 — Plan Review Observation #4 NOT remediated

Plan review round 2, observation 4:

> Default control port (`127.0.0.1:9090`) in `cmd/tickhub/feed_ctl.go` should share constant with default metrics bind address.

`feed_ctl.go` hardcodes `"127.0.0.1:9090"` as a string literal. `daemon.go` hardcodes `":9090"` as the default for `--metrics-addr`. These are not shared constants. The plan review explicitly flagged this. Not remediated.

---

## 5. Systems & Performance Violations

### P1 — Buffered channel is allocated in `EnableFeed` per-client, then the client channel is drained into a second buffered channel

`NewFeedManager` allocates `fm.tickCh` with capacity 65536. `EnableFeed` calls `feed.NewMassiveWSClient(..., 65536)`, which allocates its own internal 65536-item channel. A goroutine then bridges the two. This is two full 65536-element channel allocations (each `feed.Tick` struct cost × 65536 × 2). On re-enable cycles this doubles. The bridging goroutine also adds one extra copy per tick and one extra context check. For a 1Hz projection system this is entirely acceptable — not a hot-path concern.

### P2 — `handleControlFeed` POST: `FeedStatus()` called after action, acquires `fm.mu` again (`server.go:580`)

```go
err = fc.EnableFeed(r.Context())
...
enabled, ticks := fc.FeedStatus()
```

`EnableFeed` acquires and releases `fm.mu`. `FeedStatus()` then re-acquires it. Between the two calls, state could change (if another concurrent HTTP request fires). The returned status could therefore reflect a state from a concurrent caller, not the state after this call. Under normal usage (single operator), harmless. But under concurrent HTTP requests (not guarded at the HTTP layer), the response could be misleading. Not a blocking bug for this system; noted.

### P3 — `json.NewEncoder(w).Encode` without pooling

Minor allocation per request on control path. Acceptable for a low-frequency control endpoint.

---

## 6. Telemetry / Verification Gaps

### V1 — `feed_mgr_test.go` does not test re-enable after disable (goroutine leak, B2)

The test only exercises: initial-disabled, enable-fails-without-key, disable-when-disabled, ticks-channel-empty. It does not test a successful enable→disable→re-enable cycle, which is the exact path where B2 (goroutine leak) would manifest. The test with a real or stub `MassiveWSClient` is absent.

### V2 — `server_test.go` `mockFeedController` is not goroutine-safe

The `mockFeedController` has no mutex. `EnableFeed` and `DisableFeed` write `m.enabled` without synchronization. Tests are sequential, so no data race in the current tests, but running `go test -race` with concurrent HTTP handler calls would flag this.

### V3 — No test for standby heartbeat continuity

Ledger §5, verification item: "Assert `HeartbeatNS` advances every 250ms in SHM while feed is disabled." No automated test covers this in the diff. (May exist elsewhere in the test suite, but nothing in this change set verifies it.)

---

## 7. Verdict & Remediation

**DENIED** on the following blocking defects. Required remediations:

1. **[B2 / D1] Fix goroutine lifecycle in `EnableFeed`**: add a `sync.WaitGroup` (or done channel) to `FeedManager`. On `DisableFeed`, cancel the context and then `wg.Wait()` to block until the prior bridge goroutine has exited before returning. On `EnableFeed`, increment the WaitGroup before launching the goroutine, decrement on exit. This eliminates the stale-tick contamination window and goroutine leak on re-enable cycles.

   ```go
   // In FeedManager:
   wg sync.WaitGroup

   // In DisableFeed, after clientCancel():
   fm.mu.Unlock()
   fm.wg.Wait()
   fm.mu.Lock()
   // then proceed to client.Close(), enabled=false
   ```
   (Note: must drop mutex before `wg.Wait()` to avoid deadlock with the goroutine that may try to acquire `fm.mu` if it ever does — in this implementation it does not, so a simpler pattern works. Ensure `wg.Wait()` is called outside the mutex.)

2. **[D3] Share the default control port constant**: define a single package-level constant (e.g., `defaultControlAddr = "127.0.0.1:9090"`) in a shared file (e.g., `cmd/tickhub/constants.go` or inline in `daemon.go`) and reference it from both `daemon.go`'s `--metrics-addr` default (`":9090"`) and `feed_ctl.go`'s `--control-addr` default. The bind address `":9090"` and the connect address `"127.0.0.1:9090"` differ by host; document the relationship clearly in comments.

3. **[V1] Add re-enable cycle test to `feed_mgr_test.go`**: using a stub or mock `MassiveWSClient` (or by mocking at the `FeedManager` level), test `Enable → Disable → Enable` sequence and verify (a) no goroutines from prior session write ticks after disable, and (b) `tickCount` or state is consistent.

4. **[V2] Make `mockFeedController` goroutine-safe**: add `sync.Mutex` to `mockFeedController` in `server_test.go` for correctness under `-race`.

5. **[B3] Document or propagate `ctx` in `EnableFeed`**: either use the passed `ctx` as the parent for `clientCtx` (making `context.WithCancel(ctx)` instead of `context.WithCancel(context.Background())`), or document explicitly why `context.Background()` is intentional (the feed must outlive the call context). Given the daemon passes its signal context, using it as parent is correct and simpler.
