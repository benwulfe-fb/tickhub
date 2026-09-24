# Code Review — aiChangeLog/2026-09-24/010_feed_control_and_embedded_relay.md

_Model: Gemini 3.8 Flash (High) · Round: 3 · Verdict (parsed): APPROVED · Generated: 2026-09-24 22:25:29Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

1. **Summary of the Implemented Change**

Adds safe standby mode and dynamic feed lifecycle control to `tickhub`:
- `cmd/tickhub/daemon.go`: Introduces `FeedManager` wrapping `feed.MassiveWSClient`. Derives client context from persistent `rootCtx` (from `signal.NotifyContext`). Serializes state transitions with `sync.Mutex` and `sync.WaitGroup`. Bridges ticks into `feedMgr.Ticks()` consumed by single-threaded event loop. Adds `--feed-enabled` (default `false`) and `--api-key-file` flags.
- `cmd/tickhub/constants.go`: Defines shared constants `DefaultMetricsPort`, `DefaultMetricsBind`, and `DefaultControlAddr`.
- `cmd/tickhub/feed_ctl.go`: Implements CLI subcommand `tickhub feed <status|enable|disable>` targeting daemon HTTP control server.
- `pkg/metrics/server.go`: Adds `FeedController` interface, thread-safe `SetFeedController`, and `GET/POST /control/feed` endpoints.
- `pkg/shm/producer.go`: Guards `AnchorPublishLatencyNS` updates in `PublishTelemetry` with `if publishLatencyNS > 0`, preventing 250ms standby heartbeats from clearing anchor publish latency.
- `pkg/metrics/server_test.go`: Adds mutex-synchronized mock controller tests for `/control/feed`.
- `cmd/tickhub/feed_mgr_test.go`: Adds unit tests for lifecycle, multi-cycle enable/disable against mock WebSocket server, caller context cancellation isolation, and standby `HeartbeatNS` continuity.

---

2. **Correctness & Concurrency Bugs**

None found.
- Prior review B1 resolved: `EnableFeed` derives `clientCtx` from `fm.rootCtx` (`cmd/tickhub/daemon.go:409`), decoupling WebSocket lifecycle from transient HTTP `r.Context()`. Cancellation of HTTP caller context does not terminate feed connection (`cmd/tickhub/feed_mgr_test.go:TestFeedManagerCallerContextCancellation`).
- Prior review B2 resolved: `DisableFeed` retains `fm.mu` across `fm.clientCancel()`, `client.Close()`, and `fm.wg.Wait()` (`cmd/tickhub/daemon.go:447-463`). Eliminates race and concurrent `WaitGroup` misuse with overlapping `EnableFeed`. Bridge goroutine acquires no mutexes; deadlock impossible.
- Channel safety: `fm.tickCh` remains open across feed start/stop cycles. Standby mode safely blocks on `<-feedMgr.Ticks()` in event loop without busy-spinning or panic.
- Telemetry integrity: `publishLatencyNS > 0` guard (`pkg/shm/producer.go:847`) atomically preserves last measured frame publish latency during standby.

---

3. **Projection Math & Temporal Parity**

No projection math changes.
- `pkg/project` untouched. Half-open interval $[T-1\text{s}, T)$, quote-before-trade sorting, illiquid forward-fill, and feature schemas identical.
- Golden replay passes bit-identical at 109,042.8 ticks/sec.

---

4. **Deviations from the Approved Plan**

None.
- Round 1 B4 (relay server decoupling) maintained.
- Default standby mode, dynamic HTTP endpoints, CLI subcommands, and latency clobber guard implemented strictly per ledger.
- Non-blocking plan review recommendations (shared constants, regression test coverage, mutex synchronization) fully addressed.

---

5. **Systems & Performance Violations**

None.
- Zero-allocation hot path preserved in ingestion loop. Channel bridge allocates fixed 65536 buffer on startup.
- Dual cache-line isolation verified: `AnchorPublishLatencyNS`, `WatermarkBufferNS`, `DroppedTickCount`, and `TotalTickCount` update atomically on Producer Cache Line (offset `0x0040`). Consumer Cache Line (offset `0x0080`) untouched.
- `HeartbeatNS` updates atomically at offset `0x0028` on 250ms cadence.

---

6. **Telemetry / Verification Gaps**

None.
- All 7 validation stages in `scripts/validate_all.py` pass clean.
- Unit and race tests pass (`go test -v -race ./pkg/... ./cmd/...`).
- Test suite validates transient caller context cancellation (`TestFeedManagerCallerContextCancellation`), multi-cycle enable/disable transitions (`TestFeedManagerReEnableCycle`), and standby heartbeat advancement (`TestStandbyHeartbeatContinuity`).

---

7. **Verdict & Remediation**

**APPROVED**

All prior review findings (B1, B2) remediated and verified with dedicated unit tests. Concurrency, memory layout, and projection math standards satisfied. Ready to commit.
