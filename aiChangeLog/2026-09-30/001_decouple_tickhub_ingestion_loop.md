# Ledger 001: Multi-Phase Timer Flush Alignment and Starvation-Free Check-After-Ingest in TickHub

## 1. Goal & Context
During live trading on 2026-09-30 (10:22:54–10:23:55 ET), high-volume market bursts triggered `TICKHUB-publish-latency` alerts (latency spikes of 530–1195ms) and downstream engine `SLO-L3-systemic` entries pauses (watermark lag: 1489.9ms).

### Measurement
- **Cause**: At 10:23:08 ET, live telemetry logged watermark lag of 1489.9ms and tickhub publish latency > 500ms.
- **Attribution**:
  1. **Phase Offset Stride Bug in Timer Flush**: In `cmd/tickhub/daemon.go:338`, the timer flush calculated `wallAnchor := (nowNS / cadenceNS) * cadenceNS`. For a 1s cadence, `wallAnchor` only increments at integer seconds (0ms offset). Phase 1 (offset 500ms) anchors (`T + 500ms`) were NEVER flushed by `ticker.C` until `T + 1000ms`, creating an inherent 500ms latency floor whenever incoming ticks for Phase 1 symbols were quiet or delayed.
  2. **Lack of Watermark Buffer Subtraction in Timer**: `projector.Flush()` was called with integer `wallAnchor` rather than `nowNS - watermarkBufferNS`, omitting the configured 50ms watermark latency buffer.
  3. **Timer Starvation in Select**: In `cmd/tickhub/daemon.go:328`, WebSocket channel ingest and the 5ms flush timer shared an unprioritized Go `select` loop.

### Solution (Simplify-First, Starvation-Free Check-After-Ingest)
1. **Multi-Phase Target Alignment**: In `cmd/tickhub/daemon.go`, replace integer-second `wallAnchor := (nowNS / cadenceNS) * cadenceNS` with `flushTarget := nowNS - watermarkBufferNS`. In `pkg/project/projector.go:350`, `projector.Flush(flushTarget)` inspects all phases independently via `p.phaseNextAnchor[pIdx] <= targetEndNS` without re-deriving an integer stride, closing both 0ms and 500ms phases within 5ms of crossing their watermark buffer.
2. **Check-After-Ingest Pattern**: Inside `case tick := <-feedMgr.Ticks():`, immediately perform a non-blocking check on `ticker.C`:
   ```go
   select {
   case now := <-ticker.C:
       flushNow(now)
   default:
   }
   ```
   This guarantees that if the 5ms timer fired while incoming ticks were streaming, it is serviced immediately after that tick.
   When no ticks are arriving, the outer `select` blocks on `case now := <-ticker.C:` and flushes on the 5ms schedule.
   This is bidirectional starvation-free: ticks cannot starve the timer, and the timer cannot starve ticks.
3. **Zero Concurrency Overhead**: Retains `Projector`'s single-threaded event loop contract with zero data races, zero mutex overhead, and zero extra goroutines.

---

## 2. SOLID Adherence
- **Single Responsibility Principle (SRP)**:
  `daemon.go` event loop coordinates ingestion and timed frame flushing without delegating thread safety across components.
- **Open/Closed Principle (OCP)**:
  `Projector` interface and implementation remain untouched, preserving existing test invariants and bitwise parity contracts.
- **Liskov Substitution Principle (LSP)**:
  Preserves single-threaded projector semantics across all binaries (`tickhub`, `worker`, `replay`).
- **Interface Segregation Principle (ISP)**:
  No interfaces added or modified.
- **Dependency Inversion Principle (DIP)**:
  Zero external dependencies added.

---

## 3. Proposed Code Changes

### `cmd/tickhub/daemon.go`
In `runDaemon`:
- Replace lines 320-357 with `flushNow` closure and starvation-free check-after-ingest loop:
```go
	flushNow := func(now time.Time) {
		nowNS := now.UnixNano() + timeOffsetNS
		prod.PublishTelemetry(0, watermarkBufferNS, 0, tickCount)

		flushTarget := nowNS - watermarkBufferNS
		if err := projector.Flush(flushTarget); err != nil {
			log.Printf("[DAEMON] Flush error: %v", err)
		}

		if time.Since(lastReport) >= 5*time.Second {
			elapsed := now.Sub(startTime).Seconds()
			rate := float64(tickCount) / elapsed
			enabled, _ := feedMgr.FeedStatus()
			feedState := "ENABLED"
			if !enabled {
				feedState = "STANDBY"
			}
			log.Printf("[DAEMON] [%s] Ingested %d ticks (%.1f/sec), committed %d frames",
				feedState, tickCount, rate, projector.CommittedFrames())
			lastReport = now
		}
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[DAEMON] Shutdown requested. Halting...")
			prod.SetStatus(shm.StatusClosed)
			return
		case tick := <-feedMgr.Ticks():
			tickCount++
			if err := projector.IngestTick(tick); err != nil {
				log.Printf("[DAEMON] Ingest error: %v", err)
			}
			// Starvation-free check-after-ingest: if timer fired during tick burst, flush immediately
			select {
			case now := <-ticker.C:
				flushNow(now)
			default:
			}
		case now := <-ticker.C:
			flushNow(now)
		}
	}
```

---

## 4. Telemetry Plan
- `prod.PublishTelemetry` emits monotonic tick counts and heartbeat every 5ms.
- 5s logging outputs `rate` and `CommittedFrames`.
- Existing `AnchorPublishLatencyNS` tracks frame commitment duration.

---

## 5. Verification Plan
1. **Unit & Race Tests**:
   - `go test -race ./...` in `/mnt/wc/tickhub`.
2. **Parity & Integration Suite**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/validate_all.py`
3. **Plan & Code Review Gates**:
   - `scripts/planreview.py`
   - `scripts/codereview.py`

---

## 6. Plan Review Remediation

### Round 1 Findings & Dispositions
- `B3 (unmeasured goroutine split / mutex complexity) -> fixed: excised goroutine split and sync.Mutex (§1, §3)`
- `Observation 1 (mutex on IsColdStart) -> fixed: no mutex added (§3)`
- `Observation 2 (error logging in ingest loop) -> accepted-residual: existing error logging preserved (§3)`
- `Observation 3 (ingestDone leak) -> fixed: no ingestDone channel needed (§3)`

### Round 2 Findings & Dispositions
- `B2 (prioritized select starvation inversion) -> fixed: replaced outer prioritized select + continue with check-after-ingest pattern (§1, §3)`
- `Observation 4 (half-open interval check in Flush) -> fixed: confirmed Projector.Flush(targetEndNS) checks phaseNextAnchor[pIdx] <= targetEndNS directly without integer stride truncation (§1, §3)`

---

## 7. Checklist
- [x] Adversarial plan review completed (`planreview.py`)
- [x] Implement check-after-ingest loop and flushTarget in `cmd/tickhub/daemon.go`
- [x] `go test -race ./...` passes
- [x] `scripts/validate_all.py` passes 100% green
- [x] Adversarial code review completed and approved (`codereview.py`, Claude Sonnet 4.6 (Thinking), round 1)
