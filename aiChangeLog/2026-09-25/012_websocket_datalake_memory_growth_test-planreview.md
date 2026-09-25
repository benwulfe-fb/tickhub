# Plan Review — aiChangeLog/2026-09-25/012_websocket_datalake_memory_growth_test.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-25 14:22:20Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Proceed-with-noted-risks**_

---

## 1. Summary of the Proposal

Add Go runtime memory metrics (`heap_alloc`, `heap_inuse`, `heap_sys`, `heap_objects`, `num_gc`) and process RSS (`tickhub_process_rss_bytes` via `/proc/self/statm`) to the existing Prometheus `/metrics` endpoint in `pkg/metrics/server.go`. Add assertions for those metrics in `pkg/metrics/server_test.go`. Add a new Python integration test (`tests/test_websocket_memory_growth.py`) that: spins up a mock Massive.com WebSocket server, launches `tickhub daemon` against it streaming datalake Parquet ticks, polls `/metrics` every second, and asserts post-warmup heap slope < 50 KB/s and RSS growth < 5% of stable baseline.

---

## 2. Simplest Sufficient Design

The goal is: *verify whether the daemon leaks memory under sustained WebSocket ingestion*. The minimum viable path:

1. Add the six metrics to `server.go` — **required**, they don't exist yet.
2. Add the Python integration test — **required**, it's the stated deliverable.

The `server_test.go` unit assertions are **redundant**: if `/metrics` didn't emit the strings, the integration test would immediately fail at the metric-polling step. The unit test adds a file change and a test execution but catches nothing the integration test doesn't catch first.

**Cut list:**
| Component | Cut | Lost |
|-----------|-----|------|
| `server_test.go` assertions | Remove | Nothing: integration test covers presence of all six metrics end-to-end |

The plan is one file change too large. This is a minor B4 but worth naming.

---

## 3. Blocking Defects

**B4 — unnecessary complexity:** The `server_test.go` modification (changelog §"[MODIFY] `pkg/metrics/server_test.go`") adds unit assertions for metric presence. The integration test already boots the daemon and polls `/metrics`; any missing metric line causes a failure there first. The unit test file change is not additive safety — it is redundant test surface for a "is the string present" check that the end-to-end path exercises more faithfully (actual daemon binary, not test HTTP handler stub). Remove the `server_test.go` modification.

No B1, B2, or B3 defects.

---

## 4. Non-Blocking Observations

1. `runtime.ReadMemStats` triggers a stop-the-world GC pause on every `/metrics` scrape — polling every 1 second in the test will artificially suppress heap growth and inflate `num_gc`, potentially masking slow leaks by over-collecting.
2. The 5% RSS cap is stated relative to "initial stable baseline" but the baseline is not defined — if warmup RSS is still climbing at second 10, the baseline is meaningless; the test needs an explicit stabilization check before capturing baseline.
3. Datalake path `/mnt/wc/datalake/2026-09-23` is hardcoded with a specific date; CI on any other date will either skip or fail unless the fixture fallback path (`tests/fixtures/golden/datalake`) is always populated.
4. The mock WebSocket server sends "advancing timestamps" but the plan does not specify whether those timestamps respect the daemon's clock-based 1Hz projection boundary; if they all land in the same 1Hz window, projection cycles may not fire and the test may not exercise the hot path at all.
5. Goroutine leak detection is listed in the goal but not in any assertion — the test asserts heap and RSS but never checks goroutine count.

---

## 5. Methodological & Data-Alignment Concerns

N/A — this is a memory verification test, not a projection math or ABI change. The mock server's timestamp semantics (observation 4 above) are a test-validity concern but not a projection-correctness concern.

---

## 6. Missing Telemetry

The test must emit `goroutines_count` alongside heap/RSS samples, or the goroutine-leak goal stated in the opening paragraph is entirely unmeasured. The plan lists goroutine leaks as a target but provides no metric or assertion for it. Minimally: sample `/metrics` for `go_goroutines` (already standard in Go Prometheus clients, or can be derived from `runtime.NumGoroutine()` added alongside the heap stats) and assert post-warmup goroutine count is stable.

---

## 7. Verdict

**Verdict: Proceed-with-noted-risks**

The Go-side change is minimal and correct. The integration test design is sound. The one structural redundancy (unit test assertions duplicating end-to-end coverage) is minor enough not to block. The author should record two risks in the ledger: (a) per-scrape STW pause from `ReadMemStats` inflating GC count and suppressing observable leaks at 1s polling cadence — consider scraping at 5s or using `runtime.MemStats` cached on a background goroutine; (b) goroutine leak detection is stated as a goal but entirely absent from assertions — add `go_goroutines` sampling or the stated goal is unmet.
