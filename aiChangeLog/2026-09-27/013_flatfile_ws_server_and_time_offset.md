# 013 — Massive.com Flat File WebSocket Playback Server and Daemon Time Offset

## Goal & Context
Provide an authentic, end-to-end testing environment for TickHub and downstream trading engine on the GCP VM (`ccm-live-1`) over the weekend without live markets.
1. Download bonafide raw SIP flat files from Massive.com (Friday 2026-09-25 `trades_v1` and `quotes_v1`).
2. Implement a memory-bounded streaming filter utility (`scripts/filter_massive_flatfile.py`) that extracts the 72-symbol live universe (`config/live_72.yaml`) from raw Massive CSV.GZ streams and outputs a unified, chronologically sorted flat file using $O(1)$ heap memory.
3. Implement a standalone WebSocket server (`scripts/flatfile_ws_server.py`) that serves the Massive.com WebSocket wire protocol (`auth` handshake, `subscribe` confirmation, quote `Q` and trade `T` JSON event frames). Playback operates in uncompressed 1x real-time driven by elapsed stream time. The server drops prior rows and fast-forwards to virtual "now" ($T_{\text{wall}} + \Delta$) only when a client connects.
4. Add a canonical time offset option (`--time-offset` flag and `TICKHUB_TIME_OFFSET_NS` environment variable) to `tickhub daemon` (`cmd/tickhub/daemon.go`), shifting start anchor and 1Hz cadence ticker flushes to align with the virtual playback timeline.

## SOLID Adherence
- **Single Responsibility Principle (SRP)**:
  - `scripts/filter_massive_flatfile.py`: Solely responsible for streaming and filtering raw Massive CSV.GZ archives, extracting target symbols, and writing a sorted, merged flat file.
  - `scripts/flatfile_ws_server.py`: Solely responsible for WebSocket protocol emulation and timed 1x real-time streaming to connected clients.
  - `cmd/tickhub/daemon.go`: CLI entrypoint; passes configured time offset into start anchor and cadence ticker without mutating internal projection math.
- **Open/Closed Principle (OCP)**:
  - `Projector` and `shm.Producer` remain untouched; time offset modifies the external wall-clock anchor inputs to `SetStartAnchor()` and `Flush()`.
- **Liskov Substitution Principle (LSP)**:
  - `flatfile_ws_server.py` produces JSON arrays adhering strictly to `RawMassiveEvent` schema parsed by `feed.MassiveWSClient` (`pkg/feed/massive_ws.go`).
- **Interface Segregation Principle (ISP)**:
  - WebSocket clients only interact with standard Polygon/Massive auth and subscribe actions.
- **Dependency Inversion Principle (DIP)**:
  - Time progression is decoupled from OS wall clock via explicit offset delta $\Delta$.

## Proposed Code Changes

### [ADD] `scripts/filter_massive_flatfile.py`
- Command-line utility to filter raw Massive trades and quotes `.csv.gz` files:
  - Inputs: `--trades <path>`, `--quotes <path>`, `--config <path>` (default `config/live_72.yaml`), `--output <path>`.
  - Parses target 72 symbols from config YAML into a fast lookup set.
  - Memory-bounded streaming merge ($O(1)$ memory):
    - Uses streaming `gzip.open` line iterators over trades and quotes.
    - Yields parsed records filtered by target symbol universe.
    - Merges streams using a 2-way priority queue (`heapq.merge` with key `(sip_timestamp_ns, 0 if quote else 1)`), enforcing quote-before-trade tie-breaking on identical nanoseconds.
  - Writes a compressed CSV (`.csv.gz`) with explicit columns:
    `event_type,symbol,sip_timestamp_ns,bid_px,bid_sz,ask_px,ask_sz,trade_px,trade_sz`
    - Trade ('T'): `T,symbol,sip_timestamp,0,0,0,0,price,size`
    - Quote ('Q'): `Q,symbol,sip_timestamp,bid_price,bid_size,ask_price,ask_size,0,0`

### [ADD] `scripts/flatfile_ws_server.py`
- Real-time 1x playback WebSocket server:
  - Canonical interface:
    - `--flatfile`: Path to filtered flat file.
    - `--time-offset-s`: Canonical time offset in seconds ($\Delta = T_{\text{virtual}} - T_{\text{wall}}$).
    - `--speed`: Speed multiplier (default 1.0 for 1x real-time).
    - `--host`, `--port`: Bind address (default `0.0.0.0:8765`).
  - Wire protocol & connection lifecycle:
    - Waits for client connection (idle before connection).
    - Handles auth handshake:
      - Client sends: `{"action":"auth", "params":"..."}`
      - Server responds: `[{"ev":"status","status":"auth_success"}]`
    - Handles subscription handshake:
      - Client sends: `{"action":"subscribe", "params":"..."}`
      - Server responds: `[{"ev":"status","status":"success"}]`
    - Fast-forward & stream start:
      - At subscription completion, captures:
        `stream_start_wall = time.time()`
        `virtual_start_ns = int((stream_start_wall + offset_s) * 1e9)`
      - Iterates through flat file, dropping all rows with `sip_timestamp_ns < virtual_start_ns`.
      - Logs drop count and starting virtual timestamp.
    - 1x Real-Time pacing formula (elapsed-time based):
      - For each event with timestamp `t_ns`:
        `event_virtual_offset_s = (t_ns - virtual_start_ns) / 1e9`
        `target_wall_time = stream_start_wall + (event_virtual_offset_s / speed)`
        `sleep_duration = target_wall_time - time.time()`
        `if sleep_duration > 0.0005: await asyncio.sleep(sleep_duration)`
      - Tracks running lag: `lag_ms = max(0.0, (time.time() - target_wall_time) * 1000.0)`.
    - Batching & dispatch:
      - Groups events occurring in the same sub-millisecond burst into JSON array payloads (up to 50 events or 10ms boundary) matching Polygon/Massive wire format:
        - Quote: `{"ev":"Q","sym":sym,"bp":bp,"ap":ap,"bs":bs,"as":as,"t":t_ns//1_000_000}`
        - Trade: `{"ev":"T","sym":sym,"p":p,"s":s,"t":t_ns//1_000_000}`

### [MODIFY] `cmd/tickhub/daemon.go`
- Canonical time offset interface:
  - Add `--time-offset` CLI flag (string, parsed as Go duration e.g. `-50h30m` or integer seconds/nanoseconds).
  - Add environment variable fallback `TICKHUB_TIME_OFFSET_NS`.
  - Resolution precedence:
    1. If `--time-offset` flag is non-empty, parse duration via `time.ParseDuration(val)`. If that errors, parse as integer nanoseconds or seconds.
    2. Else if `TICKHUB_TIME_OFFSET_NS` env var is set and non-empty, parse as `strconv.ParseInt(val, 10, 64)`.
    3. Else default `timeOffsetNS = 0`.
- Anchor & cadence coordination:
  - Calculate `effectiveStartNS := time.Now().UnixNano() + timeOffsetNS`.
  - Call `projector.SetStartAnchor(effectiveStartNS)`.
  - In cadence ticker loop:
    `nowNS := now.UnixNano() + timeOffsetNS`
    `wallAnchor := (nowNS / cadenceNS) * cadenceNS`
    `if wallAnchor > lastFlushedAnchor { _ = projector.Flush(wallAnchor); lastFlushedAnchor = wallAnchor }`
  - Telemetry: Log `[DAEMON] Configured time offset: %v (effective virtual now: %s)`.

### [ADD] `tests/test_flatfile_ws_server.py`
- Integration test with explicit verification thresholds:
  - Generates synthetic test flat file with 500 interleaved quotes and trades across 3 symbols over a 5-second simulated span.
  - Tests filter utility streaming merge, verifying output row count and quote-before-trade ordering on timestamp collisions.
  - Starts `flatfile_ws_server.py` with `--time-offset-s`.
  - Connects using `feed.MassiveWSClient` in Go or Python `websockets`.
  - Verifies:
    1. Handshake: auth succeeds and subscribe returns success within 1.0s.
    2. Fast-forward: events prior to `virtual_start_ns` are discarded.
    3. Pacing: measured playback elapsed time matches stream duration within $\pm 50\text{ms}$ tolerance at `speed=1.0` and `speed=5.0`.
    4. Data integrity: received `bp`, `ap`, `p`, `s` match source records.

## Telemetry Plan
- `flatfile_ws_server.py`:
  - `[WS-SERVER] Client connected from {addr}`
  - `[WS-SERVER] Fast-forwarded to virtual_start_ns={ts} (dropped {N} rows in {dt:.2f}s)`
  - `[WS-SERVER] Replaying at {speed}x real-time (offset: {offset_s}s)`
  - Periodic telemetry (every 10s):
    `[WS-SERVER] emitted={emitted_count} rate={rate:.1f}/s lag_ms={lag_ms:.1f} current_vtime={iso_vtime}`
  - `[WS-SERVER] Client disconnected. Stream ended.`
- `cmd/tickhub/daemon.go`:
  - `[DAEMON] Configured time offset: {offset} (effective virtual now: {timestamp})`

## Verification Plan
1. **Plan Review**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/planreview.py aiChangeLog/2026-09-27/013_flatfile_ws_server_and_time_offset.md`
2. **Go Unit & Race Tests**:
   - `/mnt/wc/go/bin/go test -v -race ./...`
3. **Integration Test**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u tests/test_flatfile_ws_server.py`
4. **Full Verification Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/validate_all.py`
5. **Code Review Gate**:
   - `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/codereview.py aiChangeLog/2026-09-27/013_flatfile_ws_server_and_time_offset.md`

## Checklist
- [ ] Working tree clean before implementation
- [ ] Raw flat file download complete and verified
- [ ] Pre-filter utility tested on 72 symbols with $O(1)$ memory
- [ ] WebSocket server tested with elapsed-time 1x real-time pace and fast-forward
- [ ] Daemon time offset verified with projection anchors
- [ ] Zero heap allocations on hot path preserved
- [ ] All tests passing
