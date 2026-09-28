# Ledger 001: Restore Lit Trade Ingestion and Snapshot Retention in TickHub

## 1. Goal & Context
During live market trading on 2026-09-28, the trading engine's Midday Mode entry qualification was blocked because `MiddayModeExecution.get_intensity()` evaluated to 0 across all symbols. Examination revealed:
1. `compacted_trades: 0` in engine telemetry across the entire session.
2. In POSIX shared memory (`/dev/shm/tickhub_live`), all 70 symbol snapshots have `last_trade_px: 0.0`, `last_trade_sz: 0.0`, and feature index 3 (`vol_1s`) is identically 0.0.
3. `tickhub daemon` ingested ~770 quotes/sec, but 0 trades reached `Projector.IngestTick()`.

### Root Cause Analysis & Wire Type Specifications
1. **Upstream Massive.com Wire Format vs Go Struct**:
   Verbatim frames from Massive Stocks WebSocket (`wss://socket.massive.com/stocks`):
   - Quotes (`ev="Q"`):
     `{"ev":"Q","sym":"AAPL","i":[604],"bx":15,"ax":12,"bp":297.83,"ap":297.86,"bs":80,"as":80,"t":1781622375952,"pt":1781622375952,"q":44424715,"z":3}`
     - `bp`, `ap`, `bs`, `as`: JSON numbers (float64)
     - `t`: JSON integer (milliseconds epoch)
   - Trades (`ev="T"`):
     `{"ev":"T","sym":"AAPL","i":"143541","x":4,"p":297.845,"s":40,"t":1781622376036,"pt":1781622376029,"q":4938312,"z":3,"trfi":202,"trft":1781622376036,"ds":"40.0"}`
     - `p`: JSON number (float64, price)
     - `s`: JSON number (integer shares)
     - `ds`: JSON string (fractional/decimal shares, e.g. `"40.0"`, `"0.740474"`)
     - `t`: JSON integer (milliseconds epoch)
   In `pkg/feed/massive_ws.go`, `RawMassiveEvent.Ds` was declared as `float64`. When `json.Unmarshal` encountered `"ds":"40.0"`, it failed with:
   `json: cannot unmarshal string into Go struct field RawMassiveEvent.ds of type float64`.
2. **Error Swallowing**:
   In `pkg/feed/massive_ws.go:readPump()`, `ParseMassiveEvents` errors were silently dropped with `if err != nil { continue }`, concealing the failure.
3. **Snapshot Trade Deduplication & SeqLock Correctness**:
   In `SymbolSnapshot`, `snap.SIPTimestampNS` is updated on every tick (both quotes and trades).
   In `ccm/live/tickhub_feed.py`, trade detection previously compared `current_t = (t_px, t_sz, sip_ts)`. If trades were ingested, updating `sip_ts` on subsequent quotes would cause `last_t != current_t` to evaluate to true for every subsequent quote, generating phantom duplicate trades.
   We reuse the existing `Conditions uint32` field in `SymbolSnapshot` (128-byte struct, offset 100) as a monotonic trade sequence counter `t_seq`.

---

## 2. SOLID Adherence
- **Single Responsibility Principle (SRP)**:
  `pkg/feed/massive_ws.go` is strictly responsible for parsing upstream WebSocket payloads into normalized `feed.Tick` records.
  `pkg/project/projector.go` is strictly responsible for updating rolling 1Hz state and writing `SymbolSnapshot`.
- **Open/Closed Principle (OCP)**:
  `FlexFloat64` unmarshals `ds` whether sent as a string (`"40.0"`) or numeric float (`40.0`), without altering the normalized `Tick` contract.
- **Liskov Substitution Principle (LSP)**:
  Preserves `feed.Tick` structure and ABI layout of `SymbolSnapshot` (128 bytes) identically across Go, C, and Python.
- **Interface Segregation Principle (ISP)**:
  Reuses existing `SymbolSnapshot.Conditions` field as `t_seq` to avoid struct resizing or ABI version bump.
- **Dependency Inversion Principle (DIP)**:
  Maintains decoupled producer-consumer boundary over POSIX shared memory.

---

## 3. Proposed Code Changes

### `tickhub` Repository

#### [MODIFY] `pkg/feed/massive_ws.go`
- **Field Typing & Validation**:
  - `RawMassiveEvent`:
    - `Ds`: typed `FlexFloat64` (the single wire field delivered as a string).
    - `P`, `S`, `Bp`, `Ap`, `Bs`, `As`: kept strictly as `float64` (verbatim numeric in Massive wire spec).
    - `T`: kept strictly as `int64` (integer milliseconds; never converted through `float64` to prevent mantissa truncation).
  - Define `FlexFloat64`:
    ```go
    type FlexFloat64 float64

    func (f *FlexFloat64) UnmarshalJSON(data []byte) error {
        trimmed := bytes.TrimSpace(data)
        if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
            *f = 0
            return nil
        }
        if trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
            trimmed = trimmed[1 : len(trimmed)-1]
        }
        if len(trimmed) == 0 {
            *f = 0
            return nil
        }
        val, err := strconv.ParseFloat(string(trimmed), 64)
        if err != nil {
            return fmt.Errorf("invalid FlexFloat64 %q: %w", string(data), err)
        }
        *f = FlexFloat64(val)
        return nil
    }

    func (f FlexFloat64) Float64() float64 { return float64(f) }
    ```
  - In `ParseMassiveEvents`:
    - Support both JSON arrays `[...]` and single JSON objects `{...}`.
    - Explicit trade validation:
      ```go
      case "T":
          sz := ev.Ds.Float64()
          if sz <= 0 {
              sz = ev.S
          }
          if ev.P <= 0 || sz <= 0 || ev.T <= 0 {
              atomic.AddUint64(&invalidTradeCount, 1)
              continue
          }
          ticks = append(ticks, Tick{
              SIPTimestampNS: ev.T * NSPerMS,
              Symbol:         ev.Sym,
              Type:           TickTrade,
              Price:          ev.P,
              Size:           sz,
          })
      ```
      Guarantees zero-coercion cannot produce invalid `Price == 0`, `Size == 0`, or `Timestamp == 0` trades.
    - Log status events: `log.Printf("[MASSIVE-WS] Status event: status=%s message=%s", ev.Status, ev.Msg)`.
- **Logging Swallowed Errors**:
  - In `readPump()`, log parse failures with error reason and raw message preview (truncated to 120 chars):
    `log.Printf("[MASSIVE-WS] Failed to parse events: %v (raw: %s)", err, string(msg)[:min(len(msg), 120)])`.

#### [MODIFY] `pkg/project/projector.go`
- **SeqLock Atomic Safety & Field Audit**:
  - Audit of `Conditions`: Neither `tickhub` nor `src` branches on `snap.Conditions`. It exists as an unwritten field in `SymbolSnapshot` at offset 100.
  - In `updateSnapshot(uIdx int, tick feed.Tick)`:
    - Mutate the local `p.snapshots[uIdx]` struct:
      ```go
      if tick.Type == feed.TickTrade {
          snap.LastTradePx = tick.Price
          snap.LastTradeSz = tick.Size
          snap.Conditions++
      } else {
          snap.BidPx = tick.BidPx
          snap.AskPx = tick.AskPx
          snap.BidSz = tick.BidSz
          snap.AskSz = tick.AskSz
          if tick.BidPx > 0 && tick.AskPx > 0 {
              snap.Midprice = (tick.BidPx + tick.AskPx) / 2.0
              snap.Spread = tick.AskPx - tick.BidPx
          }
      }
      ```
    - Pass `snap` into `p.producer.WriteSnapshot(uIdx, snap)`.
    - In `Producer.WriteSnapshot`:
      1. `seq := atomic.AddUint64(&target.SeqLockSeq, 1)` (turns SeqLock ODD).
      2. Writes all payload fields: `target.LastTradePx = snap.LastTradePx`, `target.LastTradeSz = snap.LastTradeSz`, `target.Conditions = snap.Conditions`.
      3. `atomic.StoreUint64(&target.SeqLockSeq, seq+1)` (turns SeqLock EVEN).
      This guarantees `Conditions` increment is published strictly INSIDE the SeqLock memory-barrier write region.

#### [MODIFY] `pkg/feed/massive_ws_test.go`
- Add unit test `TestParseMassiveEvents_TradesAndStrings`:
  - Trade with `"ds":"40.0"` (string) parses to `TickTrade` with `Size == 40.0`.
  - Trade with `"ds":40.0` (number) parses to `TickTrade` with `Size == 40.0`.
  - Trade with `"s":100` and missing `ds` parses to `TickTrade` with `Size == 100.0`.
  - Trade with invalid `p <= 0`, `sz <= 0`, or `t <= 0` is rejected and not emitted.
  - Invalid non-empty string in `ds` returns unmarshal error.
  - Single JSON object `{...}` parses correctly.

#### [MODIFY] `pkg/project/projector_test.go`
- Add unit test verifying:
  - Ingesting a quote updates bid/ask/midprice and leaves `snap.Conditions` unchanged.
  - Ingesting a trade updates `LastTradePx`/`LastTradeSz` and increments `snap.Conditions`.
  - Ingesting subsequent quotes does not alter `snap.Conditions`, `LastTradePx`, or `LastTradeSz`.

---

### `src` Repository (CCM Engine)

#### [MODIFY] `ccm/live/tickhub_feed.py`
- In `TickHubLiveFeed.pump()`:
  - Read `t_seq = int(snap.get("conditions", 0))`.
  - Form dedup key: `current_t = t_seq`.
  - Only dispatch `on_trade` when `t_px > 0.0 and t_sz > 0.0 and t_seq > 0 and self._last_trade.get(sym) != t_seq`.
  - Set `self._last_trade[sym] = t_seq`.
  - When quote ticks arrive between trades, `t_seq` does not change, completely eliminating phantom duplicate trades.

#### [MODIFY] `tests/ccm/test_tickhub_feed.py`
- Add unit test verifying that consecutive quotes sharing the same `t_seq` do not trigger duplicate `on_trade` callbacks.

---

## 4. Telemetry Plan
- `tickhub daemon`:
  - `invalid_trade_count` counter incremented and logged on invalid trade drops.
  - Ingestion report logs `log.Printf("[DAEMON] [%s] Ingested %d ticks (%.1f/sec), committed %d frames", ...)`.
  - Parsing errors logged to stderr with raw message preview: `[MASSIVE-WS] Failed to parse events: %v (raw: %s)`.
  - Status messages logged: `[MASSIVE-WS] Status event: status=%s message=%s`.
- `ccm-live-engine`:
  - Emits `feed_tick` with `kind: "T"` to `feed_capture_*.jsonl` when capture active.
  - `self._c["trades"]` increments in `TickHubLiveFeed`.
  - `compacted_trades` increments in `engine_feed_*.jsonl`.

---

## 5. Verification Plan
1. **Local Go Unit & Race Tests**:
   - `go test -v -race ./pkg/feed/... ./pkg/project/... ./pkg/shm/...`
   - Assert all tests pass including new `TestParseMassiveEvents_TradesAndStrings`.
2. **Local C & Python ABI Validation**:
   - Run `python scripts/validate_all.py` (C compilation, ABI struct sizes, Go binaries).
   - Ensure struct size of `SymbolSnapshot` remains exactly 128 bytes.
3. **Local Engine Feed Tests**:
   - Run `pytest -v tests/ccm/test_tickhub_feed.py` in `src`.
4. **Live Canary Validation on VM (`ccm-live-1`)**:
   - Backup existing binary: `cp /usr/local/bin/tickhub ~/tickhub_bin.bak`.
   - Stage new binary to `/usr/local/bin/tickhub`.
   - Update `ccm/live/tickhub_feed.py` on VM.
   - Restart `tickhub.service` (`sudo systemctl restart tickhub.service && tickhub feed enable`).
   - Monitor live telemetry for 60 seconds:
     - Check `journalctl -u tickhub.service -n 50 --no-pager` for active tick ingestion without parse errors.
     - Probe `/dev/shm/tickhub_live` via `TickHubReader`: assert `last_trade_px > 0` and `last_trade_sz > 0`.
     - Check feature index 3 (`vol_1s`): assert non-zero on active symbols.
     - Check engine telemetry: assert `compacted_trades > 0`.
     - Check Midday mode: assert `intensity > 0`.
   - **Rollback Criterion**:
     If `compacted_trades` remains 0 or if `journalctl` shows persistent parse errors after 60s of active feed:
     1. Restore `cp ~/tickhub_bin.bak /usr/local/bin/tickhub`.
     2. Restart `sudo systemctl restart tickhub.service && tickhub feed enable`.
     3. Fall back to legacy feed via `python3 /opt/ccm/src/deploy/vm/switch_feed.py --to legacy`.

---

## 6. Checklist
- [ ] Working tree clean in both repos before plan review
- [ ] Plan review passed
- [ ] Implement `FlexFloat64` and `ParseMassiveEvents` updates in `pkg/feed/massive_ws.go`
- [ ] Implement `snap.Conditions++` in `pkg/project/projector.go` inside SeqLock write path
- [ ] Add unit tests in `pkg/feed/massive_ws_test.go` and `pkg/project/projector_test.go`
- [ ] Update `ccm/live/tickhub_feed.py` trade detection with `t_seq`
- [ ] Add test in `tests/ccm/test_tickhub_feed.py`
- [ ] Run `validate_all.py`
- [ ] Code review gate passed
- [ ] Deploy canary to VM and verify live telemetry
