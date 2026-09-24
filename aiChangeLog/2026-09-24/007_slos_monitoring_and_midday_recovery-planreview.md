# Plan Review — aiChangeLog/2026-09-24/007_slos_monitoring_and_midday_recovery.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 19:22:32Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Blocked (B2)**_

---

# Plan Review — aiChangeLog/2026-09-24/007_slos_monitoring_and_midday_recovery.md

_Round: 2 of 2 (final) · Target: `aiChangeLog/2026-09-24/007_slos_monitoring_and_midday_recovery.md`_

---

## 1. Summary of the Proposal

Formalizes 1Hz market projection engine operational SLOs (latency, snapshot read, zero local frame loss). Adds multi-tier observability: SHM producer cache-line telemetry, Prometheus HTTP server (`/metrics`, `/healthz`, `/readyz`), and ANSI terminal status inspector (`tickhub status`). Implements daemon restart recovery: warm recovery (re-attach resident SHM segment, seed up to 60 prior frames into `Projector` rolling history, atomic metadata publication) and cold recovery (reformat segment, flag `FlagColdStart` for 15s rolling return convergence). Extends ABI layout (`BootID uint64`, `Generation uint32`, `RecoveryMode uint32`, `FlagColdStart`) in Go and Python. Updates Python `TickHubReader` with liveness monitoring, transparent re-attachment, and cursor alignment. Adds Python integration test.

---

## 2. Simplest Sufficient Design

Replacing curses TUI (`cmd/tickhub/top.go`) with zero-dependency stdlib ANSI inspector (`cmd/tickhub/status.go`) resolved Round 1 over-reach.

One remaining non-essential component:

| Cut | File | Lost |
|---|---|---|
| CLI status inspector | `cmd/tickhub/status.go` | Operator convenience terminal dashboard; Prometheus HTTP endpoint already exposes all telemetry |

Core ABI extensions, HTTP server, warm/cold recovery logic, Python resilience, and integration test are load-bearing for stated goals.

---

## 3. Blocking Defects

**B2 loss (capital risk & feature corruption) / B1 goal-defeating (timeline gap reader deadlock)**

**Warm-recovery downtime gap corrupts rolling features and hangs Python reader**

- **Grounding**: §4.2 step 1:
  > "Eligibility window: `(time.Now().UnixNano() - LastWrittenAnchorNS) > 0` AND `< 60_000_000_000` (within 60 seconds of crash...)"  
  > "Step B (Read-Only Seeding): Read up to 60 previous frame slots for all phases strictly read-only to seed `Projector` rolling price history (`SeedHistory`)."  
  > "Resume WebSocket feed ingestion and 1Hz projection with zero mathematical gap."  
  §4.3 step 1:  
  > "If `Header.RecoveryMode == 1` (WarmResidentRecovered): Check if consumer's cursor anchor timestamp is still within ring buffer (`anchor >= oldest_valid_anchor`). If within buffer: update `self._boot_id = new_boot_id` ... continue without dropping cursors."

- **Mechanism**:
  1. *Producer feature corruption & capital risk (B2)*: The plan conflates pre-crash ring buffer depth (60 frames) with permissible daemon downtime. Ring buffer slots hold frames from *before* crash ($T_{\text{crash}} - 59\text{s} \dots T_{\text{crash}}$). They contain zero market data for downtime interval $(T_{\text{crash}}, T_{\text{restart}})$. If daemon is down for $\Delta t > 1\text{s}$ (e.g. 5s or 30s): new daemon seeds `Projector` with pre-crash frames ending at $T_{\text{crash}}$, then pushes first live bar at $T_{\text{restart}}$ directly after $T_{\text{crash}}$. `Projector` rolling return queue assumes consecutive 1s bars. Rolling 15s return (`ret15s`) at $T_{\text{restart}}$ evaluates current price against bar 15 steps back in queue, which actually corresponds to $T_{\text{restart}} - 15\text{s} - \Delta t_{\text{downtime}}$. The feature is mathematically corrupted across unrecorded black hole. Because daemon declared warm recovery, `FlagColdStart` is NOT asserted. Downstream live trading (`ccm-live` on GCP VM) consumes corrupted momentum/return features without warning. Direct trading capital risk.
  2. *Consumer cursor desync & hang (B1)*: If daemon is down $> 1\text{s}$, frames between $T_{\text{crash}}$ and $T_{\text{restart}}$ were never generated. In §4.3, consumer checks `cursor.anchor >= oldest_valid_anchor`. Because cursor is at $T_{\text{crash}}$ and ring buffer covers $T_{\text{crash}} - 59\text{s}$, check passes. Consumer decides "continue without dropping cursors" and requests anchor $T_{\text{crash}} + 1\text{s}$. Frame $T_{\text{crash}} + 1\text{s}$ does not exist in SHM. Reader either hangs awaiting non-existent frame, reads corrupt/stale data from prior ring cycle, or crashes on monotonic anchor check ($T_k - T_{k-1} = \text{cadence}$).

- **Required Fix**:
  1. Warm recovery with `FlagColdStart = false` and "zero mathematical gap" is physically valid ONLY if zero cadence anchors were missed: eligibility window MUST be `time.Now().UnixNano() - LastWrittenAnchorNS < CadenceInterval` (1.0s).
  2. If downtime exceeds 1 cadence interval ($\ge 1.0\text{s}$), daemon may re-attach to preserve historical ring slots for consumers, BUT it MUST assert `FlagColdStart = true` for 15 seconds until rolling queue fills with 15 contiguous live bars.
  3. §4.3 Python resilience must explicitly check for anchor gaps: if `new_daemon_anchor > cursor.anchor + CadenceInterval`, reader cannot "continue seamlessly without dropping cursors"; it must raise sequence gap, advance cursor to latest anchor, and respect `FlagColdStart`.

---

## 4. Non-Blocking Observations

1. §6 changes 1 and 5 update Go and Python ABI, but omit updating C header (`include/tickhub.h`) referenced in §8 build validation.
2. §4.3 Python `StatusBooting` retry loop caps at 500ms (50 $\times$ 10ms); slow daemon re-attach or history seeding triggers premature reader crash.
3. §4.2 cold recovery truncates segment in-place; concurrent readers with active mmaps will hit `SIGBUS` if segment is truncated while mapped.
4. §2 lists 100.000% local SHM frame delivery but 99.999% relay delivery; relay sequence gap recovery needs explicit client backoff specification.
5. §5.1 Prometheus exporter samples SHM read-only, but plan omits reader detachment and unmap lifecycle on HTTP server shutdown.

---

## 5. Methodological & Data-Alignment Concerns

- **SHM Layout & ABI Parity**: Inserting `BootID uint64` (offset 104), `Generation uint32` (offset 112), `RecoveryMode uint32` (offset 116), and `_padProducer [8]byte` (offset 120) strictly replaces `_padProducer [24]byte`. 8-byte alignment preserved across Go and Python `abi.py`. C header must mirror these exact offsets before compilation.
- **1Hz Projection Semantics**: Rolling 15-second return ($[T-15\text{s}, T)$) strictly assumes uniform 1Hz spacing. Stitching pre-crash bars across multi-second downtime breaks interval definition unless intermediate bars are forward-filled or cold-start warmup is enforced.

---

## 6. Missing Telemetry

1. Metric/log for daemon restart gap duration: `tickhub_recovery_downtime_nanoseconds = time.Now().UnixNano() - LastWrittenAnchorNS`.
2. Counter for missed frame anchors during daemon downtime: `tickhub_recovery_missed_anchors_total`.

---

## 7. Verdict

**Verdict: Blocked (B2)**

The warm-recovery protocol (§4.2) sets an eligibility window of up to 60 seconds of daemon downtime, mistakenly conflating pre-crash ring buffer depth with downtime tolerance. If the daemon is dead for more than 1 second, 1Hz frame anchors are irrevocably lost. Seeding the projector with pre-crash bars and immediately publishing without `FlagColdStart` stitches prices across an unrecorded multi-second gap, computing mathematically false 15-second returns (`ret15s`) that feed directly into live trading models (`ccm-live`), risking capital (B2). Simultaneously, the client resilience logic (§4.3) instructs readers to continue without resetting cursors whenever the cursor is within the ring buffer, causing Python readers to hang waiting for frames that were never generated during the outage (B1). Warm recovery without cold-start suppression must be restricted to sub-cadence restarts (< 1.0s), or must enforce `FlagColdStart` and explicit cursor gap-skip logic whenever an anchor is missed.
