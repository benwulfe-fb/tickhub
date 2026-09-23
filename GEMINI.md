# GEMINI.md — TickHub Operating Guide (loaded every session)

## What this repo is
**TickHub**: High-performance 1Hz market data projection and shared memory engine for quantitative trading (Go daemon, C atomic bridge, Python client / Arrow / PyTorch export). TickHub bridges irregular tick streams (live WebSocket feeds from Massive.com or historical Parquet files) to downstream quantitative research, training, and execution pipelines via low-latency POSIX shared memory (`/dev/shm`).

---

## 🚨 SSoT & Architectural Invariants (trigger: always_on)
1. **Single Source of Truth (SSoT)**:
   - All 1Hz feature calculation and rolling window math live *strictly* in `tickhub` (`pkg/project/projector.go`).
   - Downstream consumers (Python ML training dataloaders, live execution models, backtest drivers) *never* duplicate or re-implement `project1hz` math.
2. **1Hz Projection Semantics**:
   - **Half-Open Interval**: Window $[T - 1\text{s}, T)$ excludes the boundary tick at $T$ and includes $[T - 1\text{s}, T)$.
   - **Illiquid Forward-Fill**: Midprice and close persist from the previous known state when zero trades occur in the window; volume is 0.
   - **Quote-Before-Trade Tie-Breaking**: When trade and quote share the exact same nanosecond timestamp, quote is processed first to update prevailing BBO before trade volume/spread is calculated.
   - **ASCII Symbol Tie-Breaking**: Parallel feeds break timestamp ties alphabetically by symbol.
3. **Shared Memory (`/dev/shm`) Dual Cache-Line Isolation**:
   - **Producer Cache Line** (offset 64): `AnchorPublishLatencyNS`, `WatermarkBufferNS`, `LastWrittenAnchorNS`, `DroppedTickCount`, `TotalTickCount`.
   - **Consumer Cache Line** (offset 128): `LastReadAnchorNS`, `ConsumerPID`, `ConsumerHeartbeat`.
   - Never place producer-written and consumer-written fields on the same 64-byte cache line (eliminates CPU cache-line bouncing).
   - In `ModeHistoricalReplay`, producer flow control (`WaitConsumerAdvance`) blocks until consumer advances `LastReadAnchorNS`. Python `TickHubReader` must be opened `O_RDWR` (`PROT_READ | PROT_WRITE`) to commit read progress.
4. **Cross-Language ABI Parity**:
   - `GlobalHeader`: 1024 bytes (producer line at 64, consumer line at 128, phases array at 192).
   - `SymbolSnapshot`: 128 bytes (SeqLock protected).
   - `FrameHeader`: 64 bytes.
   - `SymbolDirectoryEntry`: 16 bytes.
   - `PhaseInfo`: 32 bytes.
   - Struct layouts and alignments must match identically across Go (`pkg/shm`), C (`c/`), and Python (`python/tickhub/abi.py`).
5. **Zero-Allocation Hot Path**:
   - Projection inner loops, SeqLock copies, and ring-buffer writes must not allocate on the heap during steady-state processing.

---

## 🚨 Operating Protocol (Mandatory Workflow)
Workflow for every substantive code change:
0. **Clean tree first (MANDATORY)**:
   - Before opening a new ledger or running plan review, the working tree must have no uncommitted changes under `pkg/`, `cmd/`, `python/`, `c/`, `scripts/`, `tests/`. Commit existing work first.
1. **Ledger first (MANDATORY)**:
   - Write technical strategy to `aiChangeLog/YYYY-MM-DD/SEQNO_description.md` before editing code.
   - Sections: Goal & Context, SOLID Adherence, Proposed Code Changes (`[MODIFY]`/`[ADD]`), Telemetry Plan, Verification Plan, Checklist.
2. **Plan Review (MANDATORY gate)**:
   - Run `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/planreview.py aiChangeLog/YYYY-MM-DD/SEQNO_description.md`
   - Evaluated by Claude Sonnet 4.6 (Round 1) / Gemini 3.8 Flash (Round 2+) via Antigravity `agy --print`.
   - Round cap = 2.
   - Blocking defects (`B1` goal-defeating, `B2` data loss / concurrency, `B3` unmeasured foundation, `B4` unnecessary complexity) must be remediated in place before code edits.
3. **Implement**:
   - Touch only what was planned. Maintain existing conventions and zero-alloc patterns.
4. **Validate All (MANDATORY test gate)**:
   - Run `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/validate_all.py`
   - Verifies C shared library build, ABI struct alignment, Go binary builds, Go unit & race tests (`go test -v -race ./...`), Python pytest suite (`pytest -v tests/`), and CLI smoke.
   - Fix all regressions.
5. **Code Review (MANDATORY pre-commit gate)**:
   - Run `/mnt/wc/miniconda3/envs/gpu_env/bin/python -u scripts/codereview.py aiChangeLog/YYYY-MM-DD/SEQNO_description.md`
   - Audits git diff against ledger and plan review. First line must be `APPROVED`.
   - On `DENIED`, remediate and re-run.
6. **Commit**:
   - Git hook `scripts/hooks/pre-commit` enforces `APPROVED` codereview before `git commit`.
   - `--no-verify` requires explicit user confirmation.

---

## 🛠️ Execution & Environment Standards
- **Python**: `/mnt/wc/miniconda3/envs/gpu_env/bin/python` (Python 3.11 with numpy, pyarrow, torch)
- **Go**: `/mnt/wc/go/bin/go` (Go 1.23+)
- **C Compiler**: `gcc` with `-O3 -Wall -Wextra -fPIC -std=c11`
- **Review Tool**: Antigravity CLI `agy` (`~/.local/bin/agy`)
- **No Polling Rule**: When a background task is running, never poll `manage_task(Action='status')`. Antigravity notifies reactively. Stop calling tools to yield your turn.
