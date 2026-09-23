# 003 — Port Operational Review Tooling, Git Hooks, Validation Suite, and GEMINI.md Operating Guide to TickHub

## Goal & Context

Operator directive (2026-09-23):
*"i just realized -- are changelogs, ledgers and code reviews only under the other github repo? if so, can you clone codereview and planreview into the tickhub repo and generate changelog entries the same way as the engine does (under /mnt/wc/src). add any project specific rules necessary for you to remember. we also need a flavor of validate_all. it should have similar rules about tests."*

### Problem Statement & Scope
1. **Repository Independence**: `tickhub` is an independent GitHub repository (`git@github.com:benwulfe-fb/tickhub.git`) hosted at `/mnt/wc/src/tickhub`. Prior changelog entries (001 and 002) were temporarily written into `/mnt/wc/src/aiChangeLog/2026-09-23/`.
2. **Missing Operational Tooling & Gating**:
   - `planreview.py` and `codereview.py` were missing from `tickhub/scripts/`.
   - No single-command verification gate (`validate_all.py`) existed to comprehensively validate C compilation, ABI alignment, Go unit/race tests, Python pytests, and CLI builds.
   - No pre-commit hook existed to enforce approved code reviews.
   - No repo-level `GEMINI.md` existed to enforce TickHub SSoT invariants (e.g. no duplication of `project1hz`, half-open interval $[T-1\text{s}, T)$, dual cache-line isolation, flow-control semantics).

---

## SOLID & Systems Engineering Adherence
- **Single Responsibility (SRP)**:
  - `scripts/planreview.py`: Audits proposed designs before implementation.
  - `scripts/codereview.py`: Audits code diffs against the approved ledger and plan review before commit.
  - `scripts/validate_all.py`: Single executable entrypoint running all build, ABI, race, and integration tests.
  - `scripts/hooks/pre-commit`: Enforces the code review gate at commit time.
- **Fail-Fast (Poka-Yoke)**:
  - `validate_all.py` halts immediately on any compilation error, ABI struct discrepancy, data race, or pytest failure.
  - `planreview.py` refuses to review when uncommitted code/config exists (clean-tree gate).
  - `codereview.py` defaults to `DENIED` on any ambiguity.

---

## Proposed Code Changes

### 1. `GEMINI.md` [ADD]
- TickHub operating guide loaded in all sessions.
- Invariants: SSoT for 1Hz feature calculation, $[T-1\text{s}, T)$ interval, illiquid forward-fill, quote-before-trade tie-breaking, dual cache-line isolation (`GlobalHeader`), ABI parity across C/Go/Python ctypes.
- Operating protocol: clean tree -> ledger first -> plan review gate -> implement -> validate_all gate -> code review gate -> commit.

### 2. `scripts/planreview.py` [ADD]
- Adversarial plan review runner via Antigravity `agy --print`.
- Scoped to `tickhub` directory with `CODE_PREFIXES = ("pkg/", "cmd/", "python/", "c/", "scripts/", "tests/", "examples/")`.
- Tiered model selection: Round 1 Claude Sonnet 4.6 (Thinking); Round 2+ Gemini 3.8 Flash (High).
- Enforces convergence budget: `ROUND_CAP = 2`, `HARD_CEILING = 3`, `MAX_B3_HALTS = 2`.

### 3. `scripts/codereview.py` [ADD]
- Adversarial code review auditor via Antigravity `agy --print`.
- Scoped to `tickhub` code and config paths (`pkg/`, `cmd/`, `python/`, `c/`, `scripts/`, `tests/`, `examples/`, `Makefile`, `go.mod`, `go.sum`, `pyproject.toml`).
- Uses temp workspace file delivery (`--add-dir`) to avoid Linux `MAX_ARG_STRLEN` ceilings.
- Parses `APPROVED` or `DENIED` fail-closed.

### 4. `scripts/validate_all.py` [ADD]
- Single-command verification runner:
  - Stage 1: C shared library compilation (`make -C c clean && make -C c`).
  - Stage 2: ABI struct size & cache-line offset assertions (`GlobalHeader`, `SymbolSnapshot`, `FrameHeader`, `SymbolDirectoryEntry`, `PhaseInfo`).
  - Stage 3: Go binary compilation (`bin/tickhub`, `bin/producer_helper`, `bin/producer_demo`).
  - Stage 4: Go unit & race test suite (`go test -v -race ./...`).
  - Stage 5: Python pytest suite (`pytest -v tests/`).
  - Stage 6: CLI smoke test (`bin/tickhub --help`) and git status inspection.
- Telemetry logging to console and `output/{YYMMDD}_{HHMM}_{PID}_stdout.txt`.

### 5. `scripts/hooks/pre-commit` [ADD]
- Git pre-commit hook enforcing `APPROVED` `<ledger>-codereview.md` for staged code/config.
- Installed via `git config core.hooksPath scripts/hooks`.

### 6. `cmd/tickhub/main.go` [MODIFY]
- Added clean handling for `help`, `--help`, `-h` returning exit code 0.

### 7. `.gitignore` [MODIFY]
- Added `output/` ignore rule.

### 8. `aiChangeLog/2026-09-23/` [MIGRATE]
- Copied `001_tickhub_public_api_and_shm_subsystem*` and `002_historical_replay_and_parquet_export*` from `/mnt/wc/src/aiChangeLog/2026-09-23/`.

---

## Telemetry Plan
- `scripts/validate_all.py` outputs full execution logs to `output/{YYMMDD}_{HHMM}_{PID}_stdout.txt` and `stderr.txt`.
- `scripts/planreview.py` outputs to `output/planreview___stdout.txt`.
- `scripts/codereview.py` outputs to `output/codereview___stdout.txt`.

---

## Verification Plan
1. `validate_all.py` runs all 6 stages cleanly with exit code 0.
2. `scripts/hooks/pre-commit` tested against staged files.
3. `scripts/codereview.py` runs against `aiChangeLog/2026-09-23/003_operational_review_tooling_and_validation.md` producing `APPROVED`.

---

## Checklist
- [x] Migrate `aiChangeLog/2026-09-23/001*` and `002*` into `tickhub/`
- [x] Implement `scripts/planreview.py`
- [x] Implement `scripts/codereview.py`
- [x] Implement `scripts/validate_all.py`
- [x] Implement `scripts/hooks/pre-commit` and configure `core.hooksPath`
- [x] Create `GEMINI.md` with TickHub operating rules and invariants
- [x] Run `validate_all.py` and verify all tests pass
- [x] Run `codereview.py` on ledger 003 (APPROVED in Round 1 & Round 2)
- [ ] Commit and push to `origin/main`
