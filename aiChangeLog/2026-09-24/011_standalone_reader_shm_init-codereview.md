# Code Review — aiChangeLog/2026-09-24/011_standalone_reader_shm_init.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Verdict (parsed): APPROVED · Generated: 2026-09-24 23:00:13Z · Tool: codereview.py (Antigravity `agy`)_

---

APPROVED

## 1. **Summary of the Implemented Change**
[`TickHubReader.__init__`](file:///python/tickhub/shm.py#L237) in [`python/tickhub/shm.py`](file:///python/tickhub/shm.py) signature widened to accept `config_path_or_dict: Union[str, Path, dict, None] = None` and optional `shm_path_override: Optional[str] = None`. Standalone SHM connection supported without YAML configuration. When `config_path_or_dict` is `None`, `self.config` sets to `{}`. When `dict`, assigns directly. When `str` or `Path`, checks if path starts with `"/dev/shm/"` or target file exists with magic prefix `b"TICKHUB1"`. If SHM detected, enforces conflict check against `shm_path_override` (raises `ValueError` on mismatch), assigns `shm_path_override = p_str`, sets `self.config = {}`. Inspection file open failure raises `OSError`. Non-SHM falls back to `yaml.safe_load(f)`. When phase symbol list empty, falls back to `sorted(self._symbol_to_dir_idx.keys(), key=lambda s: self._symbol_to_dir_idx[s])`. Added [`heartbeat_ns`](file:///python/tickhub/shm.py#L294) property returning daemon heartbeat in nanoseconds (`0` if `_header is None`). Added test [`test_standalone_reader_from_shm_path`](file:///tests/test_shm_cross.py#L313) in [`tests/test_shm_cross.py`](file:///tests/test_shm_cross.py) covering standalone attach, symbol discovery, heartbeat read, top-of-book snapshot, conflict validation, nonexistent path handling.

## 2. **Correctness & Concurrency Bugs**
None.
Prior review findings verified remediated:
- Prior B1: `tickhub_` heuristic removed ([`python/tickhub/shm.py:253`](file:///python/tickhub/shm.py#L253)). Path detection uses only `startswith("/dev/shm/")` or `magic == b"TICKHUB1"`.
- Prior B2: Precedence check implemented ([`python/tickhub/shm.py:265-269`](file:///python/tickhub/shm.py#L265-L269)). Positional SHM path conflicting with keyword `shm_path_override` raises explicit `ValueError`.
- Prior B3 / Telemetry: Nonexistent SHM paths verified raising `FileNotFoundError` downstream; covered by test assertion ([`tests/test_shm_cross.py:329-330`](file:///tests/test_shm_cross.py#L329-L330)).
- Prior B4: File probe exception swallowing removed ([`python/tickhub/shm.py:261-263`](file:///python/tickhub/shm.py#L261-L263)). Replaced bare `except Exception` with `except OSError as e:` re-raising descriptive `OSError`.

No buffer overruns, race conditions, memory leaks, or atomic ordering violations found.

## 3. **Projection Math & Temporal Parity**
No 1Hz projection math, look-ahead logic, temporal interval boundaries $[T-1\text{s}, T)$, quote-trade arbitration, or feature definitions modified. Symbol discovery fallback sorts directory keys deterministically by directory index `self._symbol_to_dir_idx[s]` ([`python/tickhub/shm.py:285`](file:///python/tickhub/shm.py#L285)), preserving layout parity with producer memory layout.

## 4. **Deviations from the Approved Plan**
None.
Implementation matches approved strategy in `aiChangeLog/2026-09-24/011_standalone_reader_shm_init.md` and satisfies all remediations from prior code review:
- Positive identity probe adheres to `/dev/shm/` prefix and `b"TICKHUB1"` magic bytes.
- Explicit conflict check prevents clobbering caller-provided `shm_path_override`.
- `heartbeat_ns` property includes null guard returning `0` when unattached.
- Phase symbol fallback sorted by directory index per plan review observation 3.

## 5. **Systems & Performance Violations**
None.
- Zero-allocation hot paths preserved: Changes execute solely during reader initialization.
- Symbol fallback allocation occurs once at initialization, outside cursor iteration loops.
- `heartbeat_ns` performs single ctypes attribute read; no heap allocation.
- Cache-line isolation and alignment unchanged.

## 6. **Telemetry / Verification Gaps**
None.
Unit test [`test_standalone_reader_from_shm_path`](file:///tests/test_shm_cross.py#L313) verifies:
1. Direct connection via `/dev/shm/{SHM_NAME}` without config file or dictionary.
2. Directory symbol discovery (`len(hub.symbols) > 0`, `"SPY" in hub.symbols`).
3. Non-zero `hub.heartbeat_ns`.
4. Valid top-of-book snapshot generation (`hub.snapshot("SPY")`).
5. Conflicting `shm_path_override` raising `ValueError`.
6. Nonexistent `/dev/shm/` path raising `FileNotFoundError`.

## 7. **Verdict & Remediation**
APPROVED.
All blocking defects from prior review (B1, B2, B4) and plan review observations fully remediated. Path detection robust, conflict handling explicit, error propagation clean, test coverage complete across success and failure paths. Ready to commit.
