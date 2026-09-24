# Plan Review — aiChangeLog/2026-09-24/011_standalone_reader_shm_init.md

_Model: Gemini 3.8 Flash (High) · Round: 2 · Generated: 2026-09-24 22:57:01Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 2/2 · Verdict parsed: **Proceed**_

---

## 1. Summary of the Proposal

Plan modifies `python/tickhub/shm.py` and `tests/test_shm_cross.py` to support direct attachment to shared memory without YAML configuration:
- `TickHubReader.__init__` accepts `config_path_or_dict: Union[str, Path, dict, None] = None` and `shm_path_override: Optional[str] = None`.
- For `str` / `Path`: checks `startswith("/dev/shm/")` OR first 8 bytes == `b"TICKHUB1"`. If matched, routes as `shm_path_override` and sets `self.config = {}`. Otherwise parses via `yaml.safe_load(f)`.
- If `config_path_or_dict` is `None`, sets `self.config = {}`. If `dict`, sets `self.config = config_path_or_dict`.
- If phase symbol list empty in `self.config`, falls back to `list(self._symbol_to_dir_idx.keys())` from SHM directory table.
- Exposes `@property def heartbeat_ns(self) -> int` returning `self._header.heartbeat_ns if self._header is not None else 0`.
- Adds unit test `test_standalone_reader_from_shm_path` in `tests/test_shm_cross.py` using `producer_process` fixture.

---

## 2. Simplest Sufficient Design

Plan is at minimum sufficient design to achieve standalone SHM attachment and heartbeat exposure. No superfluous files, state, or abstractions.

---

## 3. Blocking Defects

None.

Prior B1 defect resolved: brittle negative extension heuristic removed. Positive-identity check (`startswith("/dev/shm/")` or `b"TICKHUB1"` magic bytes) routes binary SHM segments correctly while allowing extensionless YAML configs to pass to `yaml.safe_load`.

---

## 4. Non-Blocking Observations

1. `startswith("/dev/shm/")` matches `/dev/shm/*.yaml` before magic bytes; check magic bytes unconditionally or verify non-YAML content to avoid misrouting configs stored in tmpfs.
2. File inspection `open(path, "rb").read(8)` must catch `OSError` to handle unreadable paths, permission errors, or raw YAML strings cleanly.
3. `list(self._symbol_to_dir_idx.keys())` relies on insertion order; sort by directory entry index (`dir_idx`) if stable phase indexing required.
4. Step 4 verification command references `r.total_symbols`; verify property exists on `TickHubReader` or use `len(r.symbols)`.
5. `shm_path_override` passed as both positional path and keyword parameter needs explicit precedence rule in `__init__`.

---

## 5. Methodological & Data-Alignment Concerns

N/A

---

## 6. Missing Telemetry

None.

---

## 7. Verdict

**Verdict: Proceed**

Round 1 blocking defect B1 resolved. Negative extension heuristic replaced by positive magic-byte check (`b"TICKHUB1"`) and `/dev/shm/` prefix routing, preventing silent misconfiguration of extensionless config files. `heartbeat_ns` property includes null guard. Scope surgical, minimal, and ready for implementation.
