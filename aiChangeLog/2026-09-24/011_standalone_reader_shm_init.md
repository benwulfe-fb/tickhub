# Technical Strategy: Standalone TickHubReader SHM Init & Heartbeat Property

## Goal & Context
Currently, `TickHubReader.__init__` expects a YAML file path or dict configuration as its first positional argument. When an external client (such as `TickHubLiveFeed` or `vm_dashboard.py` in the CCM engine) connects to a running TickHub SHM segment directly by path (e.g. `TickHubReader("/dev/shm/tickhub_live")`), `__init__` fails trying to parse the binary SHM file as UTF-8 YAML (`UnicodeDecodeError: 'utf-8' codec can't decode byte 0xa7...`). Furthermore, `heartbeat_ns` is not exposed as a top-level `@property` on `TickHubReader` (only via `_header.heartbeat_ns`), creating an ergonomic gap for monitoring tools.

This change enhances `TickHubReader` to:
1. Accept `config_path_or_dict` as optional (`None`, `dict`, `str`, or `Path`).
   - If `None`: initialize with empty config `self.config = {}`.
   - If `dict`: initialize with `self.config = config_path_or_dict`.
   - If `str` or `Path`: perform a deterministic, positive-identity check. If path starts with `/dev/shm/` OR the target file exists and starts with `MAGIC_BYTES` (`b"TICKHUB1"`), treat it as `shm_path_override` and set `self.config = {}`. Otherwise, treat as a config file and parse via `yaml.safe_load(f)`. This avoids brittle extension heuristics and ensures extensionless config files are never silently misrouted.
2. When no explicit per-phase symbol list exists in `self.config`, gracefully fall back to `list(self._symbol_to_dir_idx.keys())` from the SHM directory table (`SymbolDirectoryEntry`).
3. Expose `@property def heartbeat_ns(self) -> int` returning `self._header.heartbeat_ns` with `self._header is None` guard returning `0`.

## SOLID Adherence
- **Single Responsibility Principle (SRP)**: `TickHubReader` remains strictly a consumer-side reader of TickHub SHM memory layouts.
- **Open/Closed Principle (OCP)**: Existing consumers passing explicit YAML files or dict configurations continue to behave identically. Standalone path-based initialization is added without breaking existing contracts. Positive magic-byte and `/dev/shm/` detection prevents misrouting valid configs.
- **Liskov Substitution / Interface Segregation**: Preserves context manager and cursor reading interfaces.
- **Dependency Inversion**: Relies on SHM ABI structures rather than requiring external filesystem artifacts to discover active symbols.

## Proposed Code Changes

### [MODIFY] `python/tickhub/shm.py`
- Modify `TickHubReader.__init__` signature to `def __init__(self, config_path_or_dict: Union[str, Path, dict, None] = None, shm_path_override: Optional[str] = None):`.
- Inspect `config_path_or_dict`:
  - `None`: `self.config = {}`
  - `dict`: `self.config = config_path_or_dict`
  - `str` / `Path`: Check if `str(config_path_or_dict).startswith("/dev/shm/")`. If not, read the first 8 bytes of the file: if equal to `b"TICKHUB1"`, treat as SHM binary segment. If either condition matches, set `shm_path_override = str(config_path_or_dict)` and `self.config = {}`. Otherwise, open and parse with `yaml.safe_load(f)`.
- In phase symbol binding, fallback to `list(self._symbol_to_dir_idx.keys())` if phase symbol list is empty.
- Add `@property def heartbeat_ns(self) -> int: return self._header.heartbeat_ns if self._header is not None else 0`.

### [MODIFY] `tests/test_shm_cross.py`
- Add unit test `test_standalone_reader_from_shm_path(producer_process)` verifying that:
  - `TickHubReader(f"/dev/shm/{SHM_NAME}")` initializes successfully without a YAML config.
  - Reader discovers all symbols from directory (`len(hub.symbols) > 0`).
  - Reader reads valid top-of-book snapshot (`hub.snapshot("SPY")`).
  - Reader exposes non-zero `hub.heartbeat_ns`.

## Telemetry Plan
N/A — pure Python client library ergonomics.

## Verification Plan
1. Run `pytest -v tests/test_shm_cross.py` to verify standalone path loading and existing tests under live producer process fixture.
2. Run full test suite via `scripts/validate_all.py` (ABI parity, C build, Go tests, Python tests).
3. Run plan review and code review gates.
4. Copy updated Python package to `ccm-live-1` and verify `python3 -c 'import tickhub; r = tickhub.TickHubReader("/dev/shm/tickhub_live"); print(r.total_symbols, r.heartbeat_ns)'`.

## Checklist
- [ ] Working tree clean before starting
- [ ] Ledger written to `aiChangeLog/2026-09-24/011_standalone_reader_shm_init.md`
- [ ] Plan review passed
- [ ] Code changes implemented surgically
- [ ] All tests pass in `scripts/validate_all.py`
- [ ] Code review passed
