# Plan Review — aiChangeLog/2026-09-24/011_standalone_reader_shm_init.md

_Model: Claude Sonnet 4.6 (Thinking) · Round: 1 · Generated: 2026-09-24 22:56:06Z · Tool: planreview.py (Antigravity `agy`)_

_Budget: round 1/2 · Verdict parsed: **Blocked (B1)**_

---

## 1. Summary of the Proposal

`TickHubReader.__init__` currently requires a YAML path or dict as its first positional argument. Passing a raw SHM path (e.g. `/dev/shm/tickhub_live`) causes a `UnicodeDecodeError` because the binary SHM file is parsed as UTF-8 YAML. The plan fixes this by:

1. Making `config_path_or_dict` optional (default `None`) and adding heuristic detection — if the argument looks like an SHM path (contains `/dev/shm/`, starts with `tickhub_`, or lacks `.yaml`/`.yml` suffix), treat it as a `shm_path_override` and default `config = {}`.
2. When no explicit per-phase symbol list exists, fall back to `list(self._symbol_to_dir_idx.keys())` from the SHM directory table.
3. Expose `@property def heartbeat_ns(self) -> int` delegating to `self._header.heartbeat_ns`.
4. Add one new test in `tests/test_shm_cross.py`.

---

## 2. Simplest Sufficient Design

The stated goal is: let `TickHubReader("/dev/shm/tickhub_live")` work without a YAML config.

Minimum change to achieve that: **if the argument is not a dict and not a `.yaml`/`.yml` file, treat it as the SHM path**. The `heartbeat_ns` property is a one-liner add-on; the symbol fallback is required for standalone init to be useful. The test is required. This plan is at or near the minimum.

One potential cut: the `shm_path_override` keyword argument in the new signature. If the heuristic already routes a bare SHM path correctly, and if `shm_path_override` was already a pre-existing parameter, adding it redundantly alongside the heuristic creates two code paths to the same outcome. The changelog does not confirm `shm_path_override` is pre-existing, so this could be new dead weight — but without knowing the existing signature it cannot be called a definitive cut. **Flag as a question, not a cut.**

---

## 3. Blocking Defects

**B1 — Heuristic is ambiguous and goal-defeating for legitimate YAML paths that match the trigger conditions.**

The detection rule (`Proposed Code Changes`, `[MODIFY] shm.py`, bullet 2) fires on any path that:
- contains `/dev/shm/`, **OR**
- starts with `tickhub_`, **OR**
- does **not** end in `.yaml`/`.yml`

The third condition — *does not end in `.yaml`/`.yml`* — is the problem. Any user who passes a plain string config path without an extension (e.g. `TickHubReader("config")`, `TickHubReader("tickhub_prod")`, `TickHubReader(Path("configs/live"))`) will be silently misrouted: the reader initialises with `config = {}`, no error is raised, symbols are discovered from SHM, and the caller's intended configuration is ignored. This is a **silent misconfiguration**, not a noisy failure. The existing users whose call sites happen to omit extensions, or use extensionless symlinks, are broken with no diagnostic — exactly the opposite of the OCP claim in the plan.

The fix is a positive test, not a negative one: check whether the path exists **as a file** (or `stat` its type) rather than guessing by name suffix. If `os.path.isfile(path)` and the file is not a YAML file (or is not readable as text), fall back to SHM mode; otherwise parse as config. Alternatively, require the caller to use the explicit `shm_path_override=` keyword for the SHM case — which the signature already provides — making the heuristic unnecessary entirely.

**This defect is blocking (B1):** the plan achieves its goal only for callers whose config paths happen to end in `.yaml`/`.yml`. Any extensionless config path silently produces wrong behaviour with no error.

---

## 4. Non-Blocking Observations

1. `heartbeat_ns` property has no guard for the case where `_header` is `None` or not yet mapped; a brief mapping-error note in the docstring would help.
2. `list(self._symbol_to_dir_idx.keys())` ordering is dict-insertion order (Python 3.7+), which may differ from the order phases expect — worth asserting or sorting.
3. Verification step 4 (`ccm-live-1` smoke test) is manual and not gated; it should be listed as a hard prerequisite before declaring the deployment complete, not an afterthought.
4. The test (`test_standalone_reader_from_shm_path`) requires a live SHM segment; if run in CI without one it will silently skip or error — the plan does not address how CI handles this.
5. `shm_path_override` as both a heuristic output and a named keyword creates two ways to express the same intent; whichever survives should be the only way.

---

## 5. Methodological & Data-Alignment Concerns

N/A — this change is pure Python client ergonomics with no projection logic, SHM layout mutation, ABI change, or timing semantics.

---

## 6. Missing Telemetry

None. The plan is client-library ergonomics. The only meaningful check is whether the reader successfully attaches and reads a snapshot from an existing SHM segment, which is covered by verification step 4.

---

## 7. Verdict

**Verdict: Blocked (B1)**

The extensionless-path heuristic (`does not end in .yaml/.yml`) silently swallows any extensionless config path, replacing the caller's config with `{}` and falling back to SHM discovery — with no exception, no warning, and no way for the caller to detect it. This directly contradicts the OCP claim in the plan and can corrupt production reader state. Replace the heuristic with a positive-identity check (e.g. `os.path.exists` + file-type inspection, or simply require callers to use the explicit `shm_path_override=` keyword that the new signature already provides) before implementation.
