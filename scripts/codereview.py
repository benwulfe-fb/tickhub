#!/usr/bin/env python
"""codereview.py — adversarial CODE review of an implemented change via the Antigravity `agy` CLI.

The post-implementation bookend to `planreview.py`. Given the SEQNO ledger, it gathers the git
change set (the code/config about to be committed), and asks Claude Sonnet 4.6 (Round 1) / Gemini 3.8 Flash (Round 2+)
through `agy --print` (existing Antigravity consumer-OAuth login, no API key) to AUDIT the implementation
against the plan in the ledger and its `<ledger>-planreview.md`. The first line of the review is the
verdict `APPROVED` or `DENIED`; the review is written to `<ledger>-codereview.md`.

This is the REQUIRED code review run AFTER `validate_all.py` is green and BEFORE `git commit`.

Usage:
    python scripts/codereview.py aiChangeLog/2026-09-23/001_foo.md
    python scripts/codereview.py <ledger.md> [--model "Claude Sonnet 4.6 (Thinking)"]
                                             [--fallback "Gemini 3.8 Flash (High)"] [--timeout 900]
"""
import os
import re
import sys
import json
import signal
import shutil
import logging
import argparse
import tempfile
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SETTINGS = Path.home() / ".gemini" / "antigravity-cli" / "settings.json"

# Tiered model selection: Round 1 uses Claude Sonnet 4.6 (Thinking); Round 2+ uses Gemini 3.8 Flash (High).
R1_PRIMARY_MODEL = "Claude Sonnet 4.6 (Thinking)"
R1_FALLBACK_MODEL = "Gemini 3.8 Flash (High)"
R2_PRIMARY_MODEL = "Gemini 3.8 Flash (High)"
R2_FALLBACK_MODEL = "Gemini 3.7 Flash (High)"
PRIMARY_MODEL = R1_PRIMARY_MODEL
FALLBACK_MODEL = R1_FALLBACK_MODEL


def resolve_models_for_round(round_idx: int, explicit_model: str | None = None, explicit_fallback: str | None = None) -> tuple[str, str]:
    """Resolve (primary_model, fallback_model) given the round index and optional explicit CLI overrides."""
    if explicit_model:
        primary = explicit_model
    else:
        primary = R1_PRIMARY_MODEL if round_idx <= 1 else R2_PRIMARY_MODEL

    if explicit_fallback is not None:
        fallback = explicit_fallback
    elif "sonnet" in primary.lower() or round_idx <= 1:
        fallback = R1_FALLBACK_MODEL
    else:
        fallback = R2_FALLBACK_MODEL

    return primary, fallback


def determine_round(out_path: Path, explicit_round: int | None = None) -> int:
    """Determine the code review round: if out_path exists and is non-empty, a prior review ran -> Round 2+."""
    if explicit_round is not None:
        return explicit_round
    if out_path.exists() and out_path.stat().st_size > 0:
        return 2
    return 1


# The code/config domain the gate protects.
CODE_PREFIXES = ("pkg/", "cmd/", "python/", "c/", "scripts/", "tests/", "examples/",
                 "GEMINI.md", ".gitignore", "Makefile", "go.mod", "go.sum", "pyproject.toml")

JUNK_SUFFIXES = (".pyc", ".pyo", ".swp", ".swo", ".log", ".tmp", ".orig", ".bak")
BINARY_SUFFIXES = (".pkl", ".npz", ".npy", ".pt", ".onnx", ".parquet", ".zip", ".gz", ".png", ".so", ".a", ".o")
MAX_FILES = 500
MAX_DIFF_BYTES = 2_000_000

os.makedirs("output", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("output/codereview___stdout.txt"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("codereview")

_SETTINGS_BACKUP = None


def _restore_settings_on_signal(signum, _frame):
    if _SETTINGS_BACKUP is not None and SETTINGS.exists():
        _atomic_write(SETTINGS, _SETTINGS_BACKUP)
        logger.warning(f"Signal {signum} received; restored original Antigravity settings.json before exit.")
    sys.exit(128 + signum)


def _atomic_write(path: Path, text: str) -> None:
    """Write via temp file + os.replace so a crash mid-write cannot leave settings.json half-written."""
    tmp = path.with_suffix(path.suffix + ".crtmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def _agy_bin() -> str:
    """Resolves the `agy` executable (PATH, then standard ~/.local/bin install)."""
    found = shutil.which("agy") or str(Path.home() / ".local" / "bin" / "agy")
    if not os.path.exists(found):
        logger.error(f"[FATAL] Antigravity CLI not found at '{found}'. Is `agy` installed?")
        sys.exit(1)
    return found


def _git(args: list) -> str:
    """Run a read-only git command at repo root; return stdout."""
    proc = subprocess.run(["git", "-c", "core.pager=cat"] + args, cwd=str(ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        logger.warning(f"git {' '.join(args)} exited {proc.returncode}: {(proc.stderr or '').strip()[:200]}")
    return proc.stdout or ""


def _gather_change_set(scope_paths=None) -> tuple:
    """Collect the code/config change set about to be committed."""
    paths = list(scope_paths) if scope_paths else list(CODE_PREFIXES)
    tracked = [f for f in _git(["diff", "--name-only", "-M", "HEAD", "--"] + paths).splitlines() if f.strip()]
    untracked = [
        f for f in _git(["ls-files", "--others", "--exclude-standard", "--"] + paths).splitlines()
        if f.strip() and "__pycache__" not in f and not f.endswith(JUNK_SUFFIXES)
    ]
    if not tracked and not untracked:
        logger.error("[FATAL] No code/config change set under watched prefixes — nothing to review.")
        sys.exit(1)

    tracked_diff = _git(["diff", "-M", "HEAD", "--"] + paths)
    new_blob = "".join(
        (f"\n\n===== NEW (untracked) BINARY FILE (content omitted, "
         f"{(ROOT / f).stat().st_size / 1e6:.1f} MB): {f} ====="
         if f.endswith(BINARY_SUFFIXES) else
         f"\n\n===== NEW (untracked) FILE: {f} =====\n{(ROOT / f).read_text(errors='replace')}")
        for f in untracked)
    diff_text = tracked_diff + new_blob
    total = len(tracked) + len(untracked)
    diff_bytes = len(diff_text.encode("utf-8", "ignore"))
    if total > MAX_FILES or diff_bytes > MAX_DIFF_BYTES:
        logger.error(
            f"[FATAL] Change set too large to review atomically ({total} files, {diff_bytes} diff bytes; "
            f"caps {MAX_FILES} files / {MAX_DIFF_BYTES} bytes). Split into smaller atomic commits and re-run."
        )
        sys.exit(1)

    logger.info(
        f"Change set: {len(tracked)} tracked + {len(untracked)} untracked file(s); "
        f"diff {diff_bytes} bytes EMBEDDED in the prompt (delivered to agy as a FILE)."
    )
    return tracked, untracked, diff_text


def _build_prompt(rel_ledger: str, planreview_rel, tracked, untracked, diff_text: str,
                   ledger_text: str = "", planreview_text: str = "",
                   prior_review_text: str = "", prior_review_rel: str = "") -> str:
    """Senior code-auditor prompt: audit the implemented change against the approved plan + plan review."""
    pr_line = (f"Its plan review (the weaknesses you must confirm were remediated) is `{planreview_rel}` "
               f"(embedded below)."
              if planreview_rel else "No `-planreview.md` was found alongside the ledger (note this as a gap).")
    flist = "\n".join(f"  - {f}" for f in tracked) or "  (none)"
    ulist = "\n".join(f"  - {f}  [NEW/untracked — full content embedded below]" for f in untracked) or "  (none)"
    ledger_block = (
        f"\n\n===== LEDGER: {rel_ledger} =====\n{ledger_text}\n===== END LEDGER =====\n"
        if ledger_text else ""
    )
    planreview_block = (
        f"\n\n===== PLAN REVIEW: {planreview_rel} =====\n{planreview_text}\n===== END PLAN REVIEW =====\n"
        if planreview_text else ""
    )
    prior_block = ""
    if prior_review_text:
        prior_block = (
            f"\n\n===== PRIOR CODE REVIEW (most recent run on this ledger): {prior_review_rel} =====\n"
            f"{prior_review_text}\n===== END PRIOR CODE REVIEW =====\n\n"
            "PRIOR-REVIEW HANDLING (read this before auditing): the block above is YOUR OWN (or a prior "
            "model's) adversarial review from the LAST run against this same ledger. Cross-reference "
            "the prior review's numbered findings against the ledger's disposition of each one:\n"
            "  - If the diff shows the fix was actually applied — do not re-raise it.\n"
            "  - If the ledger's disposition refutes the finding with a checkable claim — verify it; "
            "if the refutation holds, do not re-raise it.\n"
            "  - If the ledger records an EXPLICIT operator decision to accept a residual risk — do not "
            "DENY solely on that item; note it in section 4 as 'operator-accepted, tracked'.\n"
            "  - Only DENY on: (a) a genuinely NEW defect, or (b) a prior finding whose claimed fix was NOT applied."
        )
    return (
        "You are a senior adversarial code auditor for `tickhub` — a high-performance 1Hz market data "
        "projection and shared memory engine for quantitative trading (Go, C, Python ctypes/Arrow/PyTorch), "
        "reviewing an IMPLEMENTED change before it is committed.\n\n"
        f"The plan/changelog for this change is `{rel_ledger}` (embedded below). {pr_line}\n"
        + ledger_block + planreview_block + prior_block +
        "\n\nThe change set about to be committed is embedded in full below — the tracked unified diff PLUS the "
        "entire content of every new/untracked file. Review what is IN this diff.\n"
        "IMPORTANT — SCOPED / INCREMENTAL REVIEW: judge correctness of the edits actually shown.\n"
        f"TRACKED (modified) files:\n{flist}\n"
        f"UNTRACKED (new) files:\n{ulist}\n\n"
        "Read the embedded diff and new-file content rigorously to judge correctness, 1Hz projection math parity, "
        "shared memory alignment and safety, plan adherence, and systems performance standards. Change set:\n\n"
        "```diff\n" + diff_text + "\n```\n\n"
        "Audit the implementation rigorously and skeptically. OUTPUT FORMAT (STRICT): the FIRST CHARACTER of your "
        "response must be the first letter of the verdict. Line 1 is EXACTLY one word — `APPROVED` or `DENIED` — "
        "alone, with NO markdown, NO bold, NO punctuation, and NO other text. Do NOT write any preamble or intro. "
        "Then a blank line, then a markdown review with these sections:\n"
        "1. **Summary of the Implemented Change** — your own restatement (proves you read the actual code).\n"
        "2. **Correctness & Concurrency Bugs** — concrete defects, with `file:line`; shared memory races, buffer "
        "overruns, deadlocks, atomic ordering slips, unhandled error conditions.\n"
        "3. **Projection Math & Temporal Parity** — look-ahead leakage, half-open interval $[T-1\\text{s}, T)$, "
        "illiquid forward-fill, quote-before-trade tie-breaking, symbol ASCII order, feature definition SSoT.\n"
        "4. **Deviations from the Approved Plan** — where the code diverges from the ledger / unremediated planreview findings.\n"
        "5. **Systems & Performance Violations** — zero-allocation hot paths, cache-line false sharing (dual cache-line "
        "isolation for Producer vs Consumer lines), unneeded heap escapes, bare unhandled errors.\n"
        "6. **Telemetry / Verification Gaps** — what the change must prove and does not.\n"
        "7. **Verdict & Remediation** — justify APPROVED, or on DENIED give a numbered, precise remediation list.\n\n"
        "DENY if you cannot verify correctness, if the plan was deviated from without justification, or if any "
        "concurrency/math standard is violated. Output ONLY the review document."
    )


def _payload_pointer(payload_path: str) -> str:
    """The SHORT argv prompt that points `agy` at the review payload on disk."""
    return (
        "Your complete instruction set for this task is in the file:\n"
        f"    {payload_path}\n\n"
        "Read that file IN FULL before doing anything else, then carry out the task exactly as it "
        "specifies. It contains the audit brief, the ledger, the plan review, and the entire change "
        "set under review. Do not summarise it back to me — perform the task it describes and reply "
        "with only the requested output."
    )


def _argv_guard(pointer: str) -> None:
    """Linux caps a SINGLE argv string at MAX_ARG_STRLEN (~128KB)."""
    n = len(pointer.encode("utf-8", "ignore"))
    if n > 126_000:
        logger.error(
            f"[FATAL] the argv pointer is {n} bytes > 126000 (Linux per-arg MAX_ARG_STRLEN safety bound).")
        sys.exit(1)


def _run_model(agy: str, model: str, prompt: str, timeout: int) -> str:
    """Sets the print-mode model in settings.json (atomically), runs `agy --print`, restores. Returns stdout or ''."""
    global _SETTINGS_BACKUP
    backup = SETTINGS.read_text() if SETTINGS.exists() else None
    if backup is None:
        logger.error(f"[FATAL] Antigravity settings not found at {SETTINGS}; cannot select model.")
        sys.exit(1)
    _SETTINGS_BACKUP = backup
    workspace = tempfile.mkdtemp(prefix="codereview_payload_")
    payload_path = os.path.join(workspace, "codereview_payload.md")
    try:
        payload_bytes = prompt.encode("utf-8")
        Path(payload_path).write_bytes(payload_bytes)
        pointer = _payload_pointer(payload_path)
        _argv_guard(pointer)

        cfg = json.loads(backup)
        cfg["model"] = model
        _atomic_write(SETTINGS, json.dumps(cfg, indent=2))
        logger.info(f"Selected model '{model}'; invoking `agy --print` with payload delivered as FILE "
                    f"({len(payload_bytes)} bytes at {payload_path}; timeout {timeout}s)...")
        mins = max(1, timeout // 60)
        proc = subprocess.run(
            [agy, "--print-timeout", f"{mins}m", "--add-dir", workspace, "--print", pointer],
            cwd=str(ROOT), capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        logger.error(f"Model '{model}' timed out after {timeout}s.")
        return ""
    except Exception as e:
        logger.error(f"Model '{model}' failed with exception: {e}")
        return ""
    finally:
        _atomic_write(SETTINGS, backup)
        _SETTINGS_BACKUP = None
        shutil.rmtree(workspace, ignore_errors=True)
        logger.info("Restored original Antigravity settings.json.")

    out = (proc.stdout or "").strip()
    if proc.returncode != 0 or not out:
        logger.error(f"Model '{model}' failed (exit {proc.returncode}). stderr tail: {(proc.stderr or '')[-500:]}")
        return ""
    return out


def _parse_verdict(review: str) -> str:
    """Verdict parse, fail-safe (DENIED wins on any affirmative mention)."""
    lines = review.splitlines()
    for line in lines:
        token = re.sub(r"[^A-Za-z]", "", line).upper()
        if token in ("APPROVED", "DENIED"):
            return token
    neg = {"NOT", "NEVER", "NO", "ISNT", "WASNT", "CANNOT", "CANT", "WONT", "WOULDNT", "AVOID"}
    found = set()
    for line in lines:
        words = re.findall(r"[A-Za-z']+", line.upper())
        for i, w in enumerate(words):
            if w in ("APPROVED", "DENIED") and (i == 0 or re.sub(r"[^A-Z]", "", words[i - 1]) not in neg):
                found.add(w)
    if "DENIED" in found:
        return "DENIED"
    if "APPROVED" in found:
        return "APPROVED"
    return "UNPARSEABLE"


def main() -> None:
    parser = argparse.ArgumentParser(description="Adversarial code review of an implemented change via the Antigravity CLI.")
    parser.add_argument("ledger", help="Path to the SEQNO ledger .md the change implements (absolute or repo-relative).")
    parser.add_argument("--model", default=None, help=f"Primary model label (default: '{R1_PRIMARY_MODEL}' for round 1, '{R2_PRIMARY_MODEL}' for round 2+).")
    parser.add_argument("--fallback", default=None, help=f"Fallback model label (default: '{R1_FALLBACK_MODEL}' for round 1, '{R2_FALLBACK_MODEL}' for round 2+).")
    parser.add_argument("--round", type=int, default=None, help="Explicit round number override.")
    parser.add_argument("--timeout", type=int, default=900, help="Per-model wall-clock timeout in seconds.")
    parser.add_argument("--paths", nargs="*", default=None,
                        help="Optional path prefix(es) to SCOPE the reviewed change set to.")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _restore_settings_on_signal)
    signal.signal(signal.SIGTERM, _restore_settings_on_signal)

    led = Path(args.ledger)
    if not led.is_absolute():
        led = ROOT / led
    if not led.exists():
        logger.error(f"[FATAL] Ledger not found: {args.ledger}")
        sys.exit(1)

    rel = os.path.relpath(led, ROOT)
    out_path = led.with_name(led.name[:-3] + "-codereview.md") if led.name.endswith(".md") else Path(str(led) + "-codereview.md")
    this_round = determine_round(out_path, args.round)

    pr_path = led.with_name(led.name[:-3] + "-planreview.md") if led.name.endswith(".md") else None
    pr_rel = os.path.relpath(pr_path, ROOT) if (pr_path and pr_path.exists()) else None
    if not pr_rel:
        logger.warning(f"No plan review found at {pr_path}; proceeding (planreview is enforced separately).")

    ledger_text = led.read_text(errors="replace")
    planreview_text = pr_path.read_text(errors="replace") if (pr_path and pr_path.exists()) else ""
    prior_review_text = out_path.read_text(errors="replace") if out_path.exists() else ""
    prior_review_rel = os.path.relpath(out_path, ROOT) if prior_review_text else ""
    if prior_review_text:
        logger.info(f"Found prior code review at {prior_review_rel} — embedding it for cross-run memory.")

    tracked, untracked, diff_text = _gather_change_set(args.paths)
    agy = _agy_bin()
    prompt = _build_prompt(rel, pr_rel, tracked, untracked, diff_text,
                           ledger_text=ledger_text, planreview_text=planreview_text,
                           prior_review_text=prior_review_text, prior_review_rel=prior_review_rel)

    primary_model, fallback_model = resolve_models_for_round(this_round, args.model, args.fallback)
    models = [primary_model] + ([fallback_model] if fallback_model and fallback_model != primary_model else [])
    logger.info(f"Review round: {this_round} (primary='{primary_model}', fallback='{fallback_model}')")

    review, used = "", ""
    for model in models:
        logger.info(f"Requesting adversarial code review of '{rel}' change set from '{model}'...")
        review = _run_model(agy, model, prompt, args.timeout)
        if review:
            used = model
            logger.info(f"Model '{used}' successfully produced code review for round {this_round}.")
            break
        logger.warning(f"'{model}' produced no review; escalating to next model if available.")

    if not review:
        logger.error("[FATAL] All models failed to produce a review. Escalate to the USER (check `agy` auth/quota).")
        sys.exit(1)

    verdict = _parse_verdict(review)
    if verdict == "APPROVED":
        logger.info("VERDICT: APPROVED — code/config may be committed for this ledger.")
    elif verdict == "DENIED":
        logger.warning("VERDICT: DENIED — remediate every point and re-run; do NOT commit.")
    else:
        logger.error("VERDICT: UNPARSEABLE — treat as NOT approved (do NOT commit).")

    from datetime import datetime, timezone
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    header = (f"# Code Review — {rel}\n\n_Model: {used} · Round: {this_round} · Verdict (parsed): {verdict} · "
              f"Generated: {stamp} · Tool: codereview.py (Antigravity `agy`)_\n\n---\n\n")
    Path(out_path).write_text(header + review + "\n")
    logger.info(f"Code review ({used}, {verdict}) saved to {os.path.relpath(out_path, ROOT)}")


if __name__ == "__main__":
    main()
