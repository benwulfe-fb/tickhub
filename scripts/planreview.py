#!/usr/bin/env python
"""planreview.py — adversarial plan review of a changelog via the Antigravity `agy` CLI.

Sends a named changelog to Claude Sonnet 4.6 (Round 1) / Gemini 3.8 Flash (Round 2+) through
`agy --print` (which reuses the existing Antigravity consumer-OAuth login — no API key needed)
and writes a scientific-journal style adversarial review next to the changelog as `<changelog>-planreview.md`.

This is the PRIMARY, REQUIRED plan review run BEFORE implementation starts.

Usage:
    python scripts/planreview.py aiChangeLog/2026-09-23/001_foo.md
    python scripts/planreview.py <changelog.md> [--model "Claude Sonnet 4.6 (Thinking)"]
                                                 [--fallback "Gemini 3.8 Flash (High)"]
                                                 [--timeout 900]
"""
import os
import re
import sys
import json
import shutil
import logging
import argparse
import signal
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
    """Resolve (primary_model, fallback_model) given round index and optional explicit CLI overrides."""
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


# Convergence budget
ROUND_CAP = 2          # counted rounds available without operator involvement
HARD_CEILING = ROUND_CAP + 1   # --operator-override buys exactly ONE round past the cap, never more
MAX_B3_HALTS = 2       # B3 (unmeasured-foundation) halts are uncounted, but not unbounded

os.makedirs("output", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("output/planreview___stdout.txt"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("planreview")

_SETTINGS_BACKUP = None


def _restore_settings_on_signal(signum, _frame):
    if _SETTINGS_BACKUP is not None and SETTINGS.exists():
        _atomic_write(SETTINGS, _SETTINGS_BACKUP)
        logger.warning(f"Signal {signum} received; restored original Antigravity settings.json before exit.")
    sys.exit(128 + signum)


def _atomic_write(path: Path, text: str) -> None:
    """Write via temp file + os.replace so crash mid-write cannot leave settings.json half-written."""
    tmp = path.with_suffix(path.suffix + ".prtmp")
    tmp.write_text(text)
    os.replace(tmp, path)


# Code/config domain whose uncommitted changes block a NEW plan review (clean-tree gate).
CODE_PREFIXES = ("pkg/", "cmd/", "python/", "c/", "scripts/", "tests/", "examples/",
                 "GEMINI.md", ".gitignore", "Makefile", "go.mod", "go.sum", "pyproject.toml")



def _assert_clean_code_tree() -> None:
    """Refuse to review a new plan while code/config is uncommitted — commit existing work first.

    A plan must be reviewed against a clean code/config baseline so each commit is an atomic, reviewed
    unit (paired with `codereview.py`). Parses `git status --porcelain` and fails fast on any path under CODE_PREFIXES.
    """
    proc = subprocess.run(["git", "status", "--porcelain"], cwd=str(ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        logger.warning(f"git status failed (exit {proc.returncode}); skipping clean-tree gate: {(proc.stderr or '').strip()[:200]}")
        return
    dirty = []
    for line in (proc.stdout or "").splitlines():
        if not line.strip():
            continue
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        path = path.strip().strip('"')
        if path.startswith(CODE_PREFIXES):
            dirty.append(path)
    if dirty:
        logger.error("[FATAL] Uncommitted code/config changes present — commit existing work before reviewing a new plan (clean-tree gate):")
        for p in sorted(set(dirty)):
            logger.error(f"    {p}")
        logger.error("Run `git commit` (after an APPROVED codereview) to clear the tree, then re-run planreview.py.")
        sys.exit(1)


def _canonical_out(cl: Path) -> Path:
    """`<ledger>.md` -> `<ledger>-planreview.md`."""
    return cl.with_name(cl.name[:-3] + "-planreview.md") if cl.name.endswith(".md") else Path(str(cl) + "-planreview.md")


def _archive_paths(cl: Path) -> tuple:
    """(counted-round archives, uncounted B3-halt archives) for this ledger, as sorted Path lists."""
    stem = cl.name[:-3] if cl.name.endswith(".md") else cl.name
    rounds = sorted(cl.parent.glob(f"{stem}-planreview-r*.md"))
    b3 = sorted(cl.parent.glob(f"{stem}-planreview-b3-*.md"))
    return rounds, b3


def _next_index(paths, pattern: str) -> int:
    """Highest N present in `paths` per `pattern`, plus 1."""
    hi = 0
    for p in paths:
        m = re.search(pattern, p.name)
        if m:
            hi = max(hi, int(m.group(1)))
    return hi + 1


def _seed_transition(cl: Path) -> None:
    """One-time migration for ledgers reviewed BEFORE round archiving existed."""
    canonical = _canonical_out(cl)
    rounds, b3 = _archive_paths(cl)
    if canonical.exists() and not rounds and not b3:
        stem = cl.name[:-3] if cl.name.endswith(".md") else cl.name
        seeded = cl.parent / f"{stem}-planreview-r1.md"
        seeded.write_text(canonical.read_text(errors="replace"))
        logger.info(f"Transition seeding: existing {canonical.name} recorded as round 1 ({seeded.name}).")


_VERDICT_RE = re.compile(
    r"Verdict:\s*\**\s*(Proceed-with-noted-risks|Proceed|Simplify-first|Blocked\s*\(\s*B([1-4])\s*\))",
    re.IGNORECASE)


def _parse_verdict(review: str) -> tuple:
    """Returns (verdict_text_or_empty, is_b3_halt)."""
    m = _VERDICT_RE.search(review or "")
    if not m:
        return "", False
    return m.group(1), (m.group(2) == "3")


def _assert_round_budget(cl: Path, override_reason: str) -> int:
    """Fail fast if this plan has exhausted its review budget."""
    rounds, b3 = _archive_paths(cl)
    this_round = _next_index(rounds, r"-planreview-r(\d+)\.md$")

    if len(b3) >= MAX_B3_HALTS:
        logger.error(f"[FATAL] {len(b3)} B3 (unmeasured-foundation) halts already recorded for this plan "
                     f"— the reviewer has twice found the root cause asserted rather than measured.")
        logger.error("Take the measurement and rewrite the ledger's `## Measurement` section, or open a "
                     "diagnostic ledger whose GOAL is to measure. Do not re-run this review first.")
        sys.exit(1)

    if this_round > HARD_CEILING:
        logger.error(f"[FATAL] Round {this_round} exceeds the hard ceiling of {HARD_CEILING} "
                     f"(ROUND_CAP={ROUND_CAP} + one operator override). There is no override for this.")
        logger.error("A plan that cannot converge in this budget is too large or too speculative: "
                     "abandon it and open a smaller ledger.")
        sys.exit(1)

    if this_round > ROUND_CAP and not override_reason:
        logger.error(f"[FATAL] Round-cap reached: {ROUND_CAP} plan reviews already recorded "
                     f"({', '.join(p.name for p in rounds)}).")
        logger.error("The two legitimate exits are:")
        logger.error("  (1) IMPLEMENT — remaining non-blocking findings are recorded in the ledger and "
                     "carried to codereview.py; they do not hold implementation.")
        logger.error("  (2) ABANDON this plan and open a smaller one.")
        logger.error(f"To buy exactly ONE more round, re-run with --operator-override \"<reason>\". That "
                     f"flag requires an explicit per-run OK from the USER.")
        sys.exit(1)

    if override_reason:
        logger.warning(f"OPERATOR OVERRIDE in effect for round {this_round}: {override_reason}")
    return this_round


def _agy_bin() -> str:
    """Resolves the `agy` executable (PATH, then standard ~/.local/bin install)."""
    found = shutil.which("agy") or str(Path.home() / ".local" / "bin" / "agy")
    if not os.path.exists(found):
        logger.error(f"[FATAL] Antigravity CLI not found at '{found}'. Is `agy` installed?")
        sys.exit(1)
    return found


def _build_prompt(rel_changelog: str, changelog_text: str,
                   prior_review_text: str = "", prior_review_rel: str = "",
                   this_round: int = 1) -> str:
    """Adversarial scientific-journal reviewer prompt for an as-yet-unimplemented plan."""
    prior_block = ""
    if prior_review_text:
        prior_block = (
            f"\n\n===== PRIOR PLAN REVIEW (most recent run on this changelog): {prior_review_rel} =====\n"
            f"{prior_review_text}\n===== END PRIOR PLAN REVIEW =====\n\n"
            "PRIOR-REVIEW HANDLING (read this before reviewing): the block above is a PRIOR "
            "adversarial review of an EARLIER draft of the SAME changelog. The changelog embedded "
            "below may contain 'Plan-review remediation (round N)' sections that respond point-by- "
            "point to the prior review's findings — confirming a fix, refuting a finding as "
            "mistaken, or recording an EXPLICIT decision to accept a residual risk. Cross-reference "
            "the prior review's numbered findings against the changelog's own disposition of each:\n"
            "  - If the changelog's remediation text shows the concern was actually addressed in the "
            "design — do not re-raise it.\n"
            "  - If the changelog explicitly refutes a finding with a specific, checkable claim — "
            "evaluate whether that refutation holds; if it does, do not re-raise it.\n"
            "  - If the changelog records an explicit decision to accept a residual risk — note it "
            "in section 4 as accepted/tracked rather than treating it as a fresh blocking objection.\n"
            "  - Only drive your verdict on: (a) a genuinely NEW weakness not present in the prior "
            "review or the changelog's own disposition, or (b) a prior finding whose claimed "
            "remediation, on inspection of the CURRENT changelog text, was not actually made.\n\n"
            "DISPOSITIONS ARE EVIDENCE, NOT ARGUMENT (this governs how you read the sections above):\n"
            "  - A disposition is a POINTER INTO THE DESIGN, not a claim submitted for your "
            "adjudication. Do NOT raise findings about the wording, framing, rhetoric, or "
            "persuasiveness of a disposition.\n"
            "  - If a disposition claims a fix, VERIFY IT AGAINST THE DESIGN SECTIONS. If the design "
            "shows the fix, the finding is closed — say so in one line and move on.\n"
            "  - A disposition you find unconvincing is a finding ONLY IF the underlying DESIGN "
            "defect still exists AND is blocking (B1-B4 below).\n"
            "  - Never treat a disposition's existence as new surface to attack. The design is what "
            "is under review; the dispositions only tell you where to look."
        )

    final = this_round >= ROUND_CAP
    round_block = (
        f"\n\nREVIEW ROUND {this_round} OF {ROUND_CAP} (hard budget)."
        + ("  THIS IS THE FINAL ROUND. After it, the author implements. Raise ONLY blocking defects "
           "(B1-B4 below); anything else you list will be recorded in the ledger and carried to the "
           "post-implementation code audit, which is the correct place for it. Do NOT withhold a "
           "Proceed verdict over a non-blocking observation."
           if final else
           "  A further round is available, but each round costs the author real time: do not bank "
           "findings for later rounds, and do not pad this one.")
        + "\n"
    )
    return (
        "You are a senior adversarial reviewer of change plans for `tickhub` — a high-performance "
        "1Hz market data projection and shared memory engine for quantitative trading (Go, C, Python ctypes/Arrow/PyTorch). "
        "Be genuinely skeptical and hunt hard for ways this plan fails.\n\n"
        "Your job is NOT to enumerate every imperfection. It is to do exactly two things:\n"
        "  (a) catch the small number of defects that would actually cause this plan to fail, and\n"
        "  (b) force the plan to be as SMALL as it can be while still achieving its stated goal.\n"
        "Calibrate to what is actually being changed — a helper script is not an SHM layout redesign. "
        "A long review is not a good review; a review that names the one thing that matters is.\n\n"
        + round_block + "\n"
        f"The file `{rel_changelog}` is a CHANGELOG describing a technical PLAN that has NOT yet been implemented "
        "(the code does not exist yet). Its full text is embedded below — this is the ONLY grounding available "
        "for this review. You do NOT need to, and MUST NOT claim to, independently re-locate or verify this file "
        "or any other repository file: this sandbox does not reliably expose the repository, so a claim of having "
        "read something beyond the text below would be unverifiable and must not be made. Do not claim the "
        "changelog 'does not exist' — it is embedded in full below. Do not invent, assume, or reference codebase "
        "behavior beyond what this embedded text itself describes; ground every objection in the embedded text. "
        "You are READ-ONLY regardless: do NOT edit any files, do NOT run shell commands, do NOT modify repository "
        "state.\n"
        + prior_block +
        f"\n\n===== CHANGELOG: {rel_changelog} =====\n{changelog_text}\n===== END CHANGELOG =====\n\n"
        "Be specific and cite `file:line` where the changelog itself provides it. Structure the review as markdown "
        "with these sections, in this order:\n\n"

        "1. **Summary of the Proposal** — your own restatement of what is proposed (proves you understood it).\n\n"

        "2. **Simplest Sufficient Design** — answer this BEFORE hunting for faults. Given the plan's own stated "
        "goal, what is the smallest change that achieves it? If the plan is materially larger than that, say so "
        "plainly and NAME A SPECIFIC CUT LIST: which components, state, abstractions, or files should be removed, "
        "and what is lost by removing each. Complexity that the stated goal does not require IS A DEFECT — report "
        "it as one. If the plan is already at or near the minimum, say so in one line and move on.\n\n"

        "3. **Blocking Defects** — ONLY defects in these four classes may block this plan. For each, name its "
        "class and ground it in the embedded text:\n"
        "   - **B1 goal-defeating** — the plan cannot achieve its own stated goal, or achieves it only under an "
        "assumption the plan neither states nor establishes.\n"
        "   - **B2 loss** — it risks capital, data integrity, shared memory corruption, or concurrency deadlocks.\n"
        "   - **B3 unmeasured foundation** — the plan asserts a QUANTITATIVE root cause (performance, latency, "
        "throughput, frequency, resource exhaustion, cost) AS THE JUSTIFICATION FOR ITS DESIGN, but does not "
        "present the measurement that establishes it, OR asserts WHERE the cost lives (the attribution) without "
        "measuring that attribution. Both numbers are required: the cause AND the attribution. A plan that says "
        "'X is slow because of Y, therefore build Z' without having measured that Y is in fact the cost is "
        "building on an unfalsifiable premise, and its design cannot be adjudicated from the text.\n"
        "     * WHEN B3 APPLIES: state it, name the specific measurement that is missing and how to take it, and "
        "**STOP** — do NOT review the design, do NOT raise other findings. Emit the verdict `Blocked (B3)`.\n"
        "     * B3 DOES NOT APPLY, and you must NOT raise it, when: the plan states its root cause is unknown and "
        "proposes to MEASURE or INSTRUMENT it; the plan makes no quantitative root-cause claim at all; or the "
        "plan already presents both numbers, in which case your job is to check whether they support the design.\n"
        "   - **B4 unnecessary complexity / cheaper equivalent** — a materially simpler design achieves the same "
        "stated goal. This covers STRUCTURAL over-complexity: unnecessary persistent state, unnecessary abstractions, "
        "unnecessary new components or files, a general mechanism where a specific one suffices.\n"
        "   The following are NOT blocking: style and naming; hypothetical future scale the plan does not claim to serve; "
        "and implementation-line detail (caught by code audit and tests). If there are no blocking defects, write: `None.`\n\n"

        "4. **Non-Blocking Observations** — AT MOST 5, one line each, no elaboration. These MUST NOT drive your "
        "verdict; they are recorded for the author and the later code audit. `None.` is a valid answer.\n\n"

        "5. **Methodological & Data-Alignment Concerns** — shared memory alignment, 1Hz projection semantics, "
        "look-ahead leakage, half-open interval $[T-1\\text{s}, T)$, illiquid forward-fill, quote-before-trade tie-breaking, "
        "cross-language ABI parity (Go/C/Python). Write `N/A` if not applicable.\n\n"

        "6. **Missing Telemetry** — ONLY what the plan must measure to know the change worked, or to know it must "
        "be rolled back. `None.` is a valid answer.\n\n"

        "7. **Verdict** — begin this section with a single line in EXACTLY this format, then one short paragraph "
        "of justification:\n"
        "   `**Verdict: <X>**`\n"
        "   where `<X>` is exactly one of:\n"
        "   - `Proceed` — implement as written.\n"
        "   - `Proceed-with-noted-risks` — implement; the author records your noted risks in the ledger.\n"
        "   - `Simplify-first` — the plan works, but is materially larger than its goal requires.\n"
        "   - `Blocked (B1)` / `Blocked (B2)` / `Blocked (B3)` / `Blocked (B4)` — a defect of that class must be "
        "resolved before implementation.\n"
        "   This line is parsed mechanically; do not reword it.\n\n"

        "Output ONLY the review document itself — do not narrate your tool usage."
    )


def _run_model(agy: str, model: str, prompt: str, timeout: int) -> str:
    """Sets the print-mode model in settings.json, runs `agy --print`, restores settings. Returns stdout or ''."""
    prompt_bytes = len(prompt.encode("utf-8", "ignore"))
    logger.info(f"Constructed prompt is {prompt_bytes} bytes.")
    if prompt_bytes > 126_000:
        logger.error(
            f"[FATAL] prompt is {prompt_bytes} bytes > 126000 (Linux per-arg MAX_ARG_STRLEN safety bound).")
        sys.exit(1)
    global _SETTINGS_BACKUP
    backup = SETTINGS.read_text() if SETTINGS.exists() else None
    if backup is None:
        logger.error(f"[FATAL] Antigravity settings not found at {SETTINGS}; cannot select model.")
        sys.exit(1)
    _SETTINGS_BACKUP = backup
    try:
        cfg = json.loads(backup)
        cfg["model"] = model
        _atomic_write(SETTINGS, json.dumps(cfg, indent=2))
        logger.info(f"Selected model '{model}'; invoking `agy --print` (timeout {timeout}s)...")
        mins = max(1, timeout // 60)
        proc = subprocess.run(
            [agy, "--print-timeout", f"{mins}m", "--print", prompt],
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
        logger.info("Restored original Antigravity settings.json.")

    out = (proc.stdout or "").strip()
    if proc.returncode != 0 or not out:
        logger.error(f"Model '{model}' failed (exit {proc.returncode}). stderr tail: {(proc.stderr or '')[-500:]}")
        return ""
    return out


def main() -> None:
    signal.signal(signal.SIGINT, _restore_settings_on_signal)
    signal.signal(signal.SIGTERM, _restore_settings_on_signal)

    parser = argparse.ArgumentParser(description="Adversarial plan review of a changelog via the Antigravity CLI.")
    parser.add_argument("changelog", help="Path to the changelog .md to review (absolute or repo-relative).")
    parser.add_argument("--model", default=None, help=f"Primary model label (default: '{R1_PRIMARY_MODEL}' for round 1, '{R2_PRIMARY_MODEL}' for round 2+).")
    parser.add_argument("--fallback", default=None, help=f"Fallback model label (default: '{R1_FALLBACK_MODEL}' for round 1, '{R2_FALLBACK_MODEL}' for round 2+).")
    parser.add_argument("--round", type=int, default=None, help="Explicit round number to simulate or force.")
    parser.add_argument("--timeout", type=int, default=900, help="Per-model wall-clock timeout in seconds.")
    parser.add_argument("--operator-override", default="", metavar="REASON",
                        help=f"Buy exactly ONE round past ROUND_CAP={ROUND_CAP} (never more). Requires an "
                             f"explicit per-run OK from the USER.")
    args = parser.parse_args()

    _assert_clean_code_tree()

    cl = Path(args.changelog)
    if not cl.is_absolute():
        cl = ROOT / cl
    if not cl.exists():
        logger.error(f"[FATAL] Changelog not found: {args.changelog}")
        sys.exit(1)

    rel = os.path.relpath(cl, ROOT)
    out_path = _canonical_out(cl)

    _seed_transition(cl)
    this_round = _assert_round_budget(cl, args.operator_override)
    if args.round is not None:
        this_round = args.round

    changelog_text = cl.read_text(errors="replace")
    prior_review_text = out_path.read_text(errors="replace") if out_path.exists() else ""
    prior_review_rel = os.path.relpath(out_path, ROOT) if prior_review_text else ""
    if prior_review_text:
        logger.info(f"Found prior plan review at {prior_review_rel} — embedding it so the reviewer "
                    f"has memory across rounds on this changelog.")

    agy = _agy_bin()
    prompt = _build_prompt(rel, changelog_text,
                           prior_review_text=prior_review_text, prior_review_rel=prior_review_rel,
                           this_round=this_round)

    primary_model, fallback_model = resolve_models_for_round(this_round, args.model, args.fallback)
    models = [primary_model] + ([fallback_model] if fallback_model and fallback_model != primary_model else [])
    logger.info(f"Review round: {this_round} (primary='{primary_model}', fallback='{fallback_model}')")

    review = ""
    used = ""
    for model in models:
        logger.info(f"Requesting adversarial plan review of '{rel}' from '{model}'...")
        review = _run_model(agy, model, prompt, args.timeout)
        if review:
            used = model
            logger.info(f"Model '{used}' successfully produced plan review for round {this_round}.")
            break
        logger.warning(f"'{model}' produced no review; escalating to next model if available.")

    if not review:
        logger.error("[FATAL] All models failed to produce a review. Escalate to the USER (check `agy` auth/quota).")
        sys.exit(1)

    from datetime import datetime, timezone
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

    verdict, is_b3 = _parse_verdict(review)
    rounds, b3s = _archive_paths(cl)
    stem = cl.name[:-3] if cl.name.endswith(".md") else cl.name
    if is_b3:
        n = _next_index(b3s, r"-planreview-b3-(\d+)\.md$")
        unchanged = _next_index(rounds, r"-planreview-r(\d+)\.md$")
        archive = cl.parent / f"{stem}-planreview-b3-{n}.md"
        round_label = (f"B3 halt {n}/{MAX_B3_HALTS} (UNCOUNTED — round budget unchanged, "
                       f"still {unchanged}/{ROUND_CAP})")
    else:
        archive = cl.parent / f"{stem}-planreview-r{this_round}.md"
        round_label = f"round {this_round}/{ROUND_CAP}"

    ovr = f" · OPERATOR OVERRIDE: {args.operator_override}" if args.operator_override else ""
    vtxt = verdict or "UNPARSEABLE (counts as a normal round)"
    header = (f"# Plan Review — {rel}\n\n"
              f"_Model: {used} · Round: {this_round} · Generated: {stamp} · Tool: planreview.py (Antigravity `agy`)_\n\n"
              f"_Budget: {round_label} · Verdict parsed: **{vtxt}**{ovr}_\n\n---\n\n")
    body = header + review + "\n"
    archive.write_text(body)
    Path(out_path).write_text(body)
    logger.info(f"Plan review ({used}) saved to {os.path.relpath(out_path, ROOT)} "
                f"and archived as {archive.name} [{round_label}] — verdict: {vtxt}")

    if is_b3:
        logger.warning("Verdict is Blocked (B3) — the plan's quantitative root cause is asserted, not "
                       "measured. TAKE THE MEASUREMENT and rewrite the ledger's `## Measurement` section "
                       "before re-running.")
    elif this_round >= ROUND_CAP and not str(verdict).lower().startswith("blocked"):
        logger.info(f"Final round consumed. Implement now — remaining non-blocking "
                    f"observations are recorded in the ledger and carried to codereview.py.")


if __name__ == "__main__":
    main()
