#!/usr/bin/env python
"""Stale-path gate for the 2026-07-28 venue-tree restructure.

Greps every tracked file for pre-restructure path literals and exits non-zero
with ``file:line`` for each hit, so a move that leaves a dangling reference
fails a gate instead of failing at 3am on the live box.

Run from anywhere::

    python tools/pathcheck.py          # 0 = clean, 1 = violations
    python tools/pathcheck.py -v       # also print the scan summary

Why not ripgrep: ``rg`` is not on the operator's PATH, and a gate that silently
no-ops when its engine is missing is worse than no gate. File enumeration is
delegated to ``git ls-files`` — which is always present, and which skips the
~20 GB of gitignored research data for free — and the matching is plain ``re``.

--- Marking an intentional reference ---------------------------------------

Some pre-restructure literals are CORRECT and must survive:

  * the legacy-first data shim, whose whole job is to name ``backtest/...``
    (the 20 GB substrate never physically moved — see
    ``polymarket/backtest/__init__.py``),
  * the deploy staging map, which deliberately writes the NEW local tree into
    the UNCHANGED remote layout (``$PROJECT_DIR/src/cryptobot``, ``deploy/``)
    so the EC2 needs no cutover.

Put ``legacy-path-ok`` in a trailing comment on such a line, with a reason::

    ("legacy", _REPO_ROOT / "backtest" / "data" / "x.parquet"),  # legacy-path-ok: 20 GB tree stayed put

An unexplained bare marker is a code smell; the reason is what a future reader
greps for. The marker exempts the line it sits on AND the line immediately
after it — because some formats cannot carry a trailing comment at all. A
systemd ``ExecStart=`` swallows everything after the ``=`` (a trailing
``# legacy-path-ok`` would be passed to the process as argv), so unit files put
the marker on the preceding comment line instead::

    # legacy-path-ok: remote layout is frozen until the Phase 3 cutover
    ExecStart=/home/ec2-user/cryptobot/deploy/cron.daily.sh
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

# Any line containing this token is exempt from every rule (see module docstring).
MARKER = "legacy-path-ok"

# (rule id, compiled pattern, human explanation of the correct replacement)
RULES: list[tuple[str, re.Pattern[str], str]] = [
    (
        "stale-import",
        re.compile(r"^\s*(?:from|import)\s+backtest\b"),
        "the research package is `polymarket.backtest` now",
    ),
    (
        "legacy-backtest-path",
        # A `backtest/...` PATH LITERAL: quote-prefixed (python/json/js) or
        # shell-variable-prefixed ("$REPO_ROOT/backtest/data"). Deliberately
        # NOT bare prose — a comment or .md line reading "backtest/optimize_runs
        # holds X" is a historical statement, not a dangling reference, and
        # flagging those buries the real hits under ~150 false positives.
        re.compile(r"""(?:["']|\$\{?\w+\}?/)backtest/"""),
        "research tree lives under <venue>/backtest/ (or resolve via the "
        "BT_* shim in polymarket/backtest/__init__.py)",
    ),
    (
        "legacy-backtest-root-math",
        # ROOT / "backtest" — the shim's job, nobody else's.
        re.compile(r"(?:ROOT|root)\s*/\s*[\"']backtest[\"']"),
        "import BT_DATA_DIR / BT_CACHE_DIR / BT_RUNS_DIR "
        "from polymarket.backtest instead of rebuilding the path",
    ),
    (
        "legacy-src-path",
        # Same literal-only discipline as legacy-backtest-path.
        re.compile(r"""(?:["']|\$\{?\w+\}?/)src/cryptobot"""),
        "the package is at polymarket/src/cryptobot locally "
        "(deploy stages it AS src/ on the box — mark those lines)",
    ),
    (
        "legacy-scripts-path",
        re.compile(r"(?<!\w)scripts/(?:pull_|forward_oos|pmdata_|smoke_check)"),
        "scripts/ was dissolved: pull_* deleted 2026-08-04 (git history), "
        "forward_oos_*/pmdata_* -> polymarket/backtest/{lib,fetch}/, "
        "smoke_check -> tools/",
    ),
    (
        "legacy-deploy-path",
        re.compile(r"(?<!\w)deploy/(?:deploy\.sh|install_remote|refresh-decision-table|cron\.daily)"),
        "deploy/ was split into polymarket/infra/ + <venue>/{live,recorder}/service/",
    ),
    (
        "unsanctioned-root-math",
        # parents[N] repo-root math. `parents=True` (mkdir) does not match.
        re.compile(r"\.parents\[\d+\]"),
        "resolve roots through cryptobot.config (_REPO_ROOT / DATA_ROOT / "
        "LOGS_DIR) or polymarket.backtest's REPO_ROOT, not per-file index math",
    ),
]

# Files exempt wholesale, with the reason each one is allowed to be dirty.
SKIP_EXACT: dict[str, str] = {
    # These two DEFINE and DOCUMENT the patterns; matching themselves is noise.
    "tools/pathcheck.py": "defines the rules",
    "tools/README.md": "documents the rules",
}

SKIP_PREFIXES: tuple[tuple[str, str], ...] = (
    ("docs/", "design docs + runbooks record history, including old paths"),
    (".claude/", "agent scratch, not project source"),
)

# Path SEGMENTS that exempt a file wherever they appear.
SKIP_SEGMENTS: tuple[tuple[str, str], ...] = (
    # Frozen inputs, usually JSON — which cannot carry a `legacy-path-ok`
    # comment at all. A fixture's old paths are the historical record the test
    # asserts against, not a live reference to follow.
    ("tests/fixtures/", "frozen test fixtures record the old tree verbatim"),
)

SKIP_SUFFIXES: tuple[tuple[str, str], ...] = (
    (".patch", "stash exports are frozen snapshots of the OLD tree"),
    (".ipynb", "notebook JSON carries stale outputs; nbstripout is the gate there"),
)

# Text-ish files only — anything else is skipped as binary.
BINARY_SUFFIXES = frozenset(
    {".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".zip", ".gz", ".tar",
     ".parquet", ".joblib", ".db", ".pyc", ".woff", ".woff2", ".ttf"}
)


def repo_root() -> Path:
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
        cwd=Path(__file__).resolve().parent,
    )
    return Path(out.stdout.strip())


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        capture_output=True, text=True, check=True, cwd=root,
    )
    return [p for p in out.stdout.split("\0") if p]


def skip_reason(rel: str) -> str | None:
    if rel in SKIP_EXACT:
        return SKIP_EXACT[rel]
    for prefix, why in SKIP_PREFIXES:
        if rel.startswith(prefix):
            return why
    for suffix, why in SKIP_SUFFIXES:
        if rel.endswith(suffix):
            return why
    for segment, why in SKIP_SEGMENTS:
        if segment in rel:
            return why
    if Path(rel).suffix.lower() in BINARY_SUFFIXES:
        return "binary"
    return None


def scan_line(line: str, previous: str = "") -> list[tuple[str, str]]:
    """Return [(rule_id, hint), ...] for one line ([] when clean/exempt).

    ``previous`` carries the marker for formats that cannot take a trailing
    comment (systemd directives) — see the module docstring.
    """
    if MARKER in line or MARKER in previous:
        return []
    return [(rule, hint) for rule, pattern, hint in RULES if pattern.search(line)]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="print how many files were scanned vs skipped")
    args = ap.parse_args()

    root = repo_root()
    scanned = skipped = 0
    violations: list[tuple[str, int, str, str, str]] = []

    for rel in tracked_files(root):
        if skip_reason(rel) is not None:
            skipped += 1
            continue
        path = root / rel
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            skipped += 1
            continue
        scanned += 1
        lines = text.splitlines()
        for lineno, line in enumerate(lines, start=1):
            previous = lines[lineno - 2] if lineno >= 2 else ""
            for rule, hint in scan_line(line, previous):
                violations.append((rel, lineno, rule, line.strip(), hint))

    if violations:
        print(f"pathcheck: {len(violations)} stale path reference(s)\n")
        hints: dict[str, str] = {}
        for rel, lineno, rule, line, hint in violations:
            hints[rule] = hint
            snippet = line if len(line) <= 120 else line[:117] + "..."
            print(f"{rel}:{lineno}: [{rule}] {snippet}")
        print("\nFixes:")
        for rule, hint in sorted(hints.items()):
            print(f"  {rule}: {hint}")
        print(f"\nIf a reference is intentional, add a `{MARKER}: <reason>` "
              f"comment on that line.")
        return 1

    if args.verbose:
        print(f"pathcheck: {scanned} files scanned, {skipped} skipped")
    print("pathcheck: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
