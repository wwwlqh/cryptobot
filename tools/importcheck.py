#!/usr/bin/env python
"""Venue-seam import gate for the 2026-07-28 restructure (Phase 2).

Phase 2 split three venue adapter packages out of the shared engine. The value
of that split is entirely in the DIRECTION of the arrows, and a direction is not
something a test suite notices — a green suite is perfectly compatible with core
quietly importing ``cryptobot_polymarket`` again. So the direction is a gate.

Run from anywhere::

    python tools/importcheck.py          # 0 = clean, 1 = violations
    python tools/importcheck.py -v       # also print the scan summary

Contracts
---------

**(a) core never imports a venue package.** ``cryptobot`` is the shared engine;
it reaches a venue only through ``cryptobot.venues.registry``'s lazy string map.
This is what makes "swap the venue" a config change rather than a refactor, and
it is why an unimplemented kalshi cannot break a Polymarket boot.

**(b) venue packages never import each other.** Before the split, the predict.fun
modules imported ``BookSnapshot``, ``TokenBucket``, ``FeeUnknown``,
``ShadowBookComparator`` and three recorder helpers straight out of the
Polymarket modules. Those are all in core now
(``cryptobot.venues.{types,shadow_book}``; the recorder helpers lived in
``cryptobot.venues.recorder_util`` until the book recorders were deleted
2026-08-04) precisely so this contract can hold — one class object, imported
from core by both sides.

**(c) venue packages MAY import core.** That is the whole point: adapters are
built on the shared engine.

All three contracts also apply to ``importlib.import_module("cryptobot_...")``
with a LITERAL string argument, not just to ``import`` statements. Core already
crosses the seam that way once on purpose (``cli/operator.py:87-96``, guarded so
a box without the relayer degrades to ``_RelayerNotConfigured``), and a gate
that only reads ``ast.Import``/``ast.ImportFrom`` nodes would have called that
file clean. Non-literal arguments — an f-string, a variable, the registry's own
``import_module(module_name)`` — cannot be judged without running the program
and are NOT flagged; see ``tools/README.md`` for that and the other known limit.

Exemptions, and why each exists
-------------------------------

* **Own-venue test packages** are exempt from (b) for their OWN venue only —
  ``cryptobot_polymarket.tests`` importing ``cryptobot_polymarket`` is not a
  cross-venue dependency. They stay bound by (b) for the OTHER venue.
* **``cryptobot.venues.registry``** is exempt from (a): the string map lives
  there. It names packages in strings, never in imports, so this exemption is
  mostly documentation.
* **``cryptobot/tests/``** is exempt from (a). Core's tests exercise the wired
  system end to end — the venue-dispatch tests must be able to reach both
  adapters to prove the seam routes correctly, and the pipeline tests build
  Polymarket-shaped market fixtures. Testing the seam requires standing on
  both sides of it. Production core modules get no such latitude.
* **An explicit ALLOWLIST** (below) records the core modules that still import
  Polymarket-only OPERATOR surfaces — pUSD transfers, the relayer,
  and the go/no-go CLI's OrderClient. These are NOT venue-dispatched: they run
  unconditionally, on every venue, and always did. Routing them through the
  adapter would mean inventing ~7 adapter members for operator plumbing that
  has exactly one implementation — the over-abstraction Phase 2 was told to
  avoid. Each entry carries its reason, and each is a Phase-3 candidate.

Adding to the allowlist should feel like a decision, not a convenience. A NEW
core module reaching a venue package is a design regression; make it an adapter
member instead.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from pathlib import Path

CORE_PACKAGE = "cryptobot"
#: venue package -> its src root, relative to the repo. Polymarket's adapter
#: shares ONE src root with the shared engine since 2026-08-04 (single src per
#: venue); the other two venues still nest theirs under `<venue>/live/src/`.
VENUE_PACKAGES: dict[str, str] = {
    "cryptobot_polymarket": "polymarket/src",
    "cryptobot_predictfun": "predictfun/live/src",
    "cryptobot_kalshi": "kalshi/live/src",
}

#: The one core module allowed to name venue packages (in strings, not imports).
REGISTRY_MODULE = "polymarket/src/cryptobot/venues/registry.py"

#: Core test tree — exempt from contract (a); see module docstring.
CORE_TEST_PREFIX = "polymarket/src/cryptobot/tests/"

#: (core file, venue package) -> why this import is still direct.
#: Every one of these is unconditional Polymarket/predict.fun operator or
#: observability plumbing, NOT a venue-dispatched decision-path seam.
ALLOWLIST: dict[tuple[str, str], str] = {
    (
        "polymarket/src/cryptobot/main.py", "cryptobot_polymarket",
    ): (
        "relayer submitter builders — NETWORK surfaces only. pUSD transfers "
        "are a Polymarket-only operator surface with one implementation; "
        "it runs on every venue and always did. SHRUNK by panel verdict "
        "D4-1: the stale-state WATCHDOG this entry used to cover "
        "(release_stale_transfer_lock) was pure SQLite hygiene over a "
        "core-owned operator table and moved to "
        "cryptobot.execution.operator_control, so an ALWAYS-ON core job no "
        "longer needs a venue package installed. SHRUNK again 2026-08-04: "
        "the auto-redeem limb (redeemer.py + its relayer builders) is gone."
    ),
    (
        "polymarket/src/cryptobot/cli/operator.py", "cryptobot_polymarket",
    ): (
        "operator CLI transfer preview/submit — same Polymarket-only surface "
        "as main.py's, reached from the terminal instead of the scheduler. "
        "Covers BOTH the static imports (:221, :260) and the DYNAMIC one at "
        ":87-96, `importlib.import_module('cryptobot_polymarket.relayer')`, "
        "which is deliberately guarded: a box without the relayer module "
        "degrades to _RelayerNotConfigured instead of failing to import."
    ),
    (
        "polymarket/src/cryptobot/cli/live_go_no_go.py", "cryptobot_polymarket",
    ): (
        "ONLINE PATH ONLY — the import sits inside _run_online (panel verdict "
        "D4-3), so `--offline`, whose whole point is needing no venue "
        "plumbing, imports and runs without the Polymarket package. The online "
        "path constructs a REAL OrderClient and distinguishes "
        "ClobClientUnavailable (offline gates only) from ValueError (exit 2); "
        "VenueAdapter.build_order_client is deliberately fail-soft-to-None for "
        "the scanner's boot, so it cannot express this. Phase-3 candidate: "
        "move the whole CLI into cryptobot_polymarket."
    ),
    (
        "polymarket/src/cryptobot/observability/sse_server.py", "cryptobot_polymarket",
    ): (
        "dashboard POST routes for pUSD transfer preview/submit — the same "
        "Polymarket-only surface, reached from the browser."
    ),
    (
        "polymarket/src/cryptobot/observability/snapshot_builder.py",
        "cryptobot_predictfun",
    ): (
        "predict.fun recorder root + signer address for the dashboard's PF "
        "wallet block, both inside blocks already gated on VENUE=='predictfun'. "
        "Display-only; nothing here reaches a decision."
    ),
}


def repo_root() -> Path:
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
        cwd=Path(__file__).resolve().parent,
    )
    return Path(out.stdout.strip())


def tracked_python_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--", "*.py"],
        capture_output=True, text=True, check=True, cwd=root,
    )
    return [p for p in out.stdout.split("\0") if p]


def top_level(module: str | None) -> str:
    """First dotted segment of a module path ('' for a bare relative import)."""
    return (module or "").split(".", 1)[0]


def imported_roots(tree: ast.AST) -> list[tuple[str, int]]:
    """Every top-level package name this module imports, with its line number.

    Relative imports (``from . import x``) resolve inside the current package
    and can never cross a distribution boundary, so they are skipped.
    """
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                found.append((top_level(alias.name), node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative
                continue
            found.append((top_level(node.module), node.lineno))
    return found


def _is_import_module_call(func: ast.expr) -> bool:
    """``importlib.import_module`` / ``il.import_module`` / bare
    ``import_module`` — the receiver's name is not load-bearing, the attribute
    is."""
    if isinstance(func, ast.Attribute):
        return func.attr == "import_module"
    return isinstance(func, ast.Name) and func.id == "import_module"


def dynamic_imported_roots(tree: ast.AST) -> list[tuple[str, int]]:
    """Venue packages reached through ``importlib.import_module("literal")``.

    An ``import`` statement is not the only way to cross the seam. Core already
    does it once on purpose — ``cli/operator.py:87-96`` resolves the relayer
    module dynamically so a box without it degrades to ``_RelayerNotConfigured``
    instead of failing to import — and a gate that reads only ``ast.Import`` /
    ``ast.ImportFrom`` nodes would call that file clean while core quietly
    depended on a venue.

    Only a LITERAL string argument can be judged: an f-string or a variable is
    unresolvable without running the program, so this rule is deliberately
    narrow rather than heuristic. The registry's own
    ``import_module(module_name)`` is the canonical variable form and is exempt
    anyway. Documented as a known limit in ``tools/README.md``.
    """
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _is_import_module_call(node.func):
            continue
        if not node.args:
            continue
        arg = node.args[0]
        if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
            continue
        root = top_level(arg.value)
        if root.startswith("cryptobot_"):
            found.append((root, node.lineno))
    return found


def owning_package(rel: str) -> str | None:
    """Which distribution a tracked file belongs to, by its src root."""
    for pkg, src_root in VENUE_PACKAGES.items():
        if rel.startswith(f"{src_root}/{pkg}/"):
            return pkg
    if rel.startswith("polymarket/src/cryptobot/"):
        return CORE_PACKAGE
    return None


def violations_for(rel: str, source: str) -> list[tuple[int, str, str]]:
    """[(lineno, rule, message), ...] for one file."""
    owner = owning_package(rel)
    if owner is None:
        return []
    try:
        tree = ast.parse(source, filename=rel)
    except SyntaxError as exc:  # a syntax error is compileall's job to report
        return [(exc.lineno or 0, "unparseable", f"could not parse: {exc.msg}")]

    out: list[tuple[int, str, str]] = []
    static = [(pkg, ln, False) for pkg, ln in imported_roots(tree)]
    dynamic = [(pkg, ln, True) for pkg, ln in dynamic_imported_roots(tree)]
    for root_pkg, lineno, is_dynamic in static + dynamic:
        if root_pkg not in VENUE_PACKAGES:
            continue
        how = "dynamically import" if is_dynamic else "import"
        tag = "-dynamic" if is_dynamic else ""

        if owner == CORE_PACKAGE:
            if rel == REGISTRY_MODULE or rel.startswith(CORE_TEST_PREFIX):
                continue
            if (rel, root_pkg) in ALLOWLIST:
                continue
            out.append((
                lineno, f"core{tag}-imports-venue",
                f"core `cryptobot` must not {how} `{root_pkg}` — reach venues "
                f"through cryptobot.venues.get_adapter(), or add an adapter "
                f"member for this seam.",
            ))
            continue

        # owner is a venue package
        if root_pkg == owner:
            continue
        out.append((
            lineno, f"venue{tag}-imports-venue",
            f"`{owner}` must not {how} `{root_pkg}` — anything both venues "
            f"need belongs in core (cryptobot.venues.types / .shadow_book).",
        ))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="print the scan summary even when clean")
    args = parser.parse_args()

    root = repo_root()
    scanned = 0
    found: list[tuple[str, int, str, str]] = []
    for rel in tracked_python_files(root):
        if owning_package(rel) is None:
            continue
        try:
            # utf-8-sig: a dozen core test files carry a UTF-8 BOM, which
            # ast.parse rejects as a non-printable character. Reading them as
            # plain utf-8 would report every one as "unparseable" — a gate that
            # cries wolf gets switched off.
            source = (root / rel).read_text(encoding="utf-8-sig")
        except (UnicodeDecodeError, OSError):
            continue
        scanned += 1
        for lineno, rule, message in violations_for(rel, source):
            found.append((rel, lineno, rule, message))

    if found:
        print(f"importcheck: {len(found)} venue-seam violation(s)\n")
        for rel, lineno, rule, message in found:
            print(f"{rel}:{lineno}: [{rule}] {message}")
        print(
            "\nIf an import is genuinely unavoidable operator plumbing rather "
            "than a decision-path seam, add it to ALLOWLIST in "
            "tools/importcheck.py WITH a reason."
        )
        return 1

    if args.verbose:
        print(
            f"importcheck: {scanned} files scanned, "
            f"{len(ALLOWLIST)} allowlisted core->venue import(s)"
        )
    print("importcheck: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
