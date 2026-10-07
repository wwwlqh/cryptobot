# tools/

Repo-wide maintenance, not tied to any venue.

| Tool | What it does |
| --- | --- |
| `pathcheck.py` | Stale-path gate. Greps for pre-restructure literals (`from backtest`, `"src/cryptobot`, `scripts/pull_`, `deploy/install_remote`, unsanctioned `parents[N]` root math) and exits non-zero with `file:line`. Run it before any commit that moves files. |
| `importcheck.py` | Venue-seam gate. Enforces the DIRECTION of the arrows: core never reaches a venue package except through `cryptobot.venues.registry`, and venue packages never reach each other. Exits non-zero with `file:line` + the rule. Exemptions live in the module's own `ALLOWLIST`, each with a reason. |
| `smoke_check.py` | Boots the scanner locally and waits for the first SSE tick (`--headless --timeout N` for CI-style use). Exports all four src roots on the child's `PYTHONPATH` (`_py_src_path`), mirroring `polymarket/infra/deploy.sh`. |

## `importcheck.py` — two known limits

Both are deliberate. Know them before trusting a green run.

**1. Dynamic imports are caught only by the literal-string rule.** The gate
walks for `importlib.import_module(...)` calls (any receiver; also a bare
`import_module`) and flags the first argument when it is a **string constant**
starting with `cryptobot_`. Anything it cannot read statically — an f-string, a
name, a dict lookup, `__import__`, `getattr` on an already-imported module — is
NOT flagged, because resolving it would mean running the program. The registry's
own `importlib.import_module(module_name)` is exactly that unreadable form (and
is exempt regardless). So: a determined author can still cross the seam
dynamically. The rule exists because the ONE real dynamic crossing in the tree
(`polymarket/src/cryptobot/cli/operator.py:87-96`) is a literal, and a gate that
missed it would have been reporting on a contract it could not see.

**2. The scan set is tracked files under the four package roots — nothing
else.** Enumeration is `git ls-files -- "*.py"`, then each path is mapped to an
owning distribution by its src root (`polymarket/src/cryptobot/`,
`polymarket/src/cryptobot_polymarket/`, `{predictfun,kalshi}/live/src/cryptobot_*/`);
a file that maps to none is skipped entirely. That means **untracked files are
invisible** (write the violation, don't `git add` it, and the gate is green),
and so is every `.py` outside those four package roots — `polymarket/dashboard/`,
`platform/`, `polymarket/backtest/`, `tools/` itself. Those are not part of the
runtime seam, which is why they are out of scope, but "importcheck is clean" is
a statement about the four packages only. Files that fail to decode as UTF-8 are
also skipped silently.

## The `# legacy-path-ok` marker

`pathcheck.py` skips any line carrying `legacy-path-ok`, **and the line
immediately after it** — because some formats cannot hold a trailing comment at
all. A systemd `ExecStart=` swallows everything after the `=` into argv, so unit
files put the marker on the preceding comment line instead. (`tests/fixtures/`
is skipped wholesale for the same reason: JSON has no comments, and a fixture's
old paths are the historical record the test asserts against.)

Use the marker — with a reason — for the three cases where a pre-restructure
literal is *correct*:

1. A deliberate reference to the legacy physical data location. The ~20 GB
   research tree stayed at `backtest/` on purpose; a legacy-first fallback that
   names it is right, not stale.
2. A `Path(__file__).parents[N]` bootstrap that has to run **before** any import
   can resolve (it is what puts the repo root on `sys.path`), so it cannot
   delegate to the sanctioned helper — plus the sanctioned helper itself.
3. **The deploy staging map.** `deploy.sh` deliberately writes the new local
   tree into the UNCHANGED remote layout (`$PROJECT_DIR/src/cryptobot`,
   `$PROJECT_DIR/deploy/`) so the EC2 needs no cutover. Those literals describe
   the *server*, and stay until the Phase 3 cutover retires them.

Everything else routes through `polymarket.backtest`'s `BT_*` constants or
`cryptobot.config`'s root resolution. A fourth exemption category would be the
signal that the shim is missing a key.

Engine note: enumeration is `git ls-files` and matching is Python `re`, not
ripgrep — `rg` is not on this machine's PATH, and a gate that silently no-ops
when its engine is missing is worse than no gate.
