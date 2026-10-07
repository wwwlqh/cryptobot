"""Build dashboard/snapshot.json — the static fallback context for the journal.

Reads:
  v2_step1last_live.json  - live portfolio/sleeve config, wherever
                            config.V6_CONFIG_PATH resolves it (deployed
                            runtime/config/, else polymarket/live/config/)
  --db-path PATH                           - optional cryptobot.db to hydrate
                                             operator state
  --venue {polymarket,predictfun}          - which BOX this snapshot ships to
                                             (deploy.sh passes the box role).
                                             Stamped into the payload as
                                             `venue`.

Writes:
  dashboard/snapshot.json   - static portfolio + live-state fallback context

The Strategy/Research tabs were retired (2026-07-07/07-09). The retired Research
tab's decision-table stats block was removed 2026-07-11 (grep-verified
zero-consumer: no dashboard page reads window.SNAPSHOT.decision_table — the whole
static snapshot is a fallback the live surfaces read the SSE payload over). The
mode_state / live_wallet blocks and the operator events/transfers history went
the same way 2026-08-05. The portfolio + operator blocks stay as the static
fallback (review #16 keeps their shape in lockstep with the live payload). Run
after a config change to refresh the fallback.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve()
# ROOT must satisfy BOTH tree layouts, and a fixed parent depth cannot:
# locally this file is `<repo>/polymarket/dashboard/build_snapshot.py` (the
# dashboard moved under the venue on 2026-08-04, so the repo root is TWO up),
# while deploy.sh stages it flat as `$PROJECT_DIR/dashboard/build_snapshot.py`
# (project root ONE up). Walk up for the repo marker exactly like
# cryptobot.config._resolve_repo_root does, and fall back to the deployed
# shape — no pyproject.toml ships in the runtime bundle.
ROOT = _HERE.parent.parent
for _candidate in _HERE.parents:
    if (_candidate / "pyproject.toml").is_file():
        ROOT = _candidate
        break
    if (_candidate / "polymarket" / "src" / "cryptobot").is_dir():
        ROOT = _candidate
        break
# The package roots differ between the local tree and the deployed bundle —
# deploy.sh maps one onto the other, so try both shapes.
#
# Phase 2 (2026-07-28) added the venue adapter packages. They are NOT optional
# here: the snapshot books cash P&L through execution.grade_fees, which prices
# every row through cryptobot.venues -> the venue adapter. Locally they sit in
# their venue trees (the Polymarket adapter shares `polymarket/src` with the
# engine since 2026-08-04); in the deployed bundle deploy.sh stages them flat
# next to cryptobot in src/, so `ROOT / "src"` already covers them there.
for _src in (
    ROOT / "polymarket" / "src",
    ROOT / "predictfun" / "live" / "src",
    ROOT / "src",
):
    if _src.is_dir() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))

# V6_CONFIG tracks whatever the live loader reads (config.V6_CONFIG_PATH) so
# the static snapshot's wallet-group caps + existence guard follow the deployed
# config automatically. Fallback = v2_step1last_live.json (exp044 retired
# 2026-07-09; its config no longer exists) — snapshot build must never hard-fail.
# The fallback chain mirrors config._resolve_artifact: deployed runtime/ copy,
# then the pre-2026-07-28 research-tree path, then the venue live config.
try:
    from cryptobot import config as _cfg

    V6_CONFIG = Path(_cfg.V6_CONFIG_PATH)
except Exception:  # noqa: BLE001
    for _candidate in (
        ROOT / "runtime" / "config" / "twap_symmetric_live.json",
        ROOT / "polymarket" / "live" / "config" / "twap_symmetric_live.json",
    ):
        V6_CONFIG = _candidate
        if _candidate.is_file():
            break
OUT = Path(__file__).parent / "snapshot.json"


def load_portfolio() -> dict | None:
    """Return the live portfolio block for the dashboard's static fallback.

    Delegates to the LIVE loaders (`load_sleeves` + `_portfolio_block`) so the
    static snapshot can never drift from the SSE payload's portfolio shape or
    values (review #16). This is why the exp044 migration — which retired
    `dry_run_recommended.json` and flipped `config.V6_CONFIG_PATH` to
    `exp044_live.json` — required no per-field reparsing here: the static
    snapshot follows the live loader automatically. `_portfolio_block` already
    forces `max_walk_price` to the runtime override (config.MAX_WALK_PRICE),
    counts shared-equity groups once, and now carries the ORB per-coin filters.

    Returns None when the deployed config artifact is absent or unreadable;
    the dashboard then relies on the live SSE `portfolio` block.
    """
    if not V6_CONFIG.exists():
        return None
    try:
        from cryptobot.observability.snapshot_builder import _portfolio_block
        from cryptobot.strategy.portfolio_loader import load_sleeves

        return _portfolio_block(load_sleeves())
    except Exception:  # noqa: BLE001 — snapshot build must never hard-fail
        return None


def _round_money(value):
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _json_dict(raw) -> dict:
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _readiness_checks(payload: dict) -> list[dict]:
    checks = payload.get("readiness_check_results")
    if not isinstance(checks, list):
        return []
    out = []
    for item in checks:
        if not isinstance(item, dict):
            continue
        out.append({
            "name": str(item.get("name") or ""),
            "passed": bool(item.get("passed")),
            "detail": str(item.get("detail") or ""),
        })
    return out


def load_live_state(db_path: Path | None) -> dict:
    """Hydrate the LIVE operator block from the live SQLite DB.

    Returns a dict carrying `operator` (singleton state + readiness). When
    `db_path` is None or the DB is missing, the block defaults to the
    schema's SQL DEFAULT — operator STOPPED_PROCESSING. This matches what
    the live `snapshot_builder` would produce on a fresh deploy.
    (The `wallet_groups` cap-utilization block was dropped 2026-07-14 with
    the retired group bar; `mode_state`, `live_wallet` and the operator
    events/transfers history followed 2026-08-05 — no dashboard surface
    read any of them off the static snapshot.)
    """
    defaults = {
        "operator": {
            "state": "STOPPED_PROCESSING",
            "boot_dry_run": None,
            "reason": None,
            "updated_at": None,
            "version": 0,
            "readiness": {
                "status": None,
                "refreshed_at": None,
                "checks": [],
            },
        },
    }
    if db_path is None or not Path(db_path).exists():
        return defaults

    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    except sqlite3.OperationalError:
        return defaults
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('PRAGMA busy_timeout=5000')
        conn.execute('PRAGMA query_only=ON')
    except sqlite3.OperationalError:
        pass
    try:
        operator = dict(defaults["operator"])
        try:
            op_row = conn.execute(
                "SELECT state, boot_dry_run, reason, updated_at, "
                "       COALESCE(version, 0) AS version "
                "FROM operator_control_state WHERE id=1"
            ).fetchone()
            if op_row is not None:
                operator = {
                    "state": op_row["state"],
                    "boot_dry_run": (
                        bool(op_row["boot_dry_run"])
                        if op_row["boot_dry_run"] is not None else None
                    ),
                    "reason": op_row["reason"],
                    "updated_at": op_row["updated_at"],
                    "version": int(op_row["version"] or 0),
                    "readiness": {
                        "status": None,
                        "refreshed_at": None,
                        "checks": [],
                    },
                }
        except sqlite3.OperationalError:
            pass

        try:
            r = conn.execute(
                "SELECT status, created_at, clob_balance_usd, chain_balance_usd, "
                "       report_json "
                "FROM live_readiness_reports "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if r is not None:
                report_json = _json_dict(r["report_json"])
                operator["readiness"] = {
                    "status": r["status"],
                    "refreshed_at": r["created_at"],
                    "clob_balance_usd": _round_money(r["clob_balance_usd"]),
                    "chain_balance_usd": _round_money(r["chain_balance_usd"]),
                    "wallet_balance_freshness_s": report_json.get(
                        "wallet_balance_freshness_s"
                    ),
                    "checks": _readiness_checks(report_json),
                }
        except sqlite3.OperationalError:
            pass

        return {
            "operator": operator,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="optional cryptobot.db to hydrate operator state",
    )
    parser.add_argument(
        "--venue",
        choices=("polymarket", "predictfun"),
        default=os.environ.get("CRYPTOBOT_BOX_ROLE") or "polymarket",
        help=(
            "which BOX this snapshot ships to. The dashboard is ONE app "
            "parameterized by venue (the cross-venue view was retired "
            "2026-08-01), and the client reads `venue` off the payload to pick "
            "its wallet vocabulary, latency floor and fee spec — so the static "
            "pre-SSE fallback must carry it too, or the predict.fun console "
            "renders Polymarket labels until the first tick. deploy.sh passes "
            "the box role; defaults to polymarket."
        ),
    )
    args = parser.parse_args()

    live = load_live_state(args.db_path)
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "venue": args.venue,
        "portfolio": load_portfolio(),
        "operator": live["operator"],
    }
    OUT.write_text(json.dumps(snapshot, indent=2))
    print(f"wrote {OUT}", file=sys.stderr)
    if snapshot["portfolio"]:
        n_sleeves = len(snapshot["portfolio"]["sleeves"])
        print(f"  portfolio: YES ({n_sleeves} sleeves)", file=sys.stderr)
    else:
        print(f"  portfolio: NO (missing {V6_CONFIG.name})", file=sys.stderr)


if __name__ == "__main__":
    main()
