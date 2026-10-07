"""Build dashboard/snapshot.json, the static fallback the dashboard shows
before its first live update.

Reads the strategy config (wherever config.V6_CONFIG_PATH resolves it) and,
with ``--db-path``, operator state from cryptobot.db. Writes
dashboard/snapshot.json with the portfolio and operator blocks, in the same
shape as the live payload. Run after a config change to refresh it.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve()
# Locally this file is <repo>/polymarket/dashboard/build_snapshot.py; deploy.sh
# stages it as $PROJECT_DIR/dashboard/build_snapshot.py. Find the repo root the
# same way cryptobot.config does, falling back to the deployed layout.
ROOT = _HERE.parent.parent
for _candidate in _HERE.parents:
    if (_candidate / "pyproject.toml").is_file():
        ROOT = _candidate
        break
    if (_candidate / "polymarket" / "src" / "cryptobot").is_dir():
        ROOT = _candidate
        break
# Package roots: polymarket/src locally, src/ in the deployed bundle.
for _src in (
    ROOT / "polymarket" / "src",
    ROOT / "src",
):
    if _src.is_dir() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))

# Use the same config file the live loader reads. If cryptobot cannot be
# imported, fall back to the deployed copy, then the repo copy; building the
# snapshot must never fail outright.
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
    """Return the portfolio block for the static fallback.

    Uses the live loaders (``load_sleeves`` + ``_portfolio_block``), so the
    static snapshot always matches the live payload's portfolio block.

    Returns None when the config is missing or unreadable; the dashboard then
    waits for the live ``portfolio`` block.
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
    """Read the operator block (state and readiness) from the SQLite DB.

    Without a database it returns the schema default, STOPPED_PROCESSING,
    which is what a fresh deploy shows.
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
    args = parser.parse_args()

    live = load_live_state(args.db_path)
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "venue": "polymarket",
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
