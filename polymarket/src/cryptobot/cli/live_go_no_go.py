"""Pre-flip readiness checklist.

  python -m cryptobot.cli.live_go_no_go --offline

Run by hand only; nothing schedules it.

``--offline`` reads the latest ``live_readiness_reports`` row without calling
the CLOB, so readiness can be checked before the bot is running. Without it the
CLI runs ``evaluate_readiness(conn, config, order_client)`` and stores a new
row.

Exit codes:
  0  READY
  1  NOT_READY (or no readiness rows in offline mode)
  2  internal error
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from cryptobot import config
from cryptobot.db import init_db
from cryptobot.execution.live_readiness import (
    evaluate_readiness,
    latest_readiness_report,
    prefetch_readiness_state,
)
from cryptobot.execution.strategy_constants import LIVE_READINESS_CACHE_TTL_S
from cryptobot.execution.wallet_topology import _validate_wallet_config
from cryptobot.runtime_secrets import get_config
from cryptobot.strategy.portfolio_loader import load_sleeves


logger = logging.getLogger(__name__)


def _print_report(payload: dict) -> None:
    """Render the report JSON as a human-readable status table."""
    status = payload.get("status") or payload.get("readiness_status") or "UNKNOWN"
    print(f"status={status}")
    funder = payload.get("funder") or payload.get("funder_address") or ""
    print(f"funder={funder}")
    clob = payload.get("clob_balance_usd")
    chain = payload.get("chain_balance_usd")
    print(f"clob_balance_usd={clob}")
    print(f"chain_balance_usd={chain}")

    inner = payload.get("report_json") if isinstance(payload, dict) else None
    results = None
    if isinstance(inner, dict):
        results = inner.get("readiness_check_results")
    if results is None:
        results = payload.get("readiness_check_results")
    if isinstance(results, list):
        print("checks:")
        for entry in results:
            name = entry.get("name", "")
            passed = entry.get("passed", False)
            detail = entry.get("detail", "")
            marker = "PASS" if passed else "FAIL"
            print(f"  [{marker}] {name}: {detail}")


def _run_online(argv) -> int:
    """Evaluate readiness against live CLOB + ledger. Persists a row."""
    # Imported here rather than at module level so ``--offline`` works without
    # the Polymarket package. This CLI builds a real OrderClient directly
    # (instead of through VenueAdapter.build_order_client) because it must tell
    # ClobClientUnavailable (offline checks only) apart from ValueError (exit
    # 2).
    from cryptobot_polymarket.walker import ClobClientUnavailable, OrderClient

    cfg = get_config()
    try:
        _validate_wallet_config(cfg.poly_signature_type, cfg.poly_funder)
    except ValueError as exc:
        logger.error("invalid wallet topology: %s", exc)
        return 2

    try:
        conn = init_db(config.DB_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.error("init_db failed: %s", exc)
        return 2

    try:
        portfolio = load_sleeves()
    except Exception as exc:  # noqa: BLE001
        logger.error("load_sleeves failed: %s", exc)
        conn.close()
        return 2

    order_client: Optional[OrderClient] = None
    try:
        order_client = OrderClient(cfg)
    except ClobClientUnavailable as exc:
        logger.warning("clob_client_unavailable: %s (running offline gates only)", exc)
    except ValueError as exc:
        logger.error("OrderClient init failed: %s", exc)
        conn.close()
        return 2

    # Pinned to polymarket (not config.VENUE): this gate reads POLY_FUNDER pUSD
    # and always did, on every venue. Preserved exactly; resolved through the
    # registry so core never names the venue package directly.
    from cryptobot.venues import get_adapter

    chain_balance_fetcher = (
        get_adapter("polymarket").build_balance_fetcher(cfg)
        if getattr(cfg, "live_onchain_verify_enabled", True)
        else None
    )
    prefetched = prefetch_readiness_state(
        order_client=order_client,
        chain_balance_fetcher=chain_balance_fetcher,
    )
    report = evaluate_readiness(
        conn,
        config=cfg,
        order_client=order_client,
        portfolio=portfolio,
        chain_balance_fetcher=chain_balance_fetcher,
        prefetched=prefetched,
    )
    _print_report(
        {
            "status": report.status,
            "funder": report.funder,
            "clob_balance_usd": report.clob_balance_usd,
            "chain_balance_usd": report.chain_balance_usd,
            "readiness_check_results": [
                {"name": c.name, "passed": c.passed, "detail": c.detail}
                for c in report.checks
            ],
        }
    )
    conn.close()
    return 0 if report.is_ready else 1


def _readiness_age_s(conn, created_at) -> Optional[float]:
    """Age in seconds of `created_at` per SQLite's clock (mirrors the
    julianday convention used by pipeline._default_readiness_fresh)."""
    try:
        row = conn.execute(
            "SELECT (julianday('now') - julianday(?)) * 86400 AS age_s",
            (created_at,),
        ).fetchone()
    except Exception:  # noqa: BLE001
        return None
    if row is None or row["age_s"] is None:
        return None
    try:
        return float(row["age_s"])
    except (TypeError, ValueError):
        return None


def _run_offline(argv) -> int:
    """Read the latest persisted readiness row without contacting CLOB."""
    max_age_s = getattr(argv, "max_age", None)
    if max_age_s is None:
        max_age_s = LIVE_READINESS_CACHE_TTL_S
    try:
        conn = init_db(config.DB_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.error("init_db failed: %s", exc)
        return 2
    try:
        latest = latest_readiness_report(conn)
        if latest is None:
            print("status=NO_REPORT")
            print("checks:")
            print("  no live_readiness_reports row found — run without --offline")
            return 1
        _print_report(latest)
        age_s = _readiness_age_s(conn, latest.get("created_at"))
        if age_s is not None:
            print(f"report_age_s={age_s:.0f}")
        if latest["status"] != "READY":
            return 1
        # A days-old READY snapshot (taken before a balance drain / key
        # rotation / env drift) must not report the host as go-live ready.
        if age_s is None or age_s > float(max_age_s):
            print(
                f"status=STALE (age_s={age_s} > max_age_s={max_age_s})"
            )
            return 1
        return 0
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="cryptobot.cli.live_go_no_go",
        description="Pre-flip readiness checklist",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="read latest report without calling CLOB",
    )
    parser.add_argument(
        "--max-age",
        type=int,
        default=None,
        help=(
            "In --offline mode, reject (exit 1, status=STALE) a READY row "
            "older than this many seconds. Defaults to "
            "LIVE_READINESS_CACHE_TTL_S."
        ),
    )
    args = parser.parse_args(argv)
    if args.offline:
        return _run_offline(args)
    return _run_online(args)


if __name__ == "__main__":
    sys.exit(main())
