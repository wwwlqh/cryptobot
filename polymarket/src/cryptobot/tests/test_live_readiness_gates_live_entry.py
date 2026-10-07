"""U15b E2E: stale readiness â†’ no LIVE entries even with operator=LIVE.

Verifies the pipeline's pre-gate readiness check at the wallet level.
The plan's D-LIVE-5 + D-LIVE-20 combo says: even when the global
operator state is LIVE, a wallet that already flipped to LIVE must
also have a fresh `live_readiness_reports.status=READY` row before any
cell goes through the OrderClient. When the row goes stale (older than
`LIVE_READINESS_CACHE_TTL_S`), the pipeline emits SKIP:readiness_stale.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    boot_operator_state,
    set_wallet_mode,
    start_processing,
)
from cryptobot.execution.pipeline import run_scanner_tick
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio
from cryptobot.tests._fixtures import seed_for_tests
from cryptobot.tests.test_pipeline_live_routing import (
    StubOrderClient,
    StubDeps,
    _build_markets,
    _qualifying_v6_5m_now,
    _seed_live_readiness,
    _spots,
    _stub_deps,
    _stub_v6_books,
)


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    conn = init_db(tmp_path / "readiness.db")
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    yield conn
    conn.close()


def test_stale_readiness_blocks_live_entry(db: sqlite3.Connection) -> None:
    """Wallet is LIVE + operator is LIVE, but the readiness row is from
    an hour ago. Pipeline should emit SKIP:readiness_stale and not
    invoke the OrderClient."""
    portfolio = legacy_portfolio()
    start_processing(db)
    _seed_live_readiness(db)
    set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

    # Backdate the readiness row well past LIVE_READINESS_CACHE_TTL_S.
    stale_ts = (
        datetime.now(timezone.utc) - timedelta(hours=1)
    ).strftime("%Y-%m-%d %H:%M:%S")
    db.execute(
        "UPDATE live_readiness_reports SET created_at=?",
        (stale_ts,),
    )
    db.commit()

    now_ts = _qualifying_v6_5m_now()
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup=_stub_v6_books(now_ts),
        spot_data=_spots(now_ts=now_ts),
    )
    order_client = StubOrderClient(balance=200.0)
    # Override the readiness loader to use the real durable readiness gate
    # so this test exercises the production path.
    deps = _stub_deps(stub, order_client=order_client)
    # Reset the always-fresh stub the helper installs by default.
    deps.live_readiness_loader = None
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    assert result.skips_by_reason.get("readiness_stale", 0) > 0
    assert order_client.calls == []
    live_rows = db.execute(
        "SELECT COUNT(*) AS n FROM ledger WHERE mode='LIVE'"
    ).fetchone()
    assert live_rows["n"] == 0


def test_no_readiness_row_blocks_live_entry(db: sqlite3.Connection) -> None:
    """Phase 1-style deploy with no readiness writer: LIVE entries must
    SKIP rather than silently succeed."""
    portfolio = legacy_portfolio()
    start_processing(db)
    # Seed readiness so DRY â†’ LIVE flip passes the live_action_guard,
    # then delete the row to simulate the Phase 1 / writer-paused state.
    _seed_live_readiness(db)
    set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")
    db.execute("DELETE FROM live_readiness_reports")
    db.commit()

    now_ts = _qualifying_v6_5m_now()
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup=_stub_v6_books(now_ts),
        spot_data=_spots(now_ts=now_ts),
    )
    order_client = StubOrderClient(balance=200.0)
    deps = _stub_deps(stub, order_client=order_client)
    deps.live_readiness_loader = None
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)
    assert result.skips_by_reason.get("readiness_stale", 0) > 0
    assert order_client.calls == []
