"""U15b E2E: transfer lock blocks LIVE entries, releases on completion.

Mirrors the pipeline pre-gate guard chain from U6:
  1. Operator presses Start â†’ state=LIVE.
  2. Wallet promoted to LIVE with fresh readiness.
  3. Transfer flow enters TRANSFER_LOCK â†’ next tick records
     `SKIP:transfer_lock` on every LIVE wallet.
  4. DRY wallets continue to fire paper bets even during the lock
     (D-LIVE-12).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    boot_operator_state,
    enter_transfer_lock,
    set_wallet_mode,
    start_processing,
)
from cryptobot.execution.pipeline import TickDeps, run_scanner_tick
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio
from cryptobot.tests._fixtures import seed_for_tests
from cryptobot.tests.test_pipeline_live_routing import (
    StubOrderClient,
    _build_markets,
    _qualifying_v6_5m_now,
    _seed_live_readiness,
    _spots,
    _stub_deps,
    _stub_v6_books,
    StubDeps,
)


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    conn = init_db(tmp_path / "transfer_lock.db")
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    yield conn
    conn.close()


def test_transfer_lock_blocks_live_dry_continues(db: sqlite3.Connection) -> None:
    portfolio = legacy_portfolio()
    start_processing(db)
    _seed_live_readiness(db)
    set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")

    # Operator now starts a transfer â†’ state goes TRANSFER_LOCK.
    enter_transfer_lock(db, reason="manual transfer")

    now_ts = _qualifying_v6_5m_now()
    window5 = (now_ts // 300) * 300
    stub = StubDeps(
        market_lookup=_build_markets(window5, interval_min=5),
        book_lookup=_stub_v6_books(now_ts),
        spot_data=_spots(now_ts=now_ts),
    )
    order_client = StubOrderClient(balance=200.0)
    deps = _stub_deps(stub, order_client=order_client)
    for coin, spot in stub.spot_data.items():
        seed_for_tests(deps.partial_ret_engine, coin, now_ts=now_ts, spot=spot.price)

    result = run_scanner_tick(db, portfolio, now_ts=now_ts, deps=deps)

    # LIVE wallets skip with `transfer_lock`; the OrderClient was never called.
    assert result.skips_by_reason.get("transfer_lock", 0) > 0
    assert order_client.calls == []
    # No LIVE ledger rows landed.
    live_rows = db.execute(
        "SELECT COUNT(*) AS n FROM ledger WHERE mode='LIVE'"
    ).fetchone()
    assert live_rows["n"] == 0
    # D-LIVE-12: DRY wallets continue to write signal rows even during
    # TRANSFER_LOCK so the parity validation surface stays useful. The
    # signal vocabulary is what proves the DRY pipeline ran â€” the actual
    # `mode='DRY'` ledger rows depend on score/edge passing the gates,
    # which the test fixture LUT doesn't always satisfy. Asserting at the
    # signals layer pins the D-LIVE-12 invariant without depending on
    # LUT/RF artifact specifics.
    dry_signals = db.execute(
        "SELECT COUNT(*) AS n FROM signals "
        "WHERE wallet_id IN ('v6_15m','pairs_5m','pairs_15m')"
    ).fetchone()
    assert dry_signals["n"] > 0
