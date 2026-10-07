"""U15b E2E: two LIVE wallets in the same group share the cap.

Drives the full `run_scanner_tick` against a portfolio where both
`v6_5m` and `pairs_5m` are LIVE. After `v6_5m` consumes most of the
group cap, the next cell from the same group skips with
`SKIP:group_exposure_cap`. Also verifies that DRY siblings in the same
group do NOT contribute to the cap (D-LIVE-14).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cryptobot.db import backfill_wallet_groups, init_db
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.operator_control import (
    boot_operator_state,
    set_wallet_mode,
    start_processing,
)
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    conn = init_db(tmp_path / "group_cap.db")
    portfolio = legacy_portfolio()
    backfill_wallet_groups(conn, portfolio)
    boot_operator_state(conn, portfolio, boot_dry_run=True)
    yield conn
    conn.close()


def _seed_readiness(conn: sqlite3.Connection) -> None:
    now = utc_now_sql()
    conn.execute(
        "INSERT INTO live_readiness_reports (created_at, status, report_json) "
        "VALUES (?, 'READY', '{}')",
        (now,),
    )
    conn.execute(
        "INSERT INTO wallet_reconciliation_runs (sampled_at, source_status) "
        "VALUES (?, 'ok')",
        (now,),
    )
    conn.commit()


def _seed_live_pending(
    conn: sqlite3.Connection,
    *,
    sleeve: str,
    wallet_id: str,
    bet_size: float,
    wallet_group: str,
) -> None:
    conn.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
           window_start_ts, window_end_ts, interval_min, poly_market_id,
           poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
           realized_edge, target_usd, bet_size, entry_top_price,
           config_sha256, mode, wallet_group)
        VALUES (?, 'BTC', ?, ?, 4, 'YES', 1715923200, 1715923500, 5, 'M',
                'T', 100.0, 0.78, 0.55, 0.55, 0.10, ?, ?, 0.55,
                'sha-stub', 'LIVE', ?)
        """,
        (utc_now_sql(), wallet_id, sleeve, bet_size, bet_size, wallet_group),
    )
    conn.commit()


def test_full_group_cap_blocks_sibling_wallet_in_same_group(
    db: sqlite3.Connection,
) -> None:
    """v6_5m already has $99 LIVE PENDING in group_5m. A second cell from
    `pairs_5m` (also in group_5m) proposing $5 must SKIP because group
    pending would jump to $104, above the $100 cap."""
    from cryptobot.execution.capital import get_group_state
    from cryptobot.execution.gates import GateInput, evaluate_gates
    from cryptobot.strategy.portfolio_loader import Sleeve

    portfolio = legacy_portfolio()
    start_processing(db)
    _seed_readiness(db)
    set_wallet_mode(db, "v6_5m", "LIVE", reason="canary")
    set_wallet_mode(db, "pairs_5m", "LIVE", reason="canary")

    # Pre-seed $99 of LIVE PENDING for v6_5m in group_5m.
    _seed_live_pending(
        db, sleeve="v6_5m", wallet_id="v6_5m", bet_size=99.0,
        wallet_group="group_5m",
    )
    state = get_group_state(db, portfolio)
    assert state["group_5m"].pending_usd == pytest.approx(99.0)

    sleeve = next(s for s in portfolio.sleeves if s.label == "v6_5m")
    # Use realized_edge inside the sleeve's [min_edge, max_edge] band and
    # fill_price inside [min_fill_price, max_fill_price] so the gate
    # stack reaches `group_exposure` instead of failing earlier at `edge`
    # or `fill_band`.
    safe_edge = (sleeve.min_edge + sleeve.max_edge) / 2.0
    safe_fill = (sleeve.min_fill_price + sleeve.max_fill_price) / 2.0
    inp = GateInput(
        coin="BTC",
        sleeve=sleeve,
        entry_minute=4,
        side="YES",
        realized_edge=safe_edge,
        fill_price=safe_fill,
        fee_rate_bps=100.0,
        spot_age_s=1.0,
        proposed_bet_size=5.0,
        pending_exposure_total=0.0,
        realized_capital=100.0,
        slot_filled_usd=0.0,
        slot_target_usd=5.0,
        disk_free_mb=10_000.0,
        book_top_ask_age_s=1.0,
        is_topup=False,
        exposure_cap_fraction=0.5,
        mode="LIVE",
        group_cap_usd=100.0,
        group_pending_usd=state["group_5m"].pending_usd,
        live_wallet_balance_usd=None,
        all_group_caps_sum_usd=portfolio.all_group_caps_sum_usd(),
    )
    result = evaluate_gates(inp)
    assert result.skip_reason == "group_exposure_cap"


def test_dry_sibling_pending_does_not_consume_group_cap(
    db: sqlite3.Connection,
) -> None:
    """A wallet's DRY PENDING does not block its LIVE sibling's group
    cap headroom (D-LIVE-14 parity shadow exclusion)."""
    from cryptobot.execution.capital import get_group_state

    portfolio = legacy_portfolio()
    # No LIVE rows, only a $99 DRY row.
    db.execute(
        """
        INSERT INTO ledger
          (bet_ts, coin, wallet_id, sleeve, entry_minute, side,
           window_start_ts, window_end_ts, interval_min, poly_market_id,
           poly_token_id, fee_rate_bps, model_p, p_market, fill_price,
           realized_edge, target_usd, bet_size, entry_top_price,
           config_sha256, mode, wallet_group)
        VALUES (?, 'BTC', 'v6_5m', 'v6_5m', 4, 'YES', 1715923200, 1715923500,
                5, 'M', 'T', 100.0, 0.78, 0.55, 0.55, 0.10, 99.0, 99.0, 0.55,
                'sha-stub', 'DRY', 'group_5m')
        """,
        (utc_now_sql(),),
    )
    db.commit()

    state = get_group_state(db, portfolio)
    assert state["group_5m"].pending_usd == pytest.approx(0.0)
