"""U4 capital snapshot tests.

INITIAL_BANKROLL defaults to $100 as the starting capital anchor. D14 pin:
drawdown denominator is INITIAL_BANKROLL (constant), NOT realized_capital.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from cryptobot.execution.capital import get_capital_snapshot, get_wallet_snapshots
from cryptobot.execution.ledger import BetSignal, record_bet
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio
from cryptobot.tests.test_slot_state import make_signal


def _settle_last(conn: sqlite3.Connection, pnl_net: float, outcome: str = "WIN") -> None:
    conn.execute(
        "UPDATE ledger SET outcome=?, pnl_net=?, "
        "resolution_ts=datetime('now') "
        "WHERE id = (SELECT MAX(id) FROM ledger)",
        (outcome, pnl_net),
    )


class TestExtractablePnl:
    """Audit P1 #5: extractable PnL must use the same settled predicate as
    realized_capital so a CANCELLED orphan-close loss reduces both views."""

    def test_cancelled_orphan_loss_reduces_extractable(
        self, db: sqlite3.Connection
    ) -> None:
        from cryptobot.execution.capital import (
            extractable_pnl_by_wallet,
            extractable_pnl_for_wallet,
        )

        record_bet(db, make_signal(entry_minute=4), mode="LIVE")
        _settle_last(db, 10.0, outcome="WIN")
        record_bet(db, make_signal(entry_minute=5), mode="LIVE")
        _settle_last(db, -4.0, outcome="CANCELLED")  # orphan-close real loss

        # +10 WIN minus 4 CANCELLED loss = 6, not 10.
        assert extractable_pnl_for_wallet(db, "v6_5m") == pytest.approx(6.0)
        assert extractable_pnl_by_wallet(db).get("v6_5m") == pytest.approx(6.0)

    def test_zero_pnl_cancelled_leaves_extractable_unchanged(
        self, db: sqlite3.Connection
    ) -> None:
        from cryptobot.execution.capital import extractable_pnl_for_wallet

        record_bet(db, make_signal(entry_minute=4), mode="LIVE")
        _settle_last(db, 7.0, outcome="WIN")
        record_bet(db, make_signal(entry_minute=5), mode="LIVE")
        _settle_last(db, 0.0, outcome="CANCELLED")  # reconcile-cancelled, pnl=0

        assert extractable_pnl_for_wallet(db, "v6_5m") == pytest.approx(7.0)

    def test_inflight_submitted_transfer_subtracted(
        self, db: sqlite3.Connection
    ) -> None:
        """Audit P1 #6: an in-flight SUBMITTED transfer (real withdrawal
        awaiting confirmation, never auto-promoted to CONFIRMED) must be
        subtracted from extractable so it cannot be double-spent."""
        from cryptobot.execution.capital import (
            extractable_pnl_by_wallet,
            extractable_pnl_for_wallet,
        )

        record_bet(db, make_signal(entry_minute=4), mode="LIVE")
        _settle_last(db, 20.0, outcome="WIN")
        db.execute(
            "INSERT INTO transfer_requests (actor, from_wallet, to_wallet, "
            "source_wallet_id, amount_usd, status, idempotency_key) "
            "VALUES ('test','0xf','0xr','v6_5m', 8.0, 'SUBMITTED', 'k-sub')"
        )
        db.commit()
        # 20 realized minus 8 in-flight = 12.
        assert extractable_pnl_for_wallet(db, "v6_5m") == pytest.approx(12.0)
        assert extractable_pnl_by_wallet(db).get("v6_5m") == pytest.approx(12.0)


class TestCapitalSnapshot:
    def test_empty_ledger_returns_bankroll(self, db: sqlite3.Connection) -> None:
        snap = get_capital_snapshot(db)
        assert snap.realized_capital == 100.0
        assert snap.peak_realized_capital == 100.0
        assert snap.pending_exposure == 0.0
        assert snap.drawdown_ratio == 0.0
        assert snap.drawdown_pct_peak_frac == 0.0
        assert snap.n_settled == 0

    def test_settled_pnl_aggregated(self, db: sqlite3.Connection) -> None:
        for pnl, outcome in ((2.0, "WIN"), (-1.0, "LOSS"), (3.0, "WIN")):
            record_bet(db, make_signal(bet_size=1.0))
            time.sleep(0.01)
            _settle_last(db, pnl, outcome)
        snap = get_capital_snapshot(db)
        assert snap.realized_capital == pytest.approx(104.0)
        assert snap.peak_realized_capital == pytest.approx(104.0)
        assert snap.drawdown_ratio == 0.0
        assert snap.n_settled == 3

    def test_drawdown_uses_initial_bankroll_denominator(
        self, db: sqlite3.Connection
    ) -> None:
        # D14: peak=105, realized=97 â†’ drawdown = (105-97)/100 = 0.08
        # NOT (105-97)/97 = 0.0825. The denominator pin matters.
        for pnl, outcome in ((5.0, "WIN"), (-8.0, "LOSS")):
            record_bet(db, make_signal(bet_size=1.0))
            time.sleep(0.01)
            _settle_last(db, pnl, outcome)
        snap = get_capital_snapshot(db)
        assert snap.realized_capital == pytest.approx(97.0)
        assert snap.peak_realized_capital == pytest.approx(105.0)
        assert snap.drawdown_ratio == pytest.approx(0.08, abs=1e-6)
        assert snap.drawdown_pct_peak_frac == pytest.approx(8.0 / 105.0, abs=1e-6)

    def test_pending_exposure_sums(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=4.0))
        record_bet(db, make_signal(bet_size=4.0, sleeve="v6_15m", entry_minute=7))
        snap = get_capital_snapshot(db)
        assert snap.pending_exposure == pytest.approx(8.0)
        assert snap.pending_by_sleeve == {"v6_5m": 4.0, "v6_15m": 4.0}

    def test_pnl_by_sleeve_breakdown(self, db: sqlite3.Connection) -> None:
        # one settled WIN per sleeve
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))
        time.sleep(0.01)
        _settle_last(db, 2.0)
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_15m", entry_minute=7))
        time.sleep(0.01)
        _settle_last(db, -0.5, outcome="LOSS")
        snap = get_capital_snapshot(db)
        assert snap.pnl_by_sleeve == {"v6_5m": 2.0, "v6_15m": -0.5}

    def test_pending_not_in_realized(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=10.0))
        snap = get_capital_snapshot(db)
        assert snap.realized_capital == 100.0
        assert snap.pending_exposure == 10.0


class TestWalletSnapshots:
    def test_empty_wallets_start_at_bankroll(self, db: sqlite3.Connection) -> None:
        wallets = get_wallet_snapshots(db, legacy_portfolio())
        assert set(wallets) == {"v6_5m", "v6_15m", "pairs_5m", "pairs_15m"}
        assert all(w.realized_capital == 100.0 for w in wallets.values())
        assert all(w.pending_exposure == 0.0 for w in wallets.values())

    def test_wallet_pnl_isolated_by_strategy(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))
        _settle_last(db, 12.0)
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_15m", entry_minute=7))
        _settle_last(db, -5.0, outcome="LOSS")

        wallets = get_wallet_snapshots(db, legacy_portfolio())

        assert wallets["v6_5m"].realized_capital == pytest.approx(112.0)
        assert wallets["v6_15m"].realized_capital == pytest.approx(95.0)
        assert wallets["pairs_5m"].realized_capital == pytest.approx(100.0)

    def test_pair_legs_roll_up_to_pair_wallet(self, db: sqlite3.Connection) -> None:
        record_bet(
            db,
            BetSignal(
                coin="BTC", sleeve="pairs_5m_long", entry_minute=1, side="YES",
                window_start_ts=1_715_923_200, window_end_ts=1_715_923_500,
                interval_min=5, poly_market_id="M1", poly_token_id="T1",
                fee_rate_bps=100.0, model_p=0.8, p_market=0.5, fill_price=0.5,
                walked_vwap=0.5, realized_edge=0.2, partial_ret_z_1h=0.0,
                lut_p_bucket=8, lut_hit_rate=0.8, lut_n_cum=100,
                target_usd=4.0, bet_size=4.0, slot_filled_pre=0.0,
                entry_top_price=0.5, yes_pre1_anchor=0.5,
                pair_component="pairs_5m", pair_role="long",
                config_sha256="x",
            ),
        )
        _settle_last(db, 3.0)
        record_bet(
            db,
            BetSignal(
                coin="ETH", sleeve="pairs_5m_short", entry_minute=1, side="NO",
                window_start_ts=1_715_923_200, window_end_ts=1_715_923_500,
                interval_min=5, poly_market_id="M2", poly_token_id="T2",
                fee_rate_bps=100.0, model_p=0.7, p_market=0.5, fill_price=0.5,
                walked_vwap=0.5, realized_edge=0.2, partial_ret_z_1h=0.0,
                lut_p_bucket=7, lut_hit_rate=0.7, lut_n_cum=100,
                target_usd=4.0, bet_size=4.0, slot_filled_pre=0.0,
                entry_top_price=0.5, yes_pre1_anchor=0.5,
                pair_component="pairs_5m", pair_role="short",
                config_sha256="x",
            ),
        )

        wallets = get_wallet_snapshots(db, legacy_portfolio())

        assert wallets["pairs_5m"].realized_capital == pytest.approx(103.0)
        assert wallets["pairs_5m"].pending_exposure == pytest.approx(4.0)
        assert wallets["v6_5m"].realized_capital == pytest.approx(100.0)

    def test_wallet_start_at_ignores_older_rows(self, db: sqlite3.Connection) -> None:
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))
        db.execute(
            "UPDATE ledger SET bet_ts='2026-05-23 00:00:00', outcome='WIN', "
            "pnl_net=50.0, resolution_ts='2026-05-23 00:01:00' WHERE id=1"
        )
        record_bet(db, make_signal(bet_size=2.0, sleeve="v6_5m", window_start_ts=1_715_923_500))
        db.execute(
            "UPDATE ledger SET bet_ts='2026-05-23 01:00:00', outcome='WIN', "
            "pnl_net=5.0, resolution_ts='2026-05-23 01:01:00' WHERE id=2"
        )

        wallets = get_wallet_snapshots(
            db, legacy_portfolio(), since_ts="2026-05-23 00:30:00"
        )

        assert wallets["v6_5m"].realized_capital == pytest.approx(105.0)


class _CountingConn:
    """Proxy that forwards to a real sqlite3 connection but counts the
    wallet-snapshot ledger SCANs (the 12-column ``SELECT ... FROM ledger``
    row fetch). The ``ledger_version`` invalidation PROBE is itself a (cheap)
    ledger aggregate, so it is matched on the row fetch's distinctive column
    list rather than a bare ``FROM ledger`` — the probe deliberately does NOT
    count as a scan (eliminating the per-tick full fetch is what the cache is
    for). Everything else passes straight through, so the cache behaves
    exactly as it does on a bare connection."""

    def __init__(self, real: sqlite3.Connection) -> None:
        self._real = real
        self.ledger_scans = 0

    def execute(self, sql, *args):
        if "FROM ledger" in sql and "pair_component" in sql:
            self.ledger_scans += 1
        return self._real.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._real, name)


class TestWalletSnapshotScanCache:
    """L1: the per-tick ledger scan must be served from cache while the
    ledger is unchanged, and MUST rescan on any INSERT or in-place UPDATE
    (settlement UPDATEs leave MAX(id) unchanged, so id alone is not a key)."""

    def test_scan_cached_and_invalidated_on_write(
        self, db: sqlite3.Connection
    ) -> None:
        from cryptobot.execution.capital import clear_snapshot_scan_cache

        clear_snapshot_scan_cache()
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))
        _settle_last(db, 5.0)

        conn = _CountingConn(db)
        port = legacy_portfolio()

        # First call scans the ledger once.
        s1 = get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 1

        # No ledger change → served from cache, no rescan.
        s2 = get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 1
        assert (
            s2["v6_5m"].realized_capital == s1["v6_5m"].realized_capital
        )

        # INSERT a fresh row → change signal moves → rescan.
        record_bet(
            db,
            make_signal(
                bet_size=2.0, sleeve="v6_5m", entry_minute=6,
                window_start_ts=1_715_923_500,
            ),
        )
        s3 = get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 2
        assert s3["v6_5m"].pending_exposure == pytest.approx(2.0)

        # Still unchanged → served from cache again.
        get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 2

        # UPDATE an existing row in place (settlement-style) → rescan. This is
        # the case MAX(id) would miss: no new row, but pnl_net/outcome change.
        _settle_last(db, 3.0)
        s5 = get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 3
        assert s5["v6_5m"].realized_capital == pytest.approx(108.0)
        assert s5["v6_5m"].pending_exposure == pytest.approx(0.0)

    def test_signals_insert_kept_cached_but_sweep_update_invalidates(
        self, db: sqlite3.Connection
    ) -> None:
        """Latency item #2: the invalidation signal is ledger-specific.

        The tick's own per-second signals INSERT (and any other
        unrelated-table write) must NOT invalidate — under the old
        (data_version, total_changes) signal it did, so both per-tick call
        sites rescanned the full ledger every tick. An F3 sweep-style
        settlement UPDATE (outcome/pnl_net/resolution_ts in place, MAX(id)
        unchanged) MUST invalidate — the equity-peak replay feeding the LIVE
        35% DD halt depends on catching every settlement.
        """
        from cryptobot.execution.capital import clear_snapshot_scan_cache
        from cryptobot.execution.ledger import record_signal_skip

        clear_snapshot_scan_cache()
        record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))

        conn = _CountingConn(db)
        port = legacy_portfolio()

        get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 1

        # Unrelated write #1: a signals SKIP row (the tick writes one of
        # these on nearly every scan second) → cache stays warm.
        record_signal_skip(
            db,
            created_at="2026-07-15 00:00:00",
            coin="BTC", sleeve="v6_5m", entry_minute=4, side="YES",
            window_start_ts=1_715_923_200,
            model_p=0.7, ask=0.7, edge=0.0, fee_rate_bps=100.0,
            partial_ret_z_1h=None, lut_p_bucket=None, lut_hit_rate=None,
            lut_n_cum=None, skip_reason="edge_below_gate",
        )
        get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 1, "signals INSERT must not force a rescan"

        # Unrelated write #2: a pipeline_health row (scheduler jobs write
        # these constantly) → still cached.
        db.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('tick', 'OK', 'noise')"
        )
        get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 1, "pipeline_health INSERT must not force a rescan"

        # F3 sweep-style settlement: UPDATE in place, no new row → MUST
        # invalidate, and the fresh scan must see the settled pnl.
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=5.0, "
            "resolution_ts=datetime('now') WHERE id=1"
        )
        snaps = get_wallet_snapshots(conn, port)
        assert conn.ledger_scans == 2, "sweep-style UPDATE must invalidate the cache"
        assert snaps["v6_5m"].realized_capital == pytest.approx(105.0)
        assert snaps["v6_5m"].pending_exposure == pytest.approx(0.0)
