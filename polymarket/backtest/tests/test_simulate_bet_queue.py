"""Direct tests for ``sweep_lib.simulate_bet_queue``.

This function is the canonical engine for settling a pre-sized bet
queue under shared bankroll + exposure caps. Production callers depend
on it (``simulate_portfolio.simulate_portfolio``,
``walk_forward_v6._simulate_day``); existing tests exercise those
wrappers but monkeypatch ``_collect_picks``, so the inner loop's
exposure-cap and directional-cap arithmetic is not directly pinned.

Coverage gaps these tests close (from ce-code-review run
20260520-162951-20d4acae, finding #17):

  * ``directional_cap`` blocks a same-side bet when
    pending_same_side + notional exceeds the cap, but still admits
    an opposite-side bet at the same entry_ts.
  * ``directional_cap`` set without a ``side`` column raises ValueError.
  * ``return_picks=True`` round-trips the kept rows alongside the PnL
    aggregates.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from polymarket.backtest.engine.sweep_lib import simulate_bet_queue  # noqa: E402


def _bets(rows: list[dict]) -> pd.DataFrame:
    """Build a bets frame with normalized columns."""
    return pd.DataFrame(rows)


class TestExposureCapOnly:
    """``directional_cap=None`` path — only shared notional matters."""

    def test_admits_all_bets_when_well_under_cap(self):
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=10.0, pl=1.0),
            dict(entry_ts=1, resolve_ts=100, notional=10.0, pl=2.0),
            dict(entry_ts=2, resolve_ts=100, notional=10.0, pl=-1.0),
        ])
        out = simulate_bet_queue(bets, exposure_cap=100.0)
        assert out["n_bets"] == 3
        assert out["total_pnl"] == pytest.approx(2.0)

    def test_rejects_bet_that_would_breach_cap_at_entry_ts(self):
        # Three concurrent bets, cap=25. Third (entry_ts=2, notional=10)
        # arrives while the first two (10 each) are still active.
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=10.0, pl=5.0),
            dict(entry_ts=1, resolve_ts=100, notional=10.0, pl=5.0),
            dict(entry_ts=2, resolve_ts=100, notional=10.0, pl=99.0),
        ])
        out = simulate_bet_queue(bets, exposure_cap=25.0)
        assert out["n_bets"] == 2, (
            "third bet's notional pushes pending from 20 to 30 > 25 cap; "
            "must be skipped"
        )
        assert 99.0 not in [bets.iloc[i].pl for i in range(2)]
        assert out["total_pnl"] == pytest.approx(10.0)

    def test_admits_followup_after_earlier_bet_resolves(self):
        # First bet resolves at t=50; second bet arrives at t=60 with
        # enough headroom because the first slot is free.
        bets = _bets([
            dict(entry_ts=0, resolve_ts=50, notional=20.0, pl=2.0),
            dict(entry_ts=60, resolve_ts=120, notional=20.0, pl=3.0),
        ])
        out = simulate_bet_queue(bets, exposure_cap=20.0)
        assert out["n_bets"] == 2
        assert out["total_pnl"] == pytest.approx(5.0)


class TestDirectionalCap:
    """``directional_cap`` path — per-side pending notional gate."""

    def test_blocks_same_side_when_directional_cap_breached(self):
        # Three Up bets back-to-back, directional_cap=15. First two fit
        # (5+5=10), third (5) would push to 15 which is == cap (not >).
        # Add a fourth that would breach.
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=5.0, pl=1.0, side="Up"),
            dict(entry_ts=1, resolve_ts=100, notional=5.0, pl=1.0, side="Up"),
            dict(entry_ts=2, resolve_ts=100, notional=5.0, pl=1.0, side="Up"),
            dict(entry_ts=3, resolve_ts=100, notional=5.0, pl=99.0, side="Up"),
        ])
        out = simulate_bet_queue(
            bets, exposure_cap=100.0, directional_cap=15.0
        )
        assert out["n_bets"] == 3, (
            "fourth bet would push same-side pending from 15 to 20 > 15; "
            "directional_cap must reject it"
        )
        # PnL excludes the 99.0 from the rejected fourth bet.
        assert out["total_pnl"] == pytest.approx(3.0)

    def test_opposite_side_admitted_when_same_side_cap_hit(self):
        # Pack the Up side to its cap, then submit a Down bet that
        # would breach if measured against shared pending but fits the
        # Down side trivially.
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=10.0, pl=1.0, side="Up"),
            dict(entry_ts=1, resolve_ts=100, notional=10.0, pl=1.0, side="Up"),
            dict(entry_ts=2, resolve_ts=100, notional=5.0, pl=99.0, side="Up"),  # blocked
            dict(entry_ts=3, resolve_ts=100, notional=5.0, pl=7.0, side="Down"),  # admitted
        ])
        out = simulate_bet_queue(
            bets, exposure_cap=100.0, directional_cap=20.0
        )
        assert out["n_bets"] == 3
        # The blocked Up bet's PnL (99) must not appear; the admitted
        # Down bet's PnL (7) must.
        assert out["total_pnl"] == pytest.approx(1.0 + 1.0 + 7.0)

    def test_directional_cap_requires_side_column(self):
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=5.0, pl=1.0),
        ])
        with pytest.raises(ValueError, match="directional_cap requires"):
            simulate_bet_queue(
                bets, exposure_cap=100.0, directional_cap=10.0
            )

    def test_active_side_decays_as_bets_resolve(self):
        # Up side fills the directional cap with bets resolving at t=50.
        # A new Up bet at t=60 must be admitted because the first cohort
        # is no longer active.
        bets = _bets([
            dict(entry_ts=0, resolve_ts=50, notional=10.0, pl=1.0, side="Up"),
            dict(entry_ts=1, resolve_ts=50, notional=10.0, pl=1.0, side="Up"),
            dict(entry_ts=60, resolve_ts=120, notional=10.0, pl=2.0, side="Up"),
        ])
        out = simulate_bet_queue(
            bets, exposure_cap=100.0, directional_cap=20.0
        )
        assert out["n_bets"] == 3
        assert out["total_pnl"] == pytest.approx(4.0)


class TestReturnPicks:
    """``return_picks=True`` round-trip."""

    def test_picks_match_pls_and_kept_rows(self):
        bets = _bets([
            dict(entry_ts=2, resolve_ts=100, notional=10.0, pl=5.0),
            dict(entry_ts=0, resolve_ts=100, notional=10.0, pl=-3.0),
            dict(entry_ts=1, resolve_ts=100, notional=10.0, pl=1.0),
        ])
        out = simulate_bet_queue(
            bets, exposure_cap=100.0, return_picks=True
        )
        assert out["n_bets"] == 3
        assert "picks" in out and "pls" in out
        # Picks are sorted by entry_ts.
        assert list(out["picks"]["entry_ts"]) == [0, 1, 2]
        assert list(out["picks"]["pl"]) == [-3.0, 1.0, 5.0]
        # pls aligns with picks order.
        np.testing.assert_array_equal(out["pls"], np.array([-3.0, 1.0, 5.0]))

    def test_picks_empty_dataframe_returns_empty_picks(self):
        empty = _bets([])
        out = simulate_bet_queue(
            empty, exposure_cap=100.0, return_picks=True
        )
        assert out["n_bets"] == 0
        assert out["picks"].empty
        assert out["pls"].size == 0


class TestMaxDdAcrossPnlSeries:
    """``max_dd`` is the largest peak-to-trough drop in cumulative PnL."""

    def test_max_dd_with_winner_then_loser(self):
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=10.0, pl=10.0),
            dict(entry_ts=1, resolve_ts=100, notional=10.0, pl=-3.0),
            dict(entry_ts=2, resolve_ts=100, notional=10.0, pl=-2.0),
        ])
        out = simulate_bet_queue(bets, exposure_cap=100.0)
        # cum = [10, 7, 5]; peak = [10, 10, 10]; max_dd = 5.
        assert out["max_dd"] == pytest.approx(5.0)

    def test_max_dd_zero_when_pnl_monotonically_increases(self):
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=10.0, pl=1.0),
            dict(entry_ts=1, resolve_ts=100, notional=10.0, pl=2.0),
            dict(entry_ts=2, resolve_ts=100, notional=10.0, pl=3.0),
        ])
        out = simulate_bet_queue(bets, exposure_cap=100.0)
        assert out["max_dd"] == pytest.approx(0.0)


class TestEmptyAndDegenerate:
    def test_empty_input_returns_nan_win_rate(self):
        out = simulate_bet_queue(_bets([]), exposure_cap=100.0)
        assert out["n_bets"] == 0
        assert out["total_pnl"] == 0.0
        assert np.isnan(out["win_rate"])
        assert out["max_dd"] == 0.0

    def test_all_bets_rejected_returns_nan_win_rate(self):
        # Single bet exceeds the cap — nothing admitted, win_rate stays NaN.
        bets = _bets([
            dict(entry_ts=0, resolve_ts=100, notional=200.0, pl=99.0),
        ])
        out = simulate_bet_queue(bets, exposure_cap=10.0)
        assert out["n_bets"] == 0
        assert np.isnan(out["win_rate"])
        assert out["total_pnl"] == 0.0
