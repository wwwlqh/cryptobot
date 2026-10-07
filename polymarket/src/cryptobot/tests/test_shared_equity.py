"""Phase 5 shared-equity wallet tests (exp044 plan 2026-05-29-001 option B1).

The two exp044 sleeves (exp044_5m, exp044_15m) compound on ONE shared
equity, share ONE drawdown halt, and share ONE exposure cap — faithful to
exp044's single-wallet backtest. This is OPT-IN via the per-group
`shared_equity` flag: isolated wallets and non-shared groups (legacy
lut/pairs/all_4 topologies) stay byte-for-byte unchanged.

Coverage:
  - WalletGroup.shared_equity parses (True for exp044, False/absent legacy).
  - get_wallet_snapshots combines realized / peak (merged curve) / pending /
    DD across the two shared sleeves; both members report the SAME values.
  - DD-halt-together: combined DD over the halt fires for BOTH members; a
    combined DD under the halt halts NEITHER even if one sleeve alone would.
  - ISOLATED UNCHANGED: a legacy isolated/2-group portfolio is byte-identical
    to the pre-Phase-5 per-wallet computation.
  - pipeline shared-cap: sleeve B's exposure gate sees sleeve A's in-tick bet
    against the SHARED cap (the `_commit_cell` within-tick aggregation).
"""

from __future__ import annotations

import sqlite3

import pytest

from cryptobot import config
from cryptobot.execution.capital import get_wallet_snapshots
from cryptobot.execution.gates import GateInput, evaluate_gates
from cryptobot.execution.ledger import BetSignal, record_bet
from cryptobot.execution.pipeline import _wallet_drawdown_halted
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio

INITIAL = config.INITIAL_BANKROLL  # 100.0


@pytest.fixture(autouse=True)
def _enable_subminute(monkeypatch):
    """The live v2 shared-equity wallet is the checkpoint-only step1last config
    (group_live = subminute_5m + subminute_15m). load_sleeves fails closed
    unless CRYPTOBOT_SUBMINUTE_ENABLED=1 (set by the live env), so pin it on for
    the load_sleeves()-backed cases. The legacy_portfolio() cases are unaffected
    (they build their own artifact and don't read this flag)."""
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)


# ---------------------------------------------------------------------------
# seeding helpers
# ---------------------------------------------------------------------------


def _seed_pending(conn: sqlite3.Connection, sleeve: str, bet_size: float, suffix: str) -> int:
    """Insert one PENDING ledger row for `sleeve` with a unique window."""
    sig = BetSignal(
        coin="BTC", sleeve=sleeve, entry_minute=1, side="YES",
        window_start_ts=1_715_923_200, window_end_ts=1_715_923_500,
        interval_min=5 if sleeve.endswith("5m") else 15,
        poly_market_id=f"M-{sleeve}-{suffix}", poly_token_id=f"T-{sleeve}-{suffix}",
        fee_rate_bps=100.0, model_p=0.7, p_market=0.5, fill_price=0.5,
        walked_vwap=0.5, realized_edge=0.1, partial_ret_z_1h=0.0,
        lut_p_bucket=7, lut_hit_rate=0.7, lut_n_cum=100,
        target_usd=bet_size, bet_size=bet_size, slot_filled_pre=0.0,
        entry_top_price=0.5, yes_pre1_anchor=0.5, config_sha256="x",
    )
    return record_bet(conn, sig)


def _seed_settled(
    conn: sqlite3.Connection, sleeve: str, pnl: float, resolution_ts: str, suffix: str
) -> int:
    """Insert a settled row for `sleeve` with a controlled resolution_ts so
    the merged-curve peak replay order is deterministic."""
    row_id = _seed_pending(conn, sleeve, abs(pnl) + 1.0, suffix)
    outcome = "WIN" if pnl >= 0 else "LOSS"
    conn.execute(
        "UPDATE ledger SET outcome=?, pnl_net=?, resolution_ts=? WHERE id=?",
        (outcome, pnl, resolution_ts, row_id),
    )
    return row_id


# ---------------------------------------------------------------------------
# loader: shared_equity flag
# ---------------------------------------------------------------------------


def test_v2_group_parses_shared_equity_true() -> None:
    portfolio = load_sleeves()
    group = portfolio.wallet_groups[0]
    assert group.shared_equity is True
    assert portfolio.shared_equity_group_for("subminute_5m") is group
    assert portfolio.shared_equity_group_for("subminute_15m") is group


def test_legacy_group_shared_equity_false() -> None:
    portfolio = legacy_portfolio()
    assert all(g.shared_equity is False for g in portfolio.wallet_groups)
    assert portfolio.shared_equity_group_for("v6_5m") is None


# ---------------------------------------------------------------------------
# combined capital across the two shared sleeves
# ---------------------------------------------------------------------------


def test_combined_realized_adds_bankroll_once(db: sqlite3.Connection) -> None:
    """Both members report realized = 100 + (5m_pnl + 15m_pnl); the shared
    wallet's ONE bankroll is added ONCE, not once per member."""
    portfolio = load_sleeves()
    # 5m up +12, 15m down -7 (so combined != either sleeve alone).
    _seed_settled(db, "subminute_5m", 12.0, "2026-05-29 00:01:00", "a")
    _seed_settled(db, "subminute_15m", -7.0, "2026-05-29 00:02:00", "b")
    db.commit()

    snaps = get_wallet_snapshots(db, portfolio)
    s5, s15 = snaps["subminute_5m"], snaps["subminute_15m"]

    expected_realized = INITIAL + (12.0 - 7.0)  # 105, NOT 200 + ...
    assert s5.realized_capital == pytest.approx(expected_realized)
    assert s15.realized_capital == pytest.approx(expected_realized)
    assert s5.realized_capital == s15.realized_capital
    # Per-member pnl_total stays per-sleeve for dashboard attribution.
    assert s5.pnl_total == pytest.approx(12.0)
    assert s15.pnl_total == pytest.approx(-7.0)
    # initial_bankroll is the shared $100 once.
    assert s5.initial_bankroll == pytest.approx(INITIAL)
    assert s15.initial_bankroll == pytest.approx(INITIAL)


def test_kpis_rollup_does_not_double_count_shared_equity(db: sqlite3.Connection) -> None:
    """snapshot_builder portfolio KPI totals count the shared wallet ONCE, not
    once per member. The Phase-5 post-pass gives both members identical combined
    values; summing per-member would render a single $100 shared wallet as $200.
    Seeded with non-zero PnL so the doubling is detectable (empty-ledger tests
    can't catch it)."""
    from cryptobot.observability.snapshot_builder import build_payload

    portfolio = load_sleeves()
    _seed_settled(db, "subminute_5m", 12.0, "2026-05-29 00:01:00", "a")
    _seed_settled(db, "subminute_15m", -7.0, "2026-05-29 00:02:00", "b")
    _seed_pending(db, "subminute_5m", 3.0, "p1")
    _seed_pending(db, "subminute_15m", 2.0, "p2")
    db.commit()

    kpis = build_payload(db, portfolio)[0]["kpis"]
    # ONE shared $100 wallet, combined realized = 100 + (12 - 7) = 105.
    assert kpis["initial_bankroll"] == pytest.approx(100.0)   # not 200
    assert kpis["realized_capital"] == pytest.approx(105.0)   # not 210
    assert kpis["exposure_used"] == pytest.approx(5.0)        # 3 + 2, counted once
    assert kpis["exposure_cap"] == pytest.approx(105.0)       # 105 * 1.0, not 210
    assert kpis["n_pending"] == 2                              # per-sleeve count: 2 open


def test_combined_peak_is_merged_curve_not_sum_or_max(db: sqlite3.Connection) -> None:
    """combined_peak replays the MERGED settled curve. Constructed so the
    merged peak (111) differs from sum-of-per-sleeve-peaks (211) AND from
    max-of-per-sleeve-peaks (106)."""
    portfolio = load_sleeves()
    # 5m: +6 @ t1, -4 @ t4  (5m alone: 100->106->102, peak 106)
    # 15m: +5 @ t2, -8 @ t3 (15m alone: 100->105->97,  peak 105)
    # merged chronological: 100 ->106(t1)->111(t2)->103(t3)->99(t4), peak 111
    _seed_settled(db, "subminute_5m", 6.0, "2026-05-29 00:01:00", "p1")
    _seed_settled(db, "subminute_15m", 5.0, "2026-05-29 00:02:00", "p2")
    _seed_settled(db, "subminute_15m", -8.0, "2026-05-29 00:03:00", "p3")
    _seed_settled(db, "subminute_5m", -4.0, "2026-05-29 00:04:00", "p4")
    db.commit()

    snaps = get_wallet_snapshots(db, portfolio)
    s5, s15 = snaps["subminute_5m"], snaps["subminute_15m"]

    assert s5.peak_realized_capital == pytest.approx(111.0)
    assert s15.peak_realized_capital == pytest.approx(111.0)
    # NOT the sum of per-sleeve peaks (106 + 105 = 211) and NOT max (106).
    assert s5.peak_realized_capital != pytest.approx(211.0)
    assert s5.peak_realized_capital != pytest.approx(106.0)

    # Combined realized = 100 + (6-4) + (5-8) = 99.
    assert s5.realized_capital == pytest.approx(99.0)
    # Combined DD off the shared peak/realized.
    assert s5.drawdown_ratio == pytest.approx((111.0 - 99.0) / INITIAL)
    assert s5.drawdown_pct_peak_frac == pytest.approx((111.0 - 99.0) / 111.0)
    assert s15.drawdown_ratio == s5.drawdown_ratio
    assert s15.drawdown_pct_peak_frac == s5.drawdown_pct_peak_frac


def test_combined_pending_sums_members(db: sqlite3.Connection) -> None:
    portfolio = load_sleeves()
    _seed_pending(db, "subminute_5m", 4.0, "q1")
    _seed_pending(db, "subminute_15m", 6.0, "q2")
    db.commit()

    snaps = get_wallet_snapshots(db, portfolio)
    assert snaps["subminute_5m"].pending_exposure == pytest.approx(10.0)
    assert snaps["subminute_15m"].pending_exposure == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# shared drawdown halt
# ---------------------------------------------------------------------------


def test_dd_halt_fires_for_both_members(db: sqlite3.Connection) -> None:
    """Combined DD over MAX_DRAWDOWN_HALT_FRAC halts BOTH sleeves."""
    portfolio = load_sleeves()
    assert portfolio.max_drawdown_halt_frac == pytest.approx(0.40)
    # 5m: +80 @ t1, -90 @ t3 ; 15m: +10 @ t2, -10 @ t4
    # merged: 100->180->190->100->90, peak 190, realized 90,
    # dd_peak_frac = (190-90)/190 = 0.526 >= 0.50.
    _seed_settled(db, "subminute_5m", 80.0, "2026-05-29 00:01:00", "h1")
    _seed_settled(db, "subminute_15m", 10.0, "2026-05-29 00:02:00", "h2")
    _seed_settled(db, "subminute_5m", -90.0, "2026-05-29 00:03:00", "h3")
    _seed_settled(db, "subminute_15m", -10.0, "2026-05-29 00:04:00", "h4")
    db.commit()

    snaps = get_wallet_snapshots(db, portfolio)
    assert snaps["subminute_5m"].drawdown_pct_peak_frac >= 0.50
    assert _wallet_drawdown_halted(portfolio, snaps["subminute_5m"]) is True
    assert _wallet_drawdown_halted(portfolio, snaps["subminute_15m"]) is True


def test_dd_halt_held_by_combined_even_when_one_sleeve_alone_would(
    db: sqlite3.Connection,
) -> None:
    """Combined DD UNDER the halt halts NEITHER member, even though the 5m
    sleeve ALONE (down 60% off $100) would breach a per-wallet halt."""
    portfolio = load_sleeves()
    # 5m: -60 @ a1 (alone: 100->40, dd 0.60). 15m: +55 @ a2 (alone: 100->155).
    # merged: 100->40->95, peak 100, realized 95, dd_peak_frac = 0.05 < 0.50.
    _seed_settled(db, "subminute_5m", -60.0, "2026-05-29 00:01:00", "u1")
    _seed_settled(db, "subminute_15m", 55.0, "2026-05-29 00:02:00", "u2")
    db.commit()

    snaps = get_wallet_snapshots(db, portfolio)
    assert snaps["subminute_5m"].drawdown_pct_peak_frac == pytest.approx(0.05)
    assert _wallet_drawdown_halted(portfolio, snaps["subminute_5m"]) is False
    assert _wallet_drawdown_halted(portfolio, snaps["subminute_15m"]) is False


# ---------------------------------------------------------------------------
# ISOLATED UNCHANGED — the post-pass must not touch non-shared portfolios
# ---------------------------------------------------------------------------


def test_isolated_portfolio_snapshots_unchanged(db: sqlite3.Connection) -> None:
    """A legacy 2-group (non-shared) portfolio keeps per-wallet realized /
    peak / DD / pending — byte-identical to the pre-Phase-5 computation."""
    portfolio = legacy_portfolio()
    # v6_5m up +12, v6_15m down -5; pairs untouched.
    _seed_settled(db, "v6_5m", 12.0, "2026-05-29 00:01:00", "i1")
    _seed_settled(db, "v6_15m", -5.0, "2026-05-29 00:02:00", "i2")
    _seed_pending(db, "v6_5m", 3.0, "i3")
    db.commit()

    snaps = get_wallet_snapshots(db, portfolio)
    # Each wallet isolated: realized = 100 + its own pnl.
    assert snaps["v6_5m"].realized_capital == pytest.approx(112.0)
    assert snaps["v6_15m"].realized_capital == pytest.approx(95.0)
    assert snaps["pairs_5m"].realized_capital == pytest.approx(100.0)
    assert snaps["pairs_15m"].realized_capital == pytest.approx(100.0)
    # Peaks isolated: v6_5m peak 112 (off its own curve), v6_15m peak 100.
    assert snaps["v6_5m"].peak_realized_capital == pytest.approx(112.0)
    assert snaps["v6_15m"].peak_realized_capital == pytest.approx(100.0)
    # DD isolated: v6_15m (100->95) dd_ratio 0.05; v6_5m none.
    assert snaps["v6_5m"].drawdown_ratio == pytest.approx(0.0)
    assert snaps["v6_15m"].drawdown_ratio == pytest.approx(0.05)
    # Pending isolated, NOT summed across wallets.
    assert snaps["v6_5m"].pending_exposure == pytest.approx(3.0)
    assert snaps["v6_15m"].pending_exposure == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# pipeline shared exposure cap — sleeve B's gate sees sleeve A's in-tick bet
# ---------------------------------------------------------------------------


def _shared_pending_within_tick(portfolio, wallet_group, wallet_id, tick_pending_delta):
    """Mirror the `_commit_cell` within-tick pending computation (Phase 5):
    a shared_equity member aggregates ALL group members' tick deltas; any
    other wallet uses only its own delta."""
    shared_group = None
    if wallet_group is not None:
        grp = portfolio.group_by_id(wallet_group)
        if grp is not None and getattr(grp, "shared_equity", False):
            shared_group = grp
    if shared_group is not None:
        return sum(tick_pending_delta.get(m, 0.0) for m in shared_group.members)
    return tick_pending_delta.get(wallet_id, 0.0)


def _v2_sleeve(portfolio, label):
    return next(s for s in portfolio.sleeves if s.label == label)


def test_shared_cap_second_sleeve_sees_first_in_tick_bet() -> None:
    """Two cells in one tick on the two shared sleeves both honor the SHARED
    $30 exposure cap (combined realized 100 * 0.30). Sleeve A bets $28 first;
    sleeve B's gate then sees A's in-tick delta and is blocked at $4."""
    portfolio = load_sleeves()
    s15 = _v2_sleeve(portfolio, "subminute_15m")

    # Shared snapshot pending starts at 0; sleeve A (5m) commits $28 in-tick.
    tick_pending_delta = {"subminute_5m": 28.0}

    pending_within_tick = _shared_pending_within_tick(
        portfolio, "group_live", "subminute_15m", tick_pending_delta
    )
    # Sleeve B (15m) sees sleeve A's $28 even though its own delta is 0.
    assert pending_within_tick == pytest.approx(28.0)

    inp = GateInput(
        coin="BTC", sleeve=s15, entry_minute=1, side="YES",
        # fill_price in the v2 sleeve's 0.65–0.85 band so the fill-band gate
        # passes and the SHARED exposure cap is what this test exercises.
        realized_edge=0.10, fill_price=0.75, fee_rate_bps=100.0, spot_age_s=1.0,
        proposed_bet_size=4.0,
        pending_exposure_total=0.0 + pending_within_tick,  # combined snap (0) + in-tick
        realized_capital=100.0,  # shared combined realized
        slot_filled_usd=0.0, slot_target_usd=10.0,
        disk_free_mb=10_000.0, book_top_ask_age_s=0.5,
        book_total_ask_depth_usd=10_000.0,
        exposure_cap_fraction=0.30,
    )
    # 28 + 4 = 32 > 30 cap → blocked because B sees A's in-tick bet.
    assert evaluate_gates(inp).skip_reason == "exposure_cap"


def test_shared_cap_within_headroom_passes() -> None:
    """The same shared cap admits sleeve B when the combined in-tick total
    stays within the $30 cap (A bet $20, B bets $4 → $24 <= $30)."""
    portfolio = load_sleeves()
    s15 = _v2_sleeve(portfolio, "subminute_15m")
    tick_pending_delta = {"subminute_5m": 20.0}
    pending_within_tick = _shared_pending_within_tick(
        portfolio, "group_live", "subminute_15m", tick_pending_delta
    )
    inp = GateInput(
        coin="BTC", sleeve=s15, entry_minute=1, side="YES",
        # in-band fill so the fill-band gate passes; the cap is the assertion.
        realized_edge=0.10, fill_price=0.75, fee_rate_bps=100.0, spot_age_s=1.0,
        proposed_bet_size=4.0,
        pending_exposure_total=0.0 + pending_within_tick,
        realized_capital=100.0,
        slot_filled_usd=0.0, slot_target_usd=10.0,
        disk_free_mb=10_000.0, book_top_ask_age_s=0.5,
        book_total_ask_depth_usd=10_000.0,
        exposure_cap_fraction=0.30,
    )
    assert evaluate_gates(inp).passed is True


def test_isolated_wallet_does_not_aggregate_in_tick() -> None:
    """A non-shared (legacy) group member uses ONLY its own in-tick delta —
    the shared aggregation must not leak into isolated topologies."""
    portfolio = legacy_portfolio()  # group_5m/group_15m, shared_equity False
    tick_pending_delta = {"v6_5m": 28.0, "pairs_5m": 28.0}
    # Even though v6_5m and pairs_5m are in the SAME (non-shared) group_5m,
    # v6_5m's within-tick pending is ONLY its own $28 — not the $56 sum.
    pending_within_tick = _shared_pending_within_tick(
        portfolio, "group_5m", "v6_5m", tick_pending_delta
    )
    assert pending_within_tick == pytest.approx(28.0)
