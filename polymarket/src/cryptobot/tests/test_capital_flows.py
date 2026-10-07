"""Cash-flow-aware equity accounting.

`capital_flows` rows (deposit / withdrawal / rebate / adjustment) are merged
into the equity replay at their OWN timestamps so the drawdown-from-peak metric
that gates the hard DD halt, the DD brake and stake sizing runs on TRUE funded
equity instead of a hand-set seed.

Pins:
  - the schema CHECKs (kind vocabulary + sign-per-kind)
  - the flow-inclusive peak math on an exact hand-worked numeric case
  - the DELIBERATE asymmetry: drawdown_pct_peak_frac (halt/brake) is
    flow-inclusive, drawdown_ratio (D14 backtest parity) stays PURE P&L
  - a flow before the WALLET_START_AT cutover is ignored (it is already inside
    INITIAL_BANKROLL)
  - the Python per-row replay and the SQL-sourced replay agree with flows
  - the shared-equity group (the LIVE topology) applies the flows ONCE
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from cryptobot import config
from cryptobot.execution import capital
from cryptobot.execution.capital import (
    CapitalFlowError,
    get_wallet_snapshots,
    list_capital_flows,
    load_capital_flows,
    record_capital_flow,
)
from cryptobot.execution.ledger import BetSignal, record_bet
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio

# The operator's true balance at the 2026-07-21 22:11 cutover — the value
# CRYPTOBOT_INITIAL_BANKROLL reverts to once the deposits become flow rows.
SEED = 103.78


@pytest.fixture
def seeded_bankroll(monkeypatch: pytest.MonkeyPatch) -> float:
    """Pin INITIAL_BANKROLL to the real cutover balance for the exact-number
    cases. capital.py binds the constant at import (`from ... import
    INITIAL_BANKROLL`), so the MODULE attribute is what every replay reads."""
    monkeypatch.setattr(capital, "INITIAL_BANKROLL", SEED)
    return SEED


def _settled(
    conn: sqlite3.Connection,
    *,
    sleeve: str,
    pnl: float,
    resolution_ts: str,
    suffix: str,
    bet_ts: str = "2026-07-22 00:00:00",
) -> int:
    """One settled ledger row with a controlled equity-change timestamp."""
    row_id = record_bet(
        conn,
        BetSignal(
            coin="BTC", sleeve=sleeve, entry_minute=1, side="YES",
            window_start_ts=1_715_923_200, window_end_ts=1_715_923_500,
            interval_min=5 if sleeve.endswith("5m") else 15,
            poly_market_id=f"M-{sleeve}-{suffix}",
            poly_token_id=f"T-{sleeve}-{suffix}",
            fee_rate_bps=100.0, model_p=0.7, p_market=0.5, fill_price=0.5,
            walked_vwap=0.5, realized_edge=0.1, partial_ret_z_1h=0.0,
            lut_p_bucket=7, lut_hit_rate=0.7, lut_n_cum=100,
            target_usd=abs(pnl) + 1.0, bet_size=abs(pnl) + 1.0,
            slot_filled_pre=0.0, entry_top_price=0.5, yes_pre1_anchor=0.5,
            config_sha256="x",
        ),
    )
    conn.execute(
        "UPDATE ledger SET outcome=?, pnl_net=?, resolution_ts=?, bet_ts=? "
        "WHERE id=?",
        ("WIN" if pnl >= 0 else "LOSS", pnl, resolution_ts, bet_ts, row_id),
    )
    conn.commit()
    return row_id


def _flow(
    conn: sqlite3.Connection,
    *,
    kind: str,
    amount: float,
    at: str,
    note: str | None = None,
) -> None:
    """Raw insert so the math cases stay independent of the writer's
    not-in-the-future guard (and exercise the schema CHECKs directly)."""
    conn.execute(
        "INSERT INTO capital_flows (occurred_at, kind, amount_usd, note) "
        "VALUES (?, ?, ?, ?)",
        (at, kind, amount, note),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


class TestCapitalFlowsSchema:
    def test_table_and_columns_present(self, db: sqlite3.Connection) -> None:
        cols = {r[1] for r in db.execute("PRAGMA table_info(capital_flows)")}
        assert cols == {
            "id", "occurred_at", "kind", "amount_usd", "note", "source",
            "created_at",
        }

    def test_source_defaults_to_manual(self, db: sqlite3.Connection) -> None:
        _flow(db, kind="deposit", amount=10.0, at="2026-07-26 06:10:00")
        row = db.execute("SELECT source, created_at FROM capital_flows").fetchone()
        assert row["source"] == "manual"
        assert row["created_at"]

    def test_unknown_kind_rejected(self, db: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _flow(db, kind="airdrop", amount=10.0, at="2026-07-26 06:10:00")

    @pytest.mark.parametrize(
        "kind,amount",
        [("deposit", -10.0), ("rebate", -0.5), ("withdrawal", 10.0)],
    )
    def test_wrong_sign_rejected(
        self, db: sqlite3.Connection, kind: str, amount: float
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _flow(db, kind=kind, amount=amount, at="2026-07-26 06:10:00")

    @pytest.mark.parametrize("amount", [7.0, -7.0])
    def test_adjustment_accepts_either_sign(
        self, db: sqlite3.Connection, amount: float
    ) -> None:
        _flow(db, kind="adjustment", amount=amount, at="2026-07-26 06:10:00")
        assert list_capital_flows(db)[0]["amount_usd"] == pytest.approx(amount)


# ---------------------------------------------------------------------------
# the exact numeric case
# ---------------------------------------------------------------------------


class TestFlowAwarePeak:
    """Hand-worked: seed 103.78, P&L +18.22 then −43.97 (cum −25.75),
    deposits +91.65 (07-26) and +83.98 (07-27)."""

    def test_deposits_after_the_drawdown_rebase_the_percent(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        # merged series: 103.78 → 122.00 → 78.03 → 169.68 → 253.66
        _settled(db, sleeve="v6_5m", pnl=18.22,
                 resolution_ts="2026-07-22 00:05:00", suffix="a")
        _settled(db, sleeve="v6_5m", pnl=-43.97,
                 resolution_ts="2026-07-23 00:05:00", suffix="b")
        _flow(db, kind="deposit", amount=91.65, at="2026-07-26 06:10:00")
        _flow(db, kind="deposit", amount=83.98, at="2026-07-27 08:50:00")

        snap = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]

        assert snap.realized_capital == pytest.approx(253.66)
        assert snap.net_flow_usd == pytest.approx(175.63)
        assert snap.pnl_total == pytest.approx(-25.75)  # P&L stays pure
        # The deposits carry the series to a NEW high, so the funded account is
        # not in drawdown: peak == equity == 253.66.
        assert snap.peak_realized_capital == pytest.approx(253.66)
        assert snap.drawdown_pct_peak_frac == pytest.approx(0.0)
        # ...but the P&L drawdown DOLLARS are not erased: D14 still reads the
        # pure-P&L pair (peak 122.00, realized 78.03) over the constant seed.
        assert snap.drawdown_ratio == pytest.approx(43.97 / 103.78, abs=1e-9)

    def test_loss_after_the_deposits_draws_down_from_the_funded_peak(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        # merged series: 103.78 → 122.00 → 213.65 → 297.63 (peak) → 253.66
        _settled(db, sleeve="v6_5m", pnl=18.22,
                 resolution_ts="2026-07-22 00:05:00", suffix="a")
        _flow(db, kind="deposit", amount=91.65, at="2026-07-26 06:10:00")
        _flow(db, kind="deposit", amount=83.98, at="2026-07-27 08:50:00")
        _settled(db, sleeve="v6_5m", pnl=-43.97,
                 resolution_ts="2026-07-27 12:00:00", suffix="b")

        snap = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]

        assert snap.realized_capital == pytest.approx(253.66)
        assert snap.peak_realized_capital == pytest.approx(297.63)
        # (297.63 − 253.66) / 297.63 — the $43.97 loss measured against FUNDED
        # equity, not against the $122.00 pre-deposit paper peak (36.0%).
        assert snap.drawdown_pct_peak_frac == pytest.approx(
            43.97 / 297.63, abs=1e-9
        )
        assert snap.drawdown_pct_peak_frac < 43.97 / 122.00
        # D14 is blind to the deposits either way.
        assert snap.drawdown_ratio == pytest.approx(43.97 / 103.78, abs=1e-9)

    def test_withdrawal_lowers_equity_without_booking_a_loss(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        _settled(db, sleeve="v6_5m", pnl=18.22,
                 resolution_ts="2026-07-22 00:05:00", suffix="a")
        _flow(db, kind="withdrawal", amount=-22.00, at="2026-07-23 00:00:00")

        snap = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]

        assert snap.realized_capital == pytest.approx(100.00)
        # The withdrawal changes the funded balance, not trading equity, so it
        # starts a fresh halt high-water mark at the $100 that remains.
        assert snap.peak_realized_capital == pytest.approx(100.00)
        assert snap.pnl_total == pytest.approx(18.22)
        # Withdrawing cash is excluded from the halt drawdown; the P&L
        # drawdown stays zero too because nothing was lost trading.
        assert snap.drawdown_pct_peak_frac == pytest.approx(0.0)
        assert snap.drawdown_ratio == pytest.approx(0.0)

    def test_loss_after_withdrawal_uses_the_new_peak(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        _settled(db, sleeve="v6_5m", pnl=18.22,
                 resolution_ts="2026-07-22 00:05:00", suffix="a")
        _flow(db, kind="withdrawal", amount=-22.00, at="2026-07-23 00:00:00")
        _settled(db, sleeve="v6_5m", pnl=-10.00,
                 resolution_ts="2026-07-24 00:00:00", suffix="b")

        snap = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]

        assert snap.realized_capital == pytest.approx(90.00)
        assert snap.peak_realized_capital == pytest.approx(100.00)
        assert snap.drawdown_pct_peak_frac == pytest.approx(0.10)
        # The pure-P&L parity metric still sees only the $10 trading loss.
        assert snap.drawdown_ratio == pytest.approx(10.0 / 103.78, abs=1e-9)


# ---------------------------------------------------------------------------
# cutover
# ---------------------------------------------------------------------------


class TestCutover:
    def test_flow_before_cutover_is_ignored(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        """INITIAL_BANKROLL is the balance AT the cutover, so a pre-cutover
        deposit is already inside it — counting it again double-funds."""
        cutover = "2026-07-21 22:11:01"
        _flow(db, kind="deposit", amount=50.0, at="2026-07-20 00:00:00")
        _flow(db, kind="deposit", amount=91.65, at="2026-07-26 06:10:00")

        snap = get_wallet_snapshots(
            db, legacy_portfolio(), since_ts=cutover
        )["v6_5m"]

        assert snap.net_flow_usd == pytest.approx(91.65)
        assert snap.realized_capital == pytest.approx(SEED + 91.65)

    def test_load_capital_flows_filters_and_normalizes(
        self, db: sqlite3.Connection
    ) -> None:
        _flow(db, kind="deposit", amount=5.0, at="2026-07-20T00:00:00Z")
        _flow(db, kind="deposit", amount=7.0, at="2026-07-26 06:10:00")

        assert load_capital_flows(db, None) == [
            ("2026-07-20 00:00:00", 1, "deposit", 5.0),
            ("2026-07-26 06:10:00", 2, "deposit", 7.0),
        ]
        assert load_capital_flows(db, "2026-07-21 22:11:01") == [
            ("2026-07-26 06:10:00", 2, "deposit", 7.0),
        ]

    def test_missing_table_degrades_to_pure_pnl(self, db: sqlite3.Connection) -> None:
        db.execute("DROP TABLE capital_flows")
        assert load_capital_flows(db, None) == []
        assert list_capital_flows(db) == []


# ---------------------------------------------------------------------------
# python replay vs SQL replay
# ---------------------------------------------------------------------------


def test_settlement_sorts_before_a_flow_at_the_same_timestamp(
    db: sqlite3.Connection, seeded_bankroll: float
) -> None:
    """Flows carry a coarse operator-observed timestamp; a settlement's is
    exact, so within the same second the market event is the earlier one."""
    _settled(db, sleeve="v6_5m", pnl=-3.78,
             resolution_ts="2026-07-26 06:10:00", suffix="a")
    _flow(db, kind="deposit", amount=10.0, at="2026-07-26 06:10:00")

    snap = get_wallet_snapshots(db, legacy_portfolio())["v6_5m"]

    # loss first (103.78 → 100.00), then deposit (→ 110.00): peak is the end.
    # Deposit-first would have peaked at 113.78 and left a 3.3% drawdown.
    assert snap.peak_realized_capital == pytest.approx(110.0)
    assert snap.drawdown_pct_peak_frac == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# the LIVE topology: one shared equity
# ---------------------------------------------------------------------------


class TestSharedEquityFlows:
    @pytest.fixture(autouse=True)
    def _enable_subminute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)

    def test_flows_applied_once_across_both_members(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        """The deployed sleeves share ONE equity and ONE peak, so the deposit
        must land on the COMBINED replay exactly once — not once per member."""
        portfolio = load_sleeves()
        _settled(db, sleeve="subminute_5m", pnl=12.0,
                 resolution_ts="2026-07-22 00:01:00", suffix="a")
        _settled(db, sleeve="subminute_15m", pnl=-7.0,
                 resolution_ts="2026-07-22 00:02:00", suffix="b")
        _flow(db, kind="deposit", amount=91.65, at="2026-07-26 06:10:00")

        snaps = get_wallet_snapshots(db, portfolio)
        s5, s15 = snaps["subminute_5m"], snaps["subminute_15m"]

        assert s5.realized_capital == pytest.approx(SEED + 5.0 + 91.65)
        assert s5.net_flow_usd == pytest.approx(91.65)
        assert s15.realized_capital == pytest.approx(s5.realized_capital)
        assert s15.peak_realized_capital == pytest.approx(s5.peak_realized_capital)

    def test_deposit_lifts_the_shared_wallet_out_of_the_halt(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        """The halt-facing metric is the SHARED drawdown_pct_peak_frac, so a
        deposit that restores funded equity must clear the gate for BOTH."""
        from cryptobot.execution.pipeline import _wallet_drawdown_halted

        portfolio = load_sleeves()
        _settled(db, sleeve="subminute_5m", pnl=20.0,
                 resolution_ts="2026-07-22 00:01:00", suffix="a")
        _settled(db, sleeve="subminute_15m", pnl=-70.0,
                 resolution_ts="2026-07-23 00:01:00", suffix="b")

        halted = get_wallet_snapshots(db, portfolio)
        # peak 123.78, realized 53.78 → dd 56.5% >= the 40% halt.
        assert halted["subminute_5m"].drawdown_pct_peak_frac > 0.40
        assert _wallet_drawdown_halted(portfolio, halted["subminute_5m"])
        assert _wallet_drawdown_halted(portfolio, halted["subminute_15m"])

        _flow(db, kind="deposit", amount=91.65, at="2026-07-26 06:10:00")

        funded = get_wallet_snapshots(db, portfolio)
        # 53.78 + 91.65 = 145.43 is a new high → dd 0.
        assert funded["subminute_5m"].drawdown_pct_peak_frac == pytest.approx(0.0)
        assert not _wallet_drawdown_halted(portfolio, funded["subminute_5m"])
        assert not _wallet_drawdown_halted(portfolio, funded["subminute_15m"])

    def test_sizing_equity_and_exposure_cap_include_flows(
        self, db: sqlite3.Connection, seeded_bankroll: float
    ) -> None:
        """Stakes scale off realized_capital when no live CLOB balance is
        available, so the deposit has to reach the sizing anchor."""
        from cryptobot.execution import sizing_v2

        portfolio = load_sleeves()
        before = get_wallet_snapshots(db, portfolio)["subminute_5m"]
        _flow(db, kind="deposit", amount=91.65, at="2026-07-26 06:10:00")
        after = get_wallet_snapshots(db, portfolio)["subminute_5m"]

        assert after.realized_capital == pytest.approx(
            before.realized_capital + 91.65
        )
        staked_before = sizing_v2.sized_usd(
            before.realized_capital, 5, before.drawdown_pct_peak_frac
        )
        staked_after = sizing_v2.sized_usd(
            after.realized_capital, 5, after.drawdown_pct_peak_frac
        )
        assert staked_after > staked_before


# ---------------------------------------------------------------------------
# writer validation
# ---------------------------------------------------------------------------


class TestRecordCapitalFlow:
    def test_records_and_normalizes_timestamp(self, db: sqlite3.Connection) -> None:
        flow = record_capital_flow(
            db, kind="Deposit", amount_usd=91.65,
            occurred_at="2026-07-26T06:10:00Z", note="wire",
        )
        assert flow["kind"] == "deposit"
        assert flow["occurred_at"] == "2026-07-26 06:10:00"
        assert list_capital_flows(db)[0]["note"] == "wire"

    @pytest.mark.parametrize(
        "kind,amount,reason",
        [
            ("airdrop", 10.0, "bad_kind"),
            ("deposit", -10.0, "bad_sign"),
            ("withdrawal", 10.0, "bad_sign"),
            ("rebate", -1.0, "bad_sign"),
            ("deposit", 0.0, "bad_amount"),
        ],
    )
    def test_rejects_bad_kind_or_sign(
        self, db: sqlite3.Connection, kind: str, amount: float, reason: str
    ) -> None:
        with pytest.raises(CapitalFlowError) as exc:
            record_capital_flow(
                db, kind=kind, amount_usd=amount,
                occurred_at="2026-07-26 06:10:00",
            )
        assert exc.value.reason == reason
        assert list_capital_flows(db) == []

    def test_rejects_malformed_timestamp(self, db: sqlite3.Connection) -> None:
        with pytest.raises(CapitalFlowError) as exc:
            record_capital_flow(
                db, kind="deposit", amount_usd=5.0, occurred_at="26/07/2026",
            )
        assert exc.value.reason == "bad_timestamp"

    def test_rejects_future_timestamp(self, db: sqlite3.Connection) -> None:
        future = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with pytest.raises(CapitalFlowError) as exc:
            record_capital_flow(
                db, kind="deposit", amount_usd=5.0, occurred_at=future,
            )
        assert exc.value.reason == "future_timestamp"

    def test_duplicate_refused_unless_forced(self, db: sqlite3.Connection) -> None:
        kwargs = dict(
            kind="deposit", amount_usd=91.65, occurred_at="2026-07-26 06:10:00"
        )
        record_capital_flow(db, **kwargs)
        with pytest.raises(CapitalFlowError) as exc:
            record_capital_flow(db, **kwargs)
        assert exc.value.reason == "duplicate"
        assert len(list_capital_flows(db)) == 1

        record_capital_flow(db, force=True, **kwargs)
        assert len(list_capital_flows(db)) == 2


# ---------------------------------------------------------------------------
# read-only backfill proposer
# ---------------------------------------------------------------------------


def _sample(
    conn: sqlite3.Connection, at: str, balance: float, pnl: float, pending: float = 0.0
) -> None:
    conn.execute(
        "INSERT INTO bankroll_peak "
        "(sampled_at, wallet_balance, realized_pnl, pending_exposure) "
        "VALUES (?, ?, ?, ?)",
        (at, balance, pnl, pending),
    )
    conn.commit()


class TestBackfillProposer:
    def test_proposes_the_unexplained_residual_only(
        self, db: sqlite3.Connection
    ) -> None:
        from cryptobot.execution.capital_flow_backfill import propose_capital_flows

        # flat + quiet, balance tracks pnl exactly → nothing to propose
        _sample(db, "2026-07-26 05:55:00", 100.00, 0.0)
        _sample(db, "2026-07-26 06:05:00", 97.00, -3.0)
        # flat + quiet, balance jumps with no pnl → a deposit
        _sample(db, "2026-07-26 06:15:00", 188.65, -3.0)

        proposals, stats = propose_capital_flows(db)

        assert stats["flat_samples"] == 3
        assert len(proposals) == 1
        p = proposals[0]
        assert p.kind == "deposit"
        assert p.amount_usd == pytest.approx(91.65)
        assert p.occurred_at == "2026-07-26 06:10:00"  # midpoint of the gap
        assert p.window_start == "2026-07-26 06:05:00"

    def test_withdrawal_residual_is_signed(self, db: sqlite3.Connection) -> None:
        from cryptobot.execution.capital_flow_backfill import propose_capital_flows

        _sample(db, "2026-07-26 05:55:00", 100.00, 0.0)
        _sample(db, "2026-07-26 06:05:00", 60.00, 0.0)

        proposals, _ = propose_capital_flows(db)

        assert [(p.kind, p.amount_usd) for p in proposals] == [
            ("withdrawal", -40.0)
        ]

    def test_gap_with_an_open_bet_is_skipped(self, db: sqlite3.Connection) -> None:
        """Cash moves on its own schedule while a bet is live, so a residual
        spanning one is execution transit, not funding."""
        from cryptobot.execution.capital_flow_backfill import propose_capital_flows

        _settled(db, sleeve="v6_5m", pnl=-5.0,
                 resolution_ts="2026-07-26 06:12:00", suffix="a",
                 bet_ts="2026-07-26 06:08:00")
        db.execute("UPDATE ledger SET mode='LIVE'")
        db.commit()
        _sample(db, "2026-07-26 06:05:00", 100.00, 0.0)
        _sample(db, "2026-07-26 06:15:00", 188.65, 0.0)

        proposals, stats = propose_capital_flows(db)

        assert proposals == []
        assert stats["skipped_busy_gaps"] == 1

    def test_pending_sample_is_not_flat(self, db: sqlite3.Connection) -> None:
        from cryptobot.execution.capital_flow_backfill import propose_capital_flows

        _sample(db, "2026-07-26 06:05:00", 100.00, 0.0, pending=4.0)
        _sample(db, "2026-07-26 06:15:00", 188.65, 0.0)

        proposals, stats = propose_capital_flows(db)

        assert stats["flat_samples"] == 1
        assert proposals == []

    def test_sub_threshold_residual_ignored(self, db: sqlite3.Connection) -> None:
        from cryptobot.execution.capital_flow_backfill import propose_capital_flows

        _sample(db, "2026-07-26 06:05:00", 100.00, 0.0)
        _sample(db, "2026-07-26 06:15:00", 101.50, 0.0)

        assert propose_capital_flows(db)[0] == []
        assert len(propose_capital_flows(db, min_residual_usd=1.0)[0]) == 1

    def test_format_emits_runnable_record_flow_lines(
        self, db: sqlite3.Connection
    ) -> None:
        from cryptobot.execution.capital_flow_backfill import (
            format_proposals,
            propose_capital_flows,
        )

        _sample(db, "2026-07-26 06:05:00", 100.00, 0.0)
        _sample(db, "2026-07-26 06:15:00", 191.65, 0.0)
        proposals, stats = propose_capital_flows(db)

        report = format_proposals(proposals, stats)

        assert "READ-ONLY" in report
        assert (
            'record-flow --kind deposit --amount 91.65 '
            '--at "2026-07-26 06:10:00"' in report
        )
        # ASCII only — the operator reads this on a cp1252 Windows console.
        report.encode("ascii")

    def test_proposer_never_writes(self, db: sqlite3.Connection) -> None:
        from cryptobot.execution.capital_flow_backfill import propose_capital_flows

        _sample(db, "2026-07-26 06:05:00", 100.00, 0.0)
        _sample(db, "2026-07-26 06:15:00", 191.65, 0.0)

        propose_capital_flows(db)

        assert list_capital_flows(db) == []
