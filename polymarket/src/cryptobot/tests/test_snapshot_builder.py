"""U12 snapshot_builder tests â€” payload shape + KPI math."""

from __future__ import annotations

import json
import sqlite3

import pytest

from cryptobot import config
from cryptobot.venues.types import BookSnapshot
from cryptobot.execution.ledger import record_bet
from cryptobot.observability.snapshot_builder import build_payload
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot.tests._legacy_artifact import legacy_portfolio
from cryptobot.tests.test_slot_state import make_signal


@pytest.fixture(autouse=True)
def _enable_subminute(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests that call `load_sleeves()` pick up the checkpoint-only v2 config,
    which fails closed unless the sub-minute flag is on."""
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)


@pytest.fixture
def portfolio():
    return legacy_portfolio()


def test_empty_db_payload_shape(db: sqlite3.Connection, portfolio):
    payload, max_id = build_payload(db, portfolio)
    # Top-level keys required by R-NEW-12 (gates retired 2026-07-14 with the
    # Live markets & gates tab).
    for key in ("tick_ts", "kpis", "portfolio", "markets", "ledger",
                "ledger_rollup", "ledger_daily"):
        assert key in payload, f"missing {key}"
    # Empty ledger → rollup degrades gracefully (honest empty state).
    roll = payload["ledger_rollup"]
    assert roll["total"] == 0
    # All-time per-day rollup is present and empty (no settled rows).
    assert payload["ledger_daily"] == []
    assert roll["decided"] == 0
    assert roll["win_rate"] is None
    assert payload["kpis"]["initial_bankroll"] == 400.0
    assert payload["kpis"]["wallet_bankroll"] == 100.0
    assert payload["kpis"]["realized_capital"] == 400.0
    assert payload["kpis"]["pnl_24h"] == 0.0
    assert payload["kpis"]["n_pending"] == 0
    assert payload["kpis"]["exposure_cap"] == 99.94
    assert len(payload["kpis"]["wallets"]) == 4
    assert payload["kpis"]["wallets"][0]["realized_capital"] == 100.0
    assert payload["kpis"]["wallets"][0]["backtest_max_drawdown_pct"] == 33.65
    assert payload["kpis"]["max_drawdown_halt_pct"] == 50.0
    assert payload["kpis"]["trading_halted"] is False
    # Ledger-paper gross → fee → net decomposition is present (0.0 on empty).
    assert payload["kpis"]["pnl_gross_usd"] == 0.0
    assert payload["kpis"]["pnl_fee_usd"] == 0.0
    assert payload["kpis"]["pnl_net_usd"] == 0.0
    # No capital_flows rows → 0.0 rebates (not None: the ledger read succeeded).
    assert payload["kpis"]["pnl_rebate_usd"] == 0.0
    assert max_id == 0


def test_pnl_decomposition_gross_fee_net(db: sqlite3.Connection, portfolio):
    """Ledger-paper gross → fee → net. Polymarket's fee is embedded in the fill
    price and skimmed from the WINNING payout, so it is realized only on WIN
    rows; losses forfeit the stake with no payout to skim. gross − fee == net
    exactly, and net matches the ledger_rollup net (same _ledger_where filter)."""
    # WIN: bet_size 5 @ fill 0.50 → shares 10, gross 10*(1−0.50)=5.00; booked
    # net 3.00 ⇒ realized fee 2.00 (the skim that reduced the payout).
    win_id = record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=3.0, fill_price=0.5, "
        "live_filled_price=NULL, resolution_ts=datetime('now') WHERE id=?",
        (win_id,),
    )
    # LOSS: stake forfeited (−4.00), no winning payout to skim → fee 0.
    loss_id = record_bet(db, make_signal(bet_size=4.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-4.0, "
        "resolution_ts=datetime('now') WHERE id=?",
        (loss_id,),
    )
    db.commit()

    k = build_payload(db, portfolio)[0]["kpis"]
    assert k["pnl_net_usd"] == pytest.approx(-1.0)     # 3.00 − 4.00
    assert k["pnl_fee_usd"] == pytest.approx(2.0)      # win only
    assert k["pnl_gross_usd"] == pytest.approx(1.0)    # 5.00 − 4.00
    # gross − fee == net exactly (the fee reconciliation identity).
    assert k["pnl_gross_usd"] - k["pnl_fee_usd"] == pytest.approx(k["pnl_net_usd"])


def test_pnl_decomposition_uses_live_fill_pair_never_mixed(
    db: sqlite3.Connection, portfolio
):
    """M1/M2 partial-fill fix (2026-07-21): a winning LIVE row's gross/fee use
    the REALIZED (live_filled_size, live_filled_price) pair TOGETHER —
    ``live_filled_size*(1−live_filled_price) − pnl_net`` — never the intended
    bet_size divided by the realized price (the old mixing fabricated gross/fee
    on every partial fill)."""
    # LIVE partial fill: intended $5 @0.50 but only 2.5 shares landed @0.80
    # (cost $2.00): gross 2.5*(1−0.80)=0.50; booked net 0.30 ⇒ fee 0.20.
    # (The old mixed form read bet_size/0.8=6.25 shares → gross $1.25.)
    win_id = record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=0.30, fill_price=0.5, "
        "live_filled_size=2.5, live_filled_price=0.8, "
        "resolution_ts=datetime('now') WHERE id=?",
        (win_id,),
    )
    db.commit()
    k = build_payload(db, portfolio)[0]["kpis"]
    assert k["pnl_fee_usd"] == pytest.approx(0.20)
    assert k["pnl_gross_usd"] == pytest.approx(0.50)
    assert k["pnl_net_usd"] == pytest.approx(0.30)


def test_pnl_decomposition_live_price_without_size_uses_intended_pair(
    db: sqlite3.Connection, portfolio
):
    """A live price with NO landed size (reconciler never wrote
    live_filled_size) can't poison the math either: the row falls back to the
    coherent intended pair (bet_size, fill_price)."""
    # Intended pair: shares 5/0.50=10, gross 10*(1−0.50)=5.00; net 3.00 ⇒ fee 2.
    win_id = record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=3.0, fill_price=0.5, "
        "live_filled_size=NULL, live_filled_price=0.8, "
        "resolution_ts=datetime('now') WHERE id=?",
        (win_id,),
    )
    db.commit()
    k = build_payload(db, portfolio)[0]["kpis"]
    assert k["pnl_fee_usd"] == pytest.approx(2.0)
    assert k["pnl_gross_usd"] == pytest.approx(5.0)
    assert k["pnl_net_usd"] == pytest.approx(3.0)


def test_pnl_rebate_sums_rebate_flows_and_stays_out_of_net(
    db: sqlite3.Connection, portfolio
):
    """``pnl_rebate_usd`` is Σ capital_flows kind='rebate' — taker-fee rebates +
    rewards the venue pays back to the ACCOUNT. Only 'rebate' rows count
    (a deposit is funding, not earnings), and the rebate NEVER moves the
    gross/fee/net trio: no ledger row owns it, so gross − fee == net still
    holds to the cent."""
    win_id = record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=3.0, fill_price=0.5, "
        "live_filled_price=NULL, resolution_ts=datetime('now') WHERE id=?",
        (win_id,),
    )
    for kind, amount in (("rebate", 0.75), ("rebate", 1.25), ("deposit", 50.0)):
        db.execute(
            "INSERT INTO capital_flows (occurred_at, kind, amount_usd) "
            "VALUES ('2026-07-26 06:10:00', ?, ?)",
            (kind, amount),
        )
    db.commit()

    k = build_payload(db, portfolio)[0]["kpis"]
    assert k["pnl_rebate_usd"] == pytest.approx(2.0)   # 0.75 + 1.25, deposit out
    assert k["pnl_gross_usd"] == pytest.approx(5.0)
    assert k["pnl_fee_usd"] == pytest.approx(2.0)
    assert k["pnl_net_usd"] == pytest.approx(3.0)
    assert k["pnl_gross_usd"] - k["pnl_fee_usd"] == pytest.approx(k["pnl_net_usd"])


def test_pnl_rebate_uses_the_same_wallet_start_window_as_the_bets(
    db: sqlite3.Connection, portfolio, monkeypatch: pytest.MonkeyPatch
):
    """Era gate: a rebate dated BEFORE ``WALLET_START_AT`` is already inside
    INITIAL_BANKROLL, so it is excluded — the SAME cutover ``_ledger_where``
    applies to the bets, which is what makes the four columns comparable."""
    monkeypatch.setattr(config, "WALLET_START_AT", "2026-07-21T22:11:00Z")
    for at, amount in (("2026-07-20 09:00:00", 4.0), ("2026-07-26 06:10:00", 1.5)):
        db.execute(
            "INSERT INTO capital_flows (occurred_at, kind, amount_usd) "
            "VALUES (?, 'rebate', ?)",
            (at, amount),
        )
    db.commit()
    k = build_payload(db, portfolio)[0]["kpis"]
    assert k["pnl_rebate_usd"] == pytest.approx(1.5)   # pre-cutover 4.0 dropped


def test_pnl_decomposition_degrades_to_all_none_on_legacy_schema(
    db: sqlite3.Connection, portfolio
):
    """Legacy DB the decomposition SQL can't read → EVERY field of the trio +
    the rebate is None, never a half-filled strip (the Overview gates the whole
    block on gross/fee being non-null)."""
    from cryptobot.observability.snapshot_builder import _ledger_pnl_decomposition

    db.execute("ALTER TABLE ledger RENAME COLUMN live_filled_size TO lfs_legacy")
    assert _ledger_pnl_decomposition(db, portfolio) == {
        "pnl_gross_usd": None,
        "pnl_fee_usd": None,
        "pnl_net_usd": None,
        "pnl_rebate_usd": None,
    }


def test_ledger_daily_rollup_sums_to_pnl_net_usd(db: sqlite3.Connection, portfolio):
    """The server all-time per-day rollup (payload['ledger_daily']) is the SAME
    population as kpis.pnl_net_usd — same settled-row filter, same SUM(pnl_net) —
    so Σ(day.pnl) reconstructs the Overview P&L breakdown net to the cent BY
    CONSTRUCTION (the alignment guarantee: Calendar headline === Overview net).
    Also pins:
      * the MYT (UTC+8) day-key contract with a row crossing the UTC→MYT
        boundary;
      * H1 (2026-07-21): a stop-loss row keys on its IMMUTABLE exit_ts — never
        the sweep-backfilled resolution_ts, never bet_ts when exit_ts exists —
        and the bucket is invariant to a later resolution_ts backfill;
      * M4/item 17: a voided CANCELLED row (pnl_net=0, no exit_kind) is
        excluded from the day's n (its 0 pnl stays in the sum so the Σ
        identity holds);
      * that a booked stop exit is counted as a LOSS on its exit day (2026-08-05
        — the separate per-day `stops` column is gone).

    Every bet gets its OWN window_start_ts: they are four DIFFERENT market
    windows, so each is its own trade group (see the trade-group contract) —
    four rows sharing a window would be one CLOB position and would legitimately
    collapse into a single day/trade."""
    # WIN whose resolution is 2026-06-01 16:30 UTC = 2026-06-02 00:30 MYT: it
    # MUST key on the MYT day (06-02), never the UTC day (06-01).
    win_id = record_bet(db, make_signal(bet_size=4.0, sleeve="v6_5m",
                                        window_start_ts=1_715_923_200))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=2.0, fill_price=0.5, "
        "resolution_ts='2026-06-01 16:30:00' WHERE id=?",
        (win_id,),
    )
    # LOSS resolving 2026-06-01 20:00 UTC = 2026-06-02 04:00 MYT → same MYT day.
    loss_id = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m",
                                         window_start_ts=1_715_923_500))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-1.0, "
        "resolution_ts='2026-06-01 20:00:00' WHERE id=?",
        (loss_id,),
    )
    # CANCELLED stop-loss (PURE_STOP_04): booked realized loss at exit_ts
    # 2026-06-03T20:00:00Z (ISO spelling, = 2026-06-04 04:00 MYT → 06-04) while
    # bet_ts is 06-03 and resolution_ts NULL — the day MUST key on exit_ts.
    stop_id = record_bet(db, make_signal(bet_size=3.0, sleeve="v6_5m",
                                         window_start_ts=1_715_923_800))
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=-0.75, "
        "exit_kind='stop_loss', exit_price=0.40, resolution_ts=NULL, "
        "exit_ts='2026-06-03T20:00:00Z', bet_ts='2026-06-03 05:00:00' "
        "WHERE id=?",
        (stop_id,),
    )
    # Voided CANCELLED (no exit_kind, pnl_net=0 — e.g. a refunded cancel) on
    # the 06-02 MYT day: excluded from n, contributes 0 to pnl.
    void_id = record_bet(db, make_signal(bet_size=2.0, sleeve="v6_5m",
                                         window_start_ts=1_715_924_100))
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=0.0, exit_kind=NULL, "
        "resolution_ts='2026-06-01 17:00:00' WHERE id=?",
        (void_id,),
    )
    db.commit()

    payload = build_payload(db, portfolio)[0]
    daily = payload["ledger_daily"]
    k = payload["kpis"]

    # Alignment invariant: Σ(day.pnl) === kpis.pnl_net_usd (2.0 − 1.0 − 0.75).
    assert k["pnl_net_usd"] == pytest.approx(0.25)
    assert round(sum(d["pnl"] for d in daily), 2) == k["pnl_net_usd"]

    by_day = {d["date"]: d for d in daily}
    # MYT boundary: the 06-01-UTC WIN lands on the 06-02 MYT day, and no bucket
    # is keyed on the raw UTC date. The stop keys on exit_ts → 06-04 (NOT the
    # 06-03 bet_ts day).
    assert set(by_day) == {"2026-06-02", "2026-06-04"}
    assert by_day["2026-06-02"]["pnl"] == pytest.approx(1.0)  # +2.0 win − 1.0 loss
    assert by_day["2026-06-02"]["n"] == 2                     # voided excluded
    assert by_day["2026-06-02"]["wins"] == 1
    assert by_day["2026-06-02"]["losses"] == 1
    # No standalone SL column on any day any more (2026-08-05).
    assert "stops" not in by_day["2026-06-02"]
    # Stop row: counted on its exit_ts day, AS A LOSS (folded — no `stops` key).
    assert by_day["2026-06-04"]["pnl"] == pytest.approx(-0.75)
    assert by_day["2026-06-04"]["n"] == 1
    assert by_day["2026-06-04"]["wins"] == 0
    assert by_day["2026-06-04"]["losses"] == 1
    assert "stops" not in by_day["2026-06-04"]
    emitted_stop = next(row for row in payload["ledger"] if row["id"] == stop_id)
    assert emitted_stop["exit_ts"] == "2026-06-03T20:00:00Z"

    # H1 invariance: a later display-only sweep backfill stamping a (late)
    # resolution_ts on the stop row must NOT move its day bucket.
    db.execute(
        "UPDATE ledger SET resolution_ts='2026-06-09 00:00:00' WHERE id=?",
        (stop_id,),
    )
    db.commit()
    daily2 = build_payload(db, portfolio)[0]["ledger_daily"]
    by_day2 = {d["date"]: d for d in daily2}
    assert set(by_day2) == {"2026-06-02", "2026-06-04"}
    assert by_day2["2026-06-04"]["losses"] == 1

    # The emitted contract documents the exact rules the client mirrors.
    contracts = payload["contracts"]
    assert "exit_ts" in contracts["day_bucket"]
    assert "MYT(+8h)" in contracts["day_bucket"]
    assert "wins/(wins+losses)" in contracts["win_rate"]
    assert "TRADE GROUPS" in contracts["win_rate"]
    assert "window_start_ts" in contracts["trade_group"]


def test_cap_and_targets_scale_with_realized_capital(
    db: sqlite3.Connection, portfolio
):
    record_bet(db, make_signal(bet_size=5.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=32.20, "
        "resolution_ts=datetime('now') WHERE id = 1"
    )
    payload, _ = build_payload(db, portfolio)
    assert payload["kpis"]["realized_capital"] == 432.20
    assert payload["kpis"]["by_sleeve"]["v6_5m"]["realized_capital"] == 132.20
    assert payload["kpis"]["by_sleeve"]["v6_5m"]["target_bet_size"] == 3.3
    assert payload["kpis"]["by_sleeve"]["v6_15m"]["target_bet_size"] == 1.95


def test_account_capital_reconciles_to_clob_cash_basis(db: sqlite3.Connection):
    portfolio = load_sleeves()
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 03:59:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:33:56', '0xfunder', 'ok', 114.192431, "
        "        114.192431, 0, 0, '[]')"
    )
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.8471077519288, resolution_ts='2026-06-21 04:15:00' "
        "WHERE id=?",
        (rid,),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)
    kpis = payload["kpis"]

    assert kpis["initial_bankroll"] == 100.0
    assert kpis["realized_capital"] == 110.85
    assert kpis["account_capital"] == 114.19
    assert kpis["account_initial_capital"] == 103.55
    assert kpis["account_pnl_total"] == 10.64
    assert kpis["account_strategy_pnl_total"] == 10.85
    assert kpis["account_pnl_ledger_delta"] == -0.2
    assert kpis["account_capital_ts"] == "2026-06-21 04:33:56"
    assert kpis["account_initial_capital_source"] == "clob_before_first_live_bet"
    assert kpis["account_initial_capital_ts"] == "2026-06-21 03:59:00"
    assert kpis["account_capital_comparable"] is True
    assert payload["ledger_equity"]["final"] == 110.85


def test_account_capital_uses_actual_latest_reconciliation_run(
    db: sqlite3.Connection,
):
    portfolio = load_sleeves()
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 03:59:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:33:56', '0xfunder', 'ok', 114.19, "
        "        114.19, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:34:56', '0xfunder', 'clob_unreachable', NULL, "
        "        114.19, 0, 0, '[\"clob balance unavailable\"]')"
    )
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.85, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
        (rid,),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    assert payload["kpis"]["account_capital"] is None
    assert payload["kpis"]["account_capital_comparable"] is False
    assert payload["live_wallet"]["source_status"] == "clob_unreachable"


def test_account_capital_sticky_last_good_when_latest_unusable(
    db: sqlite3.Connection,
):
    """2026-07-09 sticky last-good cash basis: when the latest reconciliation
    row is unusable (null CLOB balance), the display capital falls back to the
    most recent 'ok' balance sample — even one taken MID-POSITION — labelled
    stale_clob_balance and NOT comparable. The paper strategy-equity fallback
    (the old "$120" flip) now applies only when no 'ok' sample exists within
    the staleness cap (see test_account_capital_uses_actual_latest_
    reconciliation_run, whose samples are weeks old and stay outside it)."""
    portfolio = load_sleeves()
    # Fresh 'ok' sample taken while a position was open (open_positions=1).
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES (datetime('now', '-10 minutes'), '0xfunder', 'ok', 114.19, "
        "        114.19, 0, 1, '[]')"
    )
    # Latest row: CLOB unreachable, balance NULL.
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES (datetime('now'), '0xfunder', 'clob_unreachable', NULL, "
        "        114.19, 0, 1, '[\"clob balance unavailable\"]')"
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)
    kpis = payload["kpis"]

    assert kpis["account_capital"] == 114.19
    assert kpis["account_capital_source"] == "stale_clob_balance"
    assert kpis["account_capital_ts"] is not None
    # Not strictly comparable — strict consumers keep their semantics; the UI
    # shows the as-of time instead.
    assert kpis["account_capital_comparable"] is False


def test_account_capital_survives_single_partial_balance_miss(
    db: sqlite3.Connection,
):
    portfolio = load_sleeves()
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 03:59:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:12:26', '0xfunder', 'ok', 114.192431, "
        "        114.192431, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:22:26', '0xfunder', 'partial', NULL, "
        "        114.192431, 0, 0, '[]')"
    )
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.8471077519288, resolution_ts='2026-06-21 04:15:00' "
        "WHERE id=?",
        (rid,),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)
    kpis = payload["kpis"]

    assert kpis["realized_capital"] == 110.85
    assert kpis["account_capital"] == 114.19
    assert kpis["account_capital_source"] == "recent_clean_clob_balance"
    assert kpis["account_capital_ts"] == "2026-06-21 04:12:26"
    assert kpis["account_initial_capital"] == 103.55
    assert kpis["account_pnl_total"] == 10.64
    assert kpis["account_capital_comparable"] is True


def test_account_capital_without_pre_live_basis_is_estimated_not_comparable(
    db: sqlite3.Connection,
):
    portfolio = load_sleeves()
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.85, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
        (rid,),
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:33:56', '0xfunder', 'ok', 114.19, "
        "        114.19, 0, 0, '[]')"
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)
    kpis = payload["kpis"]

    assert kpis["account_capital"] == 114.19
    assert kpis["account_initial_capital"] == 103.34
    assert kpis["account_pnl_total"] == 10.85
    assert kpis["account_initial_capital_source"] == "clob_minus_live_pnl"
    assert kpis["account_capital_comparable"] is False


def test_account_strategy_pnl_is_live_only(db: sqlite3.Connection):
    portfolio = load_sleeves()
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 03:59:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:33:56', '0xfunder', 'ok', 114.19, "
        "        114.19, 0, 0, '[]')"
    )
    live_id = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    dry_id = record_bet(
        db,
        make_signal(
            bet_size=4.0,
            sleeve="subminute_5m",
            window_start_ts=1_719_000_000,
        ),
        mode="DRY",
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.85, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
        (live_id,),
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:01:00', outcome='WIN', "
        "pnl_net=90.0, resolution_ts='2026-06-21 04:16:00' WHERE id=?",
        (dry_id,),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    assert payload["kpis"]["account_strategy_pnl_total"] == 10.85


def test_account_capital_not_comparable_with_open_clob_orders(
    db: sqlite3.Connection,
):
    portfolio = load_sleeves()
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 03:59:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:33:56', '0xfunder', 'ok', 114.19, "
        "        114.19, 1, 0, '[]')"
    )
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.85, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
        (rid,),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    assert payload["kpis"]["account_capital"] == 114.19
    assert payload["kpis"]["account_capital_comparable"] is False


@pytest.mark.parametrize(
    ("source_status", "open_orders", "open_positions", "settle_live"),
    [
        ("clob_unreachable", 0, 0, True),
        ("ok", 0, 1, True),
        ("ok", 0, 0, False),
    ],
)
def test_account_capital_non_comparable_live_states(
    db: sqlite3.Connection,
    source_status: str,
    open_orders: int,
    open_positions: int,
    settle_live: bool,
):
    portfolio = load_sleeves()
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 03:59:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 0, '[]')"
    )
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-21 04:33:56', '0xfunder', ?, 114.19, "
        "        114.19, ?, ?, '[]')",
        (source_status, open_orders, open_positions),
    )
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    if settle_live:
        db.execute(
            "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
            "pnl_net=10.85, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
            (rid,),
        )
    else:
        db.execute(
            "UPDATE ledger SET bet_ts='2026-06-21 04:00:00' WHERE id=?",
            (rid,),
        )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    assert payload["kpis"]["account_capital"] == 114.19
    assert payload["kpis"]["account_capital_comparable"] is False


def test_portfolio_block_includes_sleeves(db: sqlite3.Connection, portfolio):
    payload, _ = build_payload(db, portfolio)
    pf = payload["portfolio"]
    assert pf["bet_size_multiplier"] == config.BET_SIZE_MULTIPLIER
    assert pf["max_walk_price"] == config.MAX_WALK_PRICE
    sleeve_labels = [s["label"] for s in pf["sleeves"]]
    assert sleeve_labels == ["v6_5m", "v6_15m", "pairs_5m", "pairs_15m"]
    assert pf["max_drawdown_halt_frac"] == pytest.approx(0.5)
    assert pf["wallet_exposure_cap_fracs"]["pairs_15m"] == pytest.approx(
        0.18118910790805948
    )


def test_retired_payload_blocks_are_gone(db: sqlite3.Connection, portfolio):
    """The gates/gate_order stack, the legacy sub-minute console block, the
    wallet_groups cap panel, and the per-sleeve markets decision subtree all
    fed the retired Live markets & gates / Performance surfaces (retired
    2026-07-11) — dropped from the payload 2026-07-14. Only the coin universe
    survives under markets (the Journal coin filter reads markets[].coin)."""
    payload, _ = build_payload(db, portfolio)
    for retired in ("gates", "gate_order", "subminute", "wallet_groups"):
        assert retired not in payload, f"retired payload block {retired!r} re-emitted"
    assert payload["markets"] == [{"coin": c} for c in config.COINS]


def test_capital_flows_block_and_net_deposits(
    db: sqlite3.Connection, portfolio
):
    """The flows panel reads `capital_flows`; the equity/DD tile keeps its
    existing fields (already flow-inclusive) plus a `net_deposits` caption, so
    no client math changes."""
    empty, _ = build_payload(db, portfolio)
    assert empty["capital_flows"] == []
    assert empty["kpis"]["net_deposits"] == 0.0

    db.executemany(
        "INSERT INTO capital_flows (occurred_at, kind, amount_usd, note) "
        "VALUES (?, ?, ?, ?)",
        [
            ("2026-07-26 06:10:00", "deposit", 91.65, "wire"),
            ("2026-07-27 08:50:00", "withdrawal", -20.0, None),
        ],
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    assert payload["capital_flows"] == [
        {
            "occurred_at": "2026-07-26 06:10:00", "kind": "deposit",
            "amount_usd": 91.65, "note": "wire", "source": "manual",
        },
        {
            "occurred_at": "2026-07-27 08:50:00", "kind": "withdrawal",
            "amount_usd": -20.0, "note": None, "source": "manual",
        },
    ]
    kpis = payload["kpis"]
    # 4 isolated legacy wallets, each replaying the account-level flows.
    assert kpis["net_deposits"] == pytest.approx(4 * 71.65)
    assert kpis["realized_capital"] == pytest.approx(400.0 + 4 * 71.65)
    # The withdrawal re-anchors each wallet at the post-withdrawal balance, so
    # it is excluded from the halt drawdown and the aggregate is flat.
    assert kpis["peak_capital"] == pytest.approx(400.0 + 4 * 71.65)
    assert kpis["drawdown_pct_peak"] == 0.0
    # D14 stays pure P&L: no settled rows → zero, deposits notwithstanding.
    assert kpis["drawdown_pct"] == 0.0


def test_operator_payload_includes_wallet_history_and_readiness(
    db: sqlite3.Connection, portfolio
):
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, warnings_json) "
        "VALUES ('2026-06-12 12:00:00', '0xfunder', 'ok', 103.55, "
        "        103.55, 0, 2, '[]')"
    )
    db.execute(
        "INSERT INTO live_readiness_reports "
        "(created_at, status, funder, clob_balance_usd, chain_balance_usd, report_json) "
        "VALUES ('2026-06-12 12:00:01', 'READY', '0xfunder', 103.55, "
        "        103.55, ?)",
        (json.dumps({
            "readiness_check_results": [
                {"name": "clob_credentials", "passed": True, "detail": "ok"}
            ],
            "wallet_balance_freshness_s": 7,
        }),),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    assert payload["live_wallet"]["balance_usd"] == 103.55
    assert payload["live_wallet"]["chain_balance_usd"] == 103.55
    assert payload["live_wallet"]["open_positions_count"] == 2
    assert payload["operator"]["readiness"]["checks"][0]["name"] == "clob_credentials"


def test_ledger_bootstrap_limit_constant():
    """The initial-snapshot bootstrap covers the full v2-only era (~500 rows),
    raised from the original 50-row session slice on 2026-07-12 so the Journal
    'All' filter + every client ledger analytic hydrate the whole deployed
    history on a fresh load. Pin the constant so a silent revert is caught."""
    assert config.LEDGER_BOOTSTRAP_LIMIT == 500


def test_ledger_initial_bootstrap_capped_at_500(db: sqlite3.Connection, portfolio):
    n = config.LEDGER_BOOTSTRAP_LIMIT + 10
    for i in range(n):
        record_bet(db, make_signal(bet_size=1.0, window_start_ts=1_715_923_200 + i * 60))
    payload, max_id = build_payload(db, portfolio)
    # Bootstrap streams at most LEDGER_BOOTSTRAP_LIMIT (500) newest rows...
    assert len(payload["ledger"]) == config.LEDGER_BOOTSTRAP_LIMIT
    # ...while max_id still reflects the true tail so deltas resume correctly.
    assert max_id == n


def test_ledger_delta_only_new_rows(db: sqlite3.Connection, portfolio):
    for i in range(3):
        record_bet(db, make_signal(bet_size=1.0, window_start_ts=1_715_923_200 + i * 60))
    payload1, max_id1 = build_payload(db, portfolio)
    assert max_id1 == 3

    # Add two more rows.
    record_bet(db, make_signal(bet_size=1.0, window_start_ts=1_715_923_500))
    record_bet(db, make_signal(bet_size=1.0, window_start_ts=1_715_923_800))

    payload2, max_id2 = build_payload(db, portfolio, last_seen_ledger_id=max_id1)
    assert max_id2 == 5
    assert {row["id"] for row in payload2["ledger"]} == {4, 5}


def test_ledger_delta_refreshes_settled_existing_rows(
    db: sqlite3.Connection, portfolio
):
    row_id = record_bet(db, make_signal(bet_size=1.0))
    _, max_id1 = build_payload(db, portfolio)
    assert max_id1 == row_id

    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=0.62, "
        "resolution_ts=datetime('now') WHERE id=?",
        (row_id,),
    )
    db.commit()

    payload2, max_id2 = build_payload(db, portfolio, last_seen_ledger_id=max_id1)
    assert max_id2 == max_id1
    assert [row["id"] for row in payload2["ledger"]] == [row_id]
    assert payload2["ledger"][0]["outcome"] == "WIN"
    assert payload2["ledger"][0]["pnl_net"] == 0.62


def test_ledger_respects_wallet_start_cutover(
    db: sqlite3.Connection,
    portfolio,
    monkeypatch: pytest.MonkeyPatch,
):
    old_id = record_bet(db, make_signal(bet_size=1.0, window_start_ts=1_715_923_200))
    new_id = record_bet(db, make_signal(bet_size=1.0, window_start_ts=1_715_923_500))
    db.execute("UPDATE ledger SET bet_ts='2026-05-24 00:40:00' WHERE id=?", (old_id,))
    db.execute("UPDATE ledger SET bet_ts='2026-05-24 00:50:00' WHERE id=?", (new_id,))
    db.commit()
    monkeypatch.setattr(config, "WALLET_START_AT", "2026-05-24 00:45:57")

    payload, max_id = build_payload(db, portfolio)

    assert [row["id"] for row in payload["ledger"]] == [new_id]
    assert max_id == new_id


def test_ledger_rollup_is_lifetime_not_bootstrap_window(
    db: sqlite3.Connection, portfolio
):
    """Regression: the dashboard's headline win-rate read a ~50-row session
    slice, so it diverged wildly from the lifetime mean. ledger_rollup must
    aggregate the FULL active-strategy ledger regardless of the bootstrap cap."""
    for i in range(60):
        rid = record_bet(
            db,
            make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_923_200 + i * 60),
        )
        outcome = "WIN" if i % 2 == 0 else "LOSS"
        db.execute(
            "UPDATE ledger SET outcome=?, pnl_net=?, "
            "resolution_ts=datetime('now') WHERE id=?",
            (outcome, 1.0 if outcome == "WIN" else -1.0, rid),
        )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    # 60 rows all fit under the 500-row bootstrap, so they all stream...
    assert len(payload["ledger"]) == 60
    # ...and the rollup independently counts every decided bet in the full
    # ledger (its lifetime property is what this regression guards; the
    # streamed-vs-rollup population split is exercised at scale by
    # test_ledger_initial_bootstrap_capped_at_500).
    roll = payload["ledger_rollup"]
    assert roll["total"] == 60
    assert roll["decided"] == 60
    assert roll["wins"] == 30
    assert roll["losses"] == 30
    assert roll["win_rate"] == pytest.approx(0.5)


def test_ledger_block_and_rollup_exclude_off_portfolio_sleeves(
    db: sqlite3.Connection, portfolio
):
    """A retired strategy's rows (sleeve not in the loaded portfolio — e.g. the
    old v6_5m LUT sleeve after a swap) must never surface on the dashboard, in
    either the streamed ledger or the lifetime rollup. The active-sleeve filter
    is driven by ``portfolio.market_sleeves`` so it follows future swaps."""
    active_id = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))
    off_id = record_bet(db, make_signal(bet_size=1.0, sleeve="retired_lut"))
    for rid in (active_id, off_id):
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=1.0, "
            "resolution_ts=datetime('now') WHERE id=?",
            (rid,),
        )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    streamed_ids = {r["id"] for r in payload["ledger"]}
    assert active_id in streamed_ids
    assert off_id not in streamed_ids, "off-portfolio sleeve leaked into ledger"
    roll = payload["ledger_rollup"]
    assert roll["total"] == 1, "off-portfolio sleeve leaked into rollup"
    assert roll["wins"] == 1


def test_ledger_equity_series_is_record_era_and_anchored(
    db: sqlite3.Connection, portfolio
):
    """The equity curve is scoped to the RECORD ERA (operator 2026-07-24): it
    shows the deployed-strategy history since the reset anchor (not the ~50-row
    session window), and its right edge lands exactly on realized_capital. Here
    all rows are post-anchor, so the whole 60-row curve appears and stays
    anchored to capital."""
    for i in range(60):
        rid = record_bet(
            db,
            make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_923_200 + i * 60),
        )
        win = i < 35  # 35 WIN / 25 LOSS -> net +10
        db.execute(
            "UPDATE ledger SET outcome=?, pnl_net=?, "
            "resolution_ts=? WHERE id=?",
            ("WIN" if win else "LOSS", 1.0 if win else -1.0,
             f"2026-07-20 00:{i:02d}:00", rid),  # post the reset anchor
        )
    db.commit()

    payload, _ = build_payload(db, portfolio)
    eq = payload["ledger_equity"]
    realized = payload["kpis"]["realized_capital"]
    assert eq["n"] == 60                                   # era rows, not the 50 window
    assert eq["final"] == pytest.approx(realized, abs=0.01)  # anchored to capital
    assert eq["base"] == pytest.approx(400.0, abs=0.01)      # ~initial bankroll
    assert eq["points"][0]["eq"] == pytest.approx(eq["base"])
    assert eq["points"][-1]["eq"] == pytest.approx(eq["final"], abs=0.01)
    assert len(eq["points"]) <= config.LEDGER_EQUITY_MAX_POINTS


def test_ledger_equity_series_record_era_excludes_pre_anchor(
    db: sqlite3.Connection, portfolio
):
    """``record_anchor`` scopes the curve to the era: equity-change rows before
    the reset are dropped and only post-anchor rows shape the curve, while
    ``record_anchor=None`` keeps the full lifetime curve (operator 2026-07-24)."""
    from cryptobot.observability.snapshot_builder import _ledger_equity_series
    ts_by_row = [
        "2026-05-01 00:00:00", "2026-05-02 00:00:00", "2026-05-03 00:00:00",
        "2026-07-20 00:00:00", "2026-07-20 00:01:00",
        "2026-07-20 00:02:00", "2026-07-20 00:03:00",
    ]
    for i, ts in enumerate(ts_by_row):
        rid = record_bet(
            db,
            make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_923_200 + i * 60),
        )
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=1.0, resolution_ts=? "
            "WHERE id=?",
            (ts, rid),
        )
    db.commit()

    era_anchor = "2026-07-08 13:45:00"  # the v2 go-live floor
    scoped = _ledger_equity_series(
        db, portfolio, anchor=100.0, record_anchor=era_anchor
    )
    full = _ledger_equity_series(db, portfolio, anchor=100.0)
    assert full["n"] == 7                       # None => lifetime, every settled row
    assert scoped["n"] == 4                      # era => only the 4 post-anchor rows
    assert scoped["final"] == pytest.approx(100.0)   # right edge still == anchor
    assert scoped["base"] == pytest.approx(96.0)     # anchor - era_total(=+4)
    assert scoped["points"][0]["ts"] == era_anchor  # starts at reset, not first trade


def test_cash_equity_series_since_reset(db: sqlite3.Connection):
    """The cash-equity curve plots the CLOB balance since the reset (hourly avg),
    so net = final - base is the cash-basis era PnL (green when cash is up), and
    the equity tile agrees with it. Pre-anchor rows are excluded (operator
    2026-07-24)."""
    from cryptobot.observability.snapshot_builder import _cash_equity_series
    for ts, bal in [
        ("2026-07-08 12:00:00", 999.0),   # pre-anchor -> excluded
        ("2026-07-21 23:00:00", 100.0),
        ("2026-07-22 00:00:00", 103.0),
        ("2026-07-22 01:00:00", 107.0),
    ]:
        db.execute(
            "INSERT INTO live_readiness_reports "
            "(created_at, status, clob_balance_usd, report_json) "
            "VALUES (?, 'READY', ?, '{}')",
            (ts, bal),
        )
    db.commit()
    s = _cash_equity_series(db, record_anchor="2026-07-21 22:11:01")
    assert s is not None
    assert s["n"] == 3                              # 3 post-anchor hourly buckets
    assert s["base"] == pytest.approx(100.0)
    assert s["final"] == pytest.approx(107.0)
    assert s["net"] == pytest.approx(7.0)           # cash-basis era PnL (green)
    assert s["points"][0]["eq"] == pytest.approx(100.0)
    assert s["points"][-1]["eq"] == pytest.approx(107.0)
    # No cash rows in the era => None (chart falls back to the ledger curve).
    assert _cash_equity_series(db, record_anchor="2027-01-01 00:00:00") is None


def test_ledger_equity_series_downsamples_preserving_endpoints(
    db: sqlite3.Connection, portfolio
):
    """Long histories are downsampled to bound the SSE payload, but the FIRST
    and LAST points are always kept so the right edge stays on realized_capital."""
    from cryptobot.observability.snapshot_builder import _ledger_equity_series
    for i in range(12):
        rid = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m",
                                         window_start_ts=1_715_923_200 + i * 60))
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=1.0, resolution_ts=? WHERE id=?",
            (f"2026-05-24 00:{i:02d}:00", rid),
        )
    db.commit()

    series = _ledger_equity_series(db, portfolio, anchor=200.0, max_points=5)
    assert series["n"] == 12
    assert len(series["points"]) <= 5
    assert series["points"][0]["i"] == 0
    assert series["points"][-1]["i"] == 12               # last bet preserved
    assert series["base"] == pytest.approx(188.0)         # anchor - total(=12)
    assert series["points"][0]["eq"] == pytest.approx(188.0)
    assert series["final"] == pytest.approx(200.0)        # right edge == anchor
    assert series["points"][-1]["eq"] == pytest.approx(200.0)


def test_ledger_equity_series_one_point_per_trade_group(
    db: sqlite3.Connection, portfolio
):
    """The equity curve plots ONE POINT PER TRADE GROUP (operator 2026-08-09):
    a base bet and its top-up in the same market window fold into a single
    point (so the curve's trade numbers match the hero "trades" tile), and a
    voided never-filled cancel — even one carrying a non-NULL 0.0 pnl — never
    plots: only trades that actually opened shape the curve."""
    from cryptobot.observability.snapshot_builder import _ledger_equity_series
    # Base fill + top-up: SAME (coin, interval, window, side, sleeve) → 1 group.
    for pnl in (2.0, 1.0):
        rid = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m",
                                         window_start_ts=1_715_923_200))
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=?, "
            "resolution_ts='2026-05-24 00:00:00' WHERE id=?",
            (pnl, rid),
        )
    # A second, later window → its own point.
    rid2 = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m",
                                      window_start_ts=1_715_923_500))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-1.0, "
        "resolution_ts='2026-05-24 00:05:00' WHERE id=?",
        (rid2,),
    )
    # Voided never-filled cancel with a settled 0.0 pnl (pnl_net NOT NULL, so
    # only the GROUP-level voided exclusion can drop it).
    rid3 = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m",
                                      window_start_ts=1_715_923_800))
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', exit_kind=NULL, pnl_net=0.0, "
        "resolution_ts='2026-05-24 00:10:00' WHERE id=?",
        (rid3,),
    )
    db.commit()

    series = _ledger_equity_series(db, portfolio, anchor=102.0)
    assert series["n"] == 2                       # 2 opened trades, not 3 rows +1
    # points: base anchor + one per trade; the folded group lands as +3 at once.
    assert [p["i"] for p in series["points"]] == [0, 1, 2]
    assert series["base"] == pytest.approx(100.0)  # anchor − total(+2)
    assert series["points"][1]["eq"] == pytest.approx(103.0)   # base + (2+1)
    assert series["points"][2]["eq"] == pytest.approx(102.0)   # − 1.0 loss
    assert series["final"] == pytest.approx(102.0)  # right edge == anchor


def test_every_trade_count_means_settled_opened_groups(
    db: sqlite3.Connection, portfolio
):
    """ONE trade population everywhere (operator 2026-08-09).

    Every count the console labels "trades"/"bets" must mean: settled TRADE
    GROUPS that actually OPENED (>=1 real fill). A voided never-filled cancel
    is NOT a trade — it is the separate `voided` stat — and an open position is
    not a settled one.

    The pre-fix defect this pins: `settled_n += 1` fired before the `is_voided`
    branch, so the Overview hero read `opened + voided` while
    _ledger_equity_series plotted `opened` — the two numbers the 48f1ae5 commit
    was meant to reconcile disagreed by exactly the voided count.

    Fixture (7 ledger rows → 4 trade groups):
      · BTC win, base + top-up in ONE window  → 1 opened settled trade (+3.0)
      · ETH loss                              → 1 opened settled trade (−1.0)
      · SOL voided never-filled cancel (pnl_net = 0.0, NOT NULL, so only the
        GROUP-level voided rule can drop it)  → 0 trades, 1 voided
      · XRP pending, base + pending top-up    → 0 settled, 1 OPEN trade
    """
    ws = 1_715_923_200
    for pnl, pre, minute in ((2.0, 0.0, 3), (1.0, 2.0, 4)):
        rid = record_bet(db, make_signal(bet_size=2.0, coin="BTC",
                                         window_start_ts=ws,
                                         slot_filled_pre=pre,
                                         entry_minute=minute))
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=?, "
            "resolution_ts=datetime('now') WHERE id=?", (pnl, rid),
        )
    loss = record_bet(db, make_signal(bet_size=2.0, coin="ETH",
                                      window_start_ts=ws + 300))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-1.0, "
        "resolution_ts=datetime('now') WHERE id=?", (loss,),
    )
    void = record_bet(db, make_signal(bet_size=2.0, coin="SOL",
                                      window_start_ts=ws + 600))
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', exit_kind=NULL, pnl_net=0.0, "
        "resolution_ts=datetime('now') WHERE id=?", (void,),
    )
    for pre in (0.0, 2.0):
        record_bet(db, make_signal(bet_size=2.0, coin="XRP",
                                   window_start_ts=ws + 900,
                                   slot_filled_pre=pre))
    db.commit()

    payload = build_payload(db, portfolio)[0]
    assert len(payload["ledger"]) == 6      # rows are still per-FILL

    # ── lifetime rollup: explicit settled_opened, never total − open ──────
    roll = payload["ledger_rollup"]
    assert roll["total"] == 4               # audit count: opened + open + void
    assert roll["open"] == 1
    assert roll["voided"] == 1
    assert roll["settled_opened"] == 2
    # The subtraction the Journal used to do would have said 3.
    assert roll["total"] - roll["open"] == 3

    # ── analytics summary: three DISJOINT buckets ────────────────────────
    all_ = payload["ledger_analytics"]["All"]
    s = all_["summary"]
    assert s["total"] == 4
    assert s["settled"] == 2
    assert s["open"] == 1
    assert s["voided"] == 1
    assert s["settled"] + s["open"] + s["voided"] == s["total"]
    # $ columns are untouched by the count fix (voided contributes its 0.0).
    assert s["pnl"] == pytest.approx(2.0)   # +2.0 +1.0 −1.0 +0.0

    # ── every derived panel counts the SAME population ───────────────────
    by_coin = {c["coin"]: c for c in all_["coins"]}
    assert "SOL" not in by_coin or (by_coin["SOL"]["bets"] == 0
                                    and by_coin["SOL"]["resolved"] == 0)
    assert by_coin["BTC"]["bets"] == 1 and by_coin["BTC"]["resolved"] == 1
    assert by_coin["XRP"]["bets"] == 1 and by_coin["XRP"]["resolved"] == 0
    assert sum(c["resolved"] for c in all_["coins"]) == s["settled"]
    assert sum(sl["n"] for sl in all_["slices"]) == s["settled"]
    assert sum(b["n"] for b in all_["histogram"]) == s["settled"]
    # Expectancy = mean over the 2 OPENED trades (+3.0, −1.0), NOT 3 with the
    # voided $0 dragging it to 0.667.
    assert s["expectancy"] == pytest.approx(1.0)

    # ── THE invariant: hero tile == equity-curve point count ─────────────
    assert payload["ledger_equity"]["n"] == s["settled"]

    # ── per-venue split inherits it (same code path, no second patch) ────
    v_roll = payload["ledger_rollup_by_venue"]["polymarket"]
    v_an = payload["ledger_analytics_by_venue"]["polymarket"]["All"]["summary"]
    assert v_roll["settled_opened"] == 2
    assert (v_an["settled"], v_an["open"], v_an["voided"]) == (2, 1, 1)

    # ── open exposure: per-FILL and per-TRADE counts, both published ─────
    k = payload["kpis"]
    assert k["n_pending"] == 2              # 2 pending ROWS (base + top-up)
    assert k["n_pending_trades"] == 1       # …but ONE open CLOB position


def test_ledger_exposes_execution_price_used_for_pnl(
    db: sqlite3.Connection, portfolio
):
    record_bet(db, make_signal(fill_price=0.38, walked_vwap=0.3890477))

    payload, _ = build_payload(db, portfolio)

    row = payload["ledger"][0]
    assert row["wallet_id"] == "v6_5m"
    assert row["fill_price"] == 0.38
    assert row["walked_vwap"] == 0.389
    assert row["effective_fill_price"] == 0.389
    assert row["direction"] == "UP"


def test_live_ledger_exposes_actual_fill_value(
    db: sqlite3.Connection, portfolio
):
    record_bet(
        db,
        make_signal(fill_price=0.38, walked_vwap=0.3890477, bet_size=5.0),
        mode="LIVE",
        live_filled_size=8.0,
        live_filled_price=0.42,
    )

    payload, _ = build_payload(db, portfolio)

    row = payload["ledger"][0]
    assert row["mode"] == "LIVE"
    assert row["fill_price"] == 0.38
    assert row["walked_vwap"] == 0.389
    assert row["live_filled_size"] == 8.0
    assert row["live_filled_price"] == 0.42
    assert row["live_filled_value_usd"] == 3.36
    assert row["intended_bet_size"] == 5.0
    assert row["effective_bet_size"] == 3.36
    assert row["effective_fill_price"] == 0.42


def test_pending_exposure_visible_in_kpis(db: sqlite3.Connection, portfolio):
    record_bet(db, make_signal(bet_size=4.0, sleeve="v6_5m"))
    record_bet(db, make_signal(bet_size=4.0, sleeve="v6_15m", entry_minute=7))
    payload, _ = build_payload(db, portfolio)
    assert payload["kpis"]["exposure_used"] == 8.0
    assert payload["kpis"]["n_pending"] == 2
    wallets = {w["id"]: w for w in payload["kpis"]["wallets"]}
    assert wallets["v6_5m"]["exposure_used"] == 4.0
    assert wallets["v6_15m"]["exposure_used"] == 4.0
    assert payload["kpis"]["by_sleeve"]["v6_5m"]["open_notional"] == 4.0
    assert payload["kpis"]["by_sleeve"]["v6_15m"]["open_notional"] == 4.0


def test_topup_count_excludes_initial_fills(db: sqlite3.Connection, portfolio):
    record_bet(db, make_signal(bet_size=2.0, slot_filled_pre=0.0))
    record_bet(db, make_signal(bet_size=1.5, slot_filled_pre=2.0))
    payload, _ = build_payload(db, portfolio)
    assert payload["kpis"]["n_topups_24h"] == 1


def test_payload_is_strict_json(db: sqlite3.Connection, portfolio):
    """The SSE payload must survive strict JSON encoding (browser JSON.parse
    rejects NaN/Infinity). _json_safe is the guard — exercise it directly on
    the shapes that used to smuggle NaN through, plus a real payload."""
    from cryptobot.observability.snapshot_builder import _json_safe

    assert _json_safe(float("nan")) is None
    assert _json_safe(float("inf")) is None
    assert _json_safe({"a": [1.0, float("nan"), {"b": float("-inf")}]}) == {
        "a": [1.0, None, {"b": None}]
    }

    payload, _ = build_payload(db, portfolio)
    json.dumps(payload, allow_nan=False)


# ---------------------------------------------------------------------------
# U8 of the all_4 deploy plan: 4-tile rendering markers + per-wallet payload
# ---------------------------------------------------------------------------


def test_topology_marker_wallet_groups_under_v2(db: sqlite3.Connection):
    """v2 portfolio (one shared-equity wallet_group) →
    topology='wallet_groups'."""
    portfolio = load_sleeves()
    payload, _ = build_payload(db, portfolio)
    assert payload["kpis"]["topology"] == "wallet_groups"


def test_topology_marker_wallet_groups_under_legacy(
    db: sqlite3.Connection, portfolio
):
    """Legacy 2-wallet_groups portfolio → topology='wallet_groups'."""
    payload, _ = build_payload(db, portfolio)
    assert payload["kpis"]["topology"] == "wallet_groups"


def test_per_wallet_bankroll_display_usd(db: sqlite3.Connection, portfolio):
    """U8: every wallet tile carries bankroll_display_usd=100.0 so the
    frontend can render the $100 base. Distinct from realized_capital
    (which compounds with PnL)."""
    payload, _ = build_payload(db, portfolio)
    wallets = payload["kpis"]["wallets"]
    assert len(wallets) == 4
    for w in wallets:
        assert w["bankroll_display_usd"] == 100.0


def test_bankroll_display_consumed_only_by_snapshot_payload():
    """U8 field-collision audit: `bankroll_display_usd` must be a
    presentation key only — no gate / sizing primitive reads it. A grep
    across the runtime should find it ONLY in the snapshot builder."""
    import subprocess
    repo_root = (
        __import__("pathlib").Path(__file__).resolve().parents[4]  # legacy-path-ok: repo root from polymarket/src/cryptobot/tests/
    )
    result = subprocess.run(
        [
            "git", "grep", "-l", "bankroll_display_usd",
            "polymarket/src/cryptobot/",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    callers = sorted(
        {line for line in result.stdout.splitlines() if line.strip()}
    )
    # Allow snapshot_builder (writer), this test (audit), and frontend
    # consumers under polymarket/dashboard/ if any. Forbid execution/, gates,
    # strategy/, scheduler/, db/ — those would mean a sizing primitive
    # leaked into the display field.
    forbidden_prefixes = (
        "polymarket/src/cryptobot/execution/",
        "polymarket/src/cryptobot/strategy/",
        "polymarket/src/cryptobot/scheduler/",
        "polymarket/src/cryptobot/db/",
    )
    leaked = [c for c in callers if c.startswith(forbidden_prefixes)]
    assert leaked == [], (
        f"bankroll_display_usd leaked into runtime sizing/gate code: "
        f"{leaked}. It must remain a presentation-only field consumed "
        f"by snapshot_builder + dashboard frontend."
    )


def test_v2_spec_emits_bands():
    """live.v2.spec presents the deployed ask band(s), derived from the loaded
    sleeves, and no longer emits the flat single-band ask_lo/ask_hi/gate."""
    from cryptobot.observability import snapshot_builder

    portfolio = load_sleeves()
    spec = snapshot_builder._v2_spec(portfolio)

    # Consumers read spec["bands"]: the legacy flat fields are removed.
    for stale in ("ask_lo", "ask_hi", "gate"):
        assert stale not in spec, f"stale flat spec field {stale!r} still emitted"

    # Shared facts.
    assert spec["poly_fee_theta"] == pytest.approx(config.POLY_FEE_THETA)
    assert spec["intervals"] == ["5m", "15m"]
    assert spec["signal_source"] == "hgb"
    assert spec["features"] == "price,identity,time,momentum (11-set)"
    assert spec["one_per_market"] is True
    assert "first-fill-wins" in spec["selection"]
    ow = spec["offset_windows"]
    assert ow["5m"].startswith("150") and ow["5m"].endswith("299s")
    assert ow["15m"].startswith("750") and ow["15m"].endswith("899s")

    # Bands are derived from the deployed sleeves: one shared band.
    by_label = {s.label: s for s in portfolio.sleeves}
    assert len(spec["bands"]) == 1
    mid = spec["bands"][0]
    assert mid["key"] == "0.55-0.85"
    assert mid["label"] == "0.55–0.85"
    assert mid["sleeves"] == ["subminute_5m", "subminute_15m"]
    assert (mid["ask_lo"], mid["ask_hi"]) == by_label["subminute_5m"].ask_bucket_ranges[0]
    assert mid["gate"] == pytest.approx(by_label["subminute_5m"].threshold)

    # Numbers from twap_symmetric_live.json.
    assert (mid["ask_lo"], mid["ask_hi"], mid["gate"]) == (0.55, 0.85, 0.10)


def test_v2_block_threads_portfolio_into_spec(db: sqlite3.Connection):
    """The top-level live.v2.spec the dashboard renders carries the bands list
    — regression for the _v2_block -> _v2_spec(portfolio) wiring."""
    portfolio = load_sleeves()
    payload, _ = build_payload(db, portfolio)
    spec = payload["v2"]["spec"]
    assert [b["key"] for b in spec["bands"]] == ["0.55-0.85"]
    # Flat single-band presentation removed from the emitted spec.
    assert "ask_lo" not in spec


def test_portfolio_block_no_longer_carries_v2_key(db: sqlite3.Connection):
    """The dead live.portfolio.v2 config block (Strategy page deleted
    2026-07-09) is gone — sizing+spec live under live.v2, not portfolio.v2."""
    portfolio = load_sleeves()
    payload, _ = build_payload(db, portfolio)
    assert "v2" not in payload["portfolio"]


# ---------------------------------------------------------------------------
# Operator record reset (WALLET_START_AT cutover) — every v2 accounting
# surface must re-anchor, not just the ledger views
# ---------------------------------------------------------------------------
def test_record_anchor_respects_wallet_start_cutover(
    monkeypatch: pytest.MonkeyPatch,
):
    from cryptobot.observability import snapshot_builder as sb

    monkeypatch.setattr(config, "WALLET_START_AT", "")
    assert sb._record_anchor_ts() == sb._V2_GO_LIVE_TS
    # A cutover BEFORE go-live never widens the window past go-live.
    monkeypatch.setattr(config, "WALLET_START_AT", "2026-07-01 00:00:00")
    assert sb._record_anchor_ts() == sb._V2_GO_LIVE_TS
    # A reset AFTER go-live re-anchors the record to the cutover.
    monkeypatch.setattr(config, "WALLET_START_AT", "2026-07-14 12:00:00")
    assert sb._record_anchor_ts() == "2026-07-14 12:00:00"


def test_v2_sizing_peak_respects_record_anchor(
    db: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
):
    """The Subminute tab's cash peak anchors to the SAME record window as its
    sibling surfaces (bets/rejects/fill tax): sampled_at >= _record_anchor_ts()
    = max(v2 go-live, WALLET_START_AT). A record reset therefore also resets
    the high-water mark, and pre-go-live cash samples never leak in."""
    from cryptobot.observability import snapshot_builder as sb

    portfolio = load_sleeves()
    for sampled_at, balance in (
        ("2026-07-01 00:00:00", 900.0),   # pre-go-live: excluded always
        ("2026-07-09 00:00:00", 500.0),   # post-go-live, pre-reset
        ("2026-07-11 00:00:00", 150.0),   # post-reset
    ):
        db.execute(
            "INSERT INTO bankroll_peak "
            "(sampled_at, wallet_balance, realized_pnl, pending_exposure) "
            "VALUES (?, ?, 0.0, 0.0)",
            (sampled_at, balance),
        )
    db.commit()
    kpis = {
        "account_capital_live": 120.0,
        "account_capital_live_source": "balance_cache",
        "drawdown_pct_peak": 0.0,
    }

    monkeypatch.setattr(config, "WALLET_START_AT", "")
    block = sb._v2_block(db, portfolio, kpis)
    assert block["sizing"]["peak_capital"] == pytest.approx(500.0)

    sb.clear_aux_cache()
    monkeypatch.setattr(config, "WALLET_START_AT", "2026-07-10 00:00:00")
    block = sb._v2_block(db, portfolio, kpis)
    assert block["sizing"]["peak_capital"] == pytest.approx(150.0)


def test_v2_console_respects_wallet_start_cutover(
    db: sqlite3.Connection,
    monkeypatch: pytest.MonkeyPatch,
):
    """The Subminute console's recent-bets list and the reject/fill-tax stats
    re-anchor to the record cutover: pre-reset rows never resurface."""
    from cryptobot.observability import snapshot_builder as sb

    portfolio = load_sleeves()
    cutover = "2026-07-10 00:00:00"
    monkeypatch.setattr(config, "WALLET_START_AT", cutover)

    old_bet = record_bet(
        db, make_signal(bet_size=1.0, sleeve="subminute_5m",
                        window_start_ts=1_715_923_200),
    )
    new_bet = record_bet(
        db, make_signal(bet_size=1.0, sleeve="subminute_5m",
                        window_start_ts=1_715_923_500),
    )
    db.execute(
        "UPDATE ledger SET offset_sec=285, bet_ts='2026-07-09 00:00:00' "
        "WHERE id=?", (old_bet,))
    db.execute(
        "UPDATE ledger SET offset_sec=285, bet_ts='2026-07-11 00:00:00' "
        "WHERE id=?", (new_bet,))
    for created_at, outcome in (
        ("2026-07-09 00:00:00", "UP"),   # pre-cutover, resolved
        ("2026-07-11 00:00:00", "DOWN"),  # post-cutover, resolved
    ):
        db.execute(
            "INSERT INTO reject_requotes "
            "(created_at, coin, sleeve, offset_sec, window_start_ts, side, "
            " limit_price, requested_usd, resolved_outcome) "
            "VALUES (?, 'BTC', 'subminute_5m', 285, 1715923200, 'UP', "
            "        0.70, 2.0, ?)",
            (created_at, outcome),
        )
    db.commit()

    block = sb._v2_block(db, portfolio, {"drawdown_pct_peak": 0.0})

    assert [b["time"] for b in block["bets"]] == ["2026-07-11 00:00:00"]
    assert [r["time"] for r in block["rejects"]] == ["2026-07-11 00:00:00"]
    # The would-be win-rate rollup counts only the post-cutover resolution
    # (n_resolved < 3 → winrate None, but the missed-PnL total is the single
    # post-cutover LOSS counterfactual: −$2 requested).
    assert block["reject_stats"]["missed_pnl_total"] == pytest.approx(-2.0)


# ---------------------------------------------------------------------------
# Historical stop-loss rows. The bot no longer writes them, but a ledger may
# still contain them; the payload must keep counting their realized loss.
# ---------------------------------------------------------------------------
def _insert_stop_row(
    db: sqlite3.Connection,
    *,
    side: str = "YES",
    won_up,
    window_start_ts: int = 1_715_923_200,
    pnl_net: float = -1.0,
    exit_price: float = 0.50,
) -> int:
    """A stop-closed sub-minute row: outcome='CANCELLED', exit_kind='stop_loss',
    realized pnl_net, 5 shares @0.70 cost."""
    rid = record_bet(
        db,
        make_signal(sleeve="subminute_5m", side=side, bet_size=3.5,
                    fill_price=0.70, window_start_ts=window_start_ts),
        mode="LIVE",
        live_filled_size=5.0,
        live_filled_price=0.70,
    )
    won_sql = "NULL" if won_up is None else str(int(won_up))
    db.execute(
        "UPDATE ledger SET offset_sec=285, outcome='CANCELLED', pnl_net=?, "
        "exit_price=?, exit_ts='2026-07-11T00:00:00Z', exit_kind='stop_loss', "
        f"won_up={won_sql} WHERE id=?",
        (pnl_net, exit_price, rid),
    )
    db.commit()
    return rid


def test_stop_row_in_v2_bets(db: sqlite3.Connection):
    """A stop-closed row appears in live.v2.bets tagged exit_kind='stop_loss'
    with its realized loss."""
    portfolio = load_sleeves()
    _insert_stop_row(db, side="YES", won_up=1)

    payload, _ = build_payload(db, portfolio)
    bet = next(b for b in payload["v2"]["bets"] if b["exit_kind"] == "stop_loss")
    assert bet["pnl_net"] == pytest.approx(-1.0)
    assert bet["outcome"] == "CANCELLED"
    assert "stop_if_held_pnl" not in bet


def test_stop_row_pnl_in_journal_sum_legacy_cancelled_excluded(
    db: sqlite3.Connection,
):
    """A stop row's realized loss flows into the lifetime pnl rollup and equity
    series alongside WIN/LOSS rows, while a voided CANCELLED row (exit_kind
    NULL, pnl_net NULL) contributes nothing."""
    portfolio = load_sleeves()
    # WIN (+2.0), stop (-1.0), voided CANCELLED (no pnl).
    win = record_bet(db, make_signal(sleeve="subminute_5m", side="YES"),
                     mode="LIVE")
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=2.0, "
        "resolution_ts=datetime('now') WHERE id=?",
        (win,),
    )
    stop = _insert_stop_row(db, side="YES", won_up=1,
                            window_start_ts=1_715_923_800)
    legacy = record_bet(
        db, make_signal(sleeve="subminute_5m", window_start_ts=1_715_924_100),
        mode="LIVE",
    )
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=NULL, exit_kind=NULL "
        "WHERE id=?",
        (legacy,),
    )
    db.commit()

    payload, _ = build_payload(db, portfolio)

    by_id = {r["id"]: r for r in payload["ledger"]}
    assert by_id[stop]["exit_kind"] == "stop_loss"
    assert by_id[win]["exit_kind"] is None
    assert by_id[legacy]["exit_kind"] is None

    # Lifetime rollup pnl = WIN(+2.0) + stop(−1.0) = +1.0; the legacy voided
    # CANCELLED (NULL pnl) is excluded from the sum.
    roll = payload["ledger_rollup"]
    assert roll["pnl_net"] == pytest.approx(1.0)
    # 2026-08-05: the SL class is gone from the payload — a booked stop exit is
    # a LOSS, so it lands in `losses` and inside `decided`; the legacy void
    # still rides in `voided` and in neither win-rate term.
    assert "stops" not in roll
    assert roll["decided"] == 2
    assert roll["wins"] == 1
    assert roll["losses"] == 1
    assert roll["voided"] == 1
    # win_rate = 1W/(1W+1L) = 0.5 — the same number the old W·L·SL form gave.
    assert roll["win_rate"] == pytest.approx(0.5)
    # Equity series counts the WIN + the stop (2 pts); the NULL-pnl legacy row
    # is excluded (settled_cond requires pnl_net IS NOT NULL).
    assert payload["ledger_equity"]["n"] == 2


def test_equity_series_keys_stop_on_exit_ts_invariant_to_backfill(
    db: sqlite3.Connection,
):
    """Regression (the skeptic's exact scenario): the lifetime equity chart's
    peak/maxDD must key a booked-once stop-loss row on its immutable exit_ts —
    the true equity-change moment — NOT COALESCE(resolution_ts, bet_ts). This
    mirrors capital.py's DD/peak replay so the chart agrees with the capital
    tile + the LIVE hard DD halt (config.MAX_DRAWDOWN_HALT_FRAC).

    All fresh stop rows (post-7dbe00c) carry resolution_ts NULL, so the old
    key fell back to bet_ts. A stop booked BEFORE an earlier win but exited
    AFTER it would then (a) sort before the win — under-reporting the peak — and
    (b) shift the peak/maxDD if a display-only telemetry backfill later stamps
    resolution_ts. Keying on the immutable exit_ts fixes both.

    WIN +30 @resolution_ts=2026-01-01, stop −8 booked @bet_ts=2025-06-01 (BEFORE
    the win) but exited @exit_ts=2026-07-11 (AFTER it). With anchor=122 →
    base=100. The CORRECT chronological replay (win then stop):
        100 -> +30 win = 130 (PEAK) -> −8 stop = 122.
    peak=130, maxDDAbs=−8, maxDD=−8/130≈−0.0615 — identical whether resolution_ts
    is NULL or later backfilled. (The old bet_ts key gave peak 122 / maxDD −0.08
    in the NULL state, then jumped to 130 / −0.0615 once stamped — the exact
    fragility this removes.)
    """
    from cryptobot.observability.snapshot_builder import _ledger_equity_series
    portfolio = load_sleeves()

    win = record_bet(db, make_signal(sleeve="subminute_5m", side="YES"),
                     mode="LIVE")
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=30.0, won_up=1, "
        "resolution_ts='2026-01-01 00:00:00' WHERE id=?",
        (win,),
    )
    # Stop booked BEFORE the win (bet_ts) but exited AFTER it (exit_ts, set by
    # _insert_stop_row to 2026-07-11T..Z); fresh rows leave resolution_ts NULL.
    stop = _insert_stop_row(db, side="YES", won_up=1, pnl_net=-8.0,
                            window_start_ts=1_715_923_800)
    db.execute(
        "UPDATE ledger SET bet_ts='2025-06-01 00:00:00' WHERE id=?", (stop,),
    )
    db.commit()

    # NULL-resolution_ts state (fresh stop row). anchor=122 → base=122−22=100.
    null_state = _ledger_equity_series(db, portfolio, anchor=122.0)
    assert null_state["n"] == 2
    assert null_state["base"] == pytest.approx(100.0)
    assert null_state["peak"] == pytest.approx(130.0)      # NOT 122 (the bug)
    assert null_state["final"] == pytest.approx(122.0)
    assert null_state["maxDDAbs"] == pytest.approx(-8.0)
    assert null_state["maxDD"] == pytest.approx(-8.0 / 130.0, abs=1e-4)

    # A display-only backfill stamps a LATE sweep clock onto resolution_ts (a
    # value that, under the OLD key, would re-sort the stop and change the peak);
    # the exit_ts key ignores it → peak/maxDD are byte-identical (invariant).
    db.execute(
        "UPDATE ledger SET resolution_ts='2033-01-01 00:00:00' WHERE id=?",
        (stop,),
    )
    db.commit()
    stamped = _ledger_equity_series(db, portfolio, anchor=122.0)
    assert stamped["peak"] == pytest.approx(null_state["peak"])
    assert stamped["maxDD"] == pytest.approx(null_state["maxDD"])
    assert stamped["maxDDAbs"] == pytest.approx(null_state["maxDDAbs"])
    assert stamped["peak"] == pytest.approx(130.0)


# ---------------------------------------------------------------------------
# Real-time money: per-tick cash headline + unrealized open-position marks.
# ---------------------------------------------------------------------------
def _fresh_book(bid: float):
    """A book-source-shaped get(token, now_ts=) returning a snapshot whose top
    (last, ascending) bid is `bid` — the best executable sell the mark reads."""
    def get(token, *, now_ts):
        return BookSnapshot(
            token_id=token,
            asks=(),
            bids=({"price": 0.80, "size": 40.0}, {"price": bid, "size": 40.0}),
            fetched_at_ts=now_ts,
            total_ask_depth_usd=0.0,
        )
    return get


def test_live_account_capital_fallback_chain(db: sqlite3.Connection):
    """_live_account_capital resolves the per-tick cash the SAME way sizing does:
    balance_cache (exact) → live_readiness_reports mirror (≤300s) →
    reconciliation_sample (degraded, as-of surfaced) → null; account_pnl_total_live
    nets vs the audited basis anchor (account['initial'])."""
    from datetime import datetime, timedelta, timezone

    from cryptobot.observability import snapshot_builder as sb

    now = datetime.now(timezone.utc)
    account = {
        "capital": 137.34, "capital_ts": "2026-07-11 06:47:38",
        "initial": 111.34, "basis_ts": "2026-07-08 15:30:16",
    }

    # (1) balance_cache primary — exact, not degraded, nets vs the basis anchor.
    a = sb._live_account_capital(db, account, lambda: 118.42)
    assert a["account_capital_live"] == pytest.approx(118.42)
    assert a["account_capital_live_source"] == "balance_cache"
    assert a["account_capital_live_degraded"] is False
    assert a["account_pnl_total_live"] == pytest.approx(118.42 - 111.34)
    assert a["account_capital_basis_ts"] == "2026-07-08 15:30:16"

    # (3) no balance_cache + no readiness row → the reconciliation sample,
    # flagged degraded, carrying the sample's as-of ts.
    c = sb._live_account_capital(db, account, None)
    assert c["account_capital_live"] == pytest.approx(137.34)
    assert c["account_capital_live_source"] == "reconciliation_sample"
    assert c["account_capital_live_degraded"] is True
    assert c["account_capital_live_ts"] == "2026-07-11 06:47:38"

    # (2) a FRESH readiness mirror (≤300s) wins over the reconciliation sample.
    db.execute(
        "INSERT INTO live_readiness_reports "
        "(created_at, status, clob_balance_usd, chain_balance_usd, report_json) "
        "VALUES (?, 'READY', 120.5, 120.5, '{}')",
        (now.strftime("%Y-%m-%d %H:%M:%S"),),
    )
    db.commit()
    b = sb._live_account_capital(db, account, None)
    assert b["account_capital_live"] == pytest.approx(120.5)
    assert b["account_capital_live_source"] == "readiness_report"
    assert b["account_capital_live_degraded"] is False

    # A STALE readiness mirror (>300s) is ignored → back to reconciliation.
    db.execute("DELETE FROM live_readiness_reports")
    db.execute(
        "INSERT INTO live_readiness_reports "
        "(created_at, status, clob_balance_usd, chain_balance_usd, report_json) "
        "VALUES (?, 'READY', 120.5, 120.5, '{}')",
        ((now - timedelta(seconds=600)).strftime("%Y-%m-%d %H:%M:%S"),),
    )
    db.commit()
    d = sb._live_account_capital(db, account, None)
    assert d["account_capital_live_source"] == "reconciliation_sample"

    # (4) no cash basis at all → null (DRY / fresh deploy).
    e = sb._live_account_capital(db, {"capital": None, "initial": None}, None)
    assert e["account_capital_live"] is None
    assert e["account_pnl_total_live"] is None


def test_open_position_marked_to_book(db: sqlite3.Connection):
    """A PENDING position is marked to the current book (best executable sell =
    top bid) on both the ledger row and the v2 bet, and summed into the kpis
    open-position aggregate + account_equity_incl_open (cash EXCLUDES the spent
    share cost, so open value is added on top). poly_token_id is read for the
    lookup but NEVER emitted to the client."""
    from cryptobot.observability import snapshot_builder as sb

    sb.clear_aux_cache()
    portfolio = load_sleeves()
    rid = record_bet(
        db,
        make_signal(sleeve="subminute_5m", bet_size=3.5, fill_price=0.70,
                    window_start_ts=1_715_923_200),
        mode="LIVE", live_filled_size=5.0, live_filled_price=0.70,
    )
    db.execute("UPDATE ledger SET offset_sec=285 WHERE id=?", (rid,))
    db.commit()

    payload, _ = build_payload(
        db, portfolio,
        live_balance_get=lambda: 100.0,
        book_mark_get=_fresh_book(0.83),
    )
    row = next(r for r in payload["ledger"] if r["id"] == rid)
    assert row["mark_px"] == pytest.approx(0.83)
    assert row["mark_source"] == "book"
    assert row["mark_value_usd"] == pytest.approx(5.0 * 0.83)   # 4.15
    assert "poly_token_id" not in row     # server-side only, never emitted

    bet = next(b for b in payload["v2"]["bets"] if b["offset_sec"] == 285)
    assert bet["mark_px"] == pytest.approx(0.83)
    assert bet["mark_source"] == "book"

    k = payload["kpis"]
    assert k["open_positions_value_usd"] == pytest.approx(4.15)
    assert k["open_positions_marked_n"] == 1
    assert k["open_positions_entry_n"] == 0
    assert k["account_capital_live"] == pytest.approx(100.0)
    assert k["account_equity_incl_open_usd"] == pytest.approx(104.15)


def test_open_position_marks_at_entry_when_book_absent(db: sqlite3.Connection):
    """With no book source (token outside the active window / no chain wired) the
    open position marks at the entry fill price (cost basis): mark_px null,
    mark_source 'entry', mark_value = shares × entry. No live cash basis ⇒
    account_equity_incl_open degrades to null, not a crash."""
    from cryptobot.observability import snapshot_builder as sb

    sb.clear_aux_cache()
    portfolio = load_sleeves()
    rid = record_bet(
        db,
        make_signal(sleeve="subminute_5m", bet_size=3.5, fill_price=0.70,
                    window_start_ts=1_715_923_200),
        mode="LIVE", live_filled_size=5.0, live_filled_price=0.70,
    )
    db.execute("UPDATE ledger SET offset_sec=285 WHERE id=?", (rid,))
    db.commit()

    payload, _ = build_payload(db, portfolio, book_mark_get=None)
    row = next(r for r in payload["ledger"] if r["id"] == rid)
    assert row["mark_px"] is None
    assert row["mark_source"] == "entry"
    assert row["mark_value_usd"] == pytest.approx(5.0 * 0.70)   # 3.50 cost basis

    bet = next(b for b in payload["v2"]["bets"] if b["offset_sec"] == 285)
    assert bet["mark_source"] == "entry"
    assert bet["mark_px"] is None

    k = payload["kpis"]
    assert k["open_positions_value_usd"] == pytest.approx(3.50)
    assert k["open_positions_marked_n"] == 0
    assert k["open_positions_entry_n"] == 1
    assert k["account_equity_incl_open_usd"] is None


# ---------------------------------------------------------------------------
# Latency item #6: KPI group-state / extractable TTL cache
# ---------------------------------------------------------------------------


class _KpiQueryCountingConn:
    """Proxy counting the two aggregates the KPI TTL cache guards: the
    get_group_state live_order_attempts reservation sum and the
    extractable_pnl_by_wallet transfer_requests prior sum. The distinctive
    aliases keep the counters blind to the operator block's unrelated
    transfer-history SELECTs."""

    def __init__(self, real: sqlite3.Connection) -> None:
        self._real = real
        self.group_state_queries = 0
        self.extractable_queries = 0

    def execute(self, sql, *args):
        if "live_order_attempts" in sql and "AS reserved" in sql:
            self.group_state_queries += 1
        if "transfer_requests" in sql and "AS prior" in sql:
            self.extractable_queries += 1
        return self._real.execute(sql, *args)

    def __getattr__(self, name):
        return getattr(self._real, name)


class TestKpiGroupStateTtlCache:
    """_group_state_cached serves get_group_state + extractable_pnl_by_wallet
    from a 1s TTL cache keyed on the capital ledger-version probe; any ledger
    mutation refreshes immediately, while mode/operator state stay UNCACHED so
    an operator flip lands on the very next payload."""

    def test_cached_within_ttl_and_invalidated_by_ledger_write(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        from cryptobot.observability.snapshot_builder import _group_state_cached

        conn = _KpiQueryCountingConn(db)

        g1, e1 = _group_state_cached(conn, portfolio)
        assert conn.group_state_queries == 1
        assert conn.extractable_queries == 1
        assert set(g1) == {"group_5m", "group_15m"}

        # Unchanged ledger inside the TTL -> both served from cache.
        g2, e2 = _group_state_cached(conn, portfolio)
        assert conn.group_state_queries == 1
        assert conn.extractable_queries == 1
        assert g2 is g1 and e2 is e1

        # A ledger write moves the ledger-version probe -> recompute NOW
        # (not at TTL expiry): a fresh LIVE PENDING row must show up in the
        # group pending sum on the next build.
        record_bet(db, make_signal(bet_size=2.0, sleeve="v6_5m"), mode="LIVE")
        g3, _ = _group_state_cached(conn, portfolio)
        assert conn.group_state_queries == 2
        assert conn.extractable_queries == 2
        assert g3["group_5m"].pending_usd == pytest.approx(2.0)

    def test_mode_state_not_cached_wallet_flip_lands_next_build(
        self, db: sqlite3.Connection, portfolio
    ) -> None:
        # Warm the group-state cache with a first build...
        k1 = build_payload(db, portfolio)[0]["kpis"]
        assert k1["by_sleeve"]["v6_5m"]["mode"] == "DRY"
        # ...then flip the wallet LIVE. wallet_mode_state is NOT a ledger
        # table, so the group cache stays warm — but the mode must STILL
        # flip on the immediately-following build (the operator STOP /
        # mode-flip freshness guarantee).
        db.execute(
            "INSERT OR REPLACE INTO wallet_mode_state (wallet_id, mode) "
            "VALUES ('v6_5m', 'LIVE')"
        )
        db.commit()
        k2 = build_payload(db, portfolio)[0]["kpis"]
        assert k2["by_sleeve"]["v6_5m"]["mode"] == "LIVE"


# ---------------------------------------------------------------------------
# 2026-07-21 dashboard fix plan — producer-side items:
# C4 basis anchor / W-L-SL win rate / per-order latency / strategy flags /
# per-venue rollups / v2 filled_usd partial fills
# ---------------------------------------------------------------------------


def _insert_recon_run(
    db: sqlite3.Connection,
    sampled_at: str,
    balance: float,
    *,
    status: str = "ok",
    open_orders: int = 0,
    open_positions: int = 0,
) -> None:
    db.execute(
        "INSERT INTO wallet_reconciliation_runs "
        "(sampled_at, wallet_address, source_status, clob_balance_usd, "
        " chain_balance_usd, open_orders_count, open_positions_count, "
        " warnings_json) VALUES (?, '0xfunder', ?, ?, ?, ?, ?, '[]')",
        (sampled_at, status, balance, balance, open_orders, open_positions),
    )


def test_account_basis_anchor_skips_mid_position_sample(db: sqlite3.Connection):
    """C4 (REAL MONEY): the cash-basis anchor must be the newest CLEAN
    (source ok + flat book) sample before the first live bet. A newer
    MID-POSITION sample understates the basis by the spent share cost —
    anchoring on it overstated account_pnl_total by exactly that cost."""
    portfolio = load_sleeves()
    # Older CLEAN sample — the correct anchor.
    _insert_recon_run(db, "2026-06-21 03:00:00", 100.0)
    # Newer but MID-POSITION pre-live sample ($3.50 already spent on shares).
    _insert_recon_run(db, "2026-06-21 03:30:00", 96.50, open_positions=1)
    # Latest: ok + flat after settlement.
    _insert_recon_run(db, "2026-06-21 04:33:56", 110.0)
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.0, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
        (rid,),
    )
    db.commit()

    kpis = build_payload(db, portfolio)[0]["kpis"]
    assert kpis["account_initial_capital"] == 100.0        # NOT 96.50
    assert kpis["account_initial_capital_source"] == "clob_before_first_live_bet"
    assert kpis["account_initial_capital_ts"] == "2026-06-21 03:00:00"
    assert kpis["account_pnl_total"] == pytest.approx(10.0)
    assert kpis["account_capital_comparable"] is True


def test_account_basis_anchor_unclean_fallback_not_comparable(
    db: sqlite3.Connection,
):
    """C4 fallback: when ONLY mid-position samples precede the first live bet
    the basis still anchors for continuity, but it is flagged un-clean and the
    account view is NOT comparable (strict consumers ignore it)."""
    portfolio = load_sleeves()
    _insert_recon_run(db, "2026-06-21 03:30:00", 96.50, open_positions=1)
    _insert_recon_run(db, "2026-06-21 04:33:56", 110.0)
    rid = record_bet(
        db, make_signal(bet_size=4.0, sleeve="subminute_5m"), mode="LIVE"
    )
    db.execute(
        "UPDATE ledger SET bet_ts='2026-06-21 04:00:00', outcome='WIN', "
        "pnl_net=10.0, resolution_ts='2026-06-21 04:15:00' WHERE id=?",
        (rid,),
    )
    db.commit()

    kpis = build_payload(db, portfolio)[0]["kpis"]
    assert kpis["account_initial_capital"] == 96.50
    assert (
        kpis["account_initial_capital_source"]
        == "unclean_clob_before_first_live_bet"
    )
    assert kpis["account_capital_comparable"] is False


def test_rollup_win_rate_counts_stops_as_losses(
    db: sqlite3.Connection, portfolio
):
    """A booked stop exit is a LOSS. 2 WIN + 1 LOSS + 1 stop -> 2/(2+2) = 0.5
    (NOT the stop-blind 2/3); the voided cancel joins neither count. 2026-08-05:
    the stop is FOLDED into `losses` and there is no `stops` column any more —
    the win_rate number is identical to the old W·L·SL form."""
    for i, (outcome, pnl) in enumerate(
        [("WIN", 1.0), ("WIN", 1.0), ("LOSS", -1.0)]
    ):
        rid = record_bet(
            db,
            make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_923_200 + i * 300),
        )
        db.execute(
            "UPDATE ledger SET outcome=?, pnl_net=?, "
            "resolution_ts=datetime('now') WHERE id=?",
            (outcome, pnl, rid),
        )
    stop_id = record_bet(
        db, make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_924_400),
    )
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=-0.4, "
        "exit_kind='stop_loss', exit_ts=datetime('now') WHERE id=?",
        (stop_id,),
    )
    void_id = record_bet(
        db, make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_924_700),
    )
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=0.0, exit_kind=NULL "
        "WHERE id=?",
        (void_id,),
    )
    db.commit()

    roll = build_payload(db, portfolio)[0]["ledger_rollup"]
    assert roll["wins"] == 2
    assert roll["losses"] == 2      # 1 decided LOSS + 1 booked stop exit
    assert "stops" not in roll
    assert roll["voided"] == 1
    assert roll["decided"] == 4
    assert roll["win_rate"] == pytest.approx(0.5)


def test_kpis_24h_win_rate_counts_stops(db: sqlite3.Connection):
    """The 24h win rate (kpis + per-wallet tiles) treats a stop exit as a loss:
    a fresh WIN + a fresh stop -> win_rate_24h = 1/2. 2026-08-05: the kpis roll
    the stop INTO losses_24h and drop the `stops_24h` column, so the tile can
    only ever render "1W · 1L" (never "0L · 1SL")."""
    portfolio = load_sleeves()
    win = record_bet(db, make_signal(sleeve="subminute_5m", side="YES"),
                     mode="LIVE")
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=2.0, "
        "resolution_ts=datetime('now') WHERE id=?",
        (win,),
    )
    _insert_stop_row(db, side="YES", won_up=1, window_start_ts=1_715_923_800)
    db.commit()

    kpis = build_payload(db, portfolio)[0]["kpis"]
    assert kpis["wins_24h"] == 1
    assert kpis["losses_24h"] == 1          # the stop, folded in
    assert "stops_24h" not in kpis
    assert kpis["win_rate_24h"] == pytest.approx(0.5)
    wallet = next(
        w for w in kpis["wallets"] if w["id"] == "subminute_5m"
    )
    assert wallet["wins_24h"] == 1
    assert wallet["losses_24h"] == 1
    assert "stops_24h" not in wallet
    assert wallet["win_rate_24h"] == pytest.approx(0.5)


def test_per_venue_rollups_emitted(db: sqlite3.Connection, portfolio):
    """H5: per-venue lifetime + daily rollups (GROUP BY venue) ride in the
    payload with the same definitions as the combined blocks; legacy/blank
    venue folds to 'polymarket'."""
    win = record_bet(db, make_signal(bet_size=1.0, sleeve="v6_5m"))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=2.0, "
        "resolution_ts='2026-06-01 16:30:00' WHERE id=?",
        (win,),
    )
    pf = record_bet(
        db, make_signal(bet_size=1.0, sleeve="v6_5m",
                        window_start_ts=1_715_923_500),
    )
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-1.0, venue='predictfun', "
        "resolution_ts='2026-06-01 20:00:00' WHERE id=?",
        (pf,),
    )
    db.commit()

    payload = build_payload(db, portfolio)[0]
    by_venue = payload["ledger_rollup_by_venue"]
    assert set(by_venue) == {"polymarket", "predictfun"}
    assert by_venue["polymarket"]["wins"] == 1
    assert by_venue["polymarket"]["pnl_net"] == pytest.approx(2.0)
    assert by_venue["polymarket"]["win_rate"] == pytest.approx(1.0)
    assert by_venue["predictfun"]["losses"] == 1
    assert by_venue["predictfun"]["pnl_net"] == pytest.approx(-1.0)
    # Venue counts sum to the combined rollup (same population).
    roll = payload["ledger_rollup"]
    assert sum(v["total"] for v in by_venue.values()) == roll["total"]

    daily_by_venue = payload["ledger_daily_by_venue"]
    assert set(daily_by_venue) == {"polymarket", "predictfun"}
    assert daily_by_venue["predictfun"][0]["losses"] == 1
    assert daily_by_venue["predictfun"][0]["date"] == "2026-06-02"  # MYT day
    # Per-venue daily pnl sums to the combined daily pnl.
    combined = sum(d["pnl"] for d in payload["ledger_daily"])
    split = sum(
        d["pnl"] for days in daily_by_venue.values() for d in days
    )
    assert split == pytest.approx(combined)


def test_v2_bets_filled_usd_uses_live_fill_pair(db: sqlite3.Connection):
    """Item 15: the Subminute tab's filled_usd is the REALIZED live fill value
    (size x price) when the reconciler landed one — not the intended
    bet_size."""
    portfolio = load_sleeves()
    rid = record_bet(
        db,
        make_signal(sleeve="subminute_5m", bet_size=5.0, fill_price=0.5,
                    window_start_ts=1_715_923_200),
        mode="LIVE", live_filled_size=2.5, live_filled_price=0.8,
    )
    db.execute("UPDATE ledger SET offset_sec=285 WHERE id=?", (rid,))
    db.commit()

    payload, _ = build_payload(db, portfolio)
    bet = next(b for b in payload["v2"]["bets"] if b["offset_sec"] == 285)
    assert bet["stake_usd"] == pytest.approx(5.0)      # intended
    assert bet["filled_usd"] == pytest.approx(2.0)     # realized 2.5 x 0.80


def test_v2_bets_export_requested_usd_intent(db: sqlite3.Connection):
    """The fill row carries the sizer's INTENT (ledger.target_usd) as
    requested_usd. On LIVE rows bet_size is the realized fill value, so
    without this field the client's PARTIAL detector compares filled against
    itself and a partially filled FAK can never read PARTIAL (2026-08-09:
    a $6.31 target that filled $1.26 rendered as a full-size LOSS)."""
    portfolio = load_sleeves()
    rid = record_bet(
        db,
        make_signal(sleeve="subminute_5m", bet_size=1.26, target_usd=6.31,
                    fill_price=0.21, window_start_ts=1_715_923_200),
        mode="LIVE", live_filled_size=6.0, live_filled_price=0.21,
    )
    db.execute("UPDATE ledger SET offset_sec=285 WHERE id=?", (rid,))
    db.commit()

    payload, _ = build_payload(db, load_sleeves())
    bet = next(b for b in payload["v2"]["bets"] if b["offset_sec"] == 285)
    assert bet["requested_usd"] == pytest.approx(6.31)  # the intent
    assert bet["stake_usd"] == pytest.approx(1.26)      # realized


def test_latency_block_emits_honest_per_order_stats(db: sqlite3.Connection):
    """Item 27: the trace's sign/post/order values are PER-TICK SUMS across
    n_orders (the multi-coin boundary-tick 2-3x trap). latency.orders.per_order
    divides each order-bearing tick by its n_orders (weighted per order) so the
    displayed POST latency is per-order honest; the tick-level `stages` block
    stays for continuity, and the venue floor rides along for labeling."""
    from cryptobot.observability import snapshot_builder as sb

    def _trace(n_orders: int, sign: float, post: float, order: float) -> str:
        return json.dumps({
            "spot": 10.0, "ctx": 5.0, "gamma": 0.0, "book": 20.0,
            "sign": sign, "post": post, "order": order, "total": 400.0,
            "n_gamma": 0, "n_book": 1, "n_orders": n_orders,
        })

    # Tick A: ONE order, post 300ms. Tick B: TWO orders, post SUM 640ms
    # (i.e. 320ms/order) — the old tick-level view read it as 640.
    for msg in (_trace(1, 50.0, 300.0, 360.0), _trace(2, 100.0, 640.0, 700.0)):
        db.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('latency', 'OK', ?)",
            (msg,),
        )
    db.commit()
    sb.clear_aux_cache()

    block = sb._latency_block(db)
    # Tick-level stages keep the raw sums (n=2 order-bearing ticks).
    assert block["stages"]["post"]["n"] == 2
    assert block["stages"]["post"]["max"] == pytest.approx(640.0)
    # Honest per-order view: values [300, 320, 320] weighted per order.
    orders = block["orders"]
    assert orders["n_orders_total"] == 3
    assert orders["n_order_ticks"] == 2
    assert orders["n_multi_order_ticks"] == 1
    po = orders["per_order"]["post"]
    assert po["n"] == 3
    assert po["max"] == pytest.approx(320.0)
    assert po["p50"] == pytest.approx(320.0)
    # Venue floor emitted so the client labels ~300-320ms as EXPECTED.
    assert block["venue_floor"]["taker_delay_ms"] == 250
    assert block["venue_floor"]["expected_decision_to_fill_ms_lo"] == 300


def test_latency_block_never_blends_accepted_and_rejected_posts(
    db: sqlite3.Connection,
):
    """2026-07-25: rejected FAKs used to record post=0 and get averaged in.

    Both exception handlers in place_gtc_limit_order returned OrderResult with
    no post_ms, so ~55% of live orders (the whole `no_liquidity` cohort) landed
    in the trace as a zero — and this aggregation divided by ALL orders, so the
    Latency page displayed order post p50 ~= 0.0ms. The walker now measures the
    reject round trip and the trace carries it as `post_rej`/`n_rej`, which must
    be aggregated as its OWN population: an accept waits out the venue taker
    delay, a reject (HTTP 400) returns before matching.
    """
    from cryptobot.observability import snapshot_builder as sb

    def _trace(**kw) -> str:
        base = {
            "spot": 0.0, "ctx": 1.0, "gamma": 0.0, "book": 20.0,
            "sign": 1.0, "post": 0.0, "order": 400.0, "total": 500.0,
            "n_gamma": 0, "n_book": 1, "n_orders": 1,
            "post_rej": 0.0, "n_rej": 0, "pre_post": 60.0, "tail": 90.0,
        }
        base.update(kw)
        return json.dumps(base)

    rows = [
        # two accepted orders at 410/420ms ...
        _trace(post=410.0, n_orders=1, n_rej=0),
        _trace(post=420.0, n_orders=1, n_rej=0),
        # ... and three venue-refused ones at ~285ms, which must NOT drag the
        # accepted p50 toward zero (the pre-fix bug) nor be averaged with it.
        _trace(post=0.0, post_rej=280.0, n_orders=1, n_rej=1),
        _trace(post=0.0, post_rej=285.0, n_orders=1, n_rej=1),
        _trace(post=0.0, post_rej=290.0, n_orders=1, n_rej=1),
    ]
    for msg in rows:
        db.execute(
            "INSERT INTO pipeline_health (stage, status, message) "
            "VALUES ('latency', 'OK', ?)",
            (msg,),
        )
    db.commit()
    sb.clear_aux_cache()

    block = sb._latency_block(db)
    orders = block["orders"]
    assert orders["n_orders_total"] == 5
    assert orders["n_rejected"] == 3
    assert orders["n_accepted"] == 2

    acc = orders["per_order"]["post"]
    rej = orders["per_order"]["post_rej"]
    # Accepted cohort sees ONLY the two real accepts — no zero-padding.
    # (_pctl takes the lower of an even pair, so p50 of [410, 420] is 410.)
    assert acc["n"] == 2
    assert acc["p50"] == pytest.approx(410.0)
    assert acc["max"] == pytest.approx(420.0)
    assert acc["p50"] > 400.0, "rejects must not drag the accept p50 down"
    # Rejects are their own population, and are NOT free.
    assert rej["n"] == 3
    assert rej["p50"] == pytest.approx(285.0)


def test_latency_block_exposes_decision_and_freshness(db: sqlite3.Connection):
    """The page must be able to separate the movable part of the tick from the
    venue floor, and wall-clock from staleness."""
    from cryptobot.observability import snapshot_builder as sb

    msg = json.dumps({
        "spot": 0.0, "ctx": 1.0, "gamma": 0.0, "book": 20.0,
        "sign": 1.0, "post": 410.0, "order": 430.0, "total": 560.0,
        "n_gamma": 0, "n_book": 1, "n_orders": 1,
        "post_rej": 0.0, "n_rej": 0,
        "pre_post": 62.0, "tail": 95.0,
        "bk_ws": 3, "bk_pf": 5, "bk_rest": 0, "ws_age": 3600.0,
    })
    db.execute(
        "INSERT INTO pipeline_health (stage, status, message) "
        "VALUES ('latency', 'OK', ?)",
        (msg,),
    )
    db.commit()
    sb.clear_aux_cache()

    block = sb._latency_block(db)
    # Decision latency = the only client-owned segment.
    assert block["decision"]["pre_post"]["p50"] == pytest.approx(62.0)
    # The post-POST tail is reported apart from it — it delays no fill.
    assert block["decision"]["tail"]["p50"] == pytest.approx(95.0)
    # Entry-path books are split from the held-position pass's books.

    fresh = block["freshness"]
    assert fresh["sources"] == {"ws": 3, "prefetch": 5, "rest": 0}
    assert fresh["ws_age"]["p50"] == pytest.approx(3600.0)
    assert fresh["ws_over_700ms"] == 1
    assert fresh["ws_over_2s"] == 1
    assert fresh["ws_over_10s"] == 0
    # The page must be able to SAY that the prefetch majority is unmeasured
    # rather than implying ws_age describes every book read.
    assert fresh["prefetch_age_instrumented"] is False


def test_strategy_flags_block_in_payload(
    db: sqlite3.Connection, portfolio, monkeypatch: pytest.MonkeyPatch
):
    """The Config tab's strategy panel renders from payload['strategy_flags']."""
    payload, _ = build_payload(db, portfolio)
    sf = payload["strategy_flags"]
    assert sf["fak_pad"] == pytest.approx(
        float(getattr(config, "V2_FAK_PAD", 0.0))
    )
    assert sf["min_bet_usd"] == pytest.approx(config.MIN_BET_USD)
    # The block is display-only config echo — strict-JSON safe.
    json.dumps(sf, allow_nan=False)


def test_ledger_analytics_covers_full_ledger_not_the_bootstrap_window(
    db: sqlite3.Connection, portfolio
):
    """payload['ledger_analytics'] is computed over the WHOLE ledger, so the
    Overview/Journal historical panels stop describing only the newest
    LEDGER_BOOTSTRAP_LIMIT streamed rows (~500 of ~3.5k era bets) while the
    lifetime headline beside them describes all of them."""
    n = config.LEDGER_BOOTSTRAP_LIMIT + 10
    for i in range(n):
        rid = record_bet(db, make_signal(
            bet_size=1.0, window_start_ts=1_715_923_200 + i * 60,
        ))
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=0.5, "
            "resolution_ts=datetime('now') WHERE id=?", (rid,),
        )
    db.commit()

    payload = build_payload(db, portfolio)[0]
    # The streamed rows stay bounded (render performance)...
    assert len(payload["ledger"]) == config.LEDGER_BOOTSTRAP_LIMIT
    # ...while the analytics block sees every row.
    summary = payload["ledger_analytics"]["All"]["summary"]
    assert summary["total"] == n
    assert summary["settled"] == n
    assert summary["pnl"] == pytest.approx(0.5 * n)
    assert payload["ledger_analytics"]["All"]["coins"][0]["bets"] == n
    # No losses at all ⇒ the client's Infinity PF sentinel, which strict JSON
    # cannot carry, rides as pf=None + pf_unbounded=True.
    assert summary["pf"] is None
    assert summary["pf_unbounded"] is True
    json.dumps(payload["ledger_analytics"], allow_nan=False)


def test_ledger_analytics_definitions_and_ranges(
    db: sqlite3.Connection, portfolio
):
    """Mirrors the client aggSummary/aggCoins/aggSlices/aggStreaks contracts
    (wins/(wins+losses) with a booked stop counted as a loss and its realized
    loss in the PF gross loss, settled stake as the ROI denominator) and
    pre-slices by the Topbar range toggle: an OLD row is in 'All' but not in
    '7d'. Each bet takes its own market window, so each is its own trade
    group."""
    win_id = record_bet(db, make_signal(bet_size=4.0, coin="BTC",
                                        window_start_ts=1_715_923_200))
    db.execute(
        "UPDATE ledger SET outcome='WIN', pnl_net=2.0, "
        "resolution_ts=datetime('now') WHERE id=?", (win_id,),
    )
    loss_id = record_bet(db, make_signal(bet_size=4.0, coin="BTC",
                                         window_start_ts=1_715_923_500))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-1.0, "
        "resolution_ts=datetime('now') WHERE id=?", (loss_id,),
    )
    # Booked stop-loss: counts as a LOSS in the win rate and in the PF gross
    # loss. Settled LONG ago, so it drops out of the 7d slice.
    stop_id = record_bet(db, make_signal(bet_size=4.0, coin="ETH",
                                         window_start_ts=1_715_923_800))
    db.execute(
        "UPDATE ledger SET outcome='CANCELLED', pnl_net=-3.0, "
        "exit_kind='stop_loss', resolution_ts=NULL, "
        "exit_ts='2026-06-03T20:00:00Z', bet_ts='2026-06-03 05:00:00' "
        "WHERE id=?", (stop_id,),
    )
    db.commit()

    analytics = build_payload(db, portfolio)[0]["ledger_analytics"]
    all_ = analytics["All"]
    s = all_["summary"]
    # The stop is FOLDED into losses; no `stops` key survives anywhere.
    assert (s["wins"], s["losses"]) == (1, 2)
    assert "stops" not in s
    # Ratios ship rounded (6dp / 4dp) — compare at the emitted precision.
    assert s["winRate"] == pytest.approx(1 / 3, abs=1e-6)
    assert s["pf"] == pytest.approx(2.0 / 4.0)             # stop loss in gross loss
    assert s["pf_unbounded"] is False
    assert s["roi"] == pytest.approx(-2.0 / 12.0, abs=1e-4)  # settled-stake denom

    by_coin = {c["coin"]: c for c in all_["coins"]}
    assert by_coin["BTC"]["wins"] == 1 and by_coin["BTC"]["losses"] == 1
    assert by_coin["ETH"]["losses"] == 1       # the stop, as a loss
    assert "stops" not in by_coin["ETH"]
    assert by_coin["ETH"]["winRate"] == 0.0
    assert [sl["n"] for sl in all_["slices"]] == [3]    # one 5m_e4 slice
    assert all_["slices"][0]["losses"] == 2
    assert "stops" not in all_["slices"][0]
    assert len(all_["histogram"]) == 11
    assert sum(b["n"] for b in all_["histogram"]) == 3
    assert all_["streaks"]["longestW"] == 1

    # Range pre-slices: the June stop is outside 7d, the two fresh rows are in.
    assert analytics["7d"]["summary"]["total"] == 2
    assert analytics["7d"]["summary"]["losses"] == 1
    assert analytics["All"]["summary"]["total"] == 3
    # Per-venue split carries the same shape (untagged venue folds to polymarket).
    by_venue = build_payload(db, portfolio)[0]["ledger_analytics_by_venue"]
    assert by_venue["polymarket"]["All"]["summary"]["total"] == 3


def test_counts_are_trade_groups_topups_fold_into_their_base_bet(
    db: sqlite3.Connection, portfolio
):
    """CLOB semantics (operator 2026-08-05): a base bet + its top-ups in the
    SAME market window is ONE position on the venue, so every count the
    dashboard labels a trade/bet counts it ONCE — while every $ sum stays the
    per-fill Σ it always was.

    Fixture: 3 fills (base + 2 top-ups) on one BTC 5m window that WINS, plus one
    independent ETH bet that LOSES in a different window. Rows = 4, trades = 2.
    """
    ws = 1_715_923_200
    base = record_bet(db, make_signal(bet_size=2.0, coin="BTC", window_start_ts=ws))
    top1 = record_bet(db, make_signal(bet_size=1.0, coin="BTC", window_start_ts=ws,
                                      slot_filled_pre=2.0, entry_minute=4))
    top2 = record_bet(db, make_signal(bet_size=1.0, coin="BTC", window_start_ts=ws,
                                      slot_filled_pre=3.0, entry_minute=4))
    assert [r["is_topup"] for r in db.execute(
        "SELECT is_topup FROM ledger WHERE id IN (?,?,?) ORDER BY id",
        (base, top1, top2),
    ).fetchall()] == [0, 1, 1]
    for rid, pnl in ((base, 1.0), (top1, 0.5), (top2, 0.5)):
        db.execute(
            "UPDATE ledger SET outcome='WIN', pnl_net=?, "
            "resolution_ts='2026-06-01 16:30:00' WHERE id=?", (pnl, rid),
        )
    other = record_bet(db, make_signal(bet_size=4.0, coin="ETH",
                                       window_start_ts=ws + 300))
    db.execute(
        "UPDATE ledger SET outcome='LOSS', pnl_net=-4.0, "
        "resolution_ts='2026-06-01 16:35:00' WHERE id=?", (other,),
    )
    db.commit()

    payload = build_payload(db, portfolio)[0]

    # The streamed ledger still carries every FILL (the journal folds them
    # client-side) and now ships the window key it groups on.
    assert len(payload["ledger"]) == 4
    assert {r["window_start_ts"] for r in payload["ledger"]} == {ws, ws + 300}

    # Lifetime rollup: 2 trades, not 4 bets. $ sums unchanged.
    roll = payload["ledger_rollup"]
    assert roll["total"] == 2
    assert (roll["wins"], roll["losses"]) == (1, 1)
    assert roll["win_rate"] == pytest.approx(0.5)
    assert roll["pnl_net"] == pytest.approx(-2.0)   # +1.0 +0.5 +0.5 −4.0
    assert roll["stake"] == pytest.approx(8.0)      # every fill's stake

    # Per-day rollup: both trades land on the same MYT day; n counts groups
    # while pnl stays the per-row Σ (the pinned Σ(day.pnl) == pnl_net_usd).
    daily = payload["ledger_daily"]
    assert [(d["date"], d["n"], d["wins"], d["losses"]) for d in daily] == [
        ("2026-06-02", 2, 1, 1)
    ]
    assert round(sum(d["pnl"] for d in daily), 2) == payload["kpis"]["pnl_net_usd"]

    # Analytics panels: summary / per-coin / per-slice / histogram all count 2.
    all_ = payload["ledger_analytics"]["All"]
    assert all_["summary"]["total"] == 2
    assert all_["summary"]["settled"] == 2
    assert all_["summary"]["pnl"] == pytest.approx(-2.0)
    by_coin = {c["coin"]: c for c in all_["coins"]}
    assert by_coin["BTC"]["bets"] == 1 and by_coin["BTC"]["resolved"] == 1
    assert by_coin["BTC"]["pnl"] == pytest.approx(2.0)   # the whole position
    assert by_coin["BTC"]["stake"] == pytest.approx(4.0)  # all three fills
    assert by_coin["ETH"]["bets"] == 1
    assert sum(sl["n"] for sl in all_["slices"]) == 2
    assert sum(b["n"] for b in all_["histogram"]) == 2
    # The streak tape sees two decided TRADES, not four fills.
    assert len(all_["streaks"]["last"]) == 2


def test_v2_fill_rate_carries_a_rolling_24h_twin(db: sqlite3.Connection):
    """v2.fill_rate.fill_rate_24h — the Subminute page's top-right KPI.

    Same definition as the breaker's arm (Σ filled_usd / Σ requested_usd over
    non-'error' subminute_fills rows); only the WINDOW differs — orders created
    in the last 24h. Rows older than 24h and 'error' rows are excluded, and an
    empty window degrades to None (the client renders '—')."""
    from cryptobot.observability import snapshot_builder

    def _ins(created_at_sql: str, requested: float, filled: float,
             outcome: str = "accepted") -> None:
        db.execute(
            "INSERT INTO subminute_fills "
            "(coin, sleeve, offset_sec, window_start_ts, side, requested_usd, "
            " filled_usd, outcome, created_at) "
            f"VALUES ('BTC', 'subminute_5m', 5, 0, 'UP', ?, ?, ?, {created_at_sql})",
            (requested, filled, outcome),
        )

    # Empty table → None, never 0.0 (nothing measured is not "no fills").
    assert snapshot_builder._v2_fill_rate_24h(db) == {
        "fill_rate_24h": None, "n_orders_24h": 0,
    }

    _ins("datetime('now', '-2 hours')", 10.0, 4.0)
    _ins("datetime('now', '-20 hours')", 10.0, 6.0)
    # excluded: outside the window / an error row
    _ins("datetime('now', '-30 hours')", 100.0, 0.0)
    _ins("datetime('now', '-1 hours')", 100.0, 0.0, outcome="error")
    db.commit()

    got = snapshot_builder._v2_fill_rate_24h(db)
    assert got["n_orders_24h"] == 2
    assert got["fill_rate_24h"] == pytest.approx(10.0 / 20.0)

    # ...and it rides the payload block the console reads.
    snapshot_builder.clear_aux_cache()
    payload, _ = build_payload(db, load_sleeves())
    fr = payload["v2"]["fill_rate"]
    assert fr["fill_rate_24h"] == pytest.approx(0.5)
    assert fr["n_orders_24h"] == 2
    # the breaker's own window/threshold fields are untouched beside it
    assert {"fill_rate", "n_orders", "min_fill_rate", "min_orders"} <= set(fr)


def test_venue_floor_carries_the_MEASURED_accepted_post_round_trip(
    db: sqlite3.Connection,
):
    """Operator 2026-08-05: "maybe it would be 260ms — can u record based on
    the real time?"

    `venue_floor.taker_delay_ms` stays the venue's DOCUMENTED constant (250ms
    on Polymarket — a published floor, not an observation). Beside it,
    `taker_delay_measured_ms` carries the p50/p95/max/n of the ACCEPTED
    per-order POST round trip — the same accept-only cohort the accept-vs-reject
    panel shows, and the only thing the box can actually record: an accepted
    marketable order cannot return before the enforced delay elapsed, so the
    measurement is network + signing tail + delay, i.e. >= the documented floor.

    None when the window holds no accepted order (a reject-only window measures
    nothing) — the client then falls back to the documented constant."""
    from cryptobot.observability import snapshot_builder as sb

    def _trace(**kw) -> str:
        base = {
            "spot": 0.0, "ctx": 1.0, "gamma": 0.0, "book": 20.0,
            "sign": 1.0, "post": 0.0, "order": 400.0, "total": 500.0,
            "n_gamma": 0, "n_book": 1, "n_orders": 1,
            "post_rej": 0.0, "n_rej": 0,
        }
        base.update(kw)
        return json.dumps(base)

    def _insert(rows: list[str]) -> None:
        for msg in rows:
            db.execute(
                "INSERT INTO pipeline_health (stage, status, message) "
                "VALUES ('latency', 'OK', ?)",
                (msg,),
            )
        db.commit()
        sb.clear_aux_cache()

    # ── reject-only window: nothing accepted ⇒ nothing measured ──
    _insert([_trace(post=0.0, post_rej=286.0, n_rej=1)])
    vf = sb._latency_block(db)["venue_floor"]
    assert vf["taker_delay_ms"] == 250            # documented constant stays
    assert vf["taker_delay_measured_ms"] is None  # honest: measured nothing

    # ── accepted orders present ⇒ the real recorded round trip ──
    _insert([_trace(post=262.0), _trace(post=268.0), _trace(post=410.0)])
    block = sb._latency_block(db)
    vf = block["venue_floor"]
    measured = vf["taker_delay_measured_ms"]
    assert measured is not None
    assert set(measured) == {"p50", "p95", "max", "n"}
    assert measured["n"] == 3
    assert measured["p50"] == pytest.approx(268.0)
    assert measured["max"] == pytest.approx(410.0)
    # It IS the accept-only per-order cohort — one number, two places.
    assert measured == block["orders"]["per_order"]["post"]
    # Measured is the wall INCLUDING the documented delay, never below it.
    assert measured["p50"] >= vf["taker_delay_ms"]
    # The documented constant is untouched by the measurement.
    assert vf["taker_delay_ms"] == 250
    assert vf["expected_decision_to_fill_ms_lo"] == 300


def test_v2_spec_carries_the_booked_fee_calibration(db: sqlite3.Connection):
    """The Strategy tab's `fee calibration` row is payload-driven.

    POLY_FEE_CALIBRATION (0.864) scales the BOOKED taker fee only
    (execution.grade_fees); decision math keeps raw theta, which is why the
    spec still reports poly_fee_theta 0.07 beside it."""
    from cryptobot.observability import snapshot_builder as sb

    spec = sb._v2_spec(load_sleeves())
    assert spec["fee_calibration"] == pytest.approx(config.POLY_FEE_CALIBRATION)
    assert spec["fee_calibration"] == pytest.approx(0.864)
    # ...and the raw published theta is UNCHANGED beside it (decision math).
    assert spec["poly_fee_theta"] == pytest.approx(0.07)
    assert sb._v2_sizing_constants()["fee_calibration"] == pytest.approx(0.864)


def test_latency_freshness_block_carries_watchdog_counters(db: sqlite3.Connection):
    """Operator order 2026-08-06: with the WS freshness cap removed, a large
    ws_age is no longer evidence of anything by itself — the payload must ALSO
    carry the stall watchdog's verdict so the page can say whether the
    out-of-band REST cross-check is armed and agreeing."""
    from cryptobot.observability import snapshot_builder as sb
    from cryptobot.scheduler import book_ws_watchdog as wd

    msg = json.dumps({
        "spot": 0.0, "ctx": 1.0, "gamma": 0.0, "book": 20.0,
        "sign": 1.0, "post": 410.0, "order": 430.0, "total": 560.0,
        "n_gamma": 0, "n_book": 1, "n_orders": 1,
        "bk_ws": 3, "bk_pf": 0, "bk_rest": 0, "ws_age": 8000.0,
    })
    db.execute(
        "INSERT INTO pipeline_health (stage, status, message) "
        "VALUES ('latency', 'OK', ?)",
        (msg,),
    )
    db.commit()

    # Never constructed => armed False + zeros (WS off / shadow / no API).
    wd.reset_watchdog_counters()
    sb.clear_aux_cache()
    block = sb._latency_block(db)["freshness"]["watchdog"]
    assert block["armed"] is False
    assert block["evictions"] == 0

    # Armed + one eviction => the page can render it.
    class _Mirror:
        def serving_snapshots(self):
            return {}

        def evict(self, token):  # pragma: no cover - not reached here
            return False

    watchdog = wd.BookWsWatchdog(_Mirror(), lambda tokens: None)
    watchdog.n_checks, watchdog.n_matches = 4, 3
    watchdog.n_mismatches = watchdog.n_evictions = 1
    watchdog.last_mismatch_ts = 1234.0
    watchdog._publish()
    sb.clear_aux_cache()
    block = sb._latency_block(db)["freshness"]["watchdog"]
    assert block["armed"] is True
    assert (block["checks"], block["matches"]) == (4, 3)
    assert (block["mismatches"], block["evictions"]) == (1, 1)
    assert block["last_mismatch_ts"] == 1234.0
    wd.reset_watchdog_counters()
