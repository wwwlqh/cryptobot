"""Interval entry-disable gate (config.INTERVAL_5M_ENABLED, operator 2026-07-25).

The predicate ``pipeline._interval_entry_disabled`` is the single authority for
this gate's whole scope, so it is tested directly (the repo idiom — see
test_late_window_gate / test_ask_band_gate). The pipeline-level behavior (silent
sleeve skip, no API calls, 15m untouched) lives in test_pipeline_tick.py.

Covered here on top of the predicate: the scale-in top-up path, which is driven
off OPEN LEDGER ROWS rather than the tick loop's sleeve iteration and so is the
one placement path the loop's gate cannot reach. A top-up is new money into the
position, so a disabled interval must refuse it.
"""
from __future__ import annotations

import sqlite3

import pytest

from cryptobot import config
from cryptobot.execution import pipeline as P
from cryptobot.execution import scale_in


# --------------------------------------------------------------------------- predicate

def test_enabled_never_disables_any_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    # Default (flag on) is byte-identical to the pre-flag bot: nothing is gated.
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", True)
    assert P._interval_entry_disabled(5) is False
    assert P._interval_entry_disabled(15) is False
    assert P._interval_entry_disabled(None) is False


def test_disabled_gates_5m_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", False)
    assert P._interval_entry_disabled(5) is True
    # 15m — the interval that keeps trading — must be untouched.
    assert P._interval_entry_disabled(15) is False


def test_disabled_tolerates_a_missing_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    # A row with no interval_min must not be gated (fail-open on the predicate;
    # the caller's own gates still apply). Guards the ledger-driven callers,
    # where interval_min comes from a nullable column.
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", False)
    assert P._interval_entry_disabled(None) is False


# --------------------------------------------------------------------------- scale-in

def _scan_topup_interval(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch, *, interval_min: int
) -> float:
    """Drive ``scale_in._scan_topup_inner`` for one open ledger row of the given
    interval, with every downstream dependency set to blow up. Returns the
    topped-up USD — 0.0 with no explosion means the gate refused first."""
    from cryptobot.tests.test_subminute import make_subminute_signal, WINDOW_START
    from cryptobot.execution.ledger import record_bet

    ledger_id = record_bet(
        db,
        make_subminute_signal(
            coin="BTC",
            sleeve="subminute_5m" if interval_min == 5 else "subminute_15m",
            side="YES", offset_sec=275, window_start_ts=WINDOW_START,
        ),
        mode="DRY",
    )
    db.execute(
        "UPDATE ledger SET interval_min = ? WHERE id = ?", (interval_min, ledger_id)
    )
    db.commit()

    def _blow_up(*_a, **_kw):
        raise AssertionError("scale-in ran past the interval gate")

    monkeypatch.setattr(scale_in, "_fold_and_dip", _blow_up)

    return scale_in._scan_topup_inner(
        db, ledger_id, None,
        bid=0.74, held_ask=0.75, now_ts=WINDOW_START + 280,
        deps=None, wallet_snaps=None, budget=[None], triggered=False,
        cfg=scale_in.TopupConfig.from_config(),
    )


def test_scale_in_topup_refused_on_disabled_interval(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The stop pass iterates open LEDGER rows, so the tick loop's sleeve gate
    # never sees this path. A 5m position must not take new money.
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", False)
    assert _scan_topup_interval(db, monkeypatch, interval_min=5) == 0.0


def test_scale_in_topup_reaches_15m_on_disabled_5m(
    db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Proves the test above is not vacuous: with 5m off, a 15m row still runs
    # past the gate and into the top-up logic (which the blow-up stub catches).
    monkeypatch.setattr(config, "INTERVAL_5M_ENABLED", False)
    with pytest.raises(AssertionError, match="past the interval gate"):
        _scan_topup_interval(db, monkeypatch, interval_min=15)
