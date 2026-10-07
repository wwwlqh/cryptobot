"""Interval entry-disable gate (config.INTERVAL_5M_ENABLED, operator 2026-07-25).

The predicate ``pipeline._interval_entry_disabled`` is tested directly; the
pipeline-level behavior (silent sleeve skip, no API calls, 15m untouched) lives
in test_pipeline_tick.py.
"""
from __future__ import annotations


import pytest

from cryptobot import config
from cryptobot.execution import pipeline as P


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
