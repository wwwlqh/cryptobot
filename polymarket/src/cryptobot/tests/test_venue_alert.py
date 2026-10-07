"""Venue-degradation alert (pipeline._maybe_alert_venue_degradation).

Audit: on 2026-06-19 / 06-24 the Polymarket CLOB returned HTTP 425 "order
manager not ready" in multi-minute bursts; ~79 order attempts failed and NO
operator signal fired, so ~7 bet-windows were silently lost. This exercises the
threshold + cooldown that now wakes the operator.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from cryptobot.execution import pipeline


def _insert_failed(conn, n, kind="venue_unavailable", *, minutes_ago=0):
    """Insert `n` FAILED live_order_attempts rows with the given error_kind."""
    for i in range(n):
        conn.execute(
            "INSERT INTO live_order_attempts "
            "(created_at, updated_at, wallet_id, mode, coin, sleeve, "
            " entry_minute, side, window_start_ts, client_order_id, "
            " amount_usd, status, error_kind) "
            "VALUES (datetime('now', ?), datetime('now'), 'w', 'LIVE', 'BTC', "
            " 'exp044_15m', 1, 'YES', 1715923200, ?, 4.0, 'FAILED', ?)",
            (f"-{minutes_ago} minutes", f"coid-{kind}-{minutes_ago}-{i}", kind),
        )
    conn.commit()


@pytest.fixture(autouse=True)
def _reset_alert_state():
    pipeline._LAST_VENUE_ALERT_MONO.clear()
    yield
    pipeline._LAST_VENUE_ALERT_MONO.clear()


def _ctx_with_recorder():
    calls: list[dict] = []

    def notify(event_type, message, *, wallet_id=None, critical=False):
        calls.append(
            dict(
                event_type=event_type,
                message=message,
                wallet_id=wallet_id,
                critical=critical,
            )
        )

    return SimpleNamespace(notify=notify), calls


class TestVenueAlert:
    def test_fires_critical_when_threshold_crossed(self, db) -> None:
        _insert_failed(db, 3, "venue_unavailable")
        ctx, calls = _ctx_with_recorder()
        pipeline._maybe_alert_venue_degradation(
            db, ctx, error_kind="venue_unavailable",
            coin="BTC", sleeve="exp044_15m", wallet_id="w",
        )
        assert len(calls) == 1
        assert calls[0]["critical"] is True
        assert calls[0]["event_type"] == "clob_venue_degraded"
        assert "venue_unavailable" in calls[0]["message"]

    def test_post_only_counts_toward_threshold(self, db) -> None:
        # Mixed venue kinds both count; here 2 x 425 + 1 x 503 = 3.
        _insert_failed(db, 2, "venue_unavailable")
        _insert_failed(db, 1, "post_only")
        ctx, calls = _ctx_with_recorder()
        pipeline._maybe_alert_venue_degradation(
            db, ctx, error_kind="post_only",
            coin="ETH", sleeve="exp044_15m", wallet_id="w",
        )
        assert len(calls) == 1

    def test_below_threshold_no_alert(self, db) -> None:
        _insert_failed(db, 2, "venue_unavailable")
        ctx, calls = _ctx_with_recorder()
        pipeline._maybe_alert_venue_degradation(
            db, ctx, error_kind="venue_unavailable",
            coin="BTC", sleeve="exp044_15m", wallet_id="w",
        )
        assert calls == []

    def test_cooldown_dedups_repeated_fires(self, db) -> None:
        _insert_failed(db, 5, "venue_unavailable")
        ctx, calls = _ctx_with_recorder()
        for _ in range(3):
            pipeline._maybe_alert_venue_degradation(
                db, ctx, error_kind="venue_unavailable",
                coin="BTC", sleeve="exp044_15m", wallet_id="w",
            )
        # Cooldown suppresses the 2nd + 3rd within the window.
        assert len(calls) == 1

    def test_non_venue_kind_never_alerts(self, db) -> None:
        # A thin-book no_liquidity failure is expected, not alert-worthy —
        # even with a backlog of venue failures already recorded.
        _insert_failed(db, 5, "venue_unavailable")
        ctx, calls = _ctx_with_recorder()
        pipeline._maybe_alert_venue_degradation(
            db, ctx, error_kind="no_liquidity",
            coin="BTC", sleeve="exp044_15m", wallet_id="w",
        )
        assert calls == []

    def test_stale_failures_outside_window_ignored(self, db) -> None:
        _insert_failed(db, 5, "venue_unavailable", minutes_ago=30)
        ctx, calls = _ctx_with_recorder()
        pipeline._maybe_alert_venue_degradation(
            db, ctx, error_kind="venue_unavailable",
            coin="BTC", sleeve="exp044_15m", wallet_id="w",
        )
        assert calls == []
