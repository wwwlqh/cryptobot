"""U7 Polymarket Gamma adapter tests."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from cryptobot_polymarket import gamma


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _market_dict(
    coin: str,
    end_ts: int,
    fee: float | None = 100.0,
    *,
    interval_min: int = 5,
    closed: bool = False,
    outcome_prices: list[str] | None = None,
    outcomes: list[str] | None = None,
    tokens: list[str] | None = None,
) -> dict:
    """Build a Gamma /markets response shape matching the real *-updown-*
    product (verified 2026-05-17). Slug is ``{coin}-updown-{interval}m-{start_ts}``
    where start_ts = end_ts - interval_min*60."""
    end_iso = datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat()
    start_ts = end_ts - interval_min * 60
    m = {
        "slug": f"{coin.lower()}-updown-{interval_min}m-{start_ts}",
        "conditionId": f"COND-{coin}-{end_ts}",
        "clobTokenIds": tokens or [f"YES-{coin}-{end_ts}", f"NO-{coin}-{end_ts}"],
        "outcomes": outcomes or ["Up", "Down"],
        "endDate": end_iso,
        "closed": closed,
    }
    if outcome_prices is not None:
        m["outcomePrices"] = outcome_prices
    if fee is not None:
        m["feeRateBps"] = fee
    return m


def test_finds_active_market_for_coin(db: sqlite3.Connection):
    end_ts = 1_715_923_500
    start_ts = end_ts - 300

    payload = [_market_dict("BTC", end_ts, fee=120.0)]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        m = gamma.fetch_active_market(
            "BTC",
            interval_min=5,
            window_start_ts=start_ts,
            conn=db,
            client=c,
            now_ts=start_ts,
        )

    assert m is not None
    assert m.coin == "BTC"
    assert m.window_end_ts == end_ts
    assert m.fee_rate_bps == pytest.approx(120.0)
    assert m.yes_token_id.startswith("YES-BTC")


def test_fee_rate_cached_after_first_fetch(db: sqlite3.Connection):
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    payload = [_market_dict("BTC", end_ts, fee=80.0)]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=start_ts, conn=db, client=c,
            now_ts=start_ts,
        )

    row = db.execute(
        "SELECT fee_rate_bps FROM fee_rate_cache WHERE market_token_id = ?",
        (f"YES-BTC-{end_ts}",),
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(80.0)


def test_cache_hit_used_when_market_omits_fee(db: sqlite3.Connection):
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    yes_token = f"YES-BTC-{end_ts}"
    # Seed cache with fetched_at aligned to the test's now_ts reference.
    fresh_iso = datetime.fromtimestamp(start_ts - 60, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    db.execute(
        "INSERT INTO fee_rate_cache (market_token_id, fee_rate_bps, fetched_at) "
        "VALUES (?, ?, ?)",
        (yes_token, 75.0, fresh_iso),
    )
    # Server returns market WITHOUT fee — adapter should use cache.
    payload = [_market_dict("BTC", end_ts, fee=None)]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        m = gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=start_ts, conn=db, client=c,
            now_ts=start_ts,
        )
    assert m is not None
    assert m.fee_rate_bps == pytest.approx(75.0)


def test_stale_cache_falls_back_to_default_when_fee_missing(db: sqlite3.Connection):
    """Verified 2026-05-17 on live Gamma: *-updown-* markets expose
    feeRateBps=null. The actual P&L math uses POLY_FEE_THETA via
    sweep_lib.unit_pnl regardless of this value, so a missing live fee
    plus a stale cache falls back to POLY_DEFAULT_FEE_RATE_BPS — R9's
    presence check is satisfied without blocking the bot from ever
    betting on the only product it scans."""
    from cryptobot import config
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    yes_token = f"YES-BTC-{end_ts}"
    # Stale cache: fetched_at is 12h before our `now_ts` reference.
    stale_iso = datetime.fromtimestamp(
        start_ts - 12 * 3600, tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")
    db.execute(
        "INSERT INTO fee_rate_cache (market_token_id, fee_rate_bps, fetched_at) "
        "VALUES (?, ?, ?)",
        (yes_token, 75.0, stale_iso),
    )
    payload = [_market_dict("BTC", end_ts, fee=None)]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        m = gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=start_ts, conn=db, client=c,
            now_ts=start_ts,
        )
    assert m is not None and not isinstance(m, gamma.FeeUnknown)
    assert m.fee_rate_bps == pytest.approx(config.POLY_DEFAULT_FEE_RATE_BPS)


def test_fee_cache_write_works_from_thread_local_connections(tmp_path):
    """SQLITE_MISUSE fix: the Pass-1 Gamma fan-out runs fetch_active_market on
    _clob_executor worker threads, each with its OWN thread-local connection
    (``deps.conn_factory`` = ThreadLocalConnections). The always-miss
    *-updown-* fee path does an INSERT ON CONFLICT + prune DELETE on that
    connection. Drive it concurrently from two thread-local connections to the
    same file DB and assert both fee rows persist — the shared-connection
    version raised sqlite3.InterfaceError ("bad parameter or other API misuse")
    on this exact path.
    """
    import threading

    from cryptobot.db import init_db
    from cryptobot.db.connection import ThreadLocalConnections, get_connection

    end_ts = 1_715_923_500
    start_ts = end_ts - 300

    db_path = tmp_path / "fee_threads.db"
    init_db(db_path).close()  # bootstrap schema once
    conns = ThreadLocalConnections(db_path)

    def handler(request: httpx.Request) -> httpx.Response:
        slug = request.url.params.get("slug", "")
        coin = slug.split("-", 1)[0].upper()
        # feeRateBps=null -> adapter defaults + writes fee_rate_cache.
        return httpx.Response(200, json=[_market_dict(coin, end_ts, fee=None)])

    errors: list[BaseException] = []

    def worker(coin: str) -> None:
        try:
            conn = conns.get()  # THIS thread's own connection
            with _client(handler) as c:
                m = gamma.fetch_active_market(
                    coin, interval_min=5, window_start_ts=start_ts,
                    conn=conn, client=c, now_ts=start_ts,
                )
            assert m is not None and not isinstance(m, gamma.FeeUnknown)
        except BaseException as exc:  # noqa: BLE001 — surface to the main thread
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(c,)) for c in ("BTC", "ETH")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    conns.close_all()

    assert not errors, f"thread-local fee write raised: {errors!r}"

    # Both fee rows persisted — read back through a fresh independent connection.
    verify = get_connection(db_path)
    try:
        rows = {
            r[0]
            for r in verify.execute(
                "SELECT market_token_id FROM fee_rate_cache"
            ).fetchall()
        }
    finally:
        verify.close()
    assert f"YES-BTC-{end_ts}" in rows
    assert f"YES-ETH-{end_ts}" in rows


def test_no_matching_market_returns_none(db: sqlite3.Connection):
    # Gamma returns an empty array when no market matches the slug.
    with _client(lambda r: httpx.Response(200, json=[])) as c:
        m = gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=1_715_923_200,
            conn=db, client=c, now_ts=1_715_923_200,
        )
    assert m is None


def test_uses_direct_slug_lookup(db: sqlite3.Connection):
    """Adapter must hit /markets?slug={coin}-updown-{interval}m-{start_ts}
    rather than paginating the active list (verified 2026-05-17 the
    active-list scan returns zero *-updown-* markets at any offset)."""
    seen_slugs: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        # Capture the slug param so the test can assert on it.
        seen_slugs.append(req.url.params.get("slug", ""))
        # Return a deterministic payload that matches the requested slug.
        end_ts = 1_715_923_500
        return httpx.Response(200, json=[_market_dict("BTC", end_ts, fee=100.0)])

    with _client(handler) as c:
        m = gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=1_715_923_200,
            conn=db, client=c, now_ts=1_715_923_200,
        )
    assert m is not None
    assert seen_slugs == ["btc-updown-5m-1715923200"]


def test_outcome_array_routes_up_to_yes_token(db: sqlite3.Connection):
    """Real Gamma response has outcomes=['Up','Down']. The bot's YES side
    must map to the 'Up' token, NOT positional [0]."""
    end_ts = 1_715_923_500
    payload = [{
        "slug": "btc-updown-5m-1715923200",
        "conditionId": "C",
        # Reverse order intentionally — Down first, Up second.
        "clobTokenIds": ["DOWN-TOKEN", "UP-TOKEN"],
        "outcomes": ["Down", "Up"],
        "endDate": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        "feeRateBps": 100.0,
    }]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        m = gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=1_715_923_200,
            conn=db, client=c, now_ts=1_715_923_200,
        )
    assert m is not None
    assert m.yes_token_id == "UP-TOKEN"
    assert m.no_token_id == "DOWN-TOKEN"


def test_http_error_returns_none(db: sqlite3.Connection):
    def handler(req):
        raise httpx.ConnectError("simulated")

    with _client(handler) as c:
        m = gamma.fetch_active_market(
            "BTC", interval_min=5, window_start_ts=1_000_000, conn=db, client=c,
        )
    assert m is None


def test_fetch_resolved_outcome_parses_gamma_outcome_prices():
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    payload = [
        _market_dict(
            "BTC",
            end_ts,
            closed=True,
            outcome_prices=["1", "0"],
        )
    ]
    seen_params = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen_params.append(dict(req.url.params))
        return httpx.Response(200, json=payload)

    with _client(handler) as c:
        result = gamma.fetch_resolved_outcome(
            "BTC", interval_min=5, window_start_ts=start_ts, client=c,
        )

    assert seen_params[0]["closed"] == "true"
    assert result.status == gamma.RESOLUTION_RESOLVED
    assert result.won_up == 1
    assert result.up_price == pytest.approx(1.0)
    assert result.source == "polymarket_gamma"


def test_fetch_resolved_outcome_respects_reversed_outcome_order():
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    payload = [
        _market_dict(
            "BTC",
            end_ts,
            closed=True,
            outcomes=["Down", "Up"],
            tokens=["DOWN-TOKEN", "UP-TOKEN"],
            outcome_prices=["1", "0"],
        )
    ]

    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        result = gamma.fetch_resolved_outcome(
            "BTC", interval_min=5, window_start_ts=start_ts, client=c,
        )

    assert result.status == gamma.RESOLUTION_RESOLVED
    assert result.won_up == 0
    assert result.up_price == pytest.approx(0.0)


def test_fetch_resolved_outcome_void_05_05_is_not_booked_as_down():
    """Audit P2: a 0.5/0.5 void/refund must NOT settle as a real DOWN result;
    it returns RESOLUTION_VOID (won_up=None) so the sweep leaves the row
    PENDING instead of fabricating a fake loss."""
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    payload = [
        _market_dict("BTC", end_ts, closed=True, outcome_prices=["0.5", "0.5"])
    ]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        result = gamma.fetch_resolved_outcome(
            "BTC", interval_min=5, window_start_ts=start_ts, client=c,
        )
    assert result.status == gamma.RESOLUTION_VOID
    assert result.status != gamma.RESOLUTION_RESOLVED
    assert result.won_up is None


def test_fetch_resolved_outcome_decisive_still_resolves():
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    payload = [_market_dict("BTC", end_ts, closed=True, outcome_prices=["1", "0"])]
    with _client(lambda r: httpx.Response(200, json=payload)) as c:
        result = gamma.fetch_resolved_outcome(
            "BTC", interval_min=5, window_start_ts=start_ts, client=c,
        )
    assert result.status == gamma.RESOLUTION_RESOLVED
    assert result.won_up == 1


def test_fetch_resolved_outcome_open_market_returns_open_status():
    end_ts = 1_715_923_500
    start_ts = end_ts - 300
    calls = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(dict(req.url.params))
        if req.url.params.get("closed") == "true":
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[_market_dict("BTC", end_ts, closed=False)])

    with _client(handler) as c:
        result = gamma.fetch_resolved_outcome(
            "BTC", interval_min=5, window_start_ts=start_ts, client=c,
        )

    assert [c.get("closed") for c in calls] == ["true", None]
    assert result.status == gamma.RESOLUTION_OPEN
    assert result.won_up is None
