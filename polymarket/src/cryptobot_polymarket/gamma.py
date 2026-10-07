"""Polymarket Gamma API adapter — active market discovery + feeRateBps cache.

Endpoint: ``https://gamma-api.polymarket.com``

Security trust boundary (per plan): first-party httpx only, no
third-party ``polymarket*`` PyPI dependency. CI lints `pyproject.toml`
to enforce.

`fetch_active_market(coin, interval, window_start_ts)` returns the
market metadata (`condition_id`, `yes_token_id`, `no_token_id`,
`fee_rate_bps`). On miss → cached → fetched. fee_rate_bps cache has
a 6h TTL.
"""

from __future__ import annotations

import json as _json
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


# Phase 2: the market/settlement contract (FeeUnknown, the RESOLUTION_*
# vocabulary, PolymarketResolution, DECISIVE_EPSILON) moved to core
# (cryptobot.venues.types). FeeUnknown especially MUST be one class object:
# the pipeline gates with isinstance() venue-agnostically, and predict.fun
# returns the same sentinel. Re-exported so this module's surface is
# unchanged; only the FEE_UNKNOWN *instance* is per-venue.
#
# These names are IMPORTED, never re-defined here. Phase 2 originally left the
# old local literals in place below the import, which shadowed all seven of them
# — the imports were dead and the two copies could drift, so a producer here and
# a consumer in core could disagree about the same status string with nothing to
# catch it. Values were proven identical before the duplicates were deleted.
from cryptobot.venues.types import (  # noqa: F401  re-export
    DECISIVE_EPSILON,
    RESOLUTION_BAD_PAYLOAD,
    RESOLUTION_HTTP_ERROR,
    RESOLUTION_NOT_FOUND,
    RESOLUTION_OPEN,
    RESOLUTION_RESOLVED,
    RESOLUTION_VOID,
    FeeUnknown,
    PolymarketResolution,
)

FEE_UNKNOWN = FeeUnknown()

GAMMA_BASE = "https://gamma-api.polymarket.com"
FEE_TTL_S = 6 * 3600


@dataclass(frozen=True)
class PolyMarket:
    coin: str
    interval_min: int
    window_start_ts: int
    window_end_ts: int
    condition_id: str
    yes_token_id: str
    no_token_id: str
    fee_rate_bps: float


def _decode_jsonish(value):
    if isinstance(value, str):
        try:
            return _json.loads(value)
        except (ValueError, TypeError):
            return None
    return value


# ---------------------------------------------------------------------------
# A2 latency: in-process per-window market cache.
#
# A resolved market's IDENTITY (condition_id / token ids / window bounds) is
# immutable for the lifetime of its window, but fetch_active_market was hit
# once per qualifying cell per tick — re-resolving the same slug over Gamma
# dozens-to-hundreds of times per window (worse still at sub-minute
# checkpoint cadence). Cache SUCCESSFUL resolutions until window_end + a
# grace; never cache None/FeeUnknown (market may simply not be listed yet —
# the per-tick retry is the desired behavior).
# ---------------------------------------------------------------------------
import threading as _threading

_MARKET_CACHE: dict[tuple[str, int, int], PolyMarket] = {}
_MARKET_CACHE_LOCK = _threading.Lock()
_MARKET_CACHE_GRACE_S = 120.0


def clear_market_cache() -> None:
    """Test/ops hook: drop every cached market resolution. The tests'
    autouse fixture calls this so the module-level cache can never leak
    state between tests that reuse the same (coin, interval, window)."""
    with _MARKET_CACHE_LOCK:
        _MARKET_CACHE.clear()


def _market_cache_get(
    coin: str, interval_min: int, window_start_ts: int
) -> Optional[PolyMarket]:
    with _MARKET_CACHE_LOCK:
        return _MARKET_CACHE.get((coin, int(interval_min), int(window_start_ts)))


def market_cache_has(coin: str, interval_min: int, window_start_ts: int) -> bool:
    """Public probe for the pipeline's A5 next-window prefetch: True when the
    (coin, interval, window) resolution is already cached (no GET needed)."""
    return _market_cache_get(coin, interval_min, window_start_ts) is not None


def _market_cache_put(market: PolyMarket, now_ts: float) -> None:
    key = (market.coin, int(market.interval_min), int(market.window_start_ts))
    with _MARKET_CACHE_LOCK:
        # Opportunistic prune: drop entries whose window ended before the
        # grace horizon. The working set is tiny (coins × sleeves × ~2
        # windows) so a full scan is cheap.
        expired = [
            k
            for k, m in _MARKET_CACHE.items()
            if float(m.window_end_ts) + _MARKET_CACHE_GRACE_S < float(now_ts)
        ]
        for k in expired:
            _MARKET_CACHE.pop(k, None)
        _MARKET_CACHE[key] = market


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes")
    return bool(value)


def _get_fee_cached(conn: sqlite3.Connection, token_id: str, now: float) -> Optional[float]:
    try:
        row = conn.execute(
            "SELECT fee_rate_bps, fetched_at FROM fee_rate_cache "
            "WHERE market_token_id = ?",
            (token_id,),
        ).fetchone()
    except sqlite3.OperationalError as exc:
        # Schema mismatch (table missing, column renamed) — surface, don't swallow.
        logger.warning("gamma: fee_rate_cache query failed: %r", exc)
        return None
    if row is None:
        return None
    # fetched_at is text in SQL-text format ('YYYY-MM-DD HH:MM:SS'). Narrow
    # the except: a missing column or unexpected schema should surface, not
    # silently turn into a cache miss.
    try:
        ts = datetime.fromisoformat(row["fetched_at"].replace(" ", "T")).replace(
            tzinfo=timezone.utc
        )
    except (ValueError, AttributeError):
        return None
    age = now - ts.timestamp()
    if age > FEE_TTL_S:
        return None
    return float(row["fee_rate_bps"])


# Rows are pure cache (TTL = FEE_TTL_S for reads). Each new 15m market window
# mints a fresh token_id, so without eviction the table grows ~thousands of
# rows/day on the same SQLite file that carries the live ledger. Prune entries
# well past the read-TTL opportunistically on write (cheap: one bounded DELETE,
# at most once per market window).
FEE_CACHE_RETENTION_S = 7 * 24 * 3600


def _store_fee(conn: sqlite3.Connection, token_id: str, fee_rate_bps: float) -> None:
    conn.execute(
        "INSERT INTO fee_rate_cache (market_token_id, fee_rate_bps, fetched_at) "
        "VALUES (?, ?, datetime('now')) "
        "ON CONFLICT(market_token_id) DO UPDATE SET "
        "fee_rate_bps=excluded.fee_rate_bps, fetched_at=excluded.fetched_at",
        (token_id, fee_rate_bps),
    )
    try:
        conn.execute(
            "DELETE FROM fee_rate_cache "
            "WHERE fetched_at < datetime('now', ?)",
            (f"-{int(FEE_CACHE_RETENTION_S)} seconds",),
        )
    except sqlite3.OperationalError as exc:
        # Pruning is best-effort housekeeping; never let it break a fee write.
        logger.warning("gamma: fee_rate_cache prune failed: %r", exc)


def fetch_active_market(
    coin: str,
    *,
    interval_min: int,
    window_start_ts: int,
    conn: sqlite3.Connection,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    now_ts: float | None = None,
) -> Optional[PolyMarket] | FeeUnknown:
    """Resolve the Polymarket *-updown-* market for (coin, interval, window).

    Direct slug GET (replaces the previous active-list scan that paginated
    /markets?active=true&closed=false and filtered client-side — verified
    2026-05-17 that strategy returns zero updown markets at any offset
    because the global active list is sorted by volume and the short-window
    crypto product falls well below the per-page cap).

    Slug convention (confirmed against the live Gamma API):
        ``{coin.lower()}-updown-{interval_min}m-{window_start_ts}``
    where ``window_start_ts`` is the unix-epoch START of the market window
    (= the backtest's ``slug_ts``). endDate on the returned market equals
    ``window_start_ts + interval_min*60`` exactly.

    Returns:
      - ``PolyMarket`` on success
      - ``FEE_UNKNOWN`` sentinel if the market is found but feeRateBps is
        absent AND no cache + no default is in play (review #52)
      - ``None`` if the market doesn't exist yet, HTTP fails, or the
        endDate doesn't match the expected window
    """
    if now_ts is None:
        now_ts = time.time()
    window_end_ts = window_start_ts + interval_min * 60

    # A2 latency: market identity is immutable per (coin, interval, window) —
    # serve repeats from the in-process cache instead of re-resolving the
    # same slug over Gamma every tick. Only SUCCESSFUL resolutions are
    # cached; None/FeeUnknown keep the per-tick retry semantics.
    cached_market = _market_cache_get(coin, interval_min, window_start_ts)
    if cached_market is not None:
        return cached_market

    slug = f"{coin.lower()}-updown-{interval_min}m-{window_start_ts}"

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)

    try:
        try:
            resp = client.get(
                f"{GAMMA_BASE}/markets", params={"slug": slug},
            )
        except httpx.HTTPError as exc:
            logger.info("gamma: http error %r for slug=%s", exc, slug)
            return None
        if resp.status_code >= 400:
            logger.warning("gamma: status=%d for slug=%s", resp.status_code, slug)
            return None
        try:
            markets = resp.json()
        except ValueError:
            return None
    finally:
        if owned_client:
            client.close()

    if not isinstance(markets, list) or not markets:
        # Market not listed yet (the bot ticks faster than Polymarket can
        # publish the next window's market). Gate stack maps to SKIP:stale_book.
        return None

    m = markets[0]
    if not isinstance(m, dict):
        return None

    # Sanity: confirm endDate matches the expected window (defends against
    # a slug collision or Polymarket renaming the product).
    end_iso = m.get("endDate") or m.get("end_date_iso")
    if not end_iso:
        logger.info("gamma: market %s has no endDate", slug)
        return None
    try:
        end_dt = datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)
    if abs(end_dt.timestamp() - window_end_ts) > 60:
        logger.warning(
            "gamma: endDate mismatch for %s — got %s, expected window_end_ts=%d",
            slug, end_iso, window_end_ts,
        )
        return None

    cond = str(m.get("conditionId", ""))
    if not cond:
        return None

    tokens_raw = m.get("clobTokenIds")
    if isinstance(tokens_raw, str):
        try:
            tokens_raw = _json.loads(tokens_raw)
        except (ValueError, TypeError):
            return None
    if not isinstance(tokens_raw, list) or len(tokens_raw) < 2:
        return None

    # Match tokens by outcome name — Polymarket exposes `outcomes: ["Up","Down"]`
    # parallel to `clobTokenIds`. Verified case-insensitive: UP/YES → yes_token,
    # DOWN/NO → no_token (review #17).
    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = _json.loads(outcomes)
        except (ValueError, TypeError):
            outcomes = None
    yes_token = no_token = None
    if isinstance(outcomes, list) and len(outcomes) == len(tokens_raw):
        for outcome, tok in zip(outcomes, tokens_raw):
            o = str(outcome).strip().upper()
            if o in ("YES", "UP") and yes_token is None:
                yes_token = str(tok)
            elif o in ("NO", "DOWN") and no_token is None:
                no_token = str(tok)
    if yes_token is None or no_token is None:
        # Fail closed instead of guessing positional order. If the `outcomes`
        # array can't map names→tokens, a reversed clobTokenIds array would
        # make the bot buy the OPPOSITE side it scored (guaranteed adverse
        # fill, not slippage). The pipeline tolerates None as SKIP:stale_book
        # and re-resolves next tick, so refusing an unverifiable mapping is
        # strictly safer than a positional guess.
        logger.warning(
            "gamma: outcomes array not usable for %s — refusing to map "
            "tokens positionally; skipping market this tick",
            slug,
        )
        return None

    # Fee resolution. Verified 2026-05-17: every *-updown-* market exposes
    # feeRateBps=null on Gamma. The actual P&L math uses POLY_FEE_THETA via
    # the theta-shape formula regardless of this value, so fall back to
    # the configured default (config.POLY_DEFAULT_FEE_RATE_BPS) and cache it
    # — R9's fail-closed presence check is satisfied, backtest parity holds.
    fee_raw = m.get("feeRateBps")
    if fee_raw is None:
        cached = _get_fee_cached(conn, yes_token, now_ts)
        if cached is not None:
            fee_rate = cached
        else:
            from cryptobot import config as _cfg
            fee_rate = float(_cfg.POLY_DEFAULT_FEE_RATE_BPS)
            logger.info(
                "gamma: %s feeRateBps=null — defaulting to "
                "POLY_DEFAULT_FEE_RATE_BPS=%.1f (math uses POLY_FEE_THETA)",
                slug, fee_rate,
            )
            _store_fee(conn, yes_token, fee_rate)
    else:
        try:
            fee_rate = float(fee_raw)
        except (TypeError, ValueError):
            return FEE_UNKNOWN
        _store_fee(conn, yes_token, fee_rate)

    market = PolyMarket(
        coin=coin,
        interval_min=interval_min,
        window_start_ts=window_start_ts,
        window_end_ts=window_end_ts,
        condition_id=cond,
        yes_token_id=yes_token,
        no_token_id=no_token,
        fee_rate_bps=fee_rate,
    )
    _market_cache_put(market, float(now_ts))
    return market


def fetch_resolved_outcome(
    coin: str,
    *,
    interval_min: int,
    window_start_ts: int,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
) -> PolymarketResolution:
    """Fetch Gamma's final UP/DOWN outcome for a market.

    The live ledger should settle against Polymarket's actual market result
    whenever Gamma has closed ``outcomePrices``. We first ask for
    ``closed=true`` so resolved markets are found even after they leave the
    active listing. If that returns empty, we retry the plain slug query to
    distinguish "market still open" from "not found".
    """
    window_end_ts = window_start_ts + interval_min * 60
    slug = f"{coin.lower()}-updown-{interval_min}m-{window_start_ts}"

    def _make_resolution(status: str, **kwargs) -> PolymarketResolution:
        """Build a PolymarketResolution with the fixed window fields filled."""
        return PolymarketResolution(
            status=status,
            coin=coin,
            interval_min=interval_min,
            window_start_ts=window_start_ts,
            window_end_ts=window_end_ts,
            **kwargs,
        )

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    try:
        market: dict | None = None
        for params in ({"slug": slug, "closed": "true"}, {"slug": slug}):
            try:
                resp = client.get(f"{GAMMA_BASE}/markets", params=params)
            except httpx.HTTPError as exc:
                logger.info("gamma resolution: http error %r for slug=%s", exc, slug)
                return _make_resolution(RESOLUTION_HTTP_ERROR)
            if resp.status_code >= 400:
                logger.warning(
                    "gamma resolution: status=%d for slug=%s",
                    resp.status_code,
                    slug,
                )
                return _make_resolution(RESOLUTION_HTTP_ERROR)
            try:
                payload = resp.json()
            except ValueError:
                return _make_resolution(RESOLUTION_BAD_PAYLOAD)
            if isinstance(payload, list) and payload:
                if isinstance(payload[0], dict):
                    market = payload[0]
                    break
                return _make_resolution(RESOLUTION_BAD_PAYLOAD)
    finally:
        if owned_client:
            client.close()

    if market is None:
        return _make_resolution(RESOLUTION_NOT_FOUND)

    condition_id = str(market.get("conditionId", "")) or None
    end_iso = market.get("endDate") or market.get("end_date_iso")
    if end_iso:
        try:
            end_dt = datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            if abs(end_dt.timestamp() - window_end_ts) > 60:
                logger.warning(
                    "gamma resolution: endDate mismatch for %s — got %s, "
                    "expected window_end_ts=%d",
                    slug,
                    end_iso,
                    window_end_ts,
                )
                return _make_resolution(
                    RESOLUTION_BAD_PAYLOAD, condition_id=condition_id
                )
        except (TypeError, ValueError):
            return _make_resolution(
                RESOLUTION_BAD_PAYLOAD, condition_id=condition_id
            )

    if not _as_bool(market.get("closed")):
        return _make_resolution(RESOLUTION_OPEN, condition_id=condition_id)

    outcomes = _decode_jsonish(market.get("outcomes"))
    prices = _decode_jsonish(market.get("outcomePrices"))
    if (
        not isinstance(outcomes, list)
        or not isinstance(prices, list)
        or len(outcomes) != len(prices)
        or len(outcomes) < 2
    ):
        return _make_resolution(
            RESOLUTION_BAD_PAYLOAD, condition_id=condition_id
        )

    up_idx = down_idx = None
    for i, outcome in enumerate(outcomes):
        normalized = str(outcome).strip().upper()
        if normalized in ("UP", "YES") and up_idx is None:
            up_idx = i
        elif normalized in ("DOWN", "NO") and down_idx is None:
            down_idx = i
    if up_idx is None:
        return _make_resolution(
            RESOLUTION_BAD_PAYLOAD, condition_id=condition_id
        )

    try:
        up_price = float(prices[up_idx])
        down_price = float(prices[down_idx]) if down_idx is not None else None
    except (TypeError, ValueError):
        return _make_resolution(
            RESOLUTION_BAD_PAYLOAD, condition_id=condition_id
        )
    if down_price is not None and abs(up_price + down_price - 1.0) > 0.01:
        return _make_resolution(
            RESOLUTION_BAD_PAYLOAD,
            condition_id=condition_id,
            up_price=up_price,
            down_price=down_price,
        )

    # Require a DECISIVE outcome before booking a win/loss. A void/refund or
    # exact tie settles 0.5/0.5 (which passes the consistency check above);
    # treating up_price>0.5 as the only test would silently book it as a real
    # DOWN result and corrupt realized PnL / drawdown accounting.
    if not (up_price >= 1.0 - DECISIVE_EPSILON or up_price <= DECISIVE_EPSILON):
        logger.warning(
            "gamma resolution: indecisive outcomePrices for %s "
            "(up_price=%.4f) — treating as VOID, leaving unresolved",
            slug,
            up_price,
        )
        return _make_resolution(
            RESOLUTION_VOID,
            condition_id=condition_id,
            up_price=up_price,
            down_price=down_price,
        )

    return _make_resolution(
        RESOLUTION_RESOLVED,
        condition_id=condition_id,
        won_up=1 if up_price > 0.5 else 0,
        up_price=up_price,
        down_price=down_price,
    )
