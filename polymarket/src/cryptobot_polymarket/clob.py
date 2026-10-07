"""Polymarket CLOB order books over REST.

Endpoint: ``https://clob.polymarket.com/book?token_id=<id>``

Keeps the full asks and bids, sorted ascending, plus ``total_ask_depth_usd``
(dollar depth of the whole ask side), which the ``book_consumption_max`` gate
needs to match the backtest's full-book depth. ``walk_book._sorted_asks``
re-sorts anyway; sorting here makes logs easier to read.

Book age cannot be observed on this path
----------------------------------------
The ``/book`` response has no usable liveness signal (no sequence number, no
trusted hash), so ``BookSnapshot.fetched_at_ts`` is our local fetch time,
``book_top_ask_age_s`` is always ~0, and ``BOOK_STALENESS_S`` cannot catch a
stale REST book.

An unchanged book is not evidence of staleness either: quiet markets often
return the same book on every fetch, so deriving an age from "unchanged
contents" would block bets on perfectly live books. Real book age comes from
the WebSocket mirror (``ws_mirror``), which carries venue timestamps.

Rate limiting:

  - a per-IP token bucket (12 req/s, burst 12) on the single-token GET;
  - the tick collects every token it needs and makes one ``POST /books``
    batch call (``fetch_books_batch``), one request and one rate-limit token
    however many books; the per-token ``fetch_top_book`` GET is the
    fallback.
"""

from __future__ import annotations

import logging
import time

import httpx

# The book DTO and rate limiter are shared venue types in
# cryptobot.venues.types; re-exported here so there is still exactly one
# BookSnapshot class.
from cryptobot.venues.types import BookSnapshot, TokenBucket  # noqa: F401

logger = logging.getLogger(__name__)


CLOB_BASE = "https://clob.polymarket.com"


# 12 tokens / 1s + burst of 12 — supports a qualifying tick's 6 coins × 2
# sides without starving the second sleeve. The CLOB endpoint's documented
# rate limit is generous; the binding constraint in practice is Gamma at
# ~5 req/s (handled in polymarket_gamma.py).
_default_bucket = TokenBucket(rate_per_s=12.0, burst=12)


def _decode_levels(raw) -> tuple[dict[str, float], ...]:
    """Normalize one side's levels into a sorted (price ascending) tuple.

    Returns all valid levels; no local ask/bid level cap is applied.
    """
    if not isinstance(raw, list):
        return ()
    out: list[dict[str, float]] = []
    for level in raw:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level.get("price", 0))
            size = float(level.get("size", 0))
        except (TypeError, ValueError):
            continue
        if price <= 0 or size <= 0:
            continue
        out.append({"price": price, "size": size})
    out.sort(key=lambda d: d["price"])
    return tuple(out)


def fetch_top_book(
    token_id: str,
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    bucket: TokenBucket | None = None,
    now_ts: float | None = None,
) -> BookSnapshot | None:
    """Return the full sorted ask and bid snapshot for a token.

    Returns None on rate limit, HTTP error or an empty book.

    ``BookSnapshot.fetched_at_ts`` is ``now_ts``, the local fetch time, so a
    stale REST book cannot be detected from its age (see the module docstring).
    """
    bucket = bucket or _default_bucket
    if not bucket.take(1):
        logger.info("clob: rate-limited locally; skipping fetch for %s", token_id[:8])
        return None
    if now_ts is None:
        now_ts = time.time()

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    try:
        try:
            resp = client.get(f"{CLOB_BASE}/book", params={"token_id": token_id})
        except httpx.HTTPError as exc:
            logger.info("clob: http error %r", exc)
            return None
        if resp.status_code == 429:
            logger.info("clob: server 429 for %s — backing off", token_id[:8])
            bucket.notify_server_429()
            return None
        if resp.status_code >= 400:
            logger.warning("clob: status=%d", resp.status_code)
            return None
        try:
            payload = resp.json()
        except ValueError:
            return None
    finally:
        if owned_client:
            client.close()

    if not isinstance(payload, dict):
        return None

    return _snapshot_from_payload(payload, token_id=token_id, now_ts=now_ts)


def _snapshot_from_payload(
    payload: dict, *, token_id: str, now_ts: float
) -> BookSnapshot | None:
    """Build a BookSnapshot from one raw book payload (shared by the
    single-token GET and the A2 batch POST — identical parse semantics,
    including the empty-ask → None rule and total_ask_depth_usd formula)."""
    all_asks = _decode_levels(payload.get("asks", []))
    all_bids = _decode_levels(payload.get("bids", []))
    if not all_asks:
        return None

    # Full ask-book dollar depth (parity with the backtest's up_depth_l2 =
    # cumulative price*size over every ask level).
    total_ask_depth_usd = float(sum(lvl["price"] * lvl["size"] for lvl in all_asks))

    return BookSnapshot(
        token_id=token_id,
        asks=all_asks,
        bids=all_bids,
        fetched_at_ts=now_ts,
        total_ask_depth_usd=total_ask_depth_usd,
    )


def fetch_books_batch(
    token_ids: list[str] | tuple[str, ...],
    *,
    client: httpx.Client | None = None,
    timeout: float = 3.0,
    bucket: TokenBucket | None = None,
    now_ts: float | None = None,
) -> dict[str, BookSnapshot] | None:
    """Fetch every token's book in one ``POST /books`` call.

    Same wire format as py_clob_client_v2's ``get_order_books`` (body
    ``[{"token_id": <id>}, ...]``; each response item has ``asset_id`` and
    ``asks``/``bids``). Sent on our own persistent httpx client, so reads stay
    off the signing client.

    Returns ``{token_id: BookSnapshot}`` for tokens with a non-empty ask side
    (others are just missing, as if ``fetch_top_book`` had returned None), or
    ``None`` on a transport, HTTP or parse error so the caller can fall back to
    per-token GETs.

    One batch uses one rate-limit token however many books it covers.
    """
    ids = [str(t) for t in token_ids if t]
    if not ids:
        return {}
    bucket = bucket or _default_bucket
    if not bucket.take(1):
        logger.info("clob: rate-limited locally; skipping batch of %d", len(ids))
        return None
    if now_ts is None:
        now_ts = time.time()

    owned_client = client is None
    if client is None:
        client = httpx.Client(timeout=timeout)
    try:
        try:
            resp = client.post(
                f"{CLOB_BASE}/books",
                json=[{"token_id": t} for t in ids],
            )
        except httpx.HTTPError as exc:
            logger.info("clob: batch http error %r", exc)
            return None
        if resp.status_code == 429:
            logger.info("clob: server 429 on batch — backing off")
            bucket.notify_server_429()
            return None
        if resp.status_code >= 400:
            logger.warning("clob: batch status=%d", resp.status_code)
            return None
        try:
            payload = resp.json()
        except ValueError:
            return None
    finally:
        if owned_client:
            client.close()

    if not isinstance(payload, list):
        return None

    wanted = set(ids)
    out: dict[str, BookSnapshot] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        tid = str(item.get("asset_id") or item.get("token_id") or "")
        if not tid or tid not in wanted:
            continue
        snap = _snapshot_from_payload(item, token_id=tid, now_ts=now_ts)
        if snap is not None:
            out[tid] = snap
    return out
