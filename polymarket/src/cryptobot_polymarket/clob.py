"""Polymarket CLOB ask/bid snapshot via REST.

Endpoint: ``https://clob.polymarket.com/book?token_id=<id>``

Stores the full asks/bids sorted ascending PLUS ``total_ask_depth_usd``,
the dollar depth of the whole ask book — needed by the exp044
``book_consumption_max`` gate for parity with the backtest's full-book
``up_depth_l2``. ``walk_book._sorted_asks`` re-sorts defensively downstream,
but this adapter pre-sorts so logs are easier to read.

Book freshness is NOT observable on this path (audit w19oxqf24 #12)
------------------------------------------------------------------
The ``/book`` REST payload carries no usable server-side liveness signal: we
parse only ``asks`` and ``bids`` (price/size levels). There is no sequence
number and no content hash we trust, so ``BookSnapshot.fetched_at_ts`` is set
to the CALLER's ``now_ts`` — the LOCAL fetch time, not when the book itself
last changed on the venue. Consequently the downstream gates.py
``book_top_ask_age_s = now_ts - fetched_at_ts`` is effectively always ~0 and
the ``BOOK_STALENESS_S`` gate is a NO-OP for the REST path: a stale-but-
populated Polymarket book cannot be detected here.

We deliberately do NOT stamp ``fetched_at_ts`` from any first-seen / unchanged-
book heuristic. Unlike a frozen spot price (which is unambiguously a dead
feed — see ``data.binance`` audit #2), an order book that is byte-for-byte
unchanged across fetches is NORMAL for a quiet/illiquid 15m crypto market and
is NOT evidence of staleness; faking an age off "unchanged contents" would trip
``BOOK_STALENESS_S`` on perfectly live books and suppress legitimate bets. The
honest fix is to document the gap so the false sense of protection is removed
at the source. Real book-liveness detection requires the WebSocket book feed
(deferred below), which exposes monotonic update sequencing. TODO(audit #12):
wire book staleness off the WS sequence/timestamp once that feed lands.

Rate-limit defense (per plan):
  - per-IP token bucket (12 req/s, 12-token burst) on the single-token path
  - A2 latency: the orchestrator now collects every qualifying cell's token
    ids and issues ONE ``POST /books`` batch round trip per tick
    (``fetch_books_batch``) — one request and one rate-limit token
    regardless of book count. The per-token ``fetch_top_book`` GET remains
    as the fallback path. (A previous docstring here claimed a 167ms
    inter-call stagger — that was never implemented; calls were plain
    sequential blocking GETs.)

WebSocket book feed is deferred (per "Deferred for later").
"""

from __future__ import annotations

import logging
import time

import httpx

# Phase 2: the book DTO + rate limiter are the SHARED venue contract and now
# live in core (cryptobot.venues.types) — core's pipeline/scale_in/held_position_pass
# and the predict.fun adapter all need them, and defining them here made both
# depend on this package. Re-exported so this module's public surface is
# unchanged and there is still exactly ONE BookSnapshot class object.
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
    """Return the full sorted ask + bid snapshot for a token.

    Returns None on rate-limit, HTTP error, or empty book.

    NOTE (audit w19oxqf24 #12): ``BookSnapshot.fetched_at_ts`` is stamped with
    ``now_ts`` — the LOCAL fetch time, not a book-liveness timestamp. The
    ``/book`` payload exposes no trustworthy server sequence/hash, so the
    downstream ``book_top_ask_age_s`` gate cannot detect a stale-but-populated
    book on this REST path (it always sees age ~0). See the module docstring.
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
    """A2 latency: fetch EVERY token's book in ONE ``POST /books`` round trip.

    Wire format verified against py_clob_client_v2 1.0.1
    (``get_order_books`` → ``POST {CLOB_BASE}/books`` with body
    ``[{"token_id": <id>}, ...]``; response items carry ``asset_id`` +
    ``asks``/``bids`` level lists). We issue the POST first-party on the
    persistent httpx client — same trust boundary as ``fetch_top_book`` —
    so the read path stays off the signing client.

    Returns ``{token_id: BookSnapshot}`` for tokens the venue returned with
    a non-empty ask book (absent/empty tokens are simply missing — same
    semantics as ``fetch_top_book`` returning None for them). Returns
    ``None`` on transport/HTTP/parse error so the caller can fall back to
    per-token GETs.

    One batch consumes ONE rate-limit token regardless of book count.
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
