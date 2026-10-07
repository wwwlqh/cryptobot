"""WebSocket book mirror: a daemon thread that mirrors the Polymarket CLOB
order book from the public market channel, so the decision path can read a
book from a local cache instead of making an inline ``POST /books`` call.

On the REST path ``BookSnapshot.fetched_at_ts`` is our local fetch time, so a
book's age is always ~0. The market channel stamps every ``book`` and
``price_change`` message with a server ``timestamp``, so this mirror sets
``fetched_at_ts`` from the venue clock and book age becomes meaningful.

Same interface as ``scheduler.book_prefetch.BookPrefetcher``, so it plugs into
``TickDeps.book_prefetch``:

  * ``note_tokens(tokens)``: the pipeline passes the current window's tokens;
    the thread subscribes to exactly that set, and any token that left the set
    is evicted from the cache (``evict_unsubscribed``, on by default).
  * ``get(token, *, now_ts)``: returns the cached ``BookSnapshot`` only while
    the mirror is live, i.e. the recv loop got a frame (any message, pings
    included) within ``max_age_ms``. Liveness is per connection, not per book,
    so a quiet but connected book keeps serving instead of forcing REST. A
    socket that stops delivering frames (half-open, or disconnected, which
    clears the cache) ages out and serves ``None``. ``fetched_at_ts`` still
    holds the venue timestamp, for the ``book_ws_age_ms_max`` telemetry and
    the optional ``max_book_age_ms`` gate (0 = off).

Connection liveness cannot show that a particular token is fresh: every frame
refreshes it, including pings and other tokens' acks. Evicting unsubscribed
tokens is what keeps an old window's book from pricing a live order.

On any error or disconnect the cache is cleared and ``get`` returns ``None``,
so the pipeline uses the REST batch. The mirror never places, cancels or gates
an order; it is a read-only cache, off by default
(``CRYPTOBOT_BOOK_WS_ENABLED``).

``websockets`` is imported lazily inside the connection loop, so the module
imports and unit-tests cleanly without the library and without a socket.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Optional

from cryptobot.venues.shadow_book import (  # noqa: F401  re-export
    ShadowBookComparator,
)
from cryptobot.venues.types import BookSnapshot
from cryptobot_polymarket.clob import _snapshot_from_payload

logger = logging.getLogger(__name__)


# Public market channel (book snapshots and price_change deltas). No auth
# needed; only the ``user`` channel needs an API key. Overridable via env.
DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _venue_ts(msg: dict, *, recv_ts: float) -> float:
    """Best-effort server timestamp (seconds) from a market-channel message.

    Polymarket sends ``timestamp`` in epoch milliseconds. Convert ms to s, pass
    second-scale values through, and fall back to the local receive time when
    the field is missing or garbage, so a snapshot never gets an absurd age.
    """
    raw = msg.get("timestamp")
    if raw is None:
        return float(recv_ts)
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return float(recv_ts)
    if val <= 0:
        return float(recv_ts)
    # Epoch ms -> seconds; values already in seconds pass through.
    if val > 1e12:
        return val / 1000.0
    return val


def _levels_as_payload(level_map: dict[float, float]) -> list[dict[str, float]]:
    """Turn a {price: size} map into the ``[{"price","size"}]`` shape that
    ``_snapshot_from_payload`` parses (it re-sorts and drops non-positive
    levels).
    """
    return [{"price": p, "size": s} for p, s in level_map.items() if s > 0]


class ClobBookWsMirror:
    """Daemon thread mirroring the CLOB market channel into a snapshot cache.

    ``get`` / ``note_tokens`` match ``BookPrefetcher``. All network work
    happens on the daemon thread; the pipeline only touches the lock-guarded
    cache.
    """

    # Source tag reported by BookSourceChain.get_tagged.
    source_name = "ws"

    def __init__(
        self,
        *,
        max_age_ms: int,
        url: str = DEFAULT_WS_URL,
        connect_factory=None,
        reconnect_backoff_s: float = 1.0,
        reconnect_backoff_max_s: float = 30.0,
        recv_timeout_s: float = 1.0,
        max_book_age_ms: int = 0,
        evict_unsubscribed: bool = True,
    ) -> None:
        self._url = url
        self._max_age_s = max(0.05, float(max_age_ms) / 1000.0)
        # Evict a token's cache entry and ladders as soon as it leaves the
        # subscribed set, so an old window's token serves None (and the tick
        # uses REST) instead of a stale ladder. Without this, rolled-off tokens
        # stayed cached forever, and since ``get()`` only checks connection
        # liveness they kept pricing live orders: books were measured seconds
        # to minutes old while the venue pushed ~800 msg/s. On by default since
        # it can only remove stale data; disable with
        # CRYPTOBOT_BOOK_WS_EVICT_UNSUBSCRIBED=0.
        self._evict_unsubscribed = bool(evict_unsubscribed)
        # Per-token venue-book age limit (0 = off). A safety net that rarely
        # fires with eviction on. Check the ws_age telemetry before arming it:
        # if books are seconds old, a ~400ms limit rejects nearly every hit,
        # which is just a slower way of turning the mirror off.
        self._max_book_age_s = max(0.0, float(max_book_age_ms) / 1000.0)
        # Injectable for tests: a zero-argument callable returning a
        # context-manager websocket connection. The default imports
        # ``websockets`` lazily.
        self._connect_factory = connect_factory
        self._backoff_base_s = max(0.05, float(reconnect_backoff_s))
        self._backoff_max_s = max(self._backoff_base_s, float(reconnect_backoff_max_s))
        self._recv_timeout_s = max(0.05, float(recv_timeout_s))

        self._lock = threading.Lock()
        self._cache: dict[str, BookSnapshot] = {}
        # Per-token price ladders, so price_change deltas apply to the last
        # full book.
        self._asks: dict[str, dict[float, float]] = {}
        self._bids: dict[str, dict[float, float]] = {}
        self._tokens: tuple[str, ...] = ()
        self._tokens_version = 0
        # Tokens the stall watchdog evicted, waiting for a full ``book`` frame
        # before deltas may rebuild them (see ``evict``). Empty unless the
        # watchdog acts.
        self._resync: set[str] = set()
        # Monotonic time of the last frame received (any message, including
        # pings and subscription acks). ``get()`` checks freshness against
        # this, so a quiet connected book keeps serving. None until the first
        # frame, and reset to None on disconnect (_clear_cache) so a dead
        # socket serves nothing.
        self._last_recv_monotonic: Optional[float] = None
        # Optional fast-trigger callback, fired once per frame that changed the
        # ask ladder, so an entry can be re-evaluated within ~20ms instead of
        # waiting for the next 1 Hz tick. None means never called.
        self._on_ask_update: Optional[Callable[[str, float], None]] = None

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- pipeline-facing API -------------------------------------------------

    def set_ask_update_callback(
        self, callback: Optional[Callable[[str, float], None]]
    ) -> None:
        """Register the ask-update callback (``None`` clears it).

        The callback runs on the recv thread, which feeds every book read in
        the process, so:

          * it must be O(1) and non-blocking (queue and return);
          * it must not call back into this mirror (it is fired after the lock is
            released, but ``get`` takes the same plain Lock);
          * a call does not mean ``get`` will serve the token: the rebuild may have
            dropped it, and ``get`` still applies its own freshness checks.
        """
        self._on_ask_update = callback

    def _fire_ask_update(self, token: str, venue_ts: float) -> None:
        """Call the ask-update callback. Must be called with ``self._lock``
        released.
        """
        cb = self._on_ask_update
        if cb is None:
            return
        try:
            cb(token, venue_ts)
        except Exception:  # noqa: BLE001 — a bad consumer must never kill the WS
            # Must not raise: an exception here would unwind to _run, whose
            # finally clears the whole cache, so one bad callback would push
            # every read to REST.
            logger.debug("ask-update callback raised", exc_info=True)

    def note_tokens(self, tokens: list[str]) -> None:
        """Record the current window's token set; the thread resubscribes.

        Also evicts any token that left the set (see ``_evict_unsubscribed`` in
        ``__init__``); otherwise an old window's ladder stays cached and keeps
        pricing orders.
        """
        deduped = tuple(dict.fromkeys(str(t) for t in tokens if t))
        with self._lock:
            if deduped == self._tokens:
                return
            if self._evict_unsubscribed:
                keep = set(deduped)
                for gone in [t for t in self._cache if t not in keep]:
                    self._cache.pop(gone, None)
                for gone in [t for t in self._asks if t not in keep]:
                    self._asks.pop(gone, None)
                for gone in [t for t in self._bids if t not in keep]:
                    self._bids.pop(gone, None)
                # A rolled-off token gets a fresh ``book`` when it is next
                # subscribed anyway; dropping it here keeps ``_resync`` bounded
                # to the live window.
                self._resync -= {t for t in self._resync if t not in keep}
            self._tokens = deduped
            self._tokens_version += 1

    def get(self, token: str, *, now_ts: float) -> Optional[BookSnapshot]:
        """Return the cached snapshot for ``token`` while the mirror is live.

        Freshness is the age of the last frame received
        (``_last_recv_monotonic``, set by every message), not the book's last
        change. An up/down book can hold the same ladder for many seconds on a
        healthy socket, and that ladder is valid. Only a socket that stopped
        delivering frames (half-open, or disconnected, which resets liveness to
        None) ages past ``max_age`` and serves nothing, so the tick falls back
        to REST.

        ``now_ts`` is not used for freshness (kept for the ``BookPrefetcher``
        interface); ``fetched_at_ts`` still holds the venue timestamp for the
        ``book_ws_age_ms_max`` telemetry.
        """
        with self._lock:
            snap = self._cache.get(token)
            last_recv = self._last_recv_monotonic
        if snap is None or last_recv is None:
            return None
        # Serve only while the socket is delivering frames. A quiet book on a
        # live socket passes; a frozen or half-open socket does not.
        if (time.monotonic() - float(last_recv)) > self._max_age_s:
            return None
        # Optional per-token book-age limit (0 = off). Uses the same quantity
        # the tick reports as book_ws_age_ms_max, so the setting and the
        # telemetry agree.
        if self._max_book_age_s > 0.0:
            venue_ts = getattr(snap, "fetched_at_ts", None)
            if venue_ts is None:
                return None
            if (float(now_ts) - float(venue_ts)) > self._max_book_age_s:
                return None
        return snap

    # -- watchdog-facing API --------------------------------------------------
    # The live deploy sets no freshness cap, because on these books snapshot
    # age reflects how quiet the market is, not feed lag. That leaves one case
    # the mirror cannot detect itself: a connected socket (pings keep it live)
    # where updates for one token silently stop. Disconnects already clear the
    # cache and rolled-off tokens are evicted, so
    # ``scheduler.book_ws_watchdog`` uses these two methods to cross-check
    # against REST. Neither is ever called from the tick or order path.

    def serving_snapshots(self) -> dict[str, BookSnapshot]:
        """Shallow copy of the tokens currently cached.

        ``BookSnapshot`` is frozen, so the copy is safe to read on another
        thread. This is what ``get`` reads before its freshness checks; the
        watchdog wants everything that could be served.
        """
        with self._lock:
            return dict(self._cache)

    def evict(self, token: str) -> bool:
        """Drop ``token``'s snapshot and ladders; return True if one was
        cached.

        Used by the stall watchdog on a confirmed WS/REST mismatch, so the next
        read falls through to REST.

        The token stays subscribed, so the next frame could rebuild it, and a
        ``price_change`` delta applied to empty ladders would build a partial
        book (only the levels that changed), which is worse than none. Eviction
        therefore also marks the token for resync: its deltas are dropped until
        a full ``book`` frame re-seeds the ladders. Until then ``get`` serves
        None and the tick uses REST.
        """
        with self._lock:
            found = self._cache.pop(token, None) is not None
            self._asks.pop(token, None)
            self._bids.pop(token, None)
            self._resync.add(token)
        return found

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="cryptobot-book-ws-mirror", daemon=True
        )
        self._thread.start()
        logger.info(
            "book WS mirror started (url=%s, max_age=%.0fms)",
            self._url,
            self._max_age_s * 1000.0,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # -- message handling (unit-tested directly, no socket) ------------------

    def _is_unsubscribed(self, token: str) -> bool:
        """True when ``token`` is no longer subscribed (caller holds
        ``_lock``).

        ``_apply_book`` / ``_apply_price_change`` accept any token the socket
        sends, so without this check the next frame would bring back a token
        ``note_tokens`` just evicted, and if the venue merges subscriptions
        rather than replacing them eviction would do nothing at all. Filtering
        on write makes eviction reliable either way, including during the
        resubscribe round trip.

        An empty ``_tokens`` (before the first note_tokens) accepts everything.
        """
        return bool(
            self._evict_unsubscribed and self._tokens and token not in self._tokens
        )

    def _clear_cache(self) -> None:
        with self._lock:
            self._cache = {}
            self._asks = {}
            self._bids = {}
            # A reconnect resubscribes and the venue sends a full ``book`` for
            # every token, which satisfies any pending watchdog resync; keeping
            # it would only drop deltas that arrive before that book.
            self._resync = set()
            # Clear liveness on disconnect or error, so get() serves nothing
            # until the reconnected loop receives a frame.
            self._last_recv_monotonic = None

    def _rebuild(self, token: str, *, venue_ts: float) -> None:
        """Rebuild and store the frozen snapshot for ``token`` from its
        ladders.

        Uses the REST parser, so depth math and the empty-ask rule are
        identical to the batch path and snapshots from either source are
        interchangeable.
        """
        payload = {
            "asks": _levels_as_payload(self._asks.get(token, {})),
            "bids": _levels_as_payload(self._bids.get(token, {})),
        }
        snap = _snapshot_from_payload(payload, token_id=token, now_ts=venue_ts)
        if snap is None:
            # Ask side is empty: drop the snapshot so a stale one cannot keep
            # serving.
            self._cache.pop(token, None)
        else:
            self._cache[token] = snap

    def _apply_book(self, msg: dict, *, recv_ts: float) -> Optional[str]:
        token = str(msg.get("asset_id") or msg.get("token_id") or "")
        if not token:
            return None
        venue_ts = _venue_ts(msg, recv_ts=recv_ts)
        asks_raw = msg.get("asks")
        if asks_raw is None:
            asks_raw = msg.get("sells", [])
        bids_raw = msg.get("bids")
        if bids_raw is None:
            bids_raw = msg.get("buys", [])
        asks: dict[float, float] = {}
        bids: dict[float, float] = {}
        for lvl in asks_raw if isinstance(asks_raw, list) else []:
            price, size = _level_pair(lvl)
            if price is not None:
                asks[price] = size
        for lvl in bids_raw if isinstance(bids_raw, list) else []:
            price, size = _level_pair(lvl)
            if price is not None:
                bids[price] = size
        with self._lock:
            if self._is_unsubscribed(token):
                return None
            # A full ``book`` frame re-seeds both ladders, which is what a
            # watchdog eviction was waiting for.
            self._resync.discard(token)
            self._asks[token] = asks
            self._bids[token] = bids
            self._rebuild(token, venue_ts=venue_ts)
        # A ``book`` frame always replaces the ask ladder. Fired after the
        # rebuild with the lock released, so the callback sees the new snapshot
        # and cannot deadlock on self._lock.
        self._fire_ask_update(token, venue_ts)
        return token

    def _apply_price_change(self, msg: dict, *, recv_ts: float) -> Optional[str]:
        """Apply one ``price_change`` frame. These are scoped to the market,
        not to one token.

        Wire format (sampled live on open up/down books; top-level ``asset_id``
        was absent in every frame)::

            {"market": "0x...", "event_type": "price_change", "timestamp": "...",
             "price_changes": [
                {"asset_id": "<YES tok>", "price": "0.01", "size": "7933.84",
                 "side": "BUY",  "hash": "...", "best_bid": "...", "best_ask": "..."},
                {"asset_id": "<NO tok>",  "price": "0.99", "size": "7933.84",
                 "side": "SELL", ...}]}

        One frame carries changes for both tokens of the market, and the token
        id is on each change. Reading it only from the top level drops every
        delta, leaving the mirror updated only by full ``book`` frames, which
        can be seconds apart.

        The token-scoped shape (top-level ``asset_id`` plus ``changes``) is
        still accepted: a change without its own ``asset_id`` uses the frame's.

        Returns one touched token (truthy means a delta was applied), matching
        ``_apply_book``.
        """
        changes = msg.get("changes")
        if not isinstance(changes, list):
            changes = msg.get("price_changes")
        if not isinstance(changes, list):
            return None
        frame_token = str(msg.get("asset_id") or msg.get("token_id") or "")
        # Group first so each token's ladder is rebuilt once per frame.
        by_token: dict[str, list[dict]] = {}
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            token = str(ch.get("asset_id") or ch.get("token_id") or frame_token)
            if not token:
                continue
            by_token.setdefault(token, []).append(ch)
        if not by_token:
            return None
        venue_ts = _venue_ts(msg, recv_ts=recv_ts)

        touched: Optional[str] = None
        dirty_asks: list[str] = []
        with self._lock:
            for token, token_changes in by_token.items():
                if self._is_unsubscribed(token):
                    continue
                if token in self._resync:
                    # Evicted by the watchdog: a delta on empty ladders would
                    # build a partial book. Serve nothing (REST is used) until
                    # a full ``book`` frame arrives.
                    continue
                asks = self._asks.setdefault(token, {})
                bids = self._bids.setdefault(token, {})
                ask_dirty = False
                for ch in token_changes:
                    price, size = _level_pair(ch)
                    if price is None:
                        continue
                    side = str(ch.get("side", "")).upper()
                    book = asks if side in ("SELL", "ASK") else bids
                    if book is asks:
                        ask_dirty = True
                    if size <= 0:
                        book.pop(price, None)
                    else:
                        book[price] = size
                self._rebuild(token, venue_ts=venue_ts)
                touched = token
                if ask_dirty:
                    dirty_asks.append(token)
        # Bid-only changes do not fire the fast trigger, since entries are
        # priced off the ask. Uses the same side classification as the ladder
        # update (a missing or garbage ``side`` counts as a bid), so the two
        # always agree. Fired outside the lock, once per token whose ask ladder
        # changed.
        for token in dirty_asks:
            self._fire_ask_update(token, venue_ts)
        return touched

    def _handle_message(self, raw, *, recv_ts: Optional[float] = None) -> None:
        """Parse one raw frame (str, bytes, dict, or a list of them) and apply
        it.

        An unparseable or unknown message is ignored rather than ending the
        stream.
        """
        if recv_ts is None:
            recv_ts = time.time()
        # Record liveness before parsing: even an unparseable frame proves the
        # socket is delivering. ``get()`` checks freshness against this.
        with self._lock:
            self._last_recv_monotonic = time.monotonic()
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                return
        events = raw if isinstance(raw, list) else [raw]
        for msg in events:
            if not isinstance(msg, dict):
                continue
            etype = str(msg.get("event_type") or msg.get("type") or "")
            if etype == "book":
                self._apply_book(msg, recv_ts=recv_ts)
            elif etype == "price_change":
                self._apply_price_change(msg, recv_ts=recv_ts)
            # PONG, subscription ack or unknown: ignore.

    # -- connection loop -----------------------------------------------------

    def _open_connection(self):
        if self._connect_factory is not None:
            return self._connect_factory()
        # Lazy import keeps module import and unit tests free of the library
        # and of sockets. The sync API needs websockets >= 13.
        from websockets.sync.client import connect

        return connect(
            self._url,
            open_timeout=10.0,
            close_timeout=2.0,
            max_queue=64,
        )

    def _subscribe(self, ws, tokens: tuple[str, ...]) -> None:
        if not tokens:
            return
        ws.send(json.dumps({"assets_ids": list(tokens), "type": "market"}))

    def _stream_once(self) -> None:
        """One connect -> subscribe -> consume session. Returns on stop or
        error.
        """
        conn = self._open_connection()
        with conn as ws:
            local_version = -1
            while not self._stop.is_set():
                with self._lock:
                    tokens = self._tokens
                    version = self._tokens_version
                if version != local_version:
                    # The token set changed (new window): subscribe to the full
                    # set. Tokens that left it were already evicted by
                    # ``note_tokens``, so they serve None (REST) rather than a
                    # frozen ladder.
                    self._subscribe(ws, tokens)
                    local_version = version
                try:
                    raw = ws.recv(timeout=self._recv_timeout_s)
                except TimeoutError:
                    # No message this interval; loop to re-check stop and
                    # tokens.
                    continue
                self._handle_message(raw, recv_ts=time.time())

    def _run(self) -> None:
        backoff = self._backoff_base_s
        while not self._stop.is_set():
            try:
                self._stream_once()
                # Clean return (stop requested): exit without backoff.
                if self._stop.is_set():
                    break
                backoff = self._backoff_base_s
            except Exception:  # noqa: BLE001 — any WS error → fall back to REST
                logger.debug("book WS mirror stream error", exc_info=True)
            finally:
                # On any disconnect or error, serve nothing so the pipeline
                # uses the REST batch, never a stale book.
                self._clear_cache()
            if self._stop.wait(timeout=backoff):
                break
            backoff = min(self._backoff_max_s, backoff * 2.0)


def _level_pair(lvl: object) -> tuple[Optional[float], float]:
    """Extract (price, size) floats from a raw level or change dict.

    Returns ``(None, 0.0)`` when price is missing or garbage; size defaults to
    0 (a delta with size 0 removes the level).
    """
    if not isinstance(lvl, dict):
        return None, 0.0
    try:
        price = float(lvl.get("price"))
    except (TypeError, ValueError):
        return None, 0.0
    try:
        size = float(lvl.get("size", 0) or 0)
    except (TypeError, ValueError):
        size = 0.0
    return price, size
