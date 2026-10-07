"""WS book mirror — a daemon thread that mirrors the Polymarket CLOB order
book over the public *market* WebSocket channel so the decision path can read
a fresh snapshot from a local cache instead of paying the inline
``POST /books`` round-trip (observed 1-3s spikes → stale-book decisions and
FAK rejects).

Why this exists (audit #12 closeout)
------------------------------------
``polymarket_clob.BookSnapshot.fetched_at_ts`` on the REST path is the CALLER's
local fetch time, not a venue liveness signal, so ``gates.book_top_ask_age_s``
is always ~0 and ``BOOK_STALENESS_S`` is a no-op there. The market channel
pushes a server ``timestamp`` on every ``book`` / ``price_change`` message, so
this mirror finally stamps ``fetched_at_ts`` from the VENUE clock — making book
staleness real for the first time.

Contract (identical to ``scheduler.book_prefetch.BookPrefetcher`` so it drops
into the same ``TickDeps.book_prefetch`` seam):
  * ``note_tokens(tokens)`` — the pipeline pushes the current window's token
    set; the thread (re)subscribes to exactly that set, and any token that just
    LEFT the set is evicted from the cache (``evict_unsubscribed``, default on).
  * ``get(token, *, now_ts)`` — returns the cached ``BookSnapshot`` iff the
    mirror is LIVE: the recv loop received a frame (book / price_change / ping /
    any message) within ``max_age_ms``. Freshness is keyed on RECV-LIVENESS, not
    the venue book's last-change ts, so a quiet-but-connected ``*-updown-*`` book
    (which legitimately holds the same ladder for seconds) keeps serving instead
    of forcing REST every checkpoint. A socket that stops delivering frames
    (half-open, or disconnected → cache+liveness cleared) ages out and serves
    ``None``. ``fetched_at_ts`` still carries the VENUE timestamp so the tick's
    ``book_ws_age_ms_max`` telemetry can measure venue-book age against it, and
    ``max_book_age_ms`` (default 0 = off) can gate on that same quantity.

⚠️ Recv-liveness is a CONNECTION-level signal and cannot, on its own, tell you a
particular token is fresh — every frame on the socket re-stamps it, including
pings and other tokens' subscription acks. That is why eviction-on-unsubscribe
(not liveness) is what keeps a rolled-off window from pricing a live order.

Fail-safe semantics: on ANY error / disconnect the cache is cleared and
``get`` returns ``None``, so the pipeline falls through to the REST batch
exactly as today. The mirror never places, cancels, or gates an order — it is
a read-only latency cache with a kill switch (``CRYPTOBOT_BOOK_WS_ENABLED``,
default off).

The ``websockets`` dependency is imported LAZILY inside the connection loop so
this module imports (and unit-tests) cleanly on a host without the library and
without opening any socket.
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


# Public market channel — book snapshots + price_change deltas; NO auth
# required (only the `user` channel needs an L2 API key). Overridable via env
# for staging/replays.
DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _venue_ts(msg: dict, *, recv_ts: float) -> float:
    """Best-effort server timestamp (seconds) from a market-channel message.

    Polymarket stamps ``timestamp`` in epoch MILLIseconds (13 digits). We
    normalize ms→s, pass through second-scale values, and fall back to the
    local receive time when the field is absent/garbage so a snapshot is
    never stamped with an obviously-wrong (0 / far-future) age.
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
    # ms epoch (>~ Sat 2001 in ms) → seconds; keep already-second values as-is.
    if val > 1e12:
        return val / 1000.0
    return val


def _levels_as_payload(level_map: dict[float, float]) -> list[dict[str, float]]:
    """Turn a {price: size} map into the ``[{"price","size"}]`` shape the
    shared ``_snapshot_from_payload`` parser consumes (it re-sorts + drops
    non-positive levels defensively)."""
    return [{"price": p, "size": s} for p, s in level_map.items() if s > 0]


class ClobBookWsMirror:
    """Daemon thread mirroring the CLOB market channel into a snapshot cache.

    ``get`` / ``note_tokens`` match ``BookPrefetcher`` so the pipeline seam is
    unchanged. Everything network-facing lives on the daemon thread; the
    pipeline only ever touches the lock-guarded cache.
    """

    # Telemetry tag surfaced by BookSourceChain.get_tagged (W2).
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
        # ORPHAN FIX (2026-07-25). Measured on the live box: the venue pushes
        # ~822 msg/s with a p50 1ms inter-frame gap on the open *-updown-*
        # windows, yet this mirror served books with a p50 age of 6,935ms and a
        # MAX of 114,498ms — a ~7000x gap that is entirely consumer-side. Root
        # cause: `_cache` was evicted ONLY on disconnect (_clear_cache) or when a
        # token's ask side emptied (_rebuild). When a window rolled, the pipeline
        # noted a new token set, `_stream_once` re-subscribed, and the PREVIOUS
        # window's tokens stopped updating but STAYED in `_cache` forever — and
        # `get()`'s only gate was connection-level recv-liveness, which any frame
        # (a ping, another token's ack) re-stamps. So a two-windows-old ladder
        # priced live FAKs. The `_stream_once` comment claiming orphans "age out
        # of the freshness window" described a window that did not exist.
        #
        # `evict_unsubscribed` drops a token's cache + ladders the moment it
        # leaves the subscribed set, so an orphan serves None (→ REST) instead of
        # a stale ladder. Default ON: it can only ever REMOVE stale data, and the
        # fallback is the well-tested REST batch. Kill switch is
        # CRYPTOBOT_BOOK_WS_EVICT_UNSUBSCRIBED=0.
        self._evict_unsubscribed = bool(evict_unsubscribed)
        # Per-token venue-book age gate (0 = disabled = pre-2026-07-25 behavior).
        # This is the SAFETY NET, not the fix — with eviction on, ages collapse
        # and this should rarely fire. Arm it only after watching the ws_age
        # telemetry: at the pre-fix p50 of ~6.9s a ~400ms gate would reject
        # essentially every WS hit, which is equivalent to (and slower than)
        # simply setting CRYPTOBOT_BOOK_WS_ENABLED=0.
        self._max_book_age_s = max(0.0, float(max_book_age_ms) / 1000.0)
        # Injectable for tests: a zero-arg callable returning a context-manager
        # websocket connection. Default lazily imports ``websockets``.
        self._connect_factory = connect_factory
        self._backoff_base_s = max(0.05, float(reconnect_backoff_s))
        self._backoff_max_s = max(self._backoff_base_s, float(reconnect_backoff_max_s))
        self._recv_timeout_s = max(0.05, float(recv_timeout_s))

        self._lock = threading.Lock()
        self._cache: dict[str, BookSnapshot] = {}
        # Retained per-token price ladders so price_change deltas merge onto the
        # last full book rather than needing a fresh snapshot every tick.
        self._asks: dict[str, dict[float, float]] = {}
        self._bids: dict[str, dict[float, float]] = {}
        self._tokens: tuple[str, ...] = ()
        self._tokens_version = 0
        # Tokens the stall watchdog evicted, awaiting a full `book` frame before
        # deltas may rebuild them (see `evict`). Empty unless the watchdog acts
        # => every code path below is byte-identical without it.
        self._resync: set[str] = set()
        # Monotonic time of the LAST frame the recv loop received (any message,
        # including pings / sub-acks). get()'s freshness gate keys off this
        # recv-liveness rather than the venue book's last-change ts, so a quiet
        # connected book keeps serving. None until the first frame arrives and
        # reset to None on disconnect (_clear_cache) so a dead socket serves
        # nothing.
        self._last_recv_monotonic: Optional[float] = None
        # Fast-trigger EVENT rung (operator 2026-07-30): optional notifier fired
        # once per frame that changed the ASK ladder, so core can re-evaluate an
        # entry within ~20ms of the push instead of waiting for the next 1Hz tick
        # boundary (~500ms mean). None => never called => byte-identical.
        self._on_ask_update: Optional[Callable[[str, float], None]] = None

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- pipeline-facing API -------------------------------------------------

    def set_ask_update_callback(
        self, callback: Optional[Callable[[str, float], None]]
    ) -> None:
        """Register the ask-side update notifier (``None`` clears it).

        Contract for the callback — it runs ON THE RECV THREAD, which feeds every
        book read in the process:
          * it MUST be O(1) and non-blocking (queue-and-return);
          * it MUST NOT call back into this mirror (``get`` takes ``self._lock``,
            a plain Lock, so re-entering from inside a callback that we fired
            while holding it would deadlock the recv loop — which is why we fire
            AFTER releasing it);
          * a fire is NOT a promise that ``get`` will serve the token: the
            rebuild may have dropped it (ask side emptied) and ``get`` still
            applies its own recv-liveness / max-book-age gates.
        """
        self._on_ask_update = callback

    def _fire_ask_update(self, token: str, venue_ts: float) -> None:
        """Notify the ask-update callback. Called with ``self._lock`` RELEASED."""
        cb = self._on_ask_update
        if cb is None:
            return
        try:
            cb(token, venue_ts)
        except Exception:  # noqa: BLE001 — a bad consumer must never kill the WS
            # Load-bearing: an escaping exception unwinds _handle_message ->
            # _stream_once -> _run, whose finally clears the whole cache and
            # backs off. One bad callback would take the mirror down to REST.
            logger.debug("ask-update callback raised", exc_info=True)

    def note_tokens(self, tokens: list[str]) -> None:
        """Record the current window's token set; the thread (re)subscribes.

        Also EVICTS any token that just left the set (see ``_evict_unsubscribed``
        in ``__init__``). Without this a rolled-off window's ladder stayed cached
        and — because ``get()``'s gate is connection-level — kept pricing live
        orders; measured max served age was 114s.
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
                # A rolled-off token gets a fresh `book` on its next
                # subscription anyway; dropping it here keeps `_resync` bounded
                # by the live window instead of growing for the process's life.
                self._resync -= {t for t in self._resync if t not in keep}
            self._tokens = deduped
            self._tokens_version += 1

    def get(self, token: str, *, now_ts: float) -> Optional[BookSnapshot]:
        """Return the cached snapshot for ``token`` iff the mirror is LIVE.

        Freshness is keyed on RECV-LIVENESS — the monotonic age of the last
        frame the recv loop received (``_last_recv_monotonic``, re-stamped by
        every message) — NOT the venue book's last-change ts. A ``*-updown-*``
        book can hold the same ladder for many seconds while the socket stays
        healthy; that quiet ladder is VALID and keeps serving. Only a socket that
        stopped delivering frames (half-open → recv times out with no message; or
        disconnected → ``_clear_cache`` reset liveness to None) ages past
        ``max_age`` and serves nothing, so the tick falls back to REST.

        ``now_ts`` is unused for the freshness decision (kept for the
        ``BookPrefetcher``-compatible seam); the snapshot still carries the VENUE
        timestamp in ``fetched_at_ts`` for the ``book_ws_age_ms_max`` telemetry.
        """
        with self._lock:
            snap = self._cache.get(token)
            last_recv = self._last_recv_monotonic
        if snap is None or last_recv is None:
            return None
        # Recv-liveness gate: served only while the socket is still delivering
        # frames. A quiet book (venue ts old, socket live) passes; a frozen /
        # half-open socket (no frame within max_age) does not.
        if (time.monotonic() - float(last_recv)) > self._max_age_s:
            return None
        # Per-token venue-book age gate (safety net; 0 = disabled). Measured on
        # the SAME quantity the tick reports as book_ws_age_ms_max, so the knob
        # and the telemetry agree by construction.
        if self._max_book_age_s > 0.0:
            venue_ts = getattr(snap, "fetched_at_ts", None)
            if venue_ts is None:
                return None
            if (float(now_ts) - float(venue_ts)) > self._max_book_age_s:
                return None
        return snap

    # -- watchdog-facing API (2026-08-06, operator order) --------------------
    # The freshness cap was lifted (CRYPTOBOT_BOOK_WS_MAX_AGE_MS pinned to
    # 600000) because on these books snapshot age = market QUIETNESS, not feed
    # lag. That leaves one gray zone the mirror cannot self-diagnose: a socket
    # that is CONNECTED (recv-liveness keeps re-stamping off pings / other
    # tokens) while updates for one subscribed token silently stop. Connection
    # death already clears the cache and a rolled-off token is evicted, so the
    # only detector left is an out-of-band REST cross-check — which is what
    # ``scheduler.book_ws_watchdog`` does with these two methods. Both are
    # read-only w.r.t. the recv loop's own logic and are NEVER called from the
    # tick/order path.

    def serving_snapshots(self) -> dict[str, BookSnapshot]:
        """Shallow copy of the tokens currently in the serving set.

        ``BookSnapshot`` is frozen, so the copy is safe to inspect off-thread.
        The dict is what ``get`` reads, minus its liveness gates — the watchdog
        wants "what COULD be served", including entries the gates would reject.
        """
        with self._lock:
            return dict(self._cache)

    def evict(self, token: str) -> bool:
        """Drop ``token``'s snapshot + ladders; return True if one was cached.

        Used by the stall watchdog on a verified WS/REST divergence: the next
        entry-path read then falls through to REST truth instead of a ladder we
        no longer believe.

        ⚠️ The token stays SUBSCRIBED, so unlike ``note_tokens`` eviction the
        very next frame can rebuild it — and a ``price_change`` delta applied to
        empty ladders would rebuild a PARTIAL book (only the levels that
        happened to change), which is worse than no book at all. So eviction
        also arms a resync: deltas for the token are dropped until a full
        ``book`` frame re-seeds the ladders. Until then ``get`` serves None and
        the tick uses REST, which is the intended fail-safe.
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
        """True when ``token`` is no longer in the subscribed set (caller holds
        ``_lock``).

        Without this, eviction is not airtight: `_apply_book` /
        `_apply_price_change` accept ANY token the socket delivers, so the very
        next frame RESURRECTS whatever `note_tokens` just evicted — and if the
        venue merges subscriptions rather than replacing them (the pre-2026-07-25
        `_stream_once` comment asserted "the venue merges", which was never
        verified) the eviction would be a complete no-op. Filtering on write
        makes eviction deterministic regardless of which semantic the venue
        actually implements, and closes the resurrect-during-resubscribe-RTT
        window too.

        Empty ``_tokens`` = pre-first-note startup, when everything is accepted.
        """
        return bool(
            self._evict_unsubscribed and self._tokens and token not in self._tokens
        )

    def _clear_cache(self) -> None:
        with self._lock:
            self._cache = {}
            self._asks = {}
            self._bids = {}
            # A reconnect re-subscribes and the venue replays a full `book` for
            # every token, so a pending watchdog resync is satisfied by
            # construction; keeping it armed would only suppress deltas that
            # arrive before that snapshot.
            self._resync = set()
            # Recv-liveness goes false on disconnect/error so get() serves
            # nothing until the reconnected loop receives a fresh frame — a
            # half-open socket must never keep serving a stale ladder.
            self._last_recv_monotonic = None

    def _rebuild(self, token: str, *, venue_ts: float) -> None:
        """Rebuild + store the frozen snapshot for ``token`` from its ladders.

        Reuses the REST parser so depth math + the empty-ask→drop rule are
        byte-identical to the batch/GET path (snapshots are interchangeable).
        """
        payload = {
            "asks": _levels_as_payload(self._asks.get(token, {})),
            "bids": _levels_as_payload(self._bids.get(token, {})),
        }
        snap = _snapshot_from_payload(payload, token_id=token, now_ts=venue_ts)
        if snap is None:
            # Book went empty on the ask side → drop it so a stale populated
            # snapshot can't linger (get would otherwise keep serving it).
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
            # A full `book` frame re-seeds both ladders wholesale, which is
            # exactly what a watchdog eviction was waiting for.
            self._resync.discard(token)
            self._asks[token] = asks
            self._bids[token] = bids
            self._rebuild(token, venue_ts=venue_ts)
        # A `book` frame always replaces the ask ladder wholesale. Fired with the
        # lock RELEASED and AFTER the rebuild, so the callback observes the
        # committed snapshot and cannot deadlock on self._lock.
        self._fire_ask_update(token, venue_ts)
        return token

    def _apply_price_change(self, msg: dict, *, recv_ts: float) -> Optional[str]:
        """Apply one ``price_change`` frame — MARKET-scoped, not token-scoped.

        ⚠️ Wire format, verified live on the box 2026-08-06 (25s sample on the
        open ``*-updown-5m-*`` books: 10,536 ``price_change`` frames, top-level
        ``asset_id`` ABSENT in 10,536 of them, ``price_changes`` list present in
        10,536):

            {"market": "0x...", "event_type": "price_change", "timestamp": "...",
             "price_changes": [
                {"asset_id": "<YES tok>", "price": "0.01", "size": "7933.84",
                 "side": "BUY",  "hash": "...", "best_bid": "...", "best_ask": "..."},
                {"asset_id": "<NO tok>",  "price": "0.99", "size": "7933.84",
                 "side": "SELL", ...}]}

        One frame therefore carries changes for BOTH tokens of the market and
        the token id lives on each CHANGE, never at the top level. The pre-fix
        code read the token from the top level only, so ``token`` was ``""`` and
        EVERY delta was dropped: the mirror advanced solely on full ``book``
        frames (measured book-frame gaps p50 0.06-0.19s but max 2.7-7.1s), which
        is exactly the 5-9s ws_age + wrong top-of-book the stall watchdog kept
        evicting on. Those evictions were TRUE positives.

        The legacy token-scoped shape (top-level ``asset_id`` + ``changes``) is
        still accepted — every change without its own ``asset_id`` falls back to
        the frame's token — so a venue that sends either form works.

        Returns one touched token (truthiness = "a delta landed"), keeping the
        ``Optional[str]`` seam of ``_apply_book``.
        """
        changes = msg.get("changes")
        if not isinstance(changes, list):
            changes = msg.get("price_changes")
        if not isinstance(changes, list):
            return None
        frame_token = str(msg.get("asset_id") or msg.get("token_id") or "")
        # Group first so each token's ladder is rebuilt exactly once per frame.
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
                    # Watchdog-evicted: a delta on empty ladders would rebuild a
                    # PARTIAL book. Serve nothing (→ REST) until a full `book`
                    # frame arrives. Empty set on every un-evicted token, so this
                    # branch is never taken in the default configuration.
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
        # Bid-only deltas do NOT wake the fast-trigger event rung — an entry is
        # priced off the ask. Note this inherits the side classification above
        # verbatim (a missing/garbage `side` falls to bids), so the flag and the
        # ladder can never disagree about what changed. Fired outside the lock,
        # once per token whose ask ladder actually moved.
        for token in dirty_asks:
            self._fire_ask_update(token, venue_ts)
        return touched

    def _handle_message(self, raw, *, recv_ts: Optional[float] = None) -> None:
        """Parse one raw frame (str/bytes/dict or a list of them) and apply it.

        Best-effort: an unparseable / unknown message is ignored rather than
        killing the stream.
        """
        if recv_ts is None:
            recv_ts = time.time()
        # Recv-liveness: stamp the monotonic time of THIS received frame BEFORE
        # parsing (even an unparseable / unknown frame proves the socket is still
        # delivering). get()'s freshness gate keys off this, so a quiet-but-
        # connected book keeps serving while a socket that stops delivering
        # frames ages out.
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
            # PONG / subscription-ack / unknown → ignore.

    # -- connection loop -----------------------------------------------------

    def _open_connection(self):
        if self._connect_factory is not None:
            return self._connect_factory()
        # Lazy import: keeps module import + unit tests independent of the lib
        # and of any socket. The sync API landed in websockets 13.
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
        """One connect→subscribe→consume session. Returns on stop or error."""
        conn = self._open_connection()
        with conn as ws:
            local_version = -1
            while not self._stop.is_set():
                with self._lock:
                    tokens = self._tokens
                    version = self._tokens_version
                if version != local_version:
                    # Token set changed (new window) → (re)subscribe the full
                    # set. Tokens that left the set stop updating; `note_tokens`
                    # has already EVICTED their cache entries, so they serve None
                    # (→ REST) rather than a frozen ladder.
                    #
                    # The pre-2026-07-25 comment here claimed orphans "age out of
                    # the freshness window" — there was no per-token freshness
                    # window (get() gated only on connection-level recv-liveness),
                    # so they never aged out at all. That was the 114s book.
                    self._subscribe(ws, tokens)
                    local_version = version
                try:
                    raw = ws.recv(timeout=self._recv_timeout_s)
                except TimeoutError:
                    # No message this interval — loop to re-check stop/tokens.
                    continue
                self._handle_message(raw, recv_ts=time.time())

    def _run(self) -> None:
        backoff = self._backoff_base_s
        while not self._stop.is_set():
            try:
                self._stream_once()
                # Clean return (stop requested) — exit without backoff.
                if self._stop.is_set():
                    break
                backoff = self._backoff_base_s
            except Exception:  # noqa: BLE001 — any WS error → fall back to REST
                logger.debug("book WS mirror stream error", exc_info=True)
            finally:
                # On ANY disconnect/error the mirror must serve nothing so the
                # pipeline falls through to the REST batch (never a stale book).
                self._clear_cache()
            if self._stop.wait(timeout=backoff):
                break
            backoff = min(self._backoff_max_s, backoff * 2.0)


def _level_pair(lvl: object) -> tuple[Optional[float], float]:
    """Extract (price, size) floats from a raw level/change dict.

    Returns ``(None, 0.0)`` when price is missing/garbage; size defaults to 0
    (a delta with size 0 = level removal)."""
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
