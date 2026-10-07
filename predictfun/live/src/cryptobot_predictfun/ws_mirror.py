"""predict.fun WebSocket order-book mirror — a daemon thread that mirrors the
predict.fun CLOB book over the official ``predictOrderbook/{marketId}`` market
channel so the decision path can read a fresh snapshot from a local cache
instead of paying the inline ``GET /markets/orderbooks`` REST round-trip
(~245ms venue floor per read, and one draw of the 240/min per-key budget).

Why this exists (PF-only build, 2026-07-23)
-------------------------------------------
The Polymarket mirror (:mod:`cryptobot_polymarket.ws_mirror`) speaks Polymarket's
public ``market`` channel only, so ``main.py`` hard-disables the WS mirror on
predictfun today (``CRYPTOBOT_BOOK_WS_ENABLED`` ignored). This module is the
predict.fun-only equivalent: standalone, its own thread + asyncio-free sync
socket, and it NEVER touches the shared Polymarket mirror or any Polymarket
code path. It plugs into the SAME ``BookSourceChain`` seam
(:mod:`cryptobot.scheduler.book_prefetch`) as the REST prefetcher — ``get`` /
``note_tokens`` / ``noted_tokens`` match ``BookPrefetcher`` byte-for-byte — and
is intended to sit FIRST in the chain with the REST prefetch/batch as fallback.

Wire contract — RE-VERIFIED 2026-07-23 against the primary docs
---------------------------------------------------------------
Sources (dev.predict.fun WebSocket API):
  * General information .../general-information-1915499m0.md
  * Request format      .../request-format-1915501m0.md
  * Response format     .../response-format-1915502m0.md
  * Subscription topics .../subscription-topics-1915507m0.md
  * Heartbeats          .../heartbeats-1915508m0.md
  * Client example      .../client-example-1915523m0.md

  * CONNECTION: ``wss://ws.predict.fun/ws`` (TLS, port 443).
  * SUBSCRIBE / UNSUBSCRIBE (client->server):
      ``{"method": "subscribe",   "requestId": <int>, "params": ["predictOrderbook/<marketId>"]}``
      ``{"method": "unsubscribe", "requestId": <int>, "params": ["predictOrderbook/<marketId>"]}``
    Resubscribing an existing topic is a no-op ack; unsubscribe always acks.
  * REQUEST-ACK (server->client): ``{"type": "R", "requestId": <int>, "success": bool}``
    (optional ``"error"``). Informational here — we key freshness on data
    frames, not acks.
  * DATA FRAME (server->client): ``{"type": "M", "topic": "predictOrderbook/<marketId>",
    "data": {version, marketId, updateTimestampMs, orderCount, asks, bids,
    lastOrderSettled, settlementsPending}}``. On subscribe the LATEST value is
    pushed immediately as a ``type:"M"`` message, then live updates — each
    carries the FULL asks/bids (not a delta), so every frame REPLACES the
    market's book (no Polymarket-style price_change delta-merge needed).
    Levels are two-element ``[price, quantity]`` arrays: price = YES price in
    [0,1], quantity = shares; asks ascending, bids descending — the SAME shape
    as the REST ``GET /markets/{id}/orderbook`` ``data`` body, so the frame
    ``data`` feeds straight into ``clob._snapshot_from_orderbook`` and the
    resulting snapshot is byte-identical to the REST path.
  * NO AUTH: ``predictOrderbook`` is a market-data topic — no JWT / x-api-key
    (unlike the authenticated ``predictWalletEvents/{jwt}`` topic).
  * HEARTBEAT (mandatory): every 15s the server sends
    ``{"type": "M", "topic": "heartbeat", "data": <epoch-ms>}`` and the client
    MUST echo ``{"method": "heartbeat", "data": <same epoch-ms>}`` (exact value)
    within ~15s or the connection is closed at the next probe.
  * SUBSCRIPTION LIMITS: none documented (per-connection). Rate: "each key
    carries its own rate limits"; whether WS subscribe messages count against
    the 240/min REST key is UNCONFIRMED — steady-state WS is push (~0 req), so
    it FREES REST budget. VERIFY in shadow before flipping authoritative.

Wallet fill-push — ``predictWalletEvents/{jwt}`` (VERIFIED 2026-07-23, primary docs)
------------------------------------------------------------------------------------
Sources (dev.predict.fun WebSocket API):
  * Subscription topics .../subscription-topics-1915507m0 (topic + event payloads)
  * Request format      .../request-format-1915501m0 (subscribe wire, same as books)
  * Response format     .../response-format-1915502m0 (``type:"M"`` envelope)

  * TOPIC: ``predictWalletEvents/{jwt}`` — "live events for a user's own
    orders and trades"; auth is the /auth JWT EMBEDDED IN THE TOPIC STRING
    (a revoked/invalid/expired JWT answers ``invalid_credentials``; no
    snapshot on subscribe). Subscribed/unsubscribed with the SAME
    ``{"method", "requestId", "params"}`` wire as ``predictOrderbook`` and
    delivered in the same ``{"type": "M", "topic", "data"}`` envelope.
  * EVENT payload (the frame ``data``): ``type`` (the EVENT name — distinct
    from the envelope ``type:"M"``), ``orderId``, ``orderHash``,
    ``walletAddress``, ``timestamp``, plus a ``details`` block
    (market/outcome/quantity/price DISPLAY strings — never booked). Names:
    ``orderAccepted``; ``orderNotAccepted`` (reason: noMarketMatch |
    rejectedDuplicate | rejectedPostOnly); ``orderExpired``;
    ``orderCancelled`` (optional reason: selfTradePrevention);
    ``orderTransactionSubmitted`` (matched, settlement sent on-chain — kind
    SALE|PURCHASE + settlementId + fill); ``orderTransactionSuccess``
    (settlement confirmed — settlementId + fill [+ isMaker, fee]);
    ``orderTransactionFailed`` (settlement failed — settlementId + fill).
  * FILL object: ``{executedPriceWei, executedSizeWei, executedValueWei}``
    (integer strings). ``executedSizeWei`` is shares-wei — the SAME unit as
    the REST order-state ``amountFilled`` pinned by the go-live validation
    CLI — and is the ONLY executed field this module ever parses.
    ⚠️ ``executedPriceWei`` / ``executedValueWei`` units are pinned by NO
    validation anywhere in this codebase and are NEVER read: the WS path may
    supply status + shares at most, and fill VWAP booking stays AT THE CAP
    PRICE (hard invariant — see ``clob.place_gtc_limit_order``).

  The wallet topic rides THIS mirror's existing socket, heartbeat echo and
  reconnect loop (no forked connection machinery) and is armed only via
  ``enable_wallet_events`` (main.py wiring: CRYPTOBOT_PREDICTFUN_WS_WALLET_
  EVENTS=1 + a LIVE order client). Terminal events resolve a per-orderHash
  waiter (``await_fill_state``) with a poll-shaped ``{status, amountFilled}``
  dict; anything ambiguous resolves NOTHING so the order client's UNCHANGED
  GET /orders/{hash} poll stays authoritative (see ``_reduce_wallet_event``).

Book keying (see :mod:`cryptobot_predictfun.clob` module note)
--------------------------------------------------------------
predict.fun serves ONE book per integer MARKET id, quoted from the UP/indexSet-1
view. The pipeline carries outcome onChainIds, so ``get(token)`` resolves the
token to ``(market_id, 'UP'|'DOWN')`` via
:func:`cryptobot_predictfun.markets.lookup_token_market` (fail-closed None on an
unregistered token — never a guessed market/side), then builds the snapshot with
:func:`cryptobot_predictfun.clob._snapshot_from_orderbook` (UP as-is, DOWN
synthesized by complement). Both helpers are reused directly so the WS book side
+ complement are byte-identical to ``fetch_books_batch``.

Freshness + fail-open discipline
--------------------------------
``fetched_at_ts`` on every served snapshot is the LOCAL receive wall-clock time
(never the venue ``updateTimestampMs`` — mapping the venue book time onto
staleness is a false signal, same caveat as the REST path). Freshness is keyed
on RECV-LIVENESS: a snapshot is served only while the market's last frame is
younger than ``max_age_ms`` (monotonic clock). On ANY error / disconnect /
staleness the cache is cleared and ``get`` returns ``None``, so the pipeline
falls through to the REST prefetch/batch exactly as today — the mirror is a
read-only latency cache with a kill switch and never places, cancels, or gates
an order.

The ``websockets`` dependency is imported LAZILY inside ``start`` (the sync API
landed in websockets 13; it is pinned in ``deploy/requirements.runtime.txt``).
A host without the library makes ``start`` fail SOFT — the mirror stays inert,
``get`` returns ``None`` forever, boot/DRY are unaffected, REST stays
authoritative — never a raise (same fail-soft contract as ``predict_sdk``).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict
from typing import Callable, Optional

from cryptobot.venues.types import BookSnapshot
from cryptobot_predictfun.clob import _snapshot_from_orderbook
from cryptobot_predictfun.markets import lookup_token_market

# The WS/REST shadow comparator is venue-agnostic — it only drives the injected
# mirror's get()/note_tokens()/start()/stop() and reads duck-typed snapshot attrs
# (top_ask, total_ask_depth_usd) — so the predict.fun shadow path REUSES the
# core ShadowBookComparator rather than forking it (one implementation, no
# lockstep drift). Phase 2 moved that class into core, so neither venue package
# imports the other for it. The import neither touches nor changes the Polymarket
# mirror
# (Polymarket stays byte-identical); only the shadow log-line prefix is the shared
# "book WS shadow divergence" (no PF-specific tag). Re-exported under the PF name
# so ``main.py`` / tests keep a stable ``predictfun.ws_mirror`` symbol.
from cryptobot.venues.shadow_book import (
    ShadowBookComparator as PredictFunShadowBookComparator,  # noqa: F401  re-export
)

logger = logging.getLogger(__name__)


# Official predict.fun market-data WebSocket endpoint (no auth for
# predictOrderbook). Overridable via env/ctor for staging/replays.
DEFAULT_WS_URL = "wss://ws.predict.fun/ws"

_ORDERBOOK_TOPIC_PREFIX = "predictOrderbook/"
_HEARTBEAT_TOPIC = "heartbeat"
_WALLET_TOPIC_PREFIX = "predictWalletEvents/"

# Order-level kill events: the order is done and the event itself reports no
# NEW execution. ``orderTransactionFailed`` is deliberately NOT here — a failed
# settlement leaves "what actually filled" ambiguous, so it resolves nothing
# (the GET poll is authoritative); it is still terminal (wakes the waiter).
_WALLET_KILL_EVENTS = frozenset({"orderCancelled", "orderExpired", "orderNotAccepted"})
# FIFO bound on the pre-registration event buffer and the per-order settled
# accumulator. A handful of orders is ever in flight at once; 256 absorbs a
# burst without unbounded growth on a long-lived mirror.
_WALLET_STATE_MAX = 256
# Buffered in place of a resolved state when a TERMINAL event produced no
# bookable resolution (bare kill / failed settlement / unreadable-size
# Success) before the order client registered its waiter. No further event
# ever arrives for a terminal order, so dropping such an event would leave a
# late-registering ``await_fill_state`` burning its whole timeout — a
# deterministic ~2s stall per lost POST-return-vs-event-arrival race on the
# live FAK-reject/fast-requote path. Identity-compared; never handed to a
# caller (``await_fill_state`` maps it to None => go poll now).
_TERMINAL_NO_STATE: dict = {}


def _executed_size_wei(event: dict) -> Optional[int]:
    """The event's ``fill.executedSizeWei`` as a non-negative int, else None.

    shares-wei is the ONLY executed field ever parsed here — the same unit as
    the REST order-state ``amountFilled`` (pinned by the go-live validation
    CLI). ``executedPriceWei``/``executedValueWei`` are deliberately never
    read: their units are pinned by no validation, and fill VWAP booking stays
    at the cap price regardless.
    """
    fill = event.get("fill")
    if not isinstance(fill, dict):
        return None
    raw = fill.get("executedSizeWei")
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = int(str(raw))
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


def _reduce_wallet_event(
    event: dict, settled_wei: int
) -> tuple[int, Optional[dict], bool]:
    """Fold ONE predictWalletEvents payload into
    ``(new_settled_wei, resolved_state, terminal)``.

    ``settled_wei`` is the order's running shares-wei total across CONFIRMED
    settlements (``orderTransactionSuccess``) ONLY — a Submitted fill is still
    pending on-chain and a Failed one did not happen, so neither may ever be
    booked. ``resolved_state`` is the poll-shaped ``{status, amountFilled}``
    dict the order client's ``_await_terminal_state`` consumes, or None;
    ``terminal`` True wakes the waiter even WITHOUT a state, so the caller
    proceeds to the authoritative GET poll immediately instead of burning the
    WS wait (a bare FAK kill is the common no-fill reject — the fast-requote
    retry path must not get slower than today's poll).

    Poll-ordering parity (round-1 binding condition):
      * an executed partial is booked FIRST — a kill after confirmed fills
        resolves with the accumulated size (the poll path books
        ``amountFilled > 0`` before it ever looks at the kill status);
      * a Success with an unreadable executed size resolves NOTHING no matter
        what the accumulator holds — any total built without this event's
        size is knowingly incomplete, and the BUY caller books the resolved
        state inline (cash spent would exceed the booked position);
      * a kill NEVER zero-fills on its own say-so: kills carry no fill object
        per the docs, so an executed field on one (zero included) is
        off-contract — with nothing confirmed settled it resolves NOTHING.
        Never a guessed zero, or a partial-filled-then-killed FAK whose
        Success frame was missed would become an untracked naked position;
        the poll decides.
    """
    etype = str(event.get("type") or "")
    exec_size = _executed_size_wei(event)
    if etype == "orderTransactionSuccess":
        if exec_size is None:
            # Confirmed settlement with an unreadable executed size: a fill
            # DID happen but its size is unknown, so NO resolvable total
            # exists — on an empty accumulator FILLED would book the full
            # signed size (over-booking a partial), and on prior confirmed
            # settlements it would book only those while more actually
            # settled (cash spent exceeding the booked position). Wake the
            # waiter stateless; the poll reads the venue's authoritative
            # amountFilled instead.
            return settled_wei, None, True
        settled = settled_wei + exec_size
        if settled <= 0:
            # Parsed-zero settlement on an empty accumulator: nothing to book
            # yet — poll. (A parsed zero on TOP of prior settlements still
            # resolves the complete accumulated total below.)
            return settled_wei, None, True
        return settled, {"status": "FILLED", "amountFilled": str(settled)}, True
    if etype in _WALLET_KILL_EVENTS:
        if settled_wei > 0:
            return (
                settled_wei,
                {"status": "CANCELLED", "amountFilled": str(settled_wei)},
                True,
            )
        # Kill events carry no fill object per the docs, so ANY executed
        # field on one (zero included) is off-contract and never trusted for
        # a zero-book: had a reconnect gap swallowed the earlier Success, an
        # explicit "0" here would zero-book a real partial and the resulting
        # success=False BUY return would re-bet the slot (double-spend). The
        # poll reads the venue's authoritative amountFilled instead.
        return settled_wei, None, True
    if etype == "orderTransactionFailed":
        return settled_wei, None, True
    # orderAccepted / orderTransactionSubmitted (settlement pending) / unknown:
    # not terminal — keep waiting for the confirm, or the poll on timeout.
    return settled_wei, None, False


class _WalletWaiter:
    """One in-flight ``await_fill_state`` registration: the resolved
    poll-shaped state (None = "terminal but unbookable — go poll now") plus the
    event the awaiting order thread blocks on. ``state`` is written before
    ``event.set()`` under the wallet lock, so a woken reader observes a
    consistent value."""

    __slots__ = ("event", "state")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.state: Optional[dict] = None


class _MarketBook:
    """One market's last full book frame + the local monotonic/wall receive
    times. Frames REPLACE (predictOrderbook pushes full depth every update), so
    no delta-merge state is kept."""

    __slots__ = ("data", "recv_monotonic", "recv_wall")

    def __init__(self, data: dict, recv_monotonic: float, recv_wall: float) -> None:
        self.data = data
        self.recv_monotonic = recv_monotonic
        self.recv_wall = recv_wall


class PredictFunBookWsMirror:
    """Daemon thread mirroring the predict.fun ``predictOrderbook`` channel into
    a per-market snapshot cache.

    ``get`` / ``note_tokens`` / ``noted_tokens`` match ``BookPrefetcher`` so the
    ``BookSourceChain`` seam is unchanged. Everything network-facing lives on the
    daemon thread; the pipeline only ever touches the lock-guarded cache.

    Optionally ALSO carries the authenticated ``predictWalletEvents/{jwt}``
    fill-push topic on the same socket/heartbeat/reconnect loop (armed via
    ``enable_wallet_events``; see the module docstring's wallet section) —
    the order client then awaits ``await_fill_state`` before its GET poll.
    """

    # Telemetry tag surfaced by BookSourceChain.get_tagged.
    source_name = "pf_ws"

    def __init__(
        self,
        *,
        max_age_ms: int,
        url: str = DEFAULT_WS_URL,
        connect_factory=None,
        reconnect_backoff_s: float = 1.0,
        reconnect_backoff_max_s: float = 30.0,
        recv_timeout_s: float = 1.0,
    ) -> None:
        self._url = url
        self._max_age_s = max(0.05, float(max_age_ms) / 1000.0)
        # Injectable for tests: a zero-arg callable returning a context-manager
        # websocket connection. Default lazily imports ``websockets`` in start().
        self._connect_factory = connect_factory
        self._backoff_base_s = max(0.05, float(reconnect_backoff_s))
        self._backoff_max_s = max(self._backoff_base_s, float(reconnect_backoff_max_s))
        self._recv_timeout_s = max(0.05, float(recv_timeout_s))

        self._lock = threading.Lock()
        # market_id -> _MarketBook (last full frame + receive times).
        self._books: dict[str, _MarketBook] = {}
        # The window's noted OUTCOME token ids (what the pipeline passes). The
        # loop resolves these to the market set to (un)subscribe.
        self._tokens: tuple[str, ...] = ()
        self._tokens_version = 0
        # Monotonic time of the LAST frame received on ANY topic (data /
        # heartbeat / ack) — the socket-liveness signal. None until the first
        # frame and reset on disconnect so a dead socket serves nothing.
        self._last_recv_monotonic: Optional[float] = None
        self._last_recv_wall: Optional[float] = None
        # Monotonic subscribe/unsubscribe requestId counter — mutated only on
        # the loop thread.
        self._req_id = 0
        # Set True by _stream_once once a session has delivered its first frame;
        # read (and reset) by _run to recover the reconnect backoff after a
        # genuinely-healthy session. Touched only on the loop thread.
        self._session_got_frame = False

        # -- predictWalletEvents fill-push state (inert until
        # enable_wallet_events arms a JWT provider; see the module docstring's
        # wallet section). Its OWN lock: waiter signaling must never contend
        # with the tick's book reads on self._lock.
        self._wallet_lock = threading.Lock()
        self._wallet_jwt_provider: Optional[Callable[[], Optional[str]]] = None
        # Bumped by enable_wallet_events + the JWT rotation hook; the loop
        # resubscribes on mismatch (the _tokens_version idiom).
        self._wallet_jwt_version = 0
        # orderHash -> _WalletWaiter an order thread is currently blocked on.
        self._wallet_waiters: dict[str, _WalletWaiter] = {}
        # orderHash -> resolved state that arrived BEFORE the order client
        # registered (the POST-return-vs-event-arrival race). FIFO-bounded.
        self._wallet_buffer: OrderedDict[str, dict] = OrderedDict()
        # orderHash -> confirmed-settled shares-wei accumulated across
        # orderTransactionSuccess events. NOT popped on terminal: consecutive
        # settlements of one order must keep building the running total (a
        # second Success re-resolves with the full sum). FIFO-bounded.
        self._wallet_accum: OrderedDict[str, int] = OrderedDict()

        self._inert = False
        self._resolved_connect = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- pipeline-facing API -------------------------------------------------

    def note_tokens(self, tokens: list[str]) -> None:
        """Record the current window's OUTCOME token set (REPLACES wholesale);
        the loop diff-subscribes to exactly the resolved market set."""
        deduped = tuple(dict.fromkeys(str(t) for t in tokens if t))
        with self._lock:
            if deduped != self._tokens:
                self._tokens = deduped
                self._tokens_version += 1

    def noted_tokens(self) -> tuple[str, ...]:
        """The token set most recently passed to ``note_tokens`` (the
        ``BookPrefetcher`` accessor: the next-window prefetch re-notes
        ``{current ∪ next}`` because ``note_tokens`` replaces wholesale)."""
        with self._lock:
            return self._tokens

    def get(self, token: str, *, now_ts: float) -> Optional[BookSnapshot]:
        """Return the cached snapshot for outcome ``token`` iff the mirror is
        LIVE and the market's last frame is younger than ``max_age``.

        Resolves ``token`` -> ``(market_id, side)`` fail-closed (unregistered
        token -> None, fall to REST); builds the side view with the REST
        ``_snapshot_from_orderbook`` so UP/DOWN are byte-identical to the batch
        path; stamps ``fetched_at_ts`` with the LOCAL receive time (never the
        venue ``updateTimestampMs``). ``now_ts`` is unused for the freshness
        decision (kept for the ``BookPrefetcher``-compatible seam) — freshness
        is recv-liveness on the monotonic clock.
        """
        resolved = lookup_token_market(str(token))
        if resolved is None:
            # Fail-closed: outcome token not registered by discovery -> we do
            # not know its market/side, so no book (pipeline falls to REST).
            return None
        market_id, side = resolved
        with self._lock:
            book = self._books.get(str(market_id))
            if book is None:
                return None
            if (time.monotonic() - book.recv_monotonic) > self._max_age_s:
                # Frozen / quiet-past-max_age market -> stale, fall to REST.
                return None
            data = book.data
            recv_wall = book.recv_wall
        return _snapshot_from_orderbook(
            data, token_id=str(token), now_ts=recv_wall, side=side
        )

    # -- shadow telemetry accessors ------------------------------------------

    def last_update_ts(self) -> Optional[float]:
        """Local wall-clock time of the last frame on any topic, or None when
        the socket has delivered nothing (dead/inert). Read by the shadow
        comparator / tick telemetry — never a decision input."""
        with self._lock:
            return self._last_recv_wall

    def market_age_ms(self, market_id: str) -> Optional[float]:
        """Age in ms of the last frame for ``market_id`` (monotonic), or None
        when the market has no cached frame. Telemetry only."""
        with self._lock:
            book = self._books.get(str(market_id))
        if book is None:
            return None
        return (time.monotonic() - book.recv_monotonic) * 1000.0

    # -- wallet fill-push (predictWalletEvents) -------------------------------

    def enable_wallet_events(
        self, jwt_provider: Callable[[], Optional[str]]
    ) -> None:
        """Arm the authenticated ``predictWalletEvents/{jwt}`` subscription.

        ``jwt_provider`` returns the order path's CURRENT cached /auth JWT (or
        None) WITHOUT fetching (``JwtSession.peek_token``) — this side never
        spends an /auth round trip of its own; the rotation hook re-arms the
        topic once the order path refreshes. Wired by main.py only when
        CRYPTOBOT_PREDICTFUN_WS_WALLET_EVENTS is ON and a LIVE order client
        exists. Never called => no wallet subscribe is ever sent and the book
        mirror behaves exactly as the book-only build.
        """
        with self._wallet_lock:
            self._wallet_jwt_provider = jwt_provider
            self._wallet_jwt_version += 1

    def notify_jwt_rotated(self) -> None:
        """JWT rotation hook (``JwtSession.set_on_change``): bump the wallet
        subscription version so the loop drops the dead
        ``predictWalletEvents/{old-jwt}`` topic and resubscribes with the
        fresh credential on its next iteration (<= recv_timeout away)."""
        with self._wallet_lock:
            self._wallet_jwt_version += 1

    def await_fill_state(self, order_hash: str, timeout_s: float) -> Optional[dict]:
        """Block up to ``timeout_s`` for a pushed terminal state of
        ``order_hash``; returns the poll-shaped ``{status, amountFilled}``
        dict, or None => the caller falls through to the UNCHANGED GET poll.

        The already-arrived buffer is checked and the waiter registered under
        ONE hold of the wallet lock, so an event landing between the order
        POST's return and this call is neither lost nor double-delivered
        (round-1 binding condition: register + check BEFORE waiting). A
        terminal event with no bookable state wakes the wait EARLY with None
        — or, when it already arrived pre-registration, short-circuits it via
        the buffered ``_TERMINAL_NO_STATE`` marker — so a plain FAK kill
        reaches the poll immediately instead of burning the full WS wait
        regardless of which side wins the POST-return-vs-event race. A mirror
        that cannot deliver at all (never started, ``websockets``
        unimportable, loop thread dead/stopped) skips the wait the same way:
        no push can ever arrive there, and the poll must stay full-speed in
        exactly the degraded states it backstops.
        """
        key = str(order_hash or "").strip().lower()
        if not key:
            return None
        waiter = _WalletWaiter()
        with self._wallet_lock:
            buffered = self._wallet_buffer.pop(key, None)
            if buffered is not None:
                return None if buffered is _TERMINAL_NO_STATE else buffered
            thread = self._thread
            if thread is None or not thread.is_alive():
                # Buffered facts (from a previously-live socket) are still
                # consumed above; with the loop down nothing NEW can arrive,
                # so waiting would only tax every entry and stop attempt.
                return None
            self._wallet_waiters[key] = waiter
        try:
            waiter.event.wait(max(0.0, float(timeout_s)))
        finally:
            with self._wallet_lock:
                self._wallet_waiters.pop(key, None)
        return waiter.state

    def _handle_wallet_event(self, event: dict) -> None:
        """Fold one wallet-event payload into the waiter registry.

        A terminal resolution wakes the registered waiter or — when the order
        client has not registered yet — lands in the FIFO-bounded buffer for
        ``await_fill_state``'s pre-check; a terminal event with NO bookable
        state buffers the ``_TERMINAL_NO_STATE`` marker instead, because no
        further event ever arrives for a terminal order and dropping it would
        leave a late-registering waiter burning its full timeout on an
        already-decided order. Non-terminal events change nothing the caller
        can see. Disconnects need no cleanup here: an unresolved waiter simply
        times out into the poll, and buffered states are real terminal facts
        that stay valid.
        """
        order_hash = str(event.get("orderHash") or "").strip().lower()
        if not order_hash:
            return
        with self._wallet_lock:
            prior = self._wallet_accum.get(order_hash, 0)
            settled, resolved, terminal = _reduce_wallet_event(event, prior)
            if settled != prior:
                self._wallet_accum[order_hash] = settled
                self._wallet_accum.move_to_end(order_hash)
                while len(self._wallet_accum) > _WALLET_STATE_MAX:
                    self._wallet_accum.popitem(last=False)
            if not terminal:
                return
            waiter = self._wallet_waiters.get(order_hash)
            if waiter is not None:
                # State strictly before set(): the awaiting thread reads
                # .state only after wait() returns.
                waiter.state = resolved
                waiter.event.set()
                return
            # Latest terminal fact wins on re-buffer (venue events arrive in
            # order); a stateless terminal overwriting a resolved one only
            # demotes the reader to the authoritative poll — always safe.
            self._wallet_buffer[order_hash] = (
                _TERMINAL_NO_STATE if resolved is None else resolved
            )
            self._wallet_buffer.move_to_end(order_hash)
            while len(self._wallet_buffer) > _WALLET_STATE_MAX:
                self._wallet_buffer.popitem(last=False)

    def _sync_wallet_subscription(
        self, ws, current_topic: Optional[str], local_version: int
    ) -> tuple[Optional[str], int]:
        """(Re)subscribe ``predictWalletEvents/{jwt}`` when armed and the JWT
        version moved (enable_wallet_events / the rotation hook / a fresh
        session). A None jwt (order path not yet authenticated, or the cached
        token expired) unsubscribes and leaves the version UNLATCHED so the
        provider is re-read next iteration — the subscription returns the
        moment the order path holds a live credential, without this side ever
        fetching a JWT. Not armed => zero work (book-only byte-identical)."""
        with self._wallet_lock:
            provider = self._wallet_jwt_provider
            version = self._wallet_jwt_version
        if provider is None or version == local_version:
            return current_topic, local_version
        jwt = None
        try:
            jwt = provider()
        except Exception:  # noqa: BLE001 — a provider fault must not kill the stream
            logger.debug(
                "predict.fun wallet-events jwt provider raised", exc_info=True
            )
        desired = f"{_WALLET_TOPIC_PREFIX}{jwt}" if jwt else None
        if desired != current_topic:
            if current_topic is not None:
                self._send(
                    ws,
                    {
                        "method": "unsubscribe",
                        "requestId": self._next_req_id(),
                        "params": [current_topic],
                    },
                )
            if desired is not None:
                self._send(
                    ws,
                    {
                        "method": "subscribe",
                        "requestId": self._next_req_id(),
                        "params": [desired],
                    },
                )
        return desired, (version if jwt else local_version)

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None or self._inert:
            return
        try:
            self._resolved_connect = self._resolve_connect_factory()
        except Exception:  # noqa: BLE001 — missing websockets dep must fail soft
            logger.warning(
                "predict.fun WS book mirror: 'websockets' not importable — "
                "mirror INERT, REST stays authoritative (no book WS)",
                exc_info=True,
            )
            self._inert = True
            return
        self._thread = threading.Thread(
            target=self._run, name="cryptobot-pf-book-ws-mirror", daemon=True
        )
        self._thread.start()
        logger.info(
            "predict.fun WS book mirror started (url=%s, max_age=%.0fms)",
            self._url,
            self._max_age_s * 1000.0,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _resolve_connect_factory(self):
        """Return a zero-arg connection factory, importing ``websockets``
        lazily. Raises (caught by start) when the library is absent."""
        if self._connect_factory is not None:
            return self._connect_factory
        # The sync API landed in websockets 13; imported here so module import
        # + unit tests never require the library or open a socket.
        from websockets.sync.client import connect

        url = self._url
        return lambda: connect(
            url, open_timeout=10.0, close_timeout=2.0, max_queue=64
        )

    # -- message handling (unit-tested directly, no socket) ------------------

    def _clear_books(self) -> None:
        with self._lock:
            self._books = {}
            # Socket-liveness goes false on disconnect/error so get() serves
            # nothing until a reconnected loop receives a fresh frame.
            self._last_recv_monotonic = None
            self._last_recv_wall = None

    def _handle_message(self, ws, raw) -> None:
        """Parse one raw frame and apply it. Best-effort: an unparseable /
        unknown message is ignored rather than killing the stream. ``ws`` is
        needed to echo the mandatory heartbeat."""
        recv_monotonic = time.monotonic()
        recv_wall = time.time()
        # Socket-liveness: stamp BEFORE parsing (even an unparseable frame proves
        # the socket is still delivering).
        with self._lock:
            self._last_recv_monotonic = recv_monotonic
            self._last_recv_wall = recv_wall
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                return
        if not isinstance(raw, dict):
            return
        topic = str(raw.get("topic") or "")
        if topic == _HEARTBEAT_TOPIC:
            # Mandatory: echo the SAME server timestamp back exactly, or the
            # venue closes the connection at the next probe.
            self._echo_heartbeat(ws, raw.get("data"))
            return
        if str(raw.get("type") or "") != "M":
            # Request-ack ("R") / unknown -> liveness already stamped; ignore.
            return
        if topic.startswith(_WALLET_TOPIC_PREFIX):
            # Authenticated wallet fill-push (only ever subscribed when
            # enable_wallet_events armed it) — disjoint from the book topics,
            # so the book path below is untouched.
            data = raw.get("data")
            if isinstance(data, dict):
                self._handle_wallet_event(data)
            return
        if not topic.startswith(_ORDERBOOK_TOPIC_PREFIX):
            return
        market_id = topic[len(_ORDERBOOK_TOPIC_PREFIX):]
        data = raw.get("data")
        if not market_id or not isinstance(data, dict):
            return
        with self._lock:
            self._books[market_id] = _MarketBook(data, recv_monotonic, recv_wall)

    def _echo_heartbeat(self, ws, data) -> None:
        try:
            ws.send(json.dumps({"method": "heartbeat", "data": data}))
        except Exception:  # noqa: BLE001 — a failed echo just drops the socket
            logger.debug("predict.fun WS mirror heartbeat echo failed", exc_info=True)

    # -- connection loop -----------------------------------------------------

    def _next_req_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _desired_market_ids(self, tokens: tuple[str, ...]) -> frozenset[str]:
        """Resolve the noted OUTCOME tokens to the set of MARKET ids to
        subscribe (UP+DOWN of one market share a single book). Unregistered
        tokens are silently dropped — they simply get no WS book (fall to
        REST), same fail-closed posture as ``get``."""
        out: set[str] = set()
        for tok in tokens:
            resolved = lookup_token_market(tok)
            if resolved is not None:
                out.add(str(resolved[0]))
        return frozenset(out)

    def _send(self, ws, msg: dict) -> None:
        ws.send(json.dumps(msg))

    def _resubscribe_diff(
        self, ws, current: frozenset[str], desired: frozenset[str]
    ) -> None:
        """Subscribe newly-added market ids and unsubscribe removed ones."""
        for mid in sorted(current - desired):
            self._send(
                ws,
                {
                    "method": "unsubscribe",
                    "requestId": self._next_req_id(),
                    "params": [f"{_ORDERBOOK_TOPIC_PREFIX}{mid}"],
                },
            )
            with self._lock:
                self._books.pop(mid, None)
        for mid in sorted(desired - current):
            self._send(
                ws,
                {
                    "method": "subscribe",
                    "requestId": self._next_req_id(),
                    "params": [f"{_ORDERBOOK_TOPIC_PREFIX}{mid}"],
                },
            )

    def _stream_once(self) -> None:
        """One connect -> (re)subscribe -> consume session. Returns on stop or
        error. A fresh session starts with an empty subscription set, so the
        first token check resubscribes the whole current market set."""
        conn = self._resolved_connect()
        with conn as ws:
            subscribed: frozenset[str] = frozenset()
            local_version = -1
            # Wallet-events topic state is per-session too: a reconnect starts
            # unsubscribed (wallet_local_version=-1 forces a fresh diff).
            wallet_topic: Optional[str] = None
            wallet_local_version = -1
            while not self._stop.is_set():
                with self._lock:
                    tokens = self._tokens
                    version = self._tokens_version
                if version != local_version:
                    desired = self._desired_market_ids(tokens)
                    self._resubscribe_diff(ws, subscribed, desired)
                    subscribed = desired
                    local_version = version
                wallet_topic, wallet_local_version = self._sync_wallet_subscription(
                    ws, wallet_topic, wallet_local_version
                )
                try:
                    raw = ws.recv(timeout=self._recv_timeout_s)
                except TimeoutError:
                    # No message this interval — loop to re-check stop/tokens.
                    continue
                # A delivered frame proves this session is genuinely healthy — let
                # _run reset the reconnect backoff after it drops (_stream_once's
                # own normal return fires ONLY on stop, so it can't signal health).
                self._session_got_frame = True
                self._handle_message(ws, raw)

    def _run(self) -> None:
        backoff = self._backoff_base_s
        while not self._stop.is_set():
            self._session_got_frame = False
            try:
                self._stream_once()
                if self._stop.is_set():
                    break
            except Exception:  # noqa: BLE001 — any WS error -> fall back to REST
                logger.debug("predict.fun WS book mirror stream error", exc_info=True)
            finally:
                # On ANY disconnect/error serve nothing so the pipeline falls
                # through to the REST batch (never a stale book).
                self._clear_books()
            # Recover the backoff after a session that actually delivered frames
            # (a healthy endpoint that merely dropped). The reset CANNOT hang off
            # _stream_once's normal return: that path fires ONLY when stop is set
            # (its loop has no other exit), while every real disconnect leaves via
            # the except above — so without this gate the backoff would ratchet to
            # _backoff_max_s and never recover after a long-lived session.
            if self._session_got_frame:
                backoff = self._backoff_base_s
            if self._stop.wait(timeout=backoff):
                break
            backoff = min(self._backoff_max_s, backoff * 2.0)
