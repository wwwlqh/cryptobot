"""Venue-agnostic WS-vs-REST book shadow comparator.

Lifted verbatim out of core's ``cryptobot/data/clob_ws_mirror.py`` (now
``cryptobot_polymarket.ws_mirror``) in the Phase-2 venue split. It was already
venue-agnostic in fact if not in address: it only drives the injected mirror's
``note_tokens``/``start``/``stop``/``get`` and reads duck-typed snapshot attrs
(``top_ask``, ``total_ask_depth_usd``), which is exactly why the predict.fun
mirror re-exported the Polymarket class instead of forking it. Keeping it here
means neither venue package has to import the other for it.

Behavior is unchanged, including the shared ``"book WS shadow divergence"`` log
prefix (no per-venue tag) — both venues emitted that same line before.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Top-of-book comparison (shared with the WS mirror stall watchdog, 2026-08-06)
# ---------------------------------------------------------------------------
# ⚠️ This is a DIFFERENT question from ShadowBookComparator's, which is why it
# is a sibling function rather than a refactor of `observe`: the comparator asks
# "are WS and REST close enough that flipping the mirror authoritative is safe?"
# — one price TICK of slack on top_ask plus a relative tolerance on WHOLE-BOOK
# dollar depth, sampled on the decision path. The watchdog asks "is this ladder
# still the venue's ladder at all?", where one tick of drift on a quiet book is
# already the answer, so price must match EXACTLY. Only the snapshot-reading is
# genuinely shared, and it lives here so neither venue package owns it.


def top_of_book(snap) -> tuple:
    """``(ask_px, ask_sz, bid_px, bid_sz)`` from a duck-typed BookSnapshot.

    ``asks``/``bids`` are sorted ASCENDING by price (venues.types.BookSnapshot),
    so best ask = first ask and best bid = LAST bid. Missing side => ``None``
    price and 0.0 size.
    """
    asks = tuple(getattr(snap, "asks", ()) or ())
    bids = tuple(getattr(snap, "bids", ()) or ())
    ask_px = float(asks[0]["price"]) if asks else None
    ask_sz = float(asks[0].get("size", 0.0) or 0.0) if asks else 0.0
    bid_px = float(bids[-1]["price"]) if bids else None
    bid_sz = float(bids[-1].get("size", 0.0) or 0.0) if bids else 0.0
    return ask_px, ask_sz, bid_px, bid_sz


def top_of_book_matches(left, right, *, size_rel_tol: float) -> bool:
    """True when two snapshots agree on top-of-book.

    Price: EXACT on both sides (float-equality epsilon only — venue prices are
    cent ticks, so any real difference is >= 0.01 and a "close enough" tolerance
    here would hide exactly the drift the watchdog exists to catch).
    Size: relative tolerance against ``right`` (the REST/truth side), because
    sizes round differently across the batch and delta paths.
    A side present on one snapshot and absent on the other never matches.
    """
    l_ask_px, l_ask_sz, l_bid_px, l_bid_sz = top_of_book(left)
    r_ask_px, r_ask_sz, r_bid_px, r_bid_sz = top_of_book(right)
    for l_px, r_px in ((l_ask_px, r_ask_px), (l_bid_px, r_bid_px)):
        if (l_px is None) != (r_px is None):
            return False
        if l_px is not None and abs(l_px - r_px) > 1e-9:
            return False
    tol = max(0.0, float(size_rel_tol))
    for l_sz, r_sz in ((l_ask_sz, r_ask_sz), (l_bid_sz, r_bid_sz)):
        denom = max(abs(r_sz), 1e-9)
        if abs(l_sz - r_sz) / denom > tol:
            return False
    return True


class ShadowBookComparator:
    """W3 shadow-compare: run the WS mirror ALONGSIDE the authoritative REST
    book and count how far the two diverge, WITHOUT ever feeding a decision.

    Wired only when ``CRYPTOBOT_BOOK_WS_ENABLED`` and ``CRYPTOBOT_BOOK_WS_SHADOW``
    are both set. The mirror is deliberately NOT placed in the ``BookSourceChain``,
    so REST stays authoritative for every order; the pipeline just hands each
    resolved REST snapshot to ``observe`` after Pass 2. Divergences beyond one
    price tick (``top_ask``) or ``depth_rel_tol`` relative depth are counted, and
    a summary is logged at most once per ``log_min_interval_s`` so a noisy WS
    feed can't flood the log or the decision thread. Lets the operator watch the
    counters for a day before flipping the mirror authoritative.
    """

    def __init__(
        self,
        mirror,
        *,
        price_tick: float = 0.01,
        depth_rel_tol: float = 0.05,
        log_min_interval_s: float = 30.0,
    ) -> None:
        self._mirror = mirror
        self._price_tick = float(price_tick)
        self._depth_rel_tol = float(depth_rel_tol)
        self._log_min_interval_s = float(log_min_interval_s)
        self._lock = threading.Lock()
        self.n_compared = 0
        self.n_ws_missing = 0
        self.n_divergent_price = 0
        self.n_divergent_depth = 0
        self._last_log_ts = 0.0

    # -- pipeline-facing API (mirrors the BookSourceChain lifecycle) ----------

    def note_tokens(self, tokens: list[str]) -> None:
        self._mirror.note_tokens(tokens)

    def start(self) -> None:
        self._mirror.start()

    def stop(self) -> None:
        self._mirror.stop()

    def observe(self, token: str, rest_snap, *, now_ts: float) -> None:
        """Compare the WS snapshot for ``token`` against the authoritative REST
        ``rest_snap``. Counts divergences + logs rate-limited. Never raises and
        never influences the caller's book (off the decision path)."""
        try:
            ws_snap = self._mirror.get(token, now_ts=now_ts)
        except Exception:  # noqa: BLE001 — shadow compare must never fail a tick
            return
        rest_top = getattr(rest_snap, "top_ask", None)
        rest_depth = float(getattr(rest_snap, "total_ask_depth_usd", 0.0) or 0.0)
        with self._lock:
            self.n_compared += 1
            if ws_snap is None:
                self.n_ws_missing += 1
                return
            ws_top = ws_snap.top_ask
            price_div = (
                rest_top is not None
                and ws_top is not None
                and abs(float(ws_top) - float(rest_top)) > self._price_tick + 1e-9
            )
            ws_depth = float(getattr(ws_snap, "total_ask_depth_usd", 0.0) or 0.0)
            denom = max(abs(rest_depth), 1e-9)
            depth_div = abs(ws_depth - rest_depth) / denom > self._depth_rel_tol
            if price_div:
                self.n_divergent_price += 1
            if depth_div:
                self.n_divergent_depth += 1
            should_log = (price_div or depth_div) and (
                now_ts - self._last_log_ts >= self._log_min_interval_s
            )
            if should_log:
                self._last_log_ts = now_ts
                snapshot = (
                    token[:8], ws_top, rest_top, ws_depth, rest_depth,
                    self.n_compared, self.n_ws_missing,
                    self.n_divergent_price, self.n_divergent_depth,
                )
        if should_log:
            logger.warning(
                "book WS shadow divergence tok=%s ws_ask=%s rest_ask=%s "
                "ws_depth=%.1f rest_depth=%.1f (compared=%d ws_missing=%d "
                "px_div=%d depth_div=%d)",
                *snapshot,
            )


__all__ = ["ShadowBookComparator", "top_of_book", "top_of_book_matches"]
