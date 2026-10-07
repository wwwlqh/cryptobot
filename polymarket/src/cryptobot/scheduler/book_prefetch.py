"""Order-book prefetch thread.

The tick otherwise makes one inline ``POST /books`` call (~30-50ms) in
``pipeline._resolve_net_cells``. The token set is stable within a window, so a
background thread re-fetches the previous tick's tokens ``lead_ms`` before each
second boundary and the tick reads them from cache.

A cached book is only used if younger than ``max_age_ms`` at decision time
(``BookSnapshot.fetched_at_ts`` is the local fetch time). With the defaults
(lead 150ms, max age 700ms) the tick sees books at most ~lead_ms older than an
inline fetch would return. Missing tokens (first tick of a window, failed or
stale prefetch) fall back to the inline fetch, which is also what turning this
off does.

The thread is a daemon (like ``spot_prefetch.SpotPrefetcher``) and best-effort:
an exception just means the tick fetches inline.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class BookPrefetcher:
    """Daemon thread that pre-warms order-book snapshots ahead of each tick.

    ``fetch_books`` must be the SAME callable the tick's ``deps.fetch_books``
    uses (same client, same parser) so snapshots are interchangeable.
    """

    # Telemetry tag surfaced by BookSourceChain.get_tagged (W2).
    source_name = "prefetch"

    def __init__(
        self,
        fetch_books: Callable[..., Optional[dict]],
        *,
        lead_ms: int,
        max_age_ms: int,
    ) -> None:
        self._fetch_books = fetch_books
        self._lead_s = max(0.02, min(0.9, float(lead_ms) / 1000.0))
        self._max_age_s = max(0.05, float(max_age_ms) / 1000.0)
        self._lock = threading.Lock()
        self._tokens: tuple[str, ...] = ()
        self._cache: dict[str, object] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- pipeline-facing API -------------------------------------------------

    def note_tokens(self, tokens: list[str]) -> None:
        """Record the token set the tick just used; next boundary prefetches it."""
        with self._lock:
            self._tokens = tuple(dict.fromkeys(tokens))

    def noted_tokens(self) -> tuple[str, ...]:
        """The token set queued for the next boundary's prefetch.

        Read by the next-window prefetch so it can re-note the UNION of this
        set with the next window's ids — note_tokens REPLACES the tracked set
        wholesale, so the current window's tokens must be carried forward or
        they stop being prefetched mid-tail.
        """
        with self._lock:
            return self._tokens

    def get(self, token: str, *, now_ts: float) -> Optional[object]:
        """Return a cached snapshot iff it is younger than max_age at now_ts."""
        with self._lock:
            snap = self._cache.get(token)
        if snap is None:
            return None
        fetched_at = getattr(snap, "fetched_at_ts", None)
        if fetched_at is None or (float(now_ts) - float(fetched_at)) > self._max_age_s:
            return None
        return snap

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, name="cryptobot-book-prefetch", daemon=True
        )
        self._thread.start()
        logger.info(
            "book prefetch-ahead started (lead=%.0fms, max_age=%.0fms)",
            self._lead_s * 1000.0,
            self._max_age_s * 1000.0,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            next_boundary = math.floor(now) + 1.0
            wake_at = next_boundary - self._lead_s
            if wake_at <= now:
                next_boundary += 1.0
                wake_at += 1.0
            if self._stop.wait(timeout=wake_at - now):
                return
            with self._lock:
                tokens = self._tokens
            if not tokens:
                continue
            try:
                batch = self._fetch_books(list(tokens), now_ts=float(next_boundary))
            except Exception:  # noqa: BLE001 — prefetch is best-effort
                logger.debug("book prefetch failed", exc_info=True)
                continue
            if batch:
                with self._lock:
                    # replace wholesale: snapshots from older boundaries for
                    # tokens the venue omitted must not linger past max_age.
                    self._cache = dict(batch)


class BookSourceChain:
    """Compose several ``book_prefetch``-shaped sources behind one seam.

    Each source exposes ``note_tokens(tokens)`` + ``get(token, now_ts=)`` (and
    optionally ``start``/``stop``). ``get`` returns the FIRST source's fresh
    snapshot, so ordering encodes priority — e.g. ``[ws_mirror, rest_prefetch]``
    prefers the venue-timestamped WS book and falls back to the REST
    prefetch-ahead cache, then (via the pipeline) to the inline REST batch.
    ``note_tokens`` fans out to every source so they all know the window's set.
    """

    def __init__(self, sources) -> None:
        self._sources = [s for s in sources if s is not None]
        # Last token set noted to the chain. Tracked HERE (not read back from
        # the sources) so a caller can re-note a superset without dropping the
        # current window even when the only source is one that predates the
        # noted_tokens accessor (e.g. the WS mirror). See noted_tokens.
        self._noted: tuple[str, ...] = ()

    def note_tokens(self, tokens: list[str]) -> None:
        self._noted = tuple(dict.fromkeys(str(t) for t in tokens if t))
        for s in self._sources:
            try:
                s.note_tokens(tokens)
            except Exception:  # noqa: BLE001 — a bad source must not block the tick
                logger.debug("book source note_tokens failed", exc_info=True)

    def noted_tokens(self) -> tuple[str, ...]:
        """The token set most recently passed to ``note_tokens``.

        The next-window prefetch reads this to re-note ``{current ∪ next}`` —
        ``note_tokens`` REPLACES each source's set wholesale (the WS mirror even
        re-subscribes), so the current window's ids must be carried forward or
        they stop being served mid-tail.
        """
        return self._noted

    def get(self, token: str, *, now_ts: float) -> Optional[object]:
        snap, _ = self.get_tagged(token, now_ts=now_ts)
        return snap

    def get_tagged(self, token: str, *, now_ts: float):
        """Like ``get`` but also returns the served source's ``source_name``
        (or ``None`` on a miss) so the tick can attribute each book to ws /
        prefetch for the latency telemetry (W2)."""
        for s in self._sources:
            try:
                snap = s.get(token, now_ts=now_ts)
            except Exception:  # noqa: BLE001 — skip a failing source
                logger.debug("book source get failed", exc_info=True)
                continue
            if snap is not None:
                return snap, getattr(s, "source_name", "unknown")
        return None, None

    def set_ask_update_callback(self, callback) -> int:
        """Fan an ask-side book-update hook out over the sources that have one.

        Returns how many sources accepted it, so the caller can log ARMED vs
        INERT. Duck-typed exactly like ``note_tokens`` / ``get_tagged`` (core has
        no Protocol for the book-source seam): a venue mirror that grows
        ``set_ask_update_callback`` gets the fast-trigger EVENT rung for free,
        and one that has not grown it yet contributes nothing — the caller keeps
        the 250ms sweep rung rather than failing the boot.

        This is the ONLY reachable path to the mirror's hook once the sources are
        wrapped: the chain defines no ``__getattr__``, so a caller holding the
        chain cannot otherwise see a source's own methods.
        """
        wired = 0
        for s in self._sources:
            setter = getattr(s, "set_ask_update_callback", None)
            if setter is None:
                continue
            try:
                setter(callback)
            except Exception:  # noqa: BLE001 — a venue hook must never break the boot
                logger.warning("set_ask_update_callback failed", exc_info=True)
                continue
            wired += 1
        return wired

    def start(self) -> None:
        for s in self._sources:
            start = getattr(s, "start", None)
            if start is not None:
                start()

    def stop(self) -> None:
        for s in self._sources:
            stop = getattr(s, "stop", None)
            if stop is not None:
                stop()
