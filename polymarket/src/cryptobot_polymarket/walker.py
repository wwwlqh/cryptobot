"""LIVE order placement surface.

Per the 2026-05-24-001 per-strategy LIVE deploy plan:

  * §U2 lands `_validate_wallet_config` + `_SIGNATURE_TYPE_LABELS` so the
    boot path fails closed on invalid POLY_* topology.
  * §U5 lands `OrderClient`, `_classify_clob_error`, `place_gtc_limit_order`,
    and the rest of the CLOB placement surface ported
    from hightempbot/execution/walker.py. The TP/SL monitor is deliberately
    NOT ported (D-LIVE-22 — no strategy uses TP/SL in v1). The pair-orphan
    close (`_close_orphan_leg`) went with the pair sleeves on 2026-07-31.
    The orphaned book-read surface (`fetch_order_book`, `best_bid`,
    `best_ask`, `accessible_depth` and their level/sort helpers) was
    deleted on 2026-08-01 — the LIVE book path lives in
    `cryptobot.execution.walk_book`. `sync_collateral_balance` went with
    it; `check_balance` is the surviving readiness read.

The balance read path works without `py_clob_client_v2`'s order
placement — the lazy ModuleNotFoundError surfaces as `ClobClientUnavailable`
at `OrderClient.__init__`. Tests mock the package via a stub OrderClient
factory; CI never hits the real CLOB.
"""

from __future__ import annotations

import logging
import math
import re
import sqlite3
import threading
import time
from concurrent.futures import (
    Future,
    TimeoutError as FuturesTimeoutError,
)
from decimal import Decimal, ROUND_DOWN
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar

_T = TypeVar("_T")

from cryptobot import config
from cryptobot.db.connection import utc_now_sql
from cryptobot.execution.executors import clob_executor
from cryptobot.execution.types import OrderResult
from cryptobot.execution.wallet_topology import (  # noqa: F401  re-export
    _SIGNATURE_TYPE_LABELS,
    _validate_wallet_config,
)

if TYPE_CHECKING:
    from cryptobot.execution.ledger import BetSignal
    from cryptobot.runtime_secrets import Config


logger = logging.getLogger(__name__)


# Single shared executor for CLOB calls so the per-call wrapper enforces
# a timeout independently of py_clob_client_v2's internal HTTP timeout
# (mirrors hightempbot/execution/walker.py:32).
#
# Phase 2: the pool object itself moved to cryptobot.execution.executors — core's
# pipeline submits to this SAME pool for its cold-cell fan-out, and core must not
# import a venue package to get it. Same size, same thread-name prefix, one pool.
_clob_executor = clob_executor


class ClobClientUnavailable(RuntimeError):
    """Raised when `py_clob_client_v2` is not installed in the runtime."""


# Phase 2: _SIGNATURE_TYPE_LABELS + _validate_wallet_config moved to
# cryptobot.execution.wallet_topology. They validate runtime_secrets.Config
# fields and core's live-readiness gate calls them on every venue, so they
# stayed with the config they validate. Re-exported here (and still in __all__)
# so this module's surface is unchanged.
# ---------------------------------------------------------------------------
# CLOB error classification
# ---------------------------------------------------------------------------


def _coerce_fill_float(value: object) -> Optional[float]:
    """Coerce a CLOB numeric field to a positive float, else None.

    Zero or negative is treated as 'no fill' (None) — a FAK that matched
    nothing must NOT be recorded as a positive fill."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out <= 0.0:  # NaN or non-positive
        return None
    return out


def _parse_fak_fill(
    response: object,
    *,
    side: str = "BUY",
) -> tuple[Optional[float], Optional[float]]:
    """Extract (matched_size_tokens, avg_fill_price) from a CLOB FAK/FOK
    post_order response, or (None, None) when the response carries no fill
    metadata. Never fabricate an optimistic full-stake fill (audit P1 #4).

    Polymarket's post_order response reports the realized match as
    ``makingAmount``/``takingAmount`` (what OUR order gave / received), not as
    an avg-price field. Orientation depends on side: a BUY makes USDC and
    takes tokens; a SELL makes tokens and takes USDC. Realized VWAP =
    usd / tokens. 2026-07-22 live audit: without this, every stop-loss FOK
    sell fell through to the posted LIMIT (bid − chase 0.10), under-booking
    proceeds by up to the chase distance (−$1.80 across the first 6 live
    stops vs the venue's own trade prints).

    The order's limit ``price`` is still never read here — that is the
    requested price, not the realized VWAP."""
    if not isinstance(response, dict):
        return None, None

    def _first(keys: tuple[str, ...]) -> object:
        for k in keys:
            if k in response and response[k] is not None:
                return response[k]
        return None

    size = _coerce_fill_float(
        _first(("size_matched", "matched_size", "filled_size", "filled"))
    )
    price = _coerce_fill_float(
        _first(("avg_fill_price", "matched_price", "filled_price", "vwap"))
    )
    if size is not None and price is not None:
        return size, price

    making = _coerce_fill_float(
        _first(("makingAmount", "making_amount", "makerAmount", "maker_amount"))
    )
    taking = _coerce_fill_float(
        _first(("takingAmount", "taking_amount", "takerAmount", "taker_amount"))
    )
    if making is not None and taking is not None and making > 0 and taking > 0:
        if str(side).upper() == "SELL":
            tokens, usd = making, taking
        else:
            tokens, usd = taking, making
        vwap = usd / tokens
        if 0.0 < vwap < 1.0:
            return (size if size is not None else tokens), vwap
    return size, price


def _classify_clob_error(exc_or_msg: object) -> str:
    """Map a CLOB exception / error string to a coarse retry-policy bucket.

    Returns one of: "auth", "insufficient_funds", "market_closed",
    "invalid_amount", "venue_unavailable", "post_only", "no_liquidity",
    "network", "unknown".

    Substring-based on the lower-cased string repr — `py_clob_client_v2`
    surfaces these as plain RuntimeError instances today. Ported verbatim
    from hightempbot/execution/walker.py:54 so the operator's incident
    indexing carries across both bots.

    The `venue_unavailable` / `post_only` / `invalid_amount` / `no_liquidity`
    buckets were added after a 2026-06-19 / 06-24 audit where 79 order
    attempts failed as HTTP 425 "order manager not ready" but were coded
    "unknown", so nothing distinguished a venue outage (alert-worthy) from a
    thin-book no-fill (expected). `venue_unavailable` + `post_only` drive the
    operator venue-degradation alert; `no_liquidity` is an expected market
    state (non-alerting); `invalid_amount` is terminal (a re-send of the same
    over-precise amount just re-fails — see the rounding in
    `place_gtc_limit_order`).
    """
    text = str(exc_or_msg or "").lower()
    if not text:
        return "unknown"

    # Kieran-python finding #9: `import re` moved to module level (was
    # re-imported per call from inside this function body).
    if (
        "insufficient" in text
        or "not enough" in text
        or "balance too low" in text
        or re.search(r"(balance|funds).*below.*minimum", text)
        or re.search(r"below.*minimum.*(balance|funds)", text)
    ):
        return "insufficient_funds"
    if (
        any(
            k in text
            for k in (
                "signature",
                "unauthor",
                "forbidden",
                "invalid key",
                "api key",
                "api_key",
                "creds",
                "passphrase",
                "maker address not allowed",
                "deposit wallet flow",
                "signer address has to be",
                "address of the api key",
            )
        )
        # HTTP status codes must match as standalone tokens, never bare
        # substrings: CLOB reject messages embed the 0x… orderID, and a hex
        # hash containing "401"/"403" misclassified routine no_liquidity FAK
        # kills as terminal auth (observed live 2026-07-15 — silently ended
        # the requote chain after attempt 1).
        or re.search(r"\b40[13]\b", text)
    ):
        return "auth"
    if any(
        k in text
        for k in (
            "market closed",
            "market is closed",
            "expired",
            "resolved",
            "not tradable",
            "not tradeable",
        )
    ):
        return "market_closed"
    # Venue transient (HTTP 425): Polymarket's order manager returns
    # "order manager not ready, please retry" during CLOB degradation
    # (observed 2026-06-19 / 06-24 as multi-minute bursts). Retryable, but
    # classified distinctly so it is NOT lumped into "unknown" — the pipeline
    # reads it to fire the venue-degradation operator alert.
    if "order manager not ready" in text or "please retry" in text:
        return "venue_unavailable"
    # Venue post-only window (HTTP 503): only post-only orders/cancels are
    # accepted, so a marketable FAK BUY is rejected. Retryable once the window
    # lifts. Checked BEFORE the generic 5xx/network bucket (which also matches
    # "503") so this is not miscoded as a transport error.
    if "post-only" in text or "post_only" in text or "post only" in text:
        return "post_only"
    # Order validation (HTTP 400): the market-buy maker amount carried more
    # than 2-decimal accuracy. Terminal — a re-send of the identical amount
    # re-fails; `place_gtc_limit_order` now quantizes to cents to prevent it.
    if "max accuracy" in text or "invalid amount" in text:
        return "invalid_amount"
    # Thin book (HTTP 400): a FAK BUY found no marketable depth at/below the
    # cap and was killed ("no orders found to match with FAK order"). Expected
    # market state, not an error — non-alerting; a later tick may still fill.
    if "no orders found to match" in text or "partially filled or killed" in text:
        return "no_liquidity"
    if any(
        k in text
        for k in (
            "timeout",
            "timed out",
            "connection",
            "connreset",
            "read timed",
            "max retries",
            "remote disconnect",
        )
    ) or re.search(r"\b50[234]\b", text):
        return "network"
    return "unknown"


# ---------------------------------------------------------------------------
# CLOB client factory (py_clob_client_v2 wrapper)
# ---------------------------------------------------------------------------


_HTTPX_TIMEOUT_INSTALLED: bool = False
_HTTPX_TIMEOUT_LOCK = threading.Lock()

# Latency finding #8: monotonic timestamp of the most recent response on the
# shared CLOB signing pool (order POSTs, readiness balance reads, market-info
# warms, pings). Written by the response hook `_install_clob_http_timeout`
# registers when the warmth ping is enabled; read by
# `OrderClient.ping_order_pool`'s skip-if-recent guard. Plain float store
# under the GIL — a slightly stale read only costs one redundant GET.
_CLOB_LAST_RESPONSE_MONOTONIC: float = 0.0


def _note_clob_pool_response(_response: object) -> None:
    """httpx response hook — stamp signing-pool activity for the ping guard."""
    global _CLOB_LAST_RESPONSE_MONOTONIC
    _CLOB_LAST_RESPONSE_MONOTONIC = time.monotonic()


# Review F4: the replacement pool built by `_install_clob_http_timeout`,
# captured as module state at construction so `ping_order_pool` uses one
# stable reference instead of re-dereferencing the library-private
# `py_clob_client_v2.http_helpers.helpers._http_client` on every call. Stays
# None when the install failed/skipped — the ping then falls back to that
# deref.
_CLOB_HTTP_CLIENT: Optional[Any] = None


def _install_clob_http_timeout() -> None:
    """Replace py_clob_client_v2's module-level httpx.Client with one that
    sets explicit per-phase timeouts.

    The library default (5s default) is tight for CLOB under load; the bot's
    external `_with_timeout` envelope is 30s. Setting
    (connect=3, read=30, write=30, pool=5) gives socket-level failures
    plenty of room while still bounding hung calls so workers in
    `_clob_executor` are released promptly. Idempotent — patches once
    per process.

    A4 latency: connect was 10s — a dead socket burned 10s of the tick's
    order window before failing. 3s is ample for a same-continent TCP+TLS
    setup (Ireland/London ≈10ms RTT) while still failing fast. `read` stays
    30s: it must survive Polymarket's 250ms taker delay + matching, and a
    too-tight read abandons an order the venue may still fill (the U7
    reconciler would then have to recover it).
    """
    global _HTTPX_TIMEOUT_INSTALLED, _CLOB_HTTP_CLIENT
    if _HTTPX_TIMEOUT_INSTALLED:
        return
    with _HTTPX_TIMEOUT_LOCK:
        if _HTTPX_TIMEOUT_INSTALLED:
            return
        try:
            import httpx
            from py_clob_client_v2.http_helpers import helpers as _clob_helpers

            _clob_helpers._http_client = httpx.Client(
                http2=True,
                timeout=httpx.Timeout(
                    connect=3.0, read=30.0, write=30.0, pool=5.0
                ),
                # A-latency: without explicit limits httpx defaults to
                # keepalive_expiry=5s, so an idle CLOB socket is TLS-expired and
                # a fresh connect+handshake (~20-40ms) is paid before nearly
                # EVERY order POST (crypto windows fire well over 5s apart). Set
                # keepalive_expiry=65s — just above the 60s balance/readiness job
                # cadence, so that recurring read keeps at least one pooled
                # connection warm and the hot decision->fill POST reuses it. The
                # connection counts are generous headroom over the single hot
                # signing socket (warm/reconcile calls share the pool).
                # Latency finding #8: that 60s-vs-65s margin is fragile — a
                # readiness fire dropped by misfire_grace_time=2 opens a ~120s
                # gap — so the ~20s clob_ping job (CLOB_PING_SECONDS) now keeps
                # the socket hot independently of the readiness cadence.
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                    keepalive_expiry=65.0,
                ),
                # Latency finding #8: when the warmth ping is enabled, a
                # response hook stamps pool activity so ping_order_pool can
                # skip when readiness/order traffic already kept the socket
                # hot. CLOB_PING_SECONDS=0 installs no hook — byte-identical
                # to the pre-ping client.
                event_hooks=(
                    {"response": [_note_clob_pool_response]}
                    if config.CLOB_PING_SECONDS > 0
                    else None
                ),
            )
            # Review F4: capture the pool ref for ping_order_pool (stable
            # module state; no per-call deref of library-private helpers).
            _CLOB_HTTP_CLIENT = _clob_helpers._http_client
            _HTTPX_TIMEOUT_INSTALLED = True
            logger.info(
                "Installed CLOB http timeouts (connect=3s read=30s write=30s pool=5s)"
            )
            if config.CLOB_PING_SECONDS > 0:
                # Review F4 boot visibility: the response hook feeding the
                # ping's skip guard is live.
                logger.info("clob_ping warmth guard: active")
        except Exception:
            logger.warning(
                "Failed to install CLOB http timeouts — httpx default applies",
                exc_info=True,
            )
            if config.CLOB_PING_SECONDS > 0:
                # Review F4 boot visibility: with no response hook the guard
                # timestamp stays 0.0 forever, so EVERY ping fires — say so
                # once at boot instead of degrading silently.
                logger.warning(
                    "clob_ping warmth guard: inert — pings unconditional"
                )


def _make_clob_client(
    *,
    key: Optional[str] = None,
    creds: Optional[object] = None,
    signature_type: int = 0,
    funder: str = "",
) -> Any:
    # Kieran-python finding #4: the V2 CLOB client lives in
    # `py_clob_client_v2` (imported inside the try/except below). At
    # the static-typing layer we annotate as `Any` rather than a
    # TYPE_CHECKING forward-ref because the package may be absent
    # entirely on the runtime path -- a stricter annotation would force
    # type checkers to resolve an import that legitimately fails fast
    # at runtime. Callers know they're getting a ClobClient-shaped
    # object.
    """Construct a V2 CLOB client with a clear failure mode.

    `signature_type` and `funder` control which on-chain wallet the
    client signs orders for and reads balance against (see
    `_SIGNATURE_TYPE_LABELS`). Missing or zero `signature_type` means
    "EOA". An empty `funder` lets the library auto-derive (works for EOA
    and POLY_PROXY; required-explicit for POLY_1271).
    """
    try:
        from py_clob_client_v2.client import ClobClient
    except ModuleNotFoundError as exc:
        raise ClobClientUnavailable("py_clob_client_v2 is not installed") from exc

    _install_clob_http_timeout()
    signature_type, funder = _validate_wallet_config(signature_type, funder)

    kwargs: dict[str, object] = {
        "host": "https://clob.polymarket.com",
        "chain_id": 137,
        "key": key,
        "creds": creds,
    }
    if signature_type is not None and int(signature_type) != 0:
        kwargs["signature_type"] = int(signature_type)
    if funder:
        kwargs["funder"] = funder
    return ClobClient(**kwargs)


# ---------------------------------------------------------------------------
# OrderClient — the single LIVE-order placement surface
# ---------------------------------------------------------------------------


class OrderClient:
    """LIVE CLOB client for cryptobot's per-strategy deploy.

    Wraps `py_clob_client_v2` for order book queries, balance checks, and
    GTC limit-order placement. Phase 2 / U5 ships the methods; Phase 3 /
    U6 wires this into `pipeline.run_scanner_tick` when a wallet's mode
    is `LIVE` (DRY wallets stay on `walk_book.walk_book_edge_preserving`).

    Constructor failures are fail-closed:

      * `py_clob_client_v2` not installed → `ClobClientUnavailable`
      * `poly_private_key` empty → `ValueError`
      * SIG=3 + empty funder → `ValueError` via `_validate_wallet_config`

    The boot path (main.py §U2) already validates the wallet topology
    before reaching OrderClient construction, so end-users see a clean
    error from the boot gate rather than a deep stack trace from here.
    """

    def __init__(self, config: "Config") -> None:
        try:
            from py_clob_client_v2.clob_types import ApiCreds
        except ModuleNotFoundError as exc:
            raise ClobClientUnavailable(
                "py_clob_client_v2 is not installed"
            ) from exc

        key = config.poly_private_key.get_secret_value()
        if not key:
            raise ValueError("poly_private_key is required for OrderClient")

        creds = ApiCreds(
            api_key=config.poly_api_key.get_secret_value(),
            api_secret=config.poly_secret.get_secret_value(),
            api_passphrase=config.poly_passphrase.get_secret_value(),
        )

        sig_type, funder = _validate_wallet_config(
            getattr(config, "poly_signature_type", 0),
            getattr(config, "poly_funder", ""),
        )
        self._client = _make_clob_client(
            key=key, creds=creds, signature_type=sig_type, funder=funder
        )
        self._signature_type = sig_type
        self._funder = funder
        self._timeout = 30
        # A1 latency: condition_ids whose static market info (tick_size /
        # neg_risk / fee info / token→condition map) has been pre-warmed into
        # the V2 client's caches, plus the in-flight set that dedups warm
        # submissions (review finding: without it a failing warm re-queued
        # every 1Hz scoring tick). See warm_market_info(_async).
        self._warmed_condition_ids: set[str] = set()
        self._warming_condition_ids: set[str] = set()
        # A15 latency: the in-flight async warm's Future, retained per
        # condition_id so ensure_market_info_warm can JOIN it (bounded)
        # instead of firing a duplicate concurrent GET on the calling
        # thread. Entries live exactly as long as their
        # _warming_condition_ids membership (both cleaned in
        # warm_market_info's finally).
        self._warm_futures: dict[str, Future] = {}
        # L4: condition_ids for which an order-time SYNCHRONOUS warm has already
        # been attempted (success or failure). Guards against re-blocking every
        # order for the same market when the venue is down.
        self._warm_sync_attempted: set[str] = set()
        self._warmed_lock = threading.Lock()
        # A4 latency: warm the order-version probe at construction so
        # __resolve_version never GETs /version inside the first
        # create_market_order on the hot fill path. Name-mangled access is
        # deliberate — get_version() alone does not populate the private
        # __cached_version; fail-soft on any library-version drift.
        try:
            resolver = getattr(self._client, "_ClobClient__resolve_version", None)
            if resolver is not None:
                resolver()
            else:
                self._client.get_version()
        except Exception:  # noqa: BLE001 — lazy in-library fallback applies
            logger.debug("order-version warm failed (lazy fallback)", exc_info=True)
        if sig_type != 0:
            logger.info(
                "OrderClient: signature_type=%d (%s) funder=%s",
                sig_type,
                _SIGNATURE_TYPE_LABELS[sig_type],
                funder or "(auto-derived)",
            )
        if sig_type == 1 and funder:
            logger.warning(
                "OrderClient: POLY_SIGNATURE_TYPE=1 with POLY_FUNDER set. "
                "If this is a Polymarket deposit wallet, use "
                "POLY_SIGNATURE_TYPE=3 (POLY_1271) instead."
            )

    # ---- timeout envelope -------------------------------------------------

    def _with_timeout(self, fn: Callable[..., _T], *args: Any) -> _T:
        """Run a CLOB call with a bounded timeout so a hung CLOB cannot
        block the scheduler thread indefinitely.

        Kieran-python finding #3: typed via TypeVar so callers preserve
        the wrapped call's return type. Previously `fn` was untyped,
        meaning mypy could not verify call sites that thread results
        through to ledger writes — non-trivial for the real-money order
        placement path."""
        future = _clob_executor.submit(fn, *args)
        return future.result(timeout=self._timeout)

    # ---- read-only balance -----------------------------------------------

    def check_balance(self) -> Optional[float]:
        """Get pUSD (V2 collateral) balance in dollars. Returns None on API failure."""
        try:
            from py_clob_client_v2.clob_types import (
                AssetType,
                BalanceAllowanceParams,
            )

            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            result = self._with_timeout(
                self._client.get_balance_allowance, params
            )
            return int(result.get("balance", "0")) / 1e6
        except FuturesTimeoutError:
            logger.error("Balance check timed out after %ds", self._timeout)
            return None
        except Exception:
            logger.error("Failed to check CLOB balance", exc_info=True)
            return None

    # ---- signing-pool warmth ping (latency finding #8) --------------------

    def ping_order_pool(self) -> bool:
        """Keep the shared signing-pool socket warm with a GET /ok.

        Between orders the pool's only steady traffic is the 60s readiness
        balance read vs keepalive_expiry=65s (a 5s margin), and a readiness
        fire delayed past the scheduler's misfire_grace_time=2 is DROPPED —
        a ~120s gap that guarantees the socket expires, so the next order
        POST pays fresh DNS + TCP + full TLS inside post_ms (httpcore never
        resumes TLS sessions). The ~20s clob_ping scheduler job drives this
        method to keep the connection permanently hot and drain buffered
        GOAWAY/FIN frames off the decision path.

        Never blocks an order POST: runs on a scheduler-pool thread, skips
        entirely when ANY pool response landed within
        CLOB_PING_SKIP_IF_ACTIVE_S (readiness read, order POST, warm, or a
        prior ping — which also shrinks the h1.1 in-flight-ping race
        window), and overrides the pool's read=30s with the short
        CLOB_PING_TIMEOUT_S so a hung /ok releases its worker fast. /ok is
        unauthenticated — no L1/L2 headers, no nonce, no server-side
        semantics. The GET goes through the pool reference captured at
        install time (`_CLOB_HTTP_CLIENT`, review F4); only when that ref is
        absent does it fall back to dereferencing the library-private
        helper. Fail-soft: never raises; True only when a ping was actually
        sent and returned 200.
        """
        last = _CLOB_LAST_RESPONSE_MONOTONIC
        if (
            last > 0.0
            and (time.monotonic() - last) < config.CLOB_PING_SKIP_IF_ACTIVE_S
        ):
            return False
        try:
            import httpx
            from py_clob_client_v2.endpoints import OK

            pool = _CLOB_HTTP_CLIENT
            if pool is None:
                # Captured ref absent (pool install failed/skipped) — fall
                # back to the per-call library-private deref.
                from py_clob_client_v2.http_helpers import (
                    helpers as _clob_helpers,
                )

                pool = _clob_helpers._http_client
            resp = pool.get(
                f"{self._client.host}{OK}",
                timeout=httpx.Timeout(config.CLOB_PING_TIMEOUT_S),
            )
            return resp.status_code == 200
        except Exception:  # noqa: BLE001 — warmth ping must never crash its job
            logger.debug("CLOB warmth ping failed", exc_info=True)
            return False

    # ---- market-info cache pre-warm (A1 latency) --------------------------

    def warm_market_info(self, condition_id: str) -> bool:
        """Pre-populate the V2 client's per-token static caches for a market.

        Crypto up/down markets mint fresh token_ids every window, so without
        this EVERY first order in a window pays 2 sequential cache-miss GETs
        (GET market-by-token, then GET clob-market/{condition_id}) inside
        `create_market_order` — on the hot decision→fill path. One
        `get_clob_market_info(condition_id)` call primes tick_size, neg_risk,
        fee info, and the token→condition map for BOTH tokens, so placement
        becomes pure local signing + a single post_order POST.

        Fail-soft: a warm failure only means the order path falls back to the
        library's lazy cache-miss GETs. Returns True when the cache is (or
        already was) warm for this condition_id.
        """
        cid = str(condition_id or "")
        if not cid:
            return False
        with self._warmed_lock:
            if cid in self._warmed_condition_ids:
                return True
        try:
            # Review finding (adversarial fleet): call the client DIRECTLY,
            # not via _with_timeout — warm_market_info already runs ON a
            # _clob_executor worker when submitted by warm_market_info_async,
            # and _with_timeout would submit a SECOND task into the SAME
            # 16-worker pool while blocking the outer worker (2 workers per
            # warm; a warm storm could queue ahead of live order placement).
            # The patched module httpx timeouts (connect=3s read=30s) bound
            # the call.
            self._client.get_clob_market_info(cid)
        except Exception:  # noqa: BLE001 — lazy in-library fallback applies
            logger.info(
                "warm_market_info failed for %s (order path falls back to "
                "lazy cache-miss GETs)",
                cid[:10],
            )
            return False
        finally:
            with self._warmed_lock:
                self._warming_condition_ids.discard(cid)
                # A15: drop the retained Future alongside the in-flight
                # marker (a caller that already grabbed it can still join
                # a completed Future — only the lookup goes away).
                self._warm_futures.pop(cid, None)
        with self._warmed_lock:
            # Market turnover is unbounded (a fresh market every 5m per coin);
            # reset rather than grow forever — a re-warm costs one GET. Also
            # clear the LIBRARY's never-evicted per-token dicts at the same
            # boundary (review finding: warming every discovered market grows
            # them ~200x faster than lazy use did; on the 2GB t3.small that
            # is a slow unbounded creep). Clearing is benign — the library
            # lazily refetches on the next order; expired-window tokens never
            # recur. Name-mangled access is fail-soft for library drift.
            if len(self._warmed_condition_ids) > 4096:
                self._warmed_condition_ids.clear()
                for _name in (
                    "_ClobClient__tick_sizes",
                    "_ClobClient__neg_risk",
                    "_ClobClient__fee_infos",
                    "_ClobClient__fee_rates",
                    "_ClobClient__token_condition_map",
                ):
                    try:
                        _d = getattr(self._client, _name, None)
                        if isinstance(_d, dict):
                            _d.clear()
                    except Exception:  # noqa: BLE001 — cache clear is best-effort
                        pass
            self._warmed_condition_ids.add(cid)
        return True

    def warm_market_info_async(self, condition_id: str) -> None:
        """Fire-and-forget `warm_market_info` on the CLOB executor.

        The tick's fetch phase calls this at market-discovery time so the
        warm-up RTT never blocks the tick. If an order fires while the warm
        is still in flight, the library's own lazy GETs apply (benign race:
        both writers store identical values under the GIL).

        In-flight dedup (review finding): without it, a slow/failing warm is
        re-submitted on EVERY scoring tick of the window (1Hz for sub-minute
        sleeves), stacking queued duplicates in the shared executor.

        A15: the submitted Future is retained in `_warm_futures` so
        `ensure_market_info_warm` can join the in-flight warm instead of
        paying a duplicate synchronous GET on the calling thread.
        """
        cid = str(condition_id or "")
        if not cid:
            return
        with self._warmed_lock:
            if (
                cid in self._warmed_condition_ids
                or cid in self._warming_condition_ids
            ):
                return
            self._warming_condition_ids.add(cid)
            # A15: submit UNDER the lock and retain the Future so the
            # worker's completion cleanup (warm_market_info's finally, which
            # also takes the lock) can never race the store. submit() only
            # enqueues — it cannot block on the worker.
            self._warm_futures[cid] = _clob_executor.submit(
                self.warm_market_info, cid
            )

    def _spend_sync_attempt(self, cid: str) -> bool:
        """Atomically consume `cid`'s ONE order-time blocking warm attempt.

        L4 venue-down bookkeeping shared by every branch of
        `ensure_market_info_warm` that blocks (the in-flight-Future join and
        the one-shot sync GET). Returns False when the attempt was already
        spent — the caller must not block again. Fresh market every window,
        so cap-and-clear rather than grow forever (mirrors the
        _warmed_condition_ids cap in warm_market_info).
        """
        with self._warmed_lock:
            if cid in self._warm_sync_attempted:
                return False
            if len(self._warm_sync_attempted) > 4096:
                self._warm_sync_attempted.clear()
            self._warm_sync_attempted.add(cid)
            return True

    def ensure_market_info_warm(self, condition_id: str) -> bool:
        """L4 order-time warm GUARANTEE: ensure the V2 client's static
        market-info cache is populated for `condition_id` BEFORE the FAK signs.

        `warm_market_info_async` fires at market discovery but is best-effort;
        if it hasn't completed when the first order fires, `create_market_order`
        pays up to 2 sequential cache-miss GETs (~100-200ms) inside the SIGN
        phase on the hot decision→fill path. When that async warm is still in
        flight this JOINS its retained Future (A15 — bounded wait) instead of
        firing a duplicate concurrent GET; only when no warm is in flight does
        it run ONE bounded synchronous warm (respecting the client's
        connect=3s / read=30s httpx timeouts) so the cache is hot before
        placement.

        Blocks at most ONCE per condition_id — whichever bounded block runs
        (the join or the sync GET) consumes the market's single blocking
        attempt, so a timed-out join or a joined-warm FAILURE returns False
        WITHOUT chaining the sync GET behind it in the same call (review F3:
        that fall-through doubled the pre-diff ~30s worst case to ~60s
        serial, and the join itself can sit QUEUED in the shared executor
        with zero network). Later calls after a spent attempt return
        immediately (the library's own lazy cache-miss GETs then apply), so
        a down venue cannot stall every order in the window; a late async
        success still populates _warmed_condition_ids for later callers.
        Returns True when the cache is (or already was) warm.
        """
        cid = str(condition_id or "")
        if not cid:
            return False
        with self._warmed_lock:
            if cid in self._warmed_condition_ids:
                return True
            if cid in self._warm_sync_attempted:
                # Already paid one blocking warm for this market — do not
                # block again even if it failed (venue-down guard).
                return False
            fut = self._warm_futures.get(cid)
        if fut is not None:
            # A15: an async warm for this cid is already in flight — join it
            # (bounded by the same 30s envelope as the sync GET below; the
            # worker's GET is itself bounded by the client httpx timeouts)
            # instead of firing the duplicate concurrent GET this method
            # used to issue while the warm raced it on the executor.
            try:
                if fut.result(timeout=self._timeout):
                    return True
                # Joined warm FAILED (worker already logged the venue error).
            except FuturesTimeoutError:
                # The still-pending worker keeps running: if it eventually
                # succeeds, _warmed_condition_ids picks it up for later
                # callers.
                logger.info(
                    "in-flight warm join timed out for %s (order path falls "
                    "back to lazy cache-miss GETs)",
                    cid[:10],
                )
            except Exception:  # noqa: BLE001 — worker already logged it
                pass
            # Review F3: the join WAS this market's one blocking attempt —
            # mark it spent and return; never stack the sync GET behind it.
            self._spend_sync_attempt(cid)
            return False
        if not self._spend_sync_attempt(cid):
            # Lost the race to a concurrent caller that just spent it.
            return False
        # warm_market_info is idempotent, bounded by the client httpx timeouts,
        # and adds cid to _warmed_condition_ids on success.
        return self.warm_market_info(cid)

    # ---- order placement -------------------------------------------------

    def place_gtc_limit_order(
        self, signal: "BetSignal", *, client_order_id: str
    ) -> OrderResult:
        """Place a fill-and-kill (FAK) marketable buy for `signal`.

        NOTE: the method name `place_gtc_limit_order` is retained for caller/
        test/schema compatibility, but the order is posted as **FAK**, not GTC.
        Audit ww3qot1x3 (go-live blocker #1): a GTC BUY rests its unfilled
        remainder on the book — which `reconcile_open_orders` never cancels,
        which can fill minutes later at an adverse print, and which collides
        with the within-minute top-up to double-commit exposure. FAK takes the
        marketable depth at/below the limit immediately and the venue drops the
        rest, so there is never a resting remainder. This also matches the
        backtest fill model (`walk_book_edge_preserving`, marketable fills) and
        the proven hightempbot live path (walker.py OrderType.FAK).

        The caller MUST reserve a `live_order_attempts` row with this
        `client_order_id` BEFORE invoking — see `order_attempts.reserve
        _live_order_attempt`. Without that durable artifact a crash between
        CLOB submit and ledger write would leave the bot unable to recover
        the in-flight order on the next tick.

        Returns `OrderResult.success=True` on CLOB accept, with `order_id`
        and `limit_price`. The realized fill columns are populated only when
        CLOB returns explicit fill metadata; otherwise the reconciler (U7)
        back-fills the actual matched size / VWAP on a later tick.
        """
        if not client_order_id:
            raise ValueError(
                "place_gtc_limit_order requires a client_order_id; reserve a "
                "live_order_attempts row first (see order_attempts.reserve_live_order_attempt)"
            )
        # 2026-07-25 latency audit: hoisted so the FAILURE paths can report
        # timings too. Both handlers below used to build OrderResult without
        # sign_ms/post_ms, and a venue-refused FAK (HTTP 400 -> PolyApiException
        # from the clob helper) is exactly that path — so ~55% of live orders,
        # the whole `no_liquidity` cohort, were latency-INVISIBLE. Worse, the
        # dashboard averaged those missing values in as zeros and reported order
        # post p50 ~= 0.0ms.
        #
        # NB a bare hoist is not enough: post_ms is assigned AFTER the call that
        # raises, so the handlers must compute elapsed from _t_post themselves.
        _t_sign: Optional[float] = None
        _t_post: Optional[float] = None
        sign_ms: Optional[float] = None
        post_ms: Optional[float] = None

        def _elapsed_post_ms() -> Optional[float]:
            """post_ms for a path that raised inside/after post_order."""
            if _t_post is None:
                return None
            return (time.monotonic() - _t_post) * 1000.0

        try:
            from py_clob_client_v2.order_builder.constants import BUY
            from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType

            # Audit w19oxqf24 (P2): price the FAK BUY cap at the walker's
            # executable limit_price (the most-expensive book level the walk
            # consumed), NOT top_ask (signal.fill_price). bet_size was
            # accumulated across levels up to that limit, so a cap at top_ask
            # only crosses top-of-book depth and systematically under-fills
            # versus the recorded stake. Fall back to fill_price for DRY/legacy
            # signals that carry no limit_price.
            limit_price = signal.limit_price
            order_price = float(
                limit_price if limit_price is not None else signal.fill_price
            )
            if not math.isfinite(order_price) or order_price <= 0:
                return OrderResult(
                    success=False,
                    error="order_price <= 0",
                    error_kind="unknown",
                    client_order_id=client_order_id,
                )

            order_amount = float(signal.bet_size)
            if not math.isfinite(order_amount) or order_amount <= 0:
                return OrderResult(
                    success=False,
                    error="order_amount <= 0",
                    error_kind="unknown",
                    client_order_id=client_order_id,
                )
            # Polymarket validates a market-buy maker amount to a max of 2
            # decimals ("max accuracy of 2"); a higher-precision float is
            # rejected HTTP 400 (audit: 2026-06 400s). Quantize DOWN to cents
            # so we never round the bet UP past the walked stake / bankroll.
            order_amount = float(
                Decimal(str(order_amount)).quantize(
                    Decimal("0.01"), rounding=ROUND_DOWN
                )
            )
            if order_amount <= 0:
                return OrderResult(
                    success=False,
                    error="order_amount rounds below $0.01",
                    error_kind="invalid_amount",
                    client_order_id=client_order_id,
                )

            # FAK BUYs are validated by Polymarket as market buys even when
            # they carry a price cap. Use the market-order builder so maker
            # amount is cash-sized to cents and taker size to 4 decimals.
            order_args = MarketOrderArgs(
                price=order_price,
                amount=order_amount,
                side=BUY,
                token_id=signal.poly_token_id,
                order_type=OrderType.FAK,
            )
            # A0 latency instrumentation: split the placement into the sign
            # phase (create_market_order = local EIP-712 + any market-info
            # cache-miss GETs — see OrderClient.warm_market_info) and the
            # post_order round trip, so the latency trace shows exactly where
            # the decision→fill budget goes.
            _t_sign = time.monotonic()
            signed_order = self._with_timeout(
                self._client.create_market_order, order_args
            )
            sign_ms = (time.monotonic() - _t_sign) * 1000.0
            # FAK (fill-and-kill): fill marketable depth at/below the limit now,
            # kill any remainder. Never rests on the book. See docstring +
            # audit ww3qot1x3 blocker #1 for why GTC was unsafe here.
            _t_post = time.monotonic()
            response = self._with_timeout(
                self._client.post_order, signed_order, OrderType.FAK
            )
            post_ms = (time.monotonic() - _t_post) * 1000.0
            logger.info(
                "order latency: sign=%.0fms post=%.0fms token=%s",
                sign_ms,
                post_ms,
                str(signal.poly_token_id)[:8],
            )
            order_id = response.get("orderID") or response.get("order_id")
            if not order_id:
                err = response.get("error") or "CLOB response missing orderID"
                return OrderResult(
                    success=False,
                    error=str(err),
                    error_kind=_classify_clob_error(err),
                    client_order_id=client_order_id,
                    sign_ms=sign_ms,
                    post_ms=post_ms,
                )

            fill_ts = utc_now_sql()
            # Audit P1 #4: do NOT record an optimistic full-stake placeholder.
            # A FAK order fills only the marketable depth at/below the limit
            # and kills the rest, so the realized fill is frequently smaller
            # than the requested size (or zero). Recording the full requested
            # size as the realized fill let the resolution sweep settle
            # PHANTOM shares as realized PnL whenever the 60s reconciler had
            # not yet overwritten the placeholder (its only guard is
            # live_size<=0, which an always-positive placeholder passes).
            # Use the REAL matched fill from the CLOB response; when the
            # response carries no (size AND price) fill metadata, leave the
            # fill columns None so record_bet writes NULL and the sweep
            # defers settlement to reconcile_open_orders (U7).
            matched_size, matched_price = _parse_fak_fill(response, side="BUY")
            if matched_size is None or matched_price is None:
                matched_size = None
                matched_price = None
                filled_value: Optional[float] = None
            else:
                filled_value = matched_size * matched_price
            return OrderResult(
                success=True,
                order_id=str(order_id),
                client_order_id=client_order_id,
                limit_price=order_price,
                filled_size_tokens=matched_size,
                filled_price_vwap=matched_price,
                filled_value_usd=filled_value,
                fill_ts=fill_ts,
                sign_ms=sign_ms,
                post_ms=post_ms,
            )
        except FuturesTimeoutError as exc:
            return OrderResult(
                success=False,
                error=str(exc),
                error_kind="network",
                client_order_id=client_order_id,
                sign_ms=sign_ms,
                post_ms=_elapsed_post_ms(),
            )
        except Exception as exc:
            logger.error(
                "Order placement failed for %s/%s: %s",
                signal.coin,
                signal.sleeve,
                exc,
                exc_info=True,
            )
            return OrderResult(
                success=False,
                error=str(exc),
                error_kind=_classify_clob_error(exc),
                client_order_id=client_order_id,
                sign_ms=sign_ms,
                post_ms=_elapsed_post_ms(),
            )

    def fetch_order_fill(
        self,
        order_id: str,
        *,
        intent_amount_usd: Optional[float] = None,
        intent_price: Optional[float] = None,
    ) -> tuple[Optional[float], Optional[float], Optional[str]]:
        """Immediate GET of a just-posted order to read its realized fill.

        A FAK ``post_order`` response frequently carries NO fill metadata, so
        ``place_gtc_limit_order`` returns ``filled_* = None`` and the ledger
        row's ``live_filled_size`` stays NULL until the 60s reconciler
        (``reconcile_open_orders``). For a last-30s sub-minute entry that
        reconciler tick lands AFTER ``window_end``, so the PURE_STOP_04
        held-position (T3) pass — which only considers still-open windows carrying a
        realized fill — NEVER sees the position as a candidate (2026-07-10:
        13h armed / 20 losses / 0 stop attempts).

        This runs ONE follow-up GET of the order by id. The caller invokes it
        AFTER the order posted, so it is OFF the decision→fill critical path
        and bounded by the client timeout. It reuses the SAME CLOB response
        parsing the reconciler uses, so the fill it lands equals the venue
        truth the reconciler would later write (``intent_amount_usd`` /
        ``intent_price`` feed the reconciler's price-derivation fallbacks for
        a cash-sized market buy that filled below its cap).

        Returns ``(filled_size_tokens, filled_price_vwap, fill_ts)``; every
        field is None when the venue has not (yet) reported the fill. Best
        effort: any failure returns ``(None, None, None)`` and the 60s
        reconciler remains the authority."""
        if not order_id:
            return None, None, None
        try:
            from cryptobot.persistence.reconciliation import (
                _make_default_order_fetcher,
                _read_clob_order_payload,
            )
        except Exception:  # noqa: BLE001 — never break the order path on import
            logger.debug("fetch_order_fill: reconciliation import failed", exc_info=True)
            return None, None, None
        fetcher = _make_default_order_fetcher(self)
        try:
            payload = self._with_timeout(fetcher, str(order_id))
        except Exception:  # noqa: BLE001 — best-effort; reconciler is the backstop
            logger.debug(
                "fetch_order_fill: get_order failed for %s", order_id, exc_info=True
            )
            return None, None, None
        if not isinstance(payload, dict):
            return None, None, None
        _status, fill_size, fill_price, fill_ts = _read_clob_order_payload(
            payload,
            intent_amount_usd=intent_amount_usd,
            intent_price=intent_price,
        )
        if fill_size is None or fill_size <= 0:
            return None, None, None
        return fill_size, fill_price, fill_ts


__all__ = [
    "ClobClientUnavailable",
    "OrderClient",
    "OrderResult",
    "_SIGNATURE_TYPE_LABELS",
    "_classify_clob_error",
    "_make_clob_client",
    "_validate_wallet_config",
]
