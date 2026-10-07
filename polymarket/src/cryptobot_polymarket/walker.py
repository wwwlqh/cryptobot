"""Live order placement on the Polymarket CLOB.

``OrderClient`` wraps ``py_clob_client_v2`` for balance reads and FAK order
placement; ``_classify_clob_error`` maps venue errors to retry buckets. Book
reads live in ``cryptobot.execution.walk_book``.

Balance reads work without ``py_clob_client_v2``: a missing package surfaces as
``ClobClientUnavailable`` from ``OrderClient.__init__``. Tests stub the
OrderClient factory and never hit the real CLOB.
"""

from __future__ import annotations

import logging
import math
import re
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


# One shared executor for CLOB calls, so each call gets a timeout independent
# of the library's own HTTP timeout. The pool lives in
# cryptobot.execution.executors because the core pipeline submits its cold-cell
# fan-out to the same pool and must not import a venue package to reach it.
_clob_executor = clob_executor


class ClobClientUnavailable(RuntimeError):
    """Raised when `py_clob_client_v2` is not installed in the runtime."""


# ``_SIGNATURE_TYPE_LABELS`` and ``_validate_wallet_config`` live in
# cryptobot.execution.wallet_topology next to the config they validate; they
# are re-exported here.
# ---------------------------------------------------------------------------
# CLOB error classification
# ---------------------------------------------------------------------------


def _coerce_fill_float(value: object) -> Optional[float]:
    """Coerce a CLOB numeric field to a positive float, else None.

    Zero or negative means no fill: a FAK that matched nothing must not be
    recorded as a fill.
    """
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
    """Return (matched_size_tokens, avg_fill_price) from a FAK/FOK
    ``post_order`` response, or (None, None) when it carries no fill data.
    Never invents a full-stake fill.

    Polymarket reports the match as ``makingAmount``/``takingAmount`` (what our
    order gave and received), not as an average price. A BUY makes USDC and
    takes tokens; a SELL makes tokens and takes USDC. VWAP = usd / tokens.

    The order's limit ``price`` is never used here: it is the requested price,
    not the fill price.
    """
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
    """Map a CLOB exception or error string to a retry-policy bucket.

    Returns one of: "auth", "insufficient_funds", "market_closed",
    "invalid_amount", "venue_unavailable", "post_only", "no_liquidity",
    "network", "unknown".

    Matches substrings of the lower-cased message, since ``py_clob_client_v2``
    raises plain RuntimeErrors. ``venue_unavailable`` and ``post_only`` trigger
    the venue-degradation alert; ``no_liquidity`` is a normal thin-book outcome
    and does not alert; ``invalid_amount`` is terminal (resending the same
    over-precise amount fails again; see the rounding in
    ``place_gtc_limit_order``).
    """
    text = str(exc_or_msg or "").lower()
    if not text:
        return "unknown"

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
        # Match HTTP status codes as whole tokens only. Reject messages include
        # the 0x... order ID, and a hex hash containing "401" or "403" would
        # otherwise turn a routine no_liquidity reject into a terminal auth
        # error.
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
    # HTTP 425 "order manager not ready, please retry": Polymarket returns this
    # in bursts during CLOB degradation. Retryable, but kept out of "unknown"
    # because the pipeline uses it to fire the venue-degradation alert.
    if "order manager not ready" in text or "please retry" in text:
        return "venue_unavailable"
    # HTTP 503 post-only window: only post-only orders and cancels are
    # accepted, so a marketable FAK buy is rejected until it lifts. Checked
    # before the generic 5xx bucket, which also matches "503".
    if "post-only" in text or "post_only" in text or "post only" in text:
        return "post_only"
    # HTTP 400: the market-buy amount had more than 2 decimals. Terminal, since
    # the same amount fails again; ``place_gtc_limit_order`` rounds to cents to
    # avoid it.
    if "max accuracy" in text or "invalid amount" in text:
        return "invalid_amount"
    # HTTP 400 thin book: the FAK buy found no depth at or below its cap and
    # was killed. A normal market state, not an error; a later tick may still
    # fill.
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

# Monotonic time of the last response on the CLOB signing pool (orders, balance
# reads, market-info warms, pings). Written by the response hook and read by
# ``OrderClient.ping_order_pool`` to skip a ping right after other traffic. A
# slightly stale read under the GIL only costs one extra GET.
_CLOB_LAST_RESPONSE_MONOTONIC: float = 0.0


def _note_clob_pool_response(_response: object) -> None:
    """httpx response hook: record signing-pool activity for the ping guard."""
    global _CLOB_LAST_RESPONSE_MONOTONIC
    _CLOB_LAST_RESPONSE_MONOTONIC = time.monotonic()


# The pool built by ``_install_clob_http_timeout``, kept so ``ping_order_pool``
# does not reach into the library-private
# ``py_clob_client_v2.http_helpers.helpers._http_client`` on every call. None
# if the install was skipped or failed; the ping then falls back to that
# private reference.
_CLOB_HTTP_CLIENT: Optional[Any] = None


def _install_clob_http_timeout() -> None:
    """Replace py_clob_client_v2's module-level httpx.Client with one that has
    explicit per-phase timeouts.

    connect=3s fails fast on a dead socket (TCP+TLS to the venue takes ~10ms).
    read=30s must outlast the 250ms taker delay plus matching; a shorter read
    could abandon an order the venue still fills. write=30s and pool=5s bound
    hung calls so ``_clob_executor`` workers are released. Patches once per
    process.
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
                # httpx's default keepalive_expiry of 5s would make nearly
                # every order pay a fresh connect and TLS handshake (~20-40ms),
                # since orders fire more than 5s apart. 65s sits just above the
                # 60s balance-read cadence, so that read keeps a pooled
                # connection warm; the clob_ping job (CLOB_PING_SECONDS) keeps
                # it warm even when a balance read is skipped. Connection
                # limits leave headroom for the warm and reconcile calls that
                # share the pool.
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                    keepalive_expiry=65.0,
                ),
                # With the ping enabled, a response hook records pool activity
                # so the ping can skip itself. CLOB_PING_SECONDS=0 installs no
                # hook.
                event_hooks=(
                    {"response": [_note_clob_pool_response]}
                    if config.CLOB_PING_SECONDS > 0
                    else None
                ),
            )
            # Keep a stable reference to the pool for ping_order_pool.
            _CLOB_HTTP_CLIENT = _clob_helpers._http_client
            _HTTPX_TIMEOUT_INSTALLED = True
            logger.info(
                "Installed CLOB http timeouts (connect=3s read=30s write=30s pool=5s)"
            )
            if config.CLOB_PING_SECONDS > 0:
                # The response hook that feeds the ping's skip guard is
                # installed.
                logger.info("clob_ping warmth guard: active")
        except Exception:
            logger.warning(
                "Failed to install CLOB http timeouts — httpx default applies",
                exc_info=True,
            )
            if config.CLOB_PING_SECONDS > 0:
                # With no response hook the guard timestamp stays 0.0, so every
                # ping fires; log that once at boot.
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
    # Typed as ``Any``: ``py_clob_client_v2`` may be missing at runtime, so a
    # stricter annotation would make type checkers resolve an import that can
    # legitimately fail.
    """Construct a V2 CLOB client with a clear failure mode.

    ``signature_type`` and ``funder`` pick the on-chain wallet the client signs
    for and reads balance from (see ``_SIGNATURE_TYPE_LABELS``). A missing or
    zero ``signature_type`` means EOA. An empty ``funder`` lets the library
    derive it (fine for EOA and POLY_PROXY; POLY_1271 needs it explicitly).
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
# OrderClient: the single live order placement surface
# ---------------------------------------------------------------------------


class OrderClient:
    """Live CLOB client.

    Wraps ``py_clob_client_v2`` for balance checks, market-info warming and FAK
    order placement. The pipeline uses it for LIVE wallets; DRY wallets only
    walk the book.

    Construction fails closed:

      * ``py_clob_client_v2`` not installed -> ``ClobClientUnavailable``
      * empty ``poly_private_key`` -> ``ValueError``
      * signature type 3 with an empty funder -> ``ValueError`` from
        ``_validate_wallet_config``

    main.py validates the wallet setup before constructing this, so a bad
    config fails at the boot check with a clear message.
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
        # condition_ids whose static market info (tick size, neg_risk, fees,
        # token->condition map) is already cached in the client, plus the
        # in-flight set that stops a failing warm from being resubmitted every
        # tick. See warm_market_info(_async).
        self._warmed_condition_ids: set[str] = set()
        self._warming_condition_ids: set[str] = set()
        # The in-flight async warm's Future per condition_id, so
        # ensure_market_info_warm can wait on it instead of starting a
        # duplicate GET. Entries live exactly as long as the condition_id is in
        # _warming_condition_ids; both are cleared in warm_market_info's
        # finally.
        self._warm_futures: dict[str, Future] = {}
        # condition_ids that already had their one blocking warm at order time
        # (success or failure), so a down venue does not block every order for
        # that market.
        self._warm_sync_attempted: set[str] = set()
        self._warmed_lock = threading.Lock()
        # Resolve the order-version probe now, so the first create_market_order
        # does not GET /version on the fill path. get_version() alone does not
        # fill the private __cached_version, hence the name-mangled access;
        # failures are ignored in case the library changes.
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
        """Run a CLOB call with a bounded timeout so a hung CLOB cannot block
        the scheduler thread indefinitely. Generic over the return type so
        callers keep the wrapped call's type.
        """
        future = _clob_executor.submit(fn, *args)
        return future.result(timeout=self._timeout)

    # ---- read-only balance -----------------------------------------------

    def check_balance(self) -> Optional[float]:
        """pUSD (V2 collateral) balance in dollars, or None on API failure."""
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

    # ---- signing-pool warmth ping -----------------------------------------

    def ping_order_pool(self) -> bool:
        """Keep the signing-pool connection warm with ``GET /ok``.

        Called by the clob_ping scheduler job (~every 20s) so the next order
        POST does not pay DNS, TCP and a full TLS handshake (httpcore never
        resumes TLS sessions), and so buffered GOAWAY/FIN frames are drained
        off the decision path.

        Never delays an order: it runs on a scheduler thread, skips when any
        pool response arrived within CLOB_PING_SKIP_IF_ACTIVE_S, and uses the
        short CLOB_PING_TIMEOUT_S instead of the pool's 30s read timeout. /ok
        is unauthenticated and has no side effects. Uses the pool reference
        captured at install time (``_CLOB_HTTP_CLIENT``), falling back to the
        library-private client only if that is missing. Never raises; returns
        True only when a ping was sent and returned 200.
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
                # No captured pool reference; use the library-private client.
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

    # ---- market-info cache pre-warm ---------------------------------------

    def warm_market_info(self, condition_id: str) -> bool:
        """Pre-populate the client's per-token static caches for a market.

        Up/down markets get new token_ids every window, so without this the
        first order in a window makes two sequential cache-miss GETs inside
        ``create_market_order``, on the fill path. One
        ``get_clob_market_info(condition_id)`` call caches tick size, neg_risk,
        fee info and the token->condition map for both tokens, so placement is
        local signing plus one POST.

        Fail-soft: if the warm fails the order path falls back to the library's
        lazy GETs. Returns True when the cache is (or already was) warm.
        """
        cid = str(condition_id or "")
        if not cid:
            return False
        with self._warmed_lock:
            if cid in self._warmed_condition_ids:
                return True
        try:
            # Call the client directly, not through _with_timeout. When
            # submitted by warm_market_info_async this already runs on a
            # _clob_executor worker; _with_timeout would queue a second task in
            # the same pool while blocking this one, and a burst of warms could
            # then queue ahead of live orders. The client's httpx timeouts
            # bound the call.
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
                # Drop the Future along with the in-flight marker. A caller
                # already holding it can still wait on the completed Future.
                self._warm_futures.pop(cid, None)
        with self._warmed_lock:
            # A new market opens every 5 minutes per coin, so reset the set
            # rather than let it grow (a re-warm costs one GET). Also clear the
            # library's per-token dicts, which it never evicts; expired tokens
            # never come back and the library refetches lazily. Name-mangled
            # access, failures ignored.
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
        """Run ``warm_market_info`` on the CLOB executor without waiting.

        The tick calls this when it discovers a market, so the warm never
        blocks the tick. If an order fires before the warm finishes, the
        library's own lazy GETs run; both write identical values, so the race
        is harmless.

        A warm already in flight is not resubmitted, so a slow or failing warm
        does not stack duplicates in the shared executor every tick. The Future
        is kept in ``_warm_futures`` so ``ensure_market_info_warm`` can wait on
        it.
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
            # Submit and store the Future under the lock, so the worker's
            # cleanup (in warm_market_info's finally, which also takes the
            # lock) cannot run before the store. submit() only enqueues and
            # cannot block on the worker.
            self._warm_futures[cid] = _clob_executor.submit(
                self.warm_market_info, cid
            )

    def _spend_sync_attempt(self, cid: str) -> bool:
        """Atomically use up ``cid``'s single blocking warm at order time.

        Shared by both blocking branches of ``ensure_market_info_warm``
        (waiting on the in-flight Future, and the one synchronous GET). Returns
        False if the attempt was already used, so the caller must not block
        again. Capped and cleared like _warmed_condition_ids, since every
        window is a new market.
        """
        with self._warmed_lock:
            if cid in self._warm_sync_attempted:
                return False
            if len(self._warm_sync_attempted) > 4096:
                self._warm_sync_attempted.clear()
            self._warm_sync_attempted.add(cid)
            return True

    def ensure_market_info_warm(self, condition_id: str) -> bool:
        """Make sure the client's market-info cache is warm for
        ``condition_id`` before the FAK is signed.

        ``warm_market_info_async`` runs at market discovery but may not have
        finished when the first order fires; ``create_market_order`` would then
        make up to two cache-miss GETs (~100-200ms) while signing. If the async
        warm is still in flight, wait on its Future (bounded); if none is in
        flight, run one bounded synchronous warm.

        Blocks at most once per condition_id. Whichever wait runs uses up the
        market's one blocking attempt, so a timed-out or failed wait returns
        False without then also running the synchronous GET (which would double
        the worst case to ~60s). Later calls return immediately and the
        library's lazy GETs apply, so a down venue cannot stall every order in
        the window. A late async success still marks the market warm for later
        callers. Returns True when the cache is (or already was) warm.
        """
        cid = str(condition_id or "")
        if not cid:
            return False
        with self._warmed_lock:
            if cid in self._warmed_condition_ids:
                return True
            if cid in self._warm_sync_attempted:
                # Already blocked once for this market; do not block again,
                # even if it failed.
                return False
            fut = self._warm_futures.get(cid)
        if fut is not None:
            # An async warm for this cid is in flight: wait on it, bounded by
            # the same 30s envelope as the synchronous GET below, rather than
            # issuing a duplicate GET.
            try:
                if fut.result(timeout=self._timeout):
                    return True
                # The warm we waited on failed (the worker already logged the
                # error).
            except FuturesTimeoutError:
                # The worker keeps running; if it succeeds later,
                # _warmed_condition_ids picks it up for later callers.
                logger.info(
                    "in-flight warm join timed out for %s (order path falls "
                    "back to lazy cache-miss GETs)",
                    cid[:10],
                )
            except Exception:  # noqa: BLE001 — worker already logged it
                pass
            # The wait was this market's one blocking attempt: mark it used and
            # return without also running the synchronous GET.
            self._spend_sync_attempt(cid)
            return False
        if not self._spend_sync_attempt(cid):
            # Another caller used the attempt first.
            return False
        # warm_market_info is idempotent, bounded by the client's httpx
        # timeouts, and adds cid to _warmed_condition_ids on success.
        return self.warm_market_info(cid)

    # ---- order placement -------------------------------------------------

    def place_gtc_limit_order(
        self, signal: "BetSignal", *, client_order_id: str
    ) -> OrderResult:
        """Place a fill-and-kill (FAK) marketable buy for ``signal``.

        Despite the name (kept for callers, tests and schema), the order is
        FAK, not GTC. A GTC buy would leave its unfilled remainder resting on
        the book, where nothing cancels it, it can fill minutes later at a
        worse price, and it can double up with a top-up. FAK takes the depth at
        or below the limit now and the venue drops the rest. This matches the
        backtest's fill model (``walk_book_edge_preserving``).

        The caller must reserve a ``live_order_attempts`` row with this
        ``client_order_id`` first
        (``order_attempts.reserve_live_order_attempt``). Without it, a crash
        between submit and ledger write would leave an in-flight order the bot
        cannot recover.

        Returns ``OrderResult.success=True`` when the CLOB accepts, with
        ``order_id`` and ``limit_price``. Fill columns are set only when the
        response carries fill data; otherwise the reconciler fills in the
        matched size and VWAP later.
        """
        if not client_order_id:
            raise ValueError(
                "place_gtc_limit_order requires a client_order_id; reserve a "
                "live_order_attempts row first (see order_attempts.reserve_live_order_attempt)"
            )
        # Defined before the try so the failure handlers can report timings
        # too. A venue-refused FAK (HTTP 400, raised as PolyApiException) is a
        # large share of live orders, and without this they would carry no
        # sign_ms/post_ms. post_ms is assigned after the call that raises, so
        # the handlers compute it from _t_post themselves.
        _t_sign: Optional[float] = None
        _t_post: Optional[float] = None
        sign_ms: Optional[float] = None
        post_ms: Optional[float] = None

        def _elapsed_post_ms() -> Optional[float]:
            """post_ms for a path that raised inside or after post_order."""
            if _t_post is None:
                return None
            return (time.monotonic() - _t_post) * 1000.0

        try:
            from py_clob_client_v2.order_builder.constants import BUY
            from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType

            # Cap the FAK buy at the walker's executable limit_price (the most
            # expensive level the walk consumed), not top_ask. The stake was
            # accumulated across levels up to that limit, so a cap at top_ask
            # would only cross the top level and under-fill. Signals without a
            # limit_price fall back to fill_price.
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
            # Polymarket rejects a market-buy amount with more than 2 decimals
            # (HTTP 400). Round down to cents so the stake never exceeds the
            # walked amount.
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

            # Polymarket validates FAK buys as market buys even with a price
            # cap, so use the market-order builder: cash amount to cents, token
            # size to 4 decimals.
            order_args = MarketOrderArgs(
                price=order_price,
                amount=order_amount,
                side=BUY,
                token_id=signal.poly_token_id,
                order_type=OrderType.FAK,
            )
            # Time the sign phase (create_market_order: local EIP-712 signing
            # plus any market-info cache-miss GETs, see warm_market_info)
            # separately from the post_order round trip, for the latency trace.
            _t_sign = time.monotonic()
            signed_order = self._with_timeout(
                self._client.create_market_order, order_args
            )
            sign_ms = (time.monotonic() - _t_sign) * 1000.0
            # FAK: fill the depth at or below the limit now and kill the rest.
            # Never rests on the book (see docstring).
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
            # Record the real matched fill, never the requested size. A FAK
            # often fills less than requested, or nothing; recording the full
            # size would let the resolution sweep settle shares that were never
            # bought before the reconciler corrected the row. If the response
            # has no size and price, leave the fill columns None so record_bet
            # writes NULL and settlement waits for reconcile_open_orders.
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
        """Immediately GET a just-posted order to read its fill.

        A FAK ``post_order`` response often has no fill data, which leaves the
        ledger row's ``live_filled_size`` NULL until the 60s reconciler runs.
        For an entry in a market's last 30 seconds that is after
        ``window_end``, too late for anything that needs the fill while the
        market is still open.

        This makes one follow-up GET by order id, after the order is posted, so
        it is off the decision-to-fill path and bounded by the client timeout.
        It parses the response the same way the reconciler does, so it records
        the same fill the reconciler would (``intent_amount_usd`` /
        ``intent_price`` feed the reconciler's price fallbacks for a cash-sized
        buy that filled below its cap).

        Returns ``(filled_size_tokens, filled_price_vwap, fill_ts)``, all None
        when the venue has not reported a fill yet. Any failure returns
        ``(None, None, None)``; the 60s reconciler remains the authority.
        """
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
