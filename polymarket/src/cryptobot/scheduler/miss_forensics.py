"""On-chain miss forensics — WHY each venue-rejected FAK actually missed.

Operator order 2026-07-30: every missed trade (a venue `no_liquidity` FAK
reject) must be classified from CHAIN evidence — a faster TAKER beat us to the
resting ask, or the MAKER cancelled it — and the verdict must reach the
dashboard. Book re-reads cannot answer this: `reject_requotes` only records
what the book looked like ~1s LATER, so "the ask is gone" is equally
consistent with traded-away and with pulled. That ambiguity is exactly what
produced (and then refuted) the "68% maker-cancel" inference. The trade tape
is the only witness that distinguishes them.

METHOD (productionizes the 2026-07-30 manual forensic, which classified 45/45)
----------------------------------------------------------------------------
For each reject we know the exact token we sent (`live_order_attempts` carries
`poly_token_id` / `poly_market_id` for the attempt — the recorder is tokenless
and Gamma has purged these markets, so this join is the ONLY token source):

  1. COMPLEMENT TOKEN. An ask on token T is the same liquidity as a bid on its
     complement C, so a third party can consume it in TWO shapes. We resolve
     the (Up, Down) token pair from ``clob.polymarket.com/markets/{condition}``
     — the CLOB retains markets Gamma has purged.

  2. MINT-MATCH RULE. A print counts as consuming OUR ask when it is either
     a taker BUY of T at price p, or a taker SELL of C at 1-p. Testing only
     one shape silently halves the evidence. Our own funder's prints are
     excluded (a sibling attempt on the same window can fill).

  3. SETTLEMENT-LAG CORRECTION. data-api's `timestamp` IS the Polygon block
     timestamp, and a block lands ~+3s AFTER the venue matched the order
     (calibrated on 106 of our OWN fills: range +2..+4, median +3, never <=+1).
     Our reject row is stamped after the CLOB round-trip. So a print seen at
     tape dt corresponds to a VENUE MATCH at dt - lag relative to our arrival.
     Skipping this correction reads every rival print as ~3s LATE and inverts
     the verdict. The lag is re-calibrated from our own recent fills (see
     :func:`calibrate_lag_s`) and the value used is persisted per row in
     ``miss_forensics.lag_s_used`` so a historical verdict stays auditable
     even after the constant moves.

  4. LEVEL BAND. "At our level" is bounded on BOTH sides — from the ask we saw
     up to our limit (+-0.5c). The upper bound is obvious (we would not pay
     more); the lower bound is the one that is easy to omit and expensive to
     omit, because a print far BELOW our ask is a different level of the book
     (typically the mint-matched complement of a trade deep on the other side)
     and counting it CONVICTS a miss that no speed could have won. See
     :func:`level_band` for the measured cost of dropping it.

  5. VERDICTS, over venue-match dt in [-5s, +10s] of our reject:
       * ``FASTER_TAKER``  — a print inside the level band: the quote was REAL
                             and someone else took it.
       * ``REPRICED_ONLY`` — prints exist, but all OUTSIDE the band: the level
                             we wanted was never traded.
       * ``MAKER_CANCEL``  — no print at all in the window: the resting order
                             was withdrawn.
       * ``PENDING``       — transient RPC/HTTP failure or budget exhaustion;
                             retried on the next run.
       * ``UNRESOLVED``    — the venue no longer exposes both tokens for this
                             condition; terminal (nothing more to try).

  6. RIVAL MAKER. For a FASTER_TAKER we fetch the beat-us print's receipt and
     read the OrderFilled log's indexed `maker` topic — the owner of the
     resting order we lost. The exchange address + OrderFilled topic0 are
     GROUND-TRUTHED from our OWN recent fill receipts rather than hardcoded
     (see :func:`derive_chain_signature`), so a venue contract migration
     cannot silently produce zero matches.

The job is strictly off the decision path: it only ever reads `subminute_fills`
/ `live_order_attempts` and writes `miss_forensics`. Every exception is
swallowed with a WARN, the RPC/HTTP call counts are hard-capped per run, and a
failed row simply stays PENDING for the next fire. Polymarket-only — the same
core runs a predict.fun paper unit, whose tape is not on Polygon.
"""

from __future__ import annotations

import contextlib
import json
import logging
import sqlite3
import statistics
import time
from dataclasses import dataclass
from typing import Any, Callable, ContextManager, Iterable, Optional, Sequence

from cryptobot.venues.types import TokenBucket

logger = logging.getLogger(__name__)

CLOB_MARKETS = "https://clob.polymarket.com/markets"
DATA_API_TRADES = "https://data-api.polymarket.com/trades"
# Polymarket's edge rejects a bare python-urllib UA on some paths; the manual
# forensic used a browser UA throughout and every endpoint answered.
_UA = "Mozilla/5.0"
_HTTP_TIMEOUT_S = 20.0

VERDICT_FASTER_TAKER = "FASTER_TAKER"
VERDICT_REPRICED_ONLY = "REPRICED_ONLY"
VERDICT_MAKER_CANCEL = "MAKER_CANCEL"
VERDICT_PENDING = "PENDING"
VERDICT_UNRESOLVED = "UNRESOLVED"
#: Verdicts that are re-attempted on a later run. UNRESOLVED is terminal:
#: the CLOB no longer exposes the pair, so no amount of retrying helps.
RETRYABLE_VERDICTS = (VERDICT_PENDING,)

#: A print within half a cent of the level band counts as "at our level" — the
#: venue's own price strings carry more precision than the book we quoted off.
PRICE_TOL = 0.005
#: Stand-in gap between limit and ask when a legacy row recorded no quote_ask
#: (live rows always do; the limit is the ask plus the anchored FAK pad).
_ASK_FALLBACK = 0.01
#: Venue-match dt window vs the reject, in seconds. Asymmetric on purpose: a
#: print BEFORE us proves we lost the race, and a few seconds AFTER still
#: proves the level was live liquidity rather than a withdrawn quote.
WINDOW_LO_S = -5.0
WINDOW_HI_S = 10.0

DEFAULT_LAG_S = 3
_LAG_MIN_S, _LAG_MAX_S = 0, 6
#: Below this many matched own-fills the calibration is noise — keep the
#: established constant rather than chasing a 2-sample median.
_MIN_LAG_SAMPLES = 5
_LAG_TTL_S = 6 * 3600.0

#: Settlement lag means a just-created reject has no tape yet; 60s is ~20x the
#: median +3s so a fresh reject is never mis-read as MAKER_CANCEL.
MIN_AGE_S = 60
#: Beyond this the row is history — the backfill has had many chances and we
#: refuse to re-walk the whole table forever.
MAX_AGE_S = 48 * 3600
#: Per-run caps. A stalled venue must never let this job spin.
RPC_BUDGET = 200
HTTP_BUDGET = 300
MAX_ROWS_PER_RUN = 80
#: Steady-state pacing for the shared public endpoints (see _Budget). A full
#: HTTP_BUDGET run drains in ~60s, comfortably inside the 300s job cadence,
#: while the burst keeps a normal few-row pass instant.
_HTTP_RATE_PER_S = 5.0
_HTTP_BURST = 20
#: Longest we will wait on the limiter (a server 429 parks it for 10s). Past
#: this the venue is telling us to stop, so the batch ends and rows stay PENDING.
_LIMIT_WAIT_MAX_S = 15.0

# Module-level derivations, cached across fires (the process is long-lived).
# Both are re-derived on restart at a cost of a few calls.
_LAG_STATE: dict[str, float] = {}
_CHAIN_SIG: dict[str, str] = {}
_PAIR_CACHE: dict[str, tuple[str, str]] = {}
_PAIR_CACHE_MAX = 512


# ---------------------------------------------------------------------------
# Pure classification core (no I/O — unit-tested directly)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChainPrint:
    """One third-party print, normalized onto OUR token's ask price."""

    dt_s: float
    """Venue-match time minus our reject time (lag-corrected). Negative =
    matched BEFORE our order arrived, i.e. they beat us."""
    price: float
    """Price in OUR token's ask terms (a complement SELL is mapped to 1-p)."""
    size: float
    kind: str
    """``buyT`` = taker BUY of our token; ``sellC`` = taker SELL of the
    complement. Both consume the same resting ask."""
    tx_hash: Optional[str] = None
    taker: Optional[str] = None


def miss_forensics_armed(*, enabled: bool, venue: Any) -> bool:
    """Should the job be registered at all? (main.py's gate, made testable.)

    Two independent conditions, both hard:

    * the kill switch ``CRYPTOBOT_MISS_FORENSICS_ENABLED`` is on, and
    * the venue is Polymarket. The evidence here is *Polygon's* trade tape read
      through Polymarket's data-api/CLOB. The predict.fun paper unit shares this
      whole core, and arming there would look up predict.fun token ids on
      Polymarket endpoints — every lookup misses, so every row would be filed
      MAKER_CANCEL, i.e. a table of confident falsehoods rather than an empty
      one. Gated on the VENUE rather than the adapter registry because the
      METHOD is venue-specific, not just an endpoint.
    """
    return bool(enabled) and str(venue or "").strip().lower() == "polymarket"


def normalize_print(
    asset: Any, side: Any, price: Any, *, token_id: str, complement_id: Optional[str]
) -> Optional[tuple[float, str]]:
    """The MINT-MATCH rule: does this print consume an ask on ``token_id``?

    Returns ``(price_in_our_terms, kind)`` or None. A BUY of our token prints
    at p; a SELL of the complement prints at 1-p because minting a complete
    set and selling the other leg is the identical trade. Only checking the
    first shape loses roughly half the evidence.
    """
    side_u = str(side or "").upper()
    asset_s = str(asset or "")
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if asset_s and asset_s == str(token_id) and side_u == "BUY":
        return (p, "buyT")
    if complement_id and asset_s and asset_s == str(complement_id) and side_u == "SELL":
        return (1.0 - p, "sellC")
    return None


def venue_match_dt(tape_ts: Any, reject_ts: Any, lag_s: Any) -> float:
    """Tape (block) timestamp -> venue-match dt relative to our arrival.

    The block lands ``lag_s`` AFTER the venue matched, so subtracting the lag
    moves a rival print back onto our own clock. Without this every rival
    print reads ~3s late and a lost race looks like a late refill.
    """
    return float(tape_ts) - float(reject_ts) - float(lag_s)


def in_window(dt_s: float) -> bool:
    """Is this venue-match dt inside the evidence window?"""
    return WINDOW_LO_S <= dt_s <= WINDOW_HI_S


def collect_prints(
    trades: Iterable[dict],
    *,
    token_id: str,
    complement_id: Optional[str],
    reject_ts: float,
    lag_s: float,
    funder: Optional[str] = None,
) -> list[ChainPrint]:
    """Lag-correct, mint-match and window-filter a raw data-api trade list.

    Our own funder's prints are dropped: under multifill a sibling attempt on
    the same window can genuinely fill, and counting our own fill as a rival
    would manufacture a FASTER_TAKER out of our own success.
    """
    own = str(funder or "").lower()
    out: list[ChainPrint] = []
    for tr in trades or []:
        if not isinstance(tr, dict):
            continue
        if own and str(tr.get("proxyWallet") or "").lower() == own:
            continue
        ts = tr.get("timestamp")
        if ts is None:
            continue
        try:
            dt = venue_match_dt(ts, reject_ts, lag_s)
        except (TypeError, ValueError):
            continue
        if not in_window(dt):
            continue
        hit = normalize_print(
            tr.get("asset"), tr.get("side"), tr.get("price"),
            token_id=token_id, complement_id=complement_id,
        )
        if hit is None:
            continue
        price, kind = hit
        try:
            size = float(tr.get("size") or 0.0)
        except (TypeError, ValueError):
            size = 0.0
        out.append(ChainPrint(
            dt_s=round(dt, 3), price=round(price, 4), size=size, kind=kind,
            tx_hash=(str(tr["transactionHash"]) if tr.get("transactionHash") else None),
            taker=(str(tr["proxyWallet"]).lower() if tr.get("proxyWallet") else None),
        ))
    out.sort(key=lambda p: (p.dt_s, p.price))
    return out


def level_band(limit_price: Any, quote_ask: Any) -> tuple[float, float]:
    """The price band that IS "the ask we tried to take", inclusive.

    BOUNDED ON BOTH SIDES, and the lower bound is the load-bearing half. The
    upper bound is our limit: we would not have paid more. The LOWER bound is
    the ask we actually saw, because a print far below it is a DIFFERENT level
    of the book, not the resting order we lost — most often the mint-matched
    complement leg of a trade deep on the other side.

    Dropping the lower bound does not merely add noise, it systematically
    CONVICTS: every deep print becomes "our level traded", so FASTER_TAKER
    absorbs the REPRICED_ONLY and MAKER_CANCEL buckets. Measured on the 48
    live rejects since the 5m re-enable, the unbounded rule returned 91.7%
    FASTER_TAKER / 8.3% REPRICED / 0 MAKER_CANCEL, "convicting" on prints at
    0.056 and 0.180 against limits of 0.810 and 0.850; the banded rule returns
    the hand-verified 84% / 13% / 2%.

    ``quote_ask`` may be NULL on an old row — fall back to a tick under the
    limit (the limit is the ask plus the FAK pad). ``min(ask, limit)`` keeps the
    band from inverting if a row ever recorded an ask above its own limit. The
    tolerance is :data:`PRICE_TOL`, read here rather than threaded through the
    caller: it is a property of the venue's price strings, not of a call site.
    """
    lim = float(limit_price)
    ask = float(quote_ask) if quote_ask is not None else lim - _ASK_FALLBACK
    return (min(ask, lim) - PRICE_TOL, lim + PRICE_TOL)


def classify_prints(
    prints: Sequence[ChainPrint],
    limit_price: Any,
    quote_ask: Any = None,
) -> tuple[str, Optional[ChainPrint]]:
    """The verdict rule. Returns ``(verdict, earliest_beat_us_print)``.

    A print INSIDE the level band (see :func:`level_band`) means the ask we
    tried to take was genuinely traded — the quote was real and a faster taker
    got it. Prints that exist but all fall OUTSIDE the band mean the level we
    wanted never traded: the book had moved. No print at all in the window
    means the resting order left the book without trading: a maker cancel.
    """
    if not prints:
        return (VERDICT_MAKER_CANCEL, None)
    try:
        lo, hi = level_band(limit_price, quote_ask)
    except (TypeError, ValueError):
        # No limit recorded => we cannot say whether our level traded.
        return (VERDICT_UNRESOLVED, None)
    at_level = [p for p in prints if lo <= p.price <= hi]
    if not at_level:
        return (VERDICT_REPRICED_ONLY, None)
    # "Earliest" = smallest dt: the first moment our level was consumed.
    return (VERDICT_FASTER_TAKER, min(at_level, key=lambda p: (p.dt_s, p.price)))


def calibrate_lag_s(deltas: Iterable[Any]) -> Optional[int]:
    """Median ``tape_ts - our_logged_fill_second`` over OUR OWN fills.

    Returns None when the sample is too small to trust, so the caller keeps
    the established constant instead of chasing noise. Clamped to [0, 6]: a
    negative lag is physically impossible (a block cannot precede the match
    it records) and >6s means the matching is wrong, not the chain slow.
    """
    vals: list[float] = []
    for d in deltas:
        try:
            vals.append(float(d))
        except (TypeError, ValueError):
            continue
    if len(vals) < _MIN_LAG_SAMPLES:
        return None
    med = int(round(statistics.median(vals)))
    return max(_LAG_MIN_S, min(_LAG_MAX_S, med))


def addr_from_topic(topic: Any) -> Optional[str]:
    """Last 20 bytes of a 32-byte indexed topic, as a lowercase 0x address."""
    t = str(topic or "")
    if not t.startswith("0x") or len(t) < 42:
        return None
    return "0x" + t[-40:].lower()


def derive_chain_signature(
    receipts: Iterable[Optional[dict]],
) -> Optional[tuple[str, str]]:
    """Ground-truth (exchange_address, OrderFilled topic0) from OUR receipts.

    Rather than hardcoding the CTF exchange address and event hash — which a
    venue contract migration would silently invalidate, yielding zero matches
    and a table full of "no rival found" — we take the most frequent
    (address, topic0) pair across our own recent fill receipts, restricted to
    logs shaped like ``OrderFilled(bytes32 indexed orderHash, address indexed
    maker, address indexed taker, uint256 makerAssetId, uint256 takerAssetId,
    uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)``:
    4 topics and 5 data words (320 hex chars).
    """
    votes: dict[tuple[str, str], int] = {}
    for rec in receipts:
        for lg in (rec or {}).get("logs") or []:
            if not isinstance(lg, dict):
                continue
            topics = lg.get("topics") or []
            if len(topics) < 4:
                continue
            data = str(lg.get("data") or "")
            if len(data[2:] if data.startswith("0x") else data) < 320:
                continue
            key = (
                str(lg.get("address") or "").lower(),
                str(topics[0] or "").lower(),
            )
            if not key[0] or not key[1]:
                continue
            votes[key] = votes.get(key, 0) + 1
    if not votes:
        return None
    return max(votes.items(), key=lambda kv: kv[1])[0]


def rival_maker_from_logs(
    logs: Iterable[dict],
    *,
    exchange: str,
    topic0: str,
    taker_addr: Optional[str],
) -> Optional[str]:
    """The resting order's owner — the maker whose ask we lost.

    One match emits SEVERAL OrderFilled logs: one per maker order consumed,
    PLUS one for the taker's own order (whose ``maker`` field is the taker
    itself, since from the exchange's view every order has an owner). The
    rival is therefore the first log whose indexed ``maker`` is NOT the
    print's taker wallet. Keying off the taker rather than log ordering keeps
    this correct however many maker orders the fill swept.

    Returns None when the taker is unknown and every candidate is ambiguous,
    or the receipt carries no matching log at all — the caller stores NULL
    rather than guessing.
    """
    ex = str(exchange or "").lower()
    t0 = str(topic0 or "").lower()
    taker = str(taker_addr or "").lower() or None
    if not ex or not t0:
        return None
    for lg in logs or []:
        if not isinstance(lg, dict):
            continue
        if str(lg.get("address") or "").lower() != ex:
            continue
        topics = lg.get("topics") or []
        if len(topics) < 3 or str(topics[0] or "").lower() != t0:
            continue
        maker = addr_from_topic(topics[2])
        if maker is None:
            continue
        if taker is not None and maker == taker:
            continue  # this is the taker's own order leg, not a rival maker
        return maker
    return None


def token_pair_from_market(market: Any) -> Optional[tuple[str, str]]:
    """(up_token, down_token) from a CLOB ``/markets/{condition}`` payload."""
    if not isinstance(market, dict):
        return None
    by_outcome: dict[str, str] = {}
    for tok in market.get("tokens") or []:
        if not isinstance(tok, dict):
            continue
        name = str(tok.get("outcome") or "").strip().lower()
        tid = str(tok.get("token_id") or "").strip()
        if name and tid:
            by_outcome[name] = tid
    up, down = by_outcome.get("up"), by_outcome.get("down")
    if up and down:
        return (up, down)
    return None


def complement_of(token_id: str, pair: tuple[str, str]) -> Optional[str]:
    """The other leg of the pair, or None when the token isn't in it."""
    up, down = pair
    if str(token_id) == str(up):
        return str(down)
    if str(token_id) == str(down):
        return str(up)
    return None


# ---------------------------------------------------------------------------
# Transport (injectable so the whole job is testable without a network)
# ---------------------------------------------------------------------------


def make_fetch_json(client: Optional[Any] = None) -> Callable[..., Any]:
    """The default transport, over a POOLED client when one is supplied.

    Bare ``httpx.get``/``httpx.post`` construct a client per call — a fresh
    TCP+TLS handshake and socket for every row (``execution/notify.py`` ~L47
    documents the same anti-pattern and the fix). main.py hands in the
    process-wide read client so this job rides its keep-alive pool. None (tests,
    or a caller with no client) falls back to module-level httpx calls, which is
    why the fallback exists at all.
    """
    import httpx

    headers = {"User-Agent": _UA}

    def _fetch(url: str, payload: object | None = None) -> Any:
        transport = client if client is not None else httpx
        if payload is None:
            resp = transport.get(url, headers=headers, timeout=_HTTP_TIMEOUT_S)
        else:
            resp = transport.post(
                url, json=payload, headers=headers, timeout=_HTTP_TIMEOUT_S
            )
        resp.raise_for_status()
        return resp.json()

    return _fetch


def _http_status(exc: BaseException) -> Optional[int]:
    """HTTP status carried by an exception, if any (duck-typed for the seam)."""
    code = getattr(getattr(exc, "response", None), "status_code", None)
    try:
        return int(code) if code is not None else None
    except (TypeError, ValueError):
        return None


class _Budget:
    """Hard per-run call caps + a shared rate limiter.

    A venue outage must cost this job a bounded number of calls and then stop,
    leaving the remaining rows PENDING — never a retry storm against the same
    endpoint the trading path depends on. Two counters, two caps: a call that
    would exceed either raises :class:`_BudgetExhausted` and the batch stops.

    Pacing uses the codebase's own :class:`TokenBucket` rather than a retry sleep
    ladder. That is the material difference: the ladder paced RETRIES ONLY (i.e.
    only after something already failed) and did nothing about the steady rate,
    while the bucket bounds the rate itself and honours a server 429 via
    ``notify_server_429`` — the endpoints here are the SAME public data-api/CLOB
    hosts the trading path reads, so an IP-level rate strike is a trading
    outage. A transient failure is simply surfaced: the row is stored PENDING and
    retried on the next fire (300s), which is the documented recovery path.
    """

    def __init__(
        self,
        fetch_json: Callable[..., Any],
        *,
        http_budget: int,
        rpc_budget: int,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._fetch = fetch_json
        self._http_budget = int(http_budget)
        self._rpc_budget = int(rpc_budget)
        self._sleep = sleep
        self._limiter = TokenBucket(_HTTP_RATE_PER_S, _HTTP_BURST)
        self.http_calls = 0
        self.rpc_calls = 0
        self._rpc_id = 0

    def get(self, url: str) -> Any:
        if self.http_calls >= self._http_budget:
            raise _BudgetExhausted("http budget exhausted")
        self.http_calls += 1
        return self._fetch_paced(url, None)

    def rpc(self, rpc_url: str, method: str, params: list) -> Any:
        if self.rpc_calls >= self._rpc_budget:
            raise _BudgetExhausted("rpc budget exhausted")
        self.rpc_calls += 1
        self._rpc_id += 1
        out = self._fetch_paced(
            rpc_url,
            {"jsonrpc": "2.0", "id": self._rpc_id, "method": method, "params": params},
        )
        if isinstance(out, dict):
            if out.get("error"):
                raise RuntimeError(f"{method}: {out['error']}")
            return out.get("result")
        return None

    def _fetch_paced(self, url: str, payload: object | None) -> Any:
        deadline = time.monotonic() + _LIMIT_WAIT_MAX_S
        while not self._limiter.take():
            if time.monotonic() >= deadline:
                # A cooldown this long means the venue is telling us to stop.
                raise _BudgetExhausted("rate limiter cooldown")
            self._sleep(0.05)
        try:
            return self._fetch(url, payload)
        except Exception as exc:  # noqa: BLE001 — surfaced; the row stays PENDING
            if _http_status(exc) == 429:
                self._limiter.notify_server_429()
            raise


class _BudgetExhausted(RuntimeError):
    """Raised when a per-run call cap is hit; remaining rows stay PENDING."""


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _upsert(
    conn: sqlite3.Connection,
    *,
    fill_id: int,
    verdict: str,
    beat: Optional[ChainPrint],
    rival_addr: Optional[str],
    lag_s_used: float,
    detail: dict,
    write_lock: Optional[ContextManager[Any]] = None,
) -> None:
    """Idempotent, never-raising write of one verdict (fill_id is UNIQUE).

    Re-classification overwrites in place so a PENDING row settles without
    accumulating duplicates, and ``checked_at`` always reflects the run that
    produced the stored verdict — which is also what makes the dashboard's
    content probe see a reclassification.

    Swallows its own failures: forensics is observability, and there is nothing
    a caller could usefully do with the exception (the row simply stays
    unclassified and comes back next fire).
    """
    # Never truncate the encoded JSON — a sliced blob is unparseable. Shed the
    # optional print list instead so the column always holds valid JSON.
    blob = json.dumps(detail, separators=(",", ":"), default=str)
    if len(blob) > 4000:
        blob = json.dumps(
            {**{k: v for k, v in detail.items() if k != "prints"},
             "prints_omitted": len(detail.get("prints") or [])},
            separators=(",", ":"), default=str,
        )
    params = (
        int(fill_id), str(verdict),
        (float(beat.dt_s) if beat is not None else None),
        (float(beat.price) if beat is not None else None),
        (float(beat.size) if beat is not None else None),
        rival_addr, float(lag_s_used), blob,
    )
    try:
        with (write_lock if write_lock is not None else contextlib.nullcontext()):
            conn.execute(
                """
                INSERT INTO miss_forensics
                  (fill_id, verdict, beat_dt_s, beat_price, beat_size, rival_addr,
                   lag_s_used, checked_at, detail_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                ON CONFLICT(fill_id) DO UPDATE SET
                    verdict     = excluded.verdict,
                    beat_dt_s   = excluded.beat_dt_s,
                    beat_price  = excluded.beat_price,
                    beat_size   = excluded.beat_size,
                    rival_addr  = excluded.rival_addr,
                    lag_s_used  = excluded.lag_s_used,
                    checked_at  = excluded.checked_at,
                    detail_json = excluded.detail_json
                """,
                params,
            )
            conn.commit()
    except Exception:  # noqa: BLE001 — forensics must never break the process
        logger.warning(
            "miss_forensics upsert failed for fill_id=%s", fill_id, exc_info=True
        )


def select_pending_rejects(
    conn: sqlite3.Connection, *, now_ts: float, max_rows: int = MAX_ROWS_PER_RUN
) -> list[sqlite3.Row]:
    """The population: unclassified `no_liquidity` NON-top-up rejects.

    * joined to ``live_order_attempts`` on ``client_order_id`` — the only
      place the exact token/market we sent is recorded;
    * ``error_kind='no_liquidity'`` — infra/transient rejects carry no price
      information, so there is no book event to explain;
    * NOT a top-up: a rejected extra on a slot that ALREADY filled is not a
      missed trade (it mirrors pipeline's ``is_topup_attempt = slot_filled >
      0``, expressed as "an accepted fill for this slot landed earlier");
    * older than ``MIN_AGE_S`` so the chain has had time to settle, younger
      than ``MAX_AGE_S`` so the scan stays bounded;
    * not already terminally classified (PENDING rows come back).

    Oldest-first so the backfill drains deterministically.
    """
    lo = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now_ts - MAX_AGE_S))
    hi = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now_ts - MIN_AGE_S))
    placeholders = ", ".join("?" for _ in RETRYABLE_VERDICTS)
    try:
        return conn.execute(
            f"""
            SELECT f.id, f.created_at, f.coin, f.sleeve, f.offset_sec,
                   f.window_start_ts, f.side, f.limit_price, f.quote_ask,
                   f.requested_usd,
                   CAST(strftime('%s', f.created_at) AS INTEGER) AS reject_ts,
                   a.poly_market_id, a.poly_token_id
              FROM subminute_fills f
              JOIN live_order_attempts a
                ON a.client_order_id = f.client_order_id
             WHERE f.outcome = 'rejected'
               AND a.error_kind = 'no_liquidity'
               AND a.poly_token_id IS NOT NULL AND a.poly_token_id != ''
               AND a.poly_market_id IS NOT NULL AND a.poly_market_id != ''
               AND f.created_at >= ? AND f.created_at <= ?
               AND NOT EXISTS (
                     SELECT 1 FROM subminute_fills g
                      WHERE g.outcome = 'accepted'
                        AND g.coin = f.coin AND g.sleeve = f.sleeve
                        AND g.window_start_ts = f.window_start_ts
                        AND g.side = f.side
                        AND g.created_at < f.created_at)
               AND NOT EXISTS (
                     SELECT 1 FROM miss_forensics m
                      WHERE m.fill_id = f.id
                        AND m.verdict NOT IN ({placeholders}))
             ORDER BY f.id
             LIMIT ?
            """,
            (lo, hi, *RETRYABLE_VERDICTS, int(max_rows)),
        ).fetchall()
    except sqlite3.OperationalError:
        # Legacy DB that pre-dates one of these tables.
        return []


# ---------------------------------------------------------------------------
# Lag calibration + chain-signature derivation (network-backed, cached)
# ---------------------------------------------------------------------------


def _paged_trades(
    budget: _Budget,
    selector: str,
    *,
    pages: int = 4,
    oldest_needed_ts: Optional[float] = None,
) -> list[dict]:
    """Page a data-api ``/trades`` query, newest-first.

    ``selector`` is the ONLY difference between the two tapes this job reads —
    ``user=<funder>`` for our own prints (lag calibration + chain signature) and
    ``market=<condition>`` for a market's whole tape. Stops on a short page, and
    when ``oldest_needed_ts`` is given also as soon as a page reaches back past
    it (these are 5m/15m markets, so one page is normally the whole life).
    """
    out: list[dict] = []
    for page in range(pages):
        batch = budget.get(
            f"{DATA_API_TRADES}?{selector}&limit=500&offset={page * 500}"
        )
        if not isinstance(batch, list) or not batch:
            break
        rows = [x for x in batch if isinstance(x, dict)]
        out.extend(rows)
        if len(batch) < 500:
            break
        if oldest_needed_ts is not None:
            stamps = [
                x.get("timestamp") for x in rows if x.get("timestamp") is not None
            ]
            if stamps and min(float(s) for s in stamps) < oldest_needed_ts:
                break
    return out


def _refresh_lag(
    conn: sqlite3.Connection, budget: _Budget, funder: str, *, now_ts: float
) -> float:
    """Re-derive the settlement lag from our own fills, at most every 6h."""
    cached = _LAG_STATE.get("value")
    if cached is not None and (now_ts - _LAG_STATE.get("ts", 0.0)) < _LAG_TTL_S:
        return float(cached)
    lag = float(cached) if cached is not None else float(DEFAULT_LAG_S)
    try:
        rows = conn.execute(
            """
            SELECT CAST(strftime('%s', f.created_at) AS INTEGER) AS ts,
                   a.poly_token_id
              FROM subminute_fills f
              JOIN live_order_attempts a
                ON a.client_order_id = f.client_order_id
             WHERE f.outcome = 'accepted' AND f.filled_size IS NOT NULL
               AND a.poly_token_id IS NOT NULL AND a.poly_token_id != ''
               AND f.created_at >= ?
             ORDER BY f.id DESC LIMIT 250
            """,
            (time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now_ts - 3 * 86400)),),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    if not rows:
        _LAG_STATE.update(value=lag, ts=now_ts)
        return lag
    # Bucket the tape by asset ONCE (250 fills x a 2000-print tape is a 500k-pair
    # scan otherwise), keeping tape order inside each bucket so the tie-break
    # below is unchanged.
    by_asset: dict[str, list[tuple[int, int]]] = {}
    for idx, tr in enumerate(_paged_trades(budget, f"user={funder}")):
        try:
            stamp = int(tr["timestamp"])
        except (TypeError, ValueError, KeyError):
            continue
        by_asset.setdefault(str(tr.get("asset") or ""), []).append((stamp, idx))
    # Nearest tape print of the same token at or after our logged second; each
    # print is consumed once so two fills of one token can't share evidence.
    used: set[int] = set()
    deltas: list[int] = []
    for row in rows:
        best: Optional[tuple[int, int]] = None
        for stamp, idx in by_asset.get(str(row["poly_token_id"]), ()):
            if idx in used:
                continue
            d = stamp - int(row["ts"])
            if -5 <= d <= 60 and (best is None or d < best[0]):
                best = (d, idx)
        if best is not None:
            used.add(best[1])
            deltas.append(best[0])
    fresh = calibrate_lag_s(deltas)
    if fresh is not None:
        lag = float(fresh)
        logger.info(
            "miss_forensics settlement lag re-calibrated to %.0fs (n=%d own fills)",
            lag, len(deltas),
        )
    _LAG_STATE.update(value=lag, ts=now_ts)
    return lag


def _chain_signature(
    budget: _Budget, rpc_url: str, funder: str
) -> Optional[tuple[str, str]]:
    """(exchange, OrderFilled topic0), derived once per process from our fills."""
    if _CHAIN_SIG.get("exchange") and _CHAIN_SIG.get("topic0"):
        return (_CHAIN_SIG["exchange"], _CHAIN_SIG["topic0"])
    receipts: list[Optional[dict]] = []
    seen: set[str] = set()
    for tr in _paged_trades(budget, f"user={funder}", pages=1):
        h = tr.get("transactionHash")
        if not h or h in seen:
            continue
        seen.add(str(h))
        receipts.append(budget.rpc(rpc_url, "eth_getTransactionReceipt", [str(h)]))
        if len(seen) >= 3:
            break
    sig = derive_chain_signature(receipts)
    if sig is None:
        return None
    _CHAIN_SIG.update(exchange=sig[0], topic0=sig[1])
    logger.info("miss_forensics chain signature: exchange=%s topic0=%s", *sig)
    return sig


def _market_pair(budget: _Budget, condition_id: str) -> Optional[tuple[str, str]]:
    """Token pair for a condition, cached (conditions are immutable)."""
    hit = _PAIR_CACHE.get(condition_id)
    if hit is not None:
        return hit
    pair = token_pair_from_market(budget.get(f"{CLOB_MARKETS}/{condition_id}"))
    if pair is not None:
        if len(_PAIR_CACHE) >= _PAIR_CACHE_MAX:
            _PAIR_CACHE.clear()
        _PAIR_CACHE[condition_id] = pair
    return pair


# ---------------------------------------------------------------------------
# The job
# ---------------------------------------------------------------------------


def run_miss_forensics(
    conn: sqlite3.Connection,
    *,
    funder: str,
    rpc_url: str = "",
    now_ts: Optional[float] = None,
    client: Optional[Any] = None,
    fetch_json: Optional[Callable[..., Any]] = None,
    sleep: Callable[[float], None] = time.sleep,
    max_rows: int = MAX_ROWS_PER_RUN,
    http_budget: int = HTTP_BUDGET,
    rpc_budget: int = RPC_BUDGET,
    write_lock: Optional[ContextManager[Any]] = None,
) -> dict:
    """Classify one batch of unexplained rejects. Returns counters.

    Never raises for a data/venue problem: a row whose evidence could not be
    fetched is stored PENDING and retried next fire. ``rpc_url`` is optional —
    without it verdicts are still produced from the tape, only the rival maker
    address is left NULL.

    ``client`` is a pooled HTTP client (main.py passes the process-wide read
    client) so the pass does not open a fresh TLS connection per call;
    ``fetch_json`` overrides the transport wholesale and is the test seam.
    """
    if now_ts is None:
        now_ts = time.time()
    counters: dict[str, Any] = {
        "scanned": 0, "classified": 0, "pending": 0, "unresolved": 0,
        "http_calls": 0, "rpc_calls": 0,
    }
    if not funder:
        logger.warning("miss_forensics: POLY_FUNDER unset — cannot exclude our own "
                       "prints, refusing to classify")
        return counters

    rows = select_pending_rejects(conn, now_ts=now_ts, max_rows=max_rows)
    counters["scanned"] = len(rows)
    if not rows:
        return counters

    budget = _Budget(
        fetch_json or make_fetch_json(client),
        http_budget=http_budget, rpc_budget=rpc_budget, sleep=sleep,
    )
    try:
        lag_s = _refresh_lag(conn, budget, funder, now_ts=now_ts)
    except Exception:  # noqa: BLE001 — calibration is best-effort
        logger.warning("miss_forensics lag calibration failed", exc_info=True)
        lag_s = float(_LAG_STATE.get("value") or DEFAULT_LAG_S)

    tape_cache: dict[str, list[dict]] = {}
    for row in rows:
        fill_id = int(row["id"])
        detail: dict[str, Any] = {
            "coin": row["coin"], "sleeve": row["sleeve"],
            "offset_sec": row["offset_sec"], "side": row["side"],
            "window_start_ts": row["window_start_ts"],
            "limit_price": row["limit_price"], "quote_ask": row["quote_ask"],
            "condition_id": row["poly_market_id"], "token_id": row["poly_token_id"],
        }
        verdict, beat, rival = VERDICT_PENDING, None, None
        try:
            cond = str(row["poly_market_id"])
            token = str(row["poly_token_id"])
            reject_ts = float(row["reject_ts"])
            pair = _market_pair(budget, cond)
            complement = complement_of(token, pair) if pair else None
            if pair is None:
                # The CLOB no longer exposes both legs — the mint-match rule
                # cannot be applied, and no retry will change that.
                verdict = VERDICT_UNRESOLVED
                detail["reason"] = "clob market missing both tokens"
            else:
                detail["complement_id"] = complement
                if cond not in tape_cache:
                    # Oldest TAPE stamp the window can still need: dt >=
                    # WINDOW_LO_S means tape_ts >= reject_ts + lag + WINDOW_LO_S
                    # (dt = tape_ts - reject_ts - lag). 5s of slack so a
                    # boundary print is never paged past.
                    tape_cache[cond] = _paged_trades(
                        budget, f"market={cond}",
                        oldest_needed_ts=reject_ts + lag_s + WINDOW_LO_S - 5,
                    )
                prints = collect_prints(
                    tape_cache[cond], token_id=token, complement_id=complement,
                    reject_ts=reject_ts, lag_s=lag_s, funder=funder,
                )
                verdict, beat = classify_prints(
                    prints, row["limit_price"], row["quote_ask"]
                )
                # The band is persisted so a stored verdict can be re-derived
                # from the print list even after the tolerance moves.
                with contextlib.suppress(TypeError, ValueError):
                    lo_p, hi_p = level_band(row["limit_price"], row["quote_ask"])
                    detail["level_band"] = [round(lo_p, 4), round(hi_p, 4)]
                detail["n_prints_in_window"] = len(prints)
                detail["prints"] = [
                    {"dt": p.dt_s, "px": p.price, "sz": p.size, "kind": p.kind}
                    for p in prints[:12]
                ]
                if verdict == VERDICT_FASTER_TAKER and beat is not None:
                    detail["beat_kind"] = beat.kind
                    detail["beat_tx"] = beat.tx_hash
                    if rpc_url and beat.tx_hash:
                        rival = _rival_for(budget, rpc_url, funder, beat)
        except _BudgetExhausted as exc:
            logger.warning(
                "miss_forensics: %s after %d http / %d rpc calls — %d row(s) "
                "left PENDING for the next run",
                exc, budget.http_calls, budget.rpc_calls,
                len(rows) - counters["classified"] - counters["unresolved"],
            )
            detail["reason"] = str(exc)
            _upsert(
                conn, fill_id=fill_id, verdict=VERDICT_PENDING, beat=None,
                rival_addr=None, lag_s_used=lag_s, detail=detail,
                write_lock=write_lock,
            )
            counters["pending"] += 1
            break
        except Exception:  # noqa: BLE001 — one bad row must not stop the batch
            logger.warning(
                "miss_forensics: classification failed for fill_id=%s", fill_id,
                exc_info=True,
            )
            detail["reason"] = "classification error"
            verdict, beat, rival = VERDICT_PENDING, None, None

        _upsert(
            conn, fill_id=fill_id, verdict=verdict, beat=beat, rival_addr=rival,
            lag_s_used=lag_s, detail=detail, write_lock=write_lock,
        )
        if verdict == VERDICT_PENDING:
            counters["pending"] += 1
        elif verdict == VERDICT_UNRESOLVED:
            counters["unresolved"] += 1
        else:
            counters["classified"] += 1
            counters[verdict.lower()] = counters.get(verdict.lower(), 0) + 1

    counters["http_calls"] = budget.http_calls
    counters["rpc_calls"] = budget.rpc_calls
    counters["lag_s"] = lag_s
    return counters


def _rival_for(
    budget: _Budget, rpc_url: str, funder: str, beat: ChainPrint
) -> Optional[str]:
    """Resolve the rival maker for a beat-us print; None on any failure.

    Deliberately non-fatal: the VERDICT is the operator-facing fact, and a
    missing counterparty address must never downgrade it to PENDING.
    """
    try:
        sig = _chain_signature(budget, rpc_url, funder)
        if sig is None or not beat.tx_hash:
            return None
        receipt = budget.rpc(rpc_url, "eth_getTransactionReceipt", [beat.tx_hash])
        return rival_maker_from_logs(
            (receipt or {}).get("logs") or [],
            exchange=sig[0], topic0=sig[1], taker_addr=beat.taker,
        )
    except _BudgetExhausted:
        raise
    except Exception:  # noqa: BLE001 — counterparty lookup is a bonus field
        logger.warning("miss_forensics rival lookup failed", exc_info=True)
        return None
