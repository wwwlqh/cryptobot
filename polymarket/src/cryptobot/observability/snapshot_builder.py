"""Build the dashboard's SSE payload from the ledger and portfolio.

Top-level keys (stable; new fields may be added):

  tick_ts, venue, ledger_limit, kpis, portfolio, markets, ledger, ledger_rollup,
  ledger_rollup_by_venue, ledger_equity, cash_equity, ledger_daily,
  ledger_daily_by_venue, ledger_analytics, ledger_analytics_by_venue, contracts,
  strategy_flags, live_wallet, capital_flows, operator, latency, v2

``build_payload(conn, portfolio, last_seen_ledger_id=...)`` returns
``(payload_dict, max_ledger_id_in_payload)``. Callers keep ``max_ledger_id`` so
later SSE events only send new ledger rows.

Counting: the ledger stores one row per fill, but the venue holds all fills of
one market position as a single CLOB position. Every count in ``ledger_rollup``
/ ``ledger_daily`` / ``ledger_analytics`` therefore counts trade groups (see
``_TRADE_GROUP_KEY_SQL``); dollar sums are the same either way. The ``ledger``
array still streams every fill, and the dashboard folds them for display.
"""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
import statistics
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from cryptobot import config
from cryptobot.execution.capital import (
    WalletSnapshot,
    _equity_change_order_sql,
    _wallet_start_sql,
    extractable_pnl_by_wallet,
    get_group_state,
    get_wallet_snapshots,
    ledger_version,
    list_capital_flows,
    load_capital_flows,
    wallet_id_for_sleeve,
)
from cryptobot.execution.sizing_v2 import (
    DEFAULT_BASE_FRAC_15M,
    DEFAULT_BASE_FRAC_5M,
    DEFAULT_BRAKE_FLOOR,
    DEFAULT_BRAKE_SCALE,
    DEFAULT_CONC_DOWNWEIGHT,
    DEFAULT_MIN_BET_USD,
)
from cryptobot.execution.strategy_constants import (
    LIVE_BALANCE_TOLERANCE_USD,
    WALLET_SNAPSHOT_FRESHNESS_TTL_S,
)
from cryptobot.scheduler.book_ws_watchdog import (
    watchdog_counters as _watchdog_counters,
)
from cryptobot.scheduler.miss_forensics import (
    RETRYABLE_VERDICTS,
    VERDICT_FASTER_TAKER,
    VERDICT_MAKER_CANCEL,
    VERDICT_PENDING,
    VERDICT_REPRICED_ONLY,
    VERDICT_UNRESOLVED,
)
from cryptobot.venues.fees import fee_per_share
from cryptobot.strategy.portfolio_loader import V6Portfolio
from cryptobot.strategy.spec_v2 import V2_SPEC

logger = logging.getLogger(__name__)

# Stamped once at import (= process boot). Carried in every SSE payload as
# `server_boot_ts` so the client can detect a deploy/restart and reload
# itself — see the note at the payload build site.
_SERVER_BOOT_TS = int(time.time() * 1000)

# On-chain verdict read cache (see _chain_forensics_cached). Strong connection
# identity ref compared with `is`, plus a content probe — no TTL: the probe IS
# the invalidation.
_CHAIN_FORENSICS_CACHE: dict[str, Any] = {"conn": None, "key": None, "value": None}

_HISTORY_CACHE_TTL_S = 5.0
_HISTORY_CACHE: dict[str, Any] = {
    "key": None,
    "expires_at": 0.0,
    "value": None,
}


def _round_money(x: float | None) -> Optional[float]:
    if x is None:
        return None
    value = float(x)
    if not math.isfinite(value):
        return None
    return round(value, 2)


def _round_prob(x: float | None) -> Optional[float]:
    if x is None:
        return None
    value = float(x)
    if not math.isfinite(value):
        return None
    return round(value, 4)


def _round_size(x: float | None) -> Optional[float]:
    if x is None:
        return None
    value = float(x)
    if not math.isfinite(value):
        return None
    return round(value, 6)


def _direction(side: str | None) -> str | None:
    if side == "YES":
        return "UP"
    if side == "NO":
        return "DOWN"
    return side


def _row_venue(r: Any) -> str:
    """Venue tag for a ledger/journal/analytics row (deliverable 5).

    Reads the row's ``venue`` column when the SELECT carried it; ABSENT-IN-DB
    (a legacy row that predates the column, or a SELECT that did not fetch it)
    resolves to ``'polymarket'`` — the documented backward-compatible default.
    """
    try:
        v = r["venue"]
    except (IndexError, KeyError, TypeError):
        return "polymarket"
    return str(v) if v else "polymarket"


def _json_safe(value: Any) -> Any:
    """Return a JSON-strict payload: no NaN/Infinity for browser JSON.parse."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _markets_block() -> list[dict]:
    """The live scan universe, one entry per coin. The dashboard's coin filter
    reads ``markets[].coin``.
    """
    return [{"coin": coin} for coin in config.COINS]


# ---------------------------------------------------------------------------
# v2 telemetry — live sub-minute decision spec + equity-fraction sizing.
# The dashboard's Sub-minute page reads these so the deployed strategy renders
# without a hardcoded UI copy of the spec. The live strategy runs one ask band
# over each configured per-second checkpoint window across the active sleeves;
# bands and gates are read from the portfolio, first-fill-wins per
# resolution. Sizing constants come from the W5 env knobs (config.V2_*) with the
# registered spec defaults so the numbers are correct even before the pins land.
# ---------------------------------------------------------------------------
def _fee_spec_text() -> str:
    """One-line taker-fee description for the Strategy tab."""
    theta = float(getattr(config, "POLY_FEE_THETA", 0.07))
    return f"PARABOLA · POLY_FEE_theta {theta:g} · theta x ask x (1 - ask)"


def _fee_calibration() -> Optional[float]:
    """Grade-time multiplier on the booked taker fee (see execution.grade_fees)."""
    return float(getattr(config, "POLY_FEE_CALIBRATION", 0.864))


def _v2_sizing_constants() -> dict[str, Any]:
    return {
        # Defaults come from sizing_v2's DEFAULT_* constants so this panel
        # always matches the sizing code.
        "base_frac_15m": float(getattr(config, "V2_BASE_FRAC_15M", DEFAULT_BASE_FRAC_15M)),
        "base_frac_5m": float(getattr(config, "V2_BASE_FRAC_5M", DEFAULT_BASE_FRAC_5M)),
        "brake_scale": float(getattr(config, "V2_BRAKE_SCALE", DEFAULT_BRAKE_SCALE)),
        "brake_floor": float(getattr(config, "V2_BRAKE_FLOOR", DEFAULT_BRAKE_FLOOR)),
        "conc_downweight": bool(int(getattr(config, "V2_CONC_DOWNWEIGHT", DEFAULT_CONC_DOWNWEIGHT))),
        "min_bet_usd": float(getattr(config, "MIN_BET_USD", DEFAULT_MIN_BET_USD)),
        # NB: the research 0.25 "dd_rail" is deliberately NOT surfaced — it is a
        # research budget, not a live control, and confused the panel next to the
        # brake floor / hard halt. The live throttle is fully described
        # by brake_scale/brake_floor (stake throttle) + hard_halt_frac (halt).
        "poly_fee_theta": float(getattr(config, "POLY_FEE_THETA", 0.07)),
        # VENUE-AWARE taker-fee contract — what the client actually renders.
        "fee_spec": _fee_spec_text(),
        # Grade-time calibration on the BOOKED fee (decision math keeps raw
        # theta) — the honest answer to "why theta 0.07?". None off-Polymarket.
        "fee_calibration": _fee_calibration(),
        # --- Execution / capacity contract (DISPLAY ONLY) ---------------------
        # Surfaced on the Config "Subminute spec" reference tab so the deployed
        # strategy contract renders from live config, not hardcoded UI copy. No
        # gate / sizing / execution primitive reads these back — they are echoed
        # straight from config for the operator's reference document.
        "fak_pad": float(getattr(config, "V2_FAK_PAD", 0.0)),
    }


def _top_bid(book: Any) -> Optional[float]:
    """Best bid on a book snapshot, or None. Bids are sorted ascending."""
    bids = getattr(book, "bids", None) if book is not None else None
    if not bids:
        return None
    try:
        price = float(bids[-1]["price"])
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0 else None


def _strategy_flags_block() -> dict[str, Any]:
    """Execution knobs of the current deploy, shown on the Config tab."""
    return {
        "fak_pad": float(getattr(config, "V2_FAK_PAD", 0.0)),
        "min_bet_usd": float(getattr(config, "MIN_BET_USD", DEFAULT_MIN_BET_USD)),
    }


def _v2_spec(portfolio: V6Portfolio) -> dict[str, Any]:
    """The live sub-minute decision spec shown on the dashboard.

    Everything is derived from the loaded sleeves (a sleeve is sub-minute when
    it has a non-empty checkpoint_offset_set), so the page always matches the
    deployed config. Bands are a list, one entry per distinct ask band.
    """
    def _iv_minutes(interval: Optional[str]) -> int:
        try:
            return int(str(interval).rstrip("m"))
        except (TypeError, ValueError):
            return 1 << 30  # unknown intervals sort last

    # A v2 sleeve is one carrying a per-second checkpoint window.
    v2_sleeves = [
        s for s in portfolio.sleeves
        if tuple(getattr(s, "checkpoint_offset_set", ()) or ())
    ]

    # Read each per-second span from the checkpoint set rather than hardcoding
    # either the historical 30-second or current 60-second production grid.
    offset_windows: dict[str, str] = {}
    window_widths: set[int] = set()
    for s in sorted(v2_sleeves, key=lambda s: _iv_minutes(s.interval)):
        offs = tuple(s.checkpoint_offset_set)
        if offs == tuple(range(offs[0], offs[-1] + 1)):
            window_widths.add(len(offs))
        offset_windows.setdefault(s.interval, f"{offs[0]}–{offs[-1]}s")

    cadence = "configured per-second checkpoint windows"
    if len(window_widths) == 1:
        width = next(iter(window_widths))
        cadence = f"last {width}s · per-second (0–{width - 1}s)"

    # Group sleeves by ask band, ascending by ask_lo.
    by_band: dict[tuple[float, float], list] = {}
    for s in v2_sleeves:
        band = tuple(s.ask_bucket_ranges[0]) if s.ask_bucket_ranges else (0.0, 0.0)
        by_band.setdefault(band, []).append(s)

    bands: list[dict[str, Any]] = []
    for (ask_lo, ask_hi), group in sorted(by_band.items(), key=lambda kv: kv[0][0]):
        group = sorted(group, key=lambda s: (_iv_minutes(s.interval), s.label))
        bands.append({
            "key": f"{ask_lo:g}-{ask_hi:g}",
            "label": f"{ask_lo:g}–{ask_hi:g}",
            "sleeves": [s.label for s in group],
            "ask_lo": ask_lo,
            "ask_hi": ask_hi,
            # pred_edge gate on model_p − ask − fee.
            "gate": group[0].threshold,
        })

    # Shared facts: derive from a representative sleeve; fall back to the frozen
    # spec_v2 defaults so the header never renders blank on an empty portfolio.
    rep = v2_sleeves[0] if v2_sleeves else None
    intervals = sorted(
        {s.interval for s in v2_sleeves}, key=_iv_minutes
    ) or list(V2_SPEC["intervals"])
    feature_groups = getattr(rep, "feature_groups", None) or "price,identity,time,momentum"
    signal_source = getattr(rep, "signal_source", None) or "hgb"
    return {
        "intervals": intervals,
        "coins": list(config.COINS),
        "cadence": cadence,
        "offset_windows": offset_windows,
        "poly_fee_theta": float(getattr(config, "POLY_FEE_THETA", 0.07)),
        # VENUE-AWARE taker-fee contract — what the client actually renders.
        "fee_spec": _fee_spec_text(),
        # Grade-time calibration on the BOOKED fee (decision math keeps raw
        # theta) — the honest answer to "why theta 0.07?". None off-Polymarket.
        "fee_calibration": _fee_calibration(),
        "one_per_market": True,
        # band -> reachable -> dedup(first entry_ts) -> gate, then first-fill-
        # wins across both bands (one bet per resolution).
        "selection": "first-fill-wins per resolution · dedup-before-gate",
        # The model's 11 input features, shared by every sleeve.
        "features": f"{feature_groups} (11-set)",
        "signal_source": signal_source,
        "bands": bands,
    }


def _portfolio_block(portfolio: V6Portfolio) -> dict:
    sleeves = []
    for s in portfolio.sleeves:
        offsets = tuple(getattr(s, "checkpoint_offset_set", ()) or ())
        sleeves.append({
            "label": s.label,
            "kind": "v6",
            "alloc": s.alloc,
            "interval": s.interval,
            "entry_minutes": list(s.entry_minute_set),
            "capital_frac": s.capital_frac,
            "exposure_cap_frac": portfolio.exposure_cap_fraction_for(s.label),
            "backtest_max_drawdown_frac": portfolio.backtest_max_drawdown_frac_for(
                s.label
            ),
            "signal_source": s.signal_source,
            # HGB pred_edge gate. Per-sleeve value; the live MID sleeves
            # (step1last) gate 0.03.
            "threshold": s.threshold,
            "min_lut_n": s.min_lut_n,
            "max_bet_usd": s.max_bet_usd,
            "max_bet_usd_by_coin": dict(s.max_bet_usd_by_coin),
            "target_unit": "bet",
            # B1/B2 sub-minute checkpoint sleeve marker + compact summary.
            "subminute": bool(offsets),
            "checkpoint_offsets_n": len(offsets),
            "checkpoint_span": (
                f"+{offsets[0]}s…+{offsets[-1]}s" if offsets else None
            ),
        })
    return {
        "label": portfolio.run_id or "isolated_wallets",
        "bet_size_multiplier": config.BET_SIZE_MULTIPLIER,
        "shared_exposure_cap_frac": portfolio.shared_exposure_cap_fraction,
        "wallet_exposure_cap_fracs": dict(portfolio.exposure_cap_fraction_by_wallet),
        "backtest_max_drawdown_fracs": dict(
            portfolio.backtest_max_drawdown_frac_by_wallet
        ),
        "max_drawdown_halt_frac": portfolio.max_drawdown_halt_frac,
        "label_source": portfolio.label_source,
        "max_walk_price": config.MAX_WALK_PRICE,
        "sleeves": sleeves,
    }


def _wallet_payload(
    component,
    snap: WalletSnapshot,
    portfolio: V6Portfolio,
    *,
    mode_version: Optional[int] = None,
    extractable_pnl_usd: Optional[float] = None,
) -> dict:
    cap_frac = portfolio.exposure_cap_fraction_for(snap.wallet_id)
    cap_usd = snap.realized_capital * cap_frac
    backtest_dd = portfolio.backtest_max_drawdown_frac_for(snap.wallet_id)
    trading_halted = (
        portfolio.max_drawdown_halt_frac > 0
        and snap.drawdown_pct_peak_frac >= portfolio.max_drawdown_halt_frac
    )
    target = component.target_bet_size_for(snap.realized_capital)
    exposure_ratio = snap.pending_exposure / cap_usd if cap_usd > 0 else 0.0
    return {
        "id": snap.wallet_id,
        "label": snap.label,
        "kind": snap.kind,
        "ledger_sleeves": list(snap.ledger_sleeves),
        "initial_bankroll": _round_money(snap.initial_bankroll),
        # Display value for each strategy tile. Sizing uses
        # ``realized_capital``; nothing but the dashboard reads this.
        "bankroll_display_usd": _round_money(snap.initial_bankroll),
        "realized_capital": _round_money(snap.realized_capital),
        "peak_capital": _round_money(snap.peak_realized_capital),
        "pnl_total": _round_money(snap.pnl_total),
        "pnl_24h": _round_money(snap.pnl_24h),
        # 24h win rate: stop-loss exits count as losses (folded into
        # losses_24h). These are per-fill counts from the wallet snapshot
        # (capital.py), not the trade-group counts of the ledger rollups.
        "win_rate_24h": round(snap.win_rate_24h, 4) if snap.win_rate_24h is not None else None,
        "wins_24h": snap.wins_24h,
        "losses_24h": snap.settled_24h - snap.wins_24h + snap.stops_24h,
        "n_bets_24h": snap.settled_24h,
        "n_topups_24h": snap.n_topups_24h,
        "n_pending": snap.n_pending,
        "n_settled": snap.n_settled,
        "drawdown_pct": round(snap.drawdown_ratio * 100.0, 2),
        "drawdown_pct_peak": round(snap.drawdown_pct_peak_frac * 100.0, 2),
        "backtest_max_drawdown_pct": (
            round(backtest_dd * 100.0, 2) if backtest_dd is not None else None
        ),
        "max_drawdown_halt_pct": round(portfolio.max_drawdown_halt_frac * 100.0, 2),
        "trading_halted": trading_halted,
        "halt_reason": "dd_halt" if trading_halted else None,
        "exposure_used": _round_money(snap.pending_exposure),
        "exposure_cap": _round_money(cap_usd),
        "exposure_cap_frac": round(cap_frac, 6),
        "exposure_ratio": round(exposure_ratio, 4),
        "target_bet_size": _round_money(target),
        "target_unit": "bet",
        # Live/dry mode of the wallet.
        "mode": snap.mode,
        "mode_version": int(mode_version) if mode_version is not None else None,
        "wallet_group": snap.wallet_group,
        "extractable_pnl_usd": _round_money(extractable_pnl_usd),
    }


def _load_wallet_mode_versions(
    conn: sqlite3.Connection,
) -> tuple[dict[str, str], dict[str, int]]:
    """Return `(wallet_id → mode, wallet_id → version)` for the LIVE
    deploy surface. Missing wallets default to DRY / version=0."""
    try:
        rows = conn.execute(
            "SELECT wallet_id, mode, COALESCE(version, 0) AS version "
            "FROM wallet_mode_state"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}, {}
    mode_state: dict[str, str] = {}
    versions: dict[str, int] = {}
    for r in rows:
        mode_state[r["wallet_id"]] = r["mode"]
        versions[r["wallet_id"]] = int(r["version"] or 0)
    return mode_state, versions


def _where_plus(where: str, condition: str) -> str:
    return f"{where} AND {condition}" if where else f" WHERE {condition}"


def _latest_wallet_reconciliation_row(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    try:
        return conn.execute(
            "SELECT sampled_at, source_status, clob_balance_usd, "
            "       chain_balance_usd, open_orders_count, open_positions_count "
            "FROM wallet_reconciliation_runs ORDER BY id DESC LIMIT 1",
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _wallet_balance_row(
    conn: sqlite3.Connection, *, before_or_at: Optional[str] = None
) -> Optional[sqlite3.Row]:
    predicate = "clob_balance_usd IS NOT NULL"
    params: tuple[Any, ...] = ()
    order = "sampled_at DESC, id DESC"
    if before_or_at is not None:
        predicate += " AND sampled_at <= ?"
        params = (before_or_at,)
    try:
        return conn.execute(
            "SELECT sampled_at, source_status, clob_balance_usd, "
            "       open_orders_count, open_positions_count "
            f"FROM wallet_reconciliation_runs WHERE {predicate} "
            f"ORDER BY {order} LIMIT 1",
            params,
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _recent_clean_wallet_balance_row(
    conn: sqlite3.Connection, *, before_or_at: Optional[str] = None
) -> Optional[sqlite3.Row]:
    predicate = (
        "clob_balance_usd IS NOT NULL "
        "AND source_status = 'ok' "
        "AND COALESCE(open_orders_count, 0) = 0 "
        "AND COALESCE(open_positions_count, 0) = 0"
    )
    params: tuple[Any, ...] = ()
    if before_or_at is not None:
        predicate += " AND sampled_at <= ?"
        params = (before_or_at,)
    try:
        return conn.execute(
            "SELECT sampled_at, source_status, clob_balance_usd, "
            "       chain_balance_usd, open_orders_count, open_positions_count "
            f"FROM wallet_reconciliation_runs WHERE {predicate} "
            "ORDER BY sampled_at DESC, id DESC LIMIT 1",
            params,
        ).fetchone()
    except sqlite3.OperationalError:
        return None


# How old the last good CLOB balance sample may be and still be shown as
# current capital (with its as-of time). Samples normally arrive within
# WALLET_SNAPSHOT_FRESHNESS_TTL_S (900s); 4x leaves room for a missed sample
# before the Overview falls back to the paper equity.
_ACCOUNT_CAPITAL_STALE_CAP_S: int = 4 * WALLET_SNAPSHOT_FRESHNESS_TTL_S


def _recent_ok_wallet_balance_row(
    conn: sqlite3.Connection,
) -> Optional[sqlite3.Row]:
    """Most recent 'ok' reconciliation row with a CLOB balance, REGARDLESS of
    open orders/positions. An 'ok' sample taken mid-position is still the true
    CLOB cash balance at that instant — good enough for the DISPLAY capital.
    The strict clean-flat requirement (``_recent_clean_wallet_balance_row``)
    stays where it matters: establishing a comparable reconciliation basis."""
    try:
        return conn.execute(
            "SELECT sampled_at, source_status, clob_balance_usd, "
            "       open_orders_count, open_positions_count "
            "FROM wallet_reconciliation_runs "
            "WHERE clob_balance_usd IS NOT NULL AND source_status = 'ok' "
            "ORDER BY sampled_at DESC, id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _sqlite_utc_ts(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _sample_age_seconds(newer: Any, older: Any) -> Optional[float]:
    newer_dt = _sqlite_utc_ts(newer)
    older_dt = _sqlite_utc_ts(older)
    if newer_dt is None or older_dt is None:
        return None
    return (newer_dt - older_dt).total_seconds()


def _float_or_none(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _live_account_reconciliation(
    conn: sqlite3.Connection, portfolio: V6Portfolio
) -> dict[str, Any]:
    """Cash-basis account view for the operator dashboard.

    This reconciles current pUSD/CLOB balance to the wallet's cash basis. It is
    separate from strategy bankroll accounting, which remains the source for
    live sizing, drawdown gates, and backtest parity.
    """
    latest = _latest_wallet_reconciliation_row(conn)
    account_capital = None
    account_capital_ts = None
    account_capital_source = None
    account_capital_confirmed = False
    if latest is not None:
        account_capital = _float_or_none(latest["clob_balance_usd"])
        if account_capital is not None:
            account_capital_ts = latest["sampled_at"]
            account_capital_source = "clob_balance"
            account_capital_confirmed = latest["source_status"] == "ok"
        elif latest["source_status"] == "partial":
            recent = _recent_clean_wallet_balance_row(
                conn, before_or_at=latest["sampled_at"]
            )
            if recent is not None:
                recent_balance = _float_or_none(recent["clob_balance_usd"])
                latest_chain = _float_or_none(latest["chain_balance_usd"])
                age = _sample_age_seconds(latest["sampled_at"], recent["sampled_at"])
                if (
                    recent_balance is not None
                    and latest_chain is not None
                    and age is not None
                    and 0 <= age <= WALLET_SNAPSHOT_FRESHNESS_TTL_S
                    and abs(latest_chain - recent_balance)
                    <= LIVE_BALANCE_TOLERANCE_USD
                ):
                    account_capital = recent_balance
                    account_capital_ts = recent["sampled_at"]
                    account_capital_source = "recent_clean_clob_balance"
                    account_capital_confirmed = True

    if account_capital is None:
        # The latest reconciliation row is unusable (null CLOB balance or bad
        # status): fall back to the most recent 'ok' sample within the
        # staleness cap, even one taken with positions open (it is still the
        # true cash balance at that moment). confirmed stays False, so
        # ``comparable`` keeps its meaning; the UI shows the sample's as-of
        # time instead.
        stale = _recent_ok_wallet_balance_row(conn)
        if stale is not None:
            stale_balance = _float_or_none(stale["clob_balance_usd"])
            stale_dt = _sqlite_utc_ts(stale["sampled_at"])
            age_s = (
                (datetime.now(timezone.utc) - stale_dt).total_seconds()
                if stale_dt is not None
                else None
            )
            if (
                stale_balance is not None
                and age_s is not None
                # small negative slack for clock skew between writer and reader
                and -60 <= age_s <= _ACCOUNT_CAPITAL_STALE_CAP_S
            ):
                account_capital = stale_balance
                account_capital_ts = stale["sampled_at"]
                account_capital_source = "stale_clob_balance"
                account_capital_confirmed = False

    first_live_ts: Optional[str] = None
    live_pnl: Optional[float] = None
    pending_live = 0
    try:
        where, params = _ledger_where(portfolio)
        live_where = _where_plus(where, "mode = 'LIVE'")
        row = conn.execute(
            "SELECT MIN(bet_ts) AS first_live_ts, "
            "       COALESCE(SUM(CASE WHEN outcome != 'PENDING' "
            "                         AND pnl_net IS NOT NULL "
            "                    THEN pnl_net ELSE 0 END), 0.0) AS pnl, "
            "       COALESCE(SUM(CASE WHEN outcome = 'PENDING' "
            "                         THEN 1 ELSE 0 END), 0) AS pending_live "
            f"FROM ledger{live_where}",
            params,
        ).fetchone()
        if row is not None:
            first_live_ts = row["first_live_ts"]
            live_pnl = float(row["pnl"] or 0.0)
            pending_live = int(row["pending_live"] or 0)
    except sqlite3.OperationalError:
        live_pnl = None

    basis = None
    basis_ts = None
    basis_source = None
    clean_basis = False
    if account_capital is not None:
        basis_row = None
        if first_live_ts:
            # The basis anchor must be a clean sample: source_status 'ok' and a
            # flat book (no open orders or positions). A sample taken with
            # positions open understates cash by the cost of the shares held,
            # which would overstate account_pnl_total by that amount. Only a
            # clean sample sets clean_basis (and so ``comparable``). Without
            # one, the newest earlier balance is still used for continuity but
            # flagged unclean, so strict consumers ignore it.
            basis_row = _recent_clean_wallet_balance_row(
                conn, before_or_at=first_live_ts
            )
            if basis_row is not None:
                basis_source = "clob_before_first_live_bet"
                clean_basis = True
            else:
                basis_row = _wallet_balance_row(conn, before_or_at=first_live_ts)
                if basis_row is not None:
                    basis_source = "unclean_clob_before_first_live_bet"
                    clean_basis = False
        elif latest is not None:
            basis_row = latest
            basis_source = "latest_clob_no_live_bets"
            # No live bets yet: the latest sample is the basis, but it is only
            # a CLEAN basis when it is itself ok + flat-book (same C4 rule).
            clean_basis = (
                latest["source_status"] == "ok"
                and int(latest["open_orders_count"] or 0) == 0
                and int(latest["open_positions_count"] or 0) == 0
            )

        if basis_row is not None and basis_row["clob_balance_usd"] is not None:
            basis = float(basis_row["clob_balance_usd"])
            basis_ts = basis_row["sampled_at"]
        elif live_pnl is not None:
            basis = account_capital - live_pnl
            basis_source = "clob_minus_live_pnl"

    account_pnl = (
        account_capital - basis
        if account_capital is not None and basis is not None
        else None
    )
    ledger_delta = (
        account_pnl - live_pnl
        if account_pnl is not None and live_pnl is not None
        else None
    )
    comparable = bool(
        latest is not None
        and account_capital_confirmed
        and int(latest["open_orders_count"] or 0) == 0
        and int(latest["open_positions_count"] or 0) == 0
        and pending_live == 0
        and account_capital is not None
        and clean_basis
    )
    return {
        "capital": account_capital,
        "capital_ts": account_capital_ts,
        "initial": basis,
        "pnl": account_pnl,
        "strategy_pnl": live_pnl,
        "pnl_ledger_delta": ledger_delta,
        "basis_ts": basis_ts,
        "basis_source": basis_source,
        "capital_source": account_capital_source,
        "comparable": comparable,
    }


# Per-tick live cash equity uses the SAME balance live sizing reads, not the
# ~10-min reconciliation sample. Mirrors BalanceCache.hard_staleness_s (300s):
# a readiness-report mirror older than this is not fresh enough to headline.
_LIVE_BALANCE_HARD_STALENESS_S: float = 300.0


def _live_account_capital(
    conn: sqlite3.Connection,
    account: dict[str, Any],
    live_balance_get: Optional[Callable[[], Optional[float]]],
) -> dict[str, Any]:
    """Per-tick live CLOB cash equity and net for the dashboard headline.

    The headline uses the same balance live sizing reads, through a fallback
    chain:

      1. ``live_balance_get()``: the in-memory BalanceCache value (refreshed
         ~60s) -> source ``balance_cache``;
      2. the latest ``live_readiness_reports.clob_balance_usd`` if at most 300s
         old (written from the same fetch) -> ``readiness_report``;
      3. ``account['capital']``: the reconciliation sample, shown with its as-of
         time -> ``reconciliation_sample``;
      4. ``None`` (DRY or no cash basis).

    The reconciliation sample (``account``) is the audited basis.
    ``account_pnl_total_live`` nets against the same basis
    (``account['initial']``) so the cash net and the % change agree.
    """
    capital_live: Optional[float] = None
    capital_live_ts: Optional[str] = None
    capital_live_source: Optional[str] = None
    degraded = False

    # (a) per-tick in-memory balance — exact (the number sizing reads).
    if live_balance_get is not None:
        try:
            fetched = live_balance_get()
        except Exception:  # noqa: BLE001 — a bad fetch degrades to the fallbacks
            fetched = None
        if fetched is not None:
            try:
                capital_live = float(fetched)
                capital_live_ts = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                capital_live_source = "balance_cache"
            except (TypeError, ValueError):
                capital_live = None

    # (b) 60s DB mirror of the SAME fetch (readiness job), when fresh.
    if capital_live is None:
        try:
            row = conn.execute(
                "SELECT clob_balance_usd, created_at "
                "FROM live_readiness_reports "
                "WHERE clob_balance_usd IS NOT NULL "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        if row is not None:
            bal = _float_or_none(row["clob_balance_usd"])
            ts_dt = _sqlite_utc_ts(row["created_at"])
            age_s = (
                (datetime.now(timezone.utc) - ts_dt).total_seconds()
                if ts_dt is not None else None
            )
            if (
                bal is not None
                and age_s is not None
                # small negative slack for writer/reader clock skew
                and -60 <= age_s <= _LIVE_BALANCE_HARD_STALENESS_S
            ):
                capital_live = bal
                capital_live_ts = row["created_at"]
                capital_live_source = "readiness_report"

    # (c) the reconciliation sample — the DEGRADED anchor (as-of shown on screen).
    if capital_live is None and account.get("capital") is not None:
        capital_live = float(account["capital"])
        capital_live_ts = account.get("capital_ts")
        capital_live_source = "reconciliation_sample"
        degraded = True

    basis = account.get("initial")
    pnl_total_live = (
        capital_live - basis
        if capital_live is not None and basis is not None else None
    )
    return {
        "account_capital_live": _round_money(capital_live),
        "account_capital_live_ts": capital_live_ts,
        "account_capital_live_source": capital_live_source,
        "account_capital_live_degraded": degraded,
        "account_pnl_total_live": _round_money(pnl_total_live),
        "account_capital_basis_ts": account.get("basis_ts"),
    }


def _mark_open_position(
    book_mark_get: Optional[Callable[..., Any]],
    *,
    poly_token_id: Any,
    now_ts: float,
    live_filled_size: Any,
    bet_size: Any,
    fill_price: Any,
) -> dict[str, Any]:
    """Mark ONE open position to the current book (unrealized).

    Reads the SAME off-tick book source the held-position pass uses (WS mirror / prefetch
    cache only) — zero new venue calls. The chain only holds tokens in the
    CURRENT active window; a PENDING position whose window has closed MISSES the
    cache → mark falls back to the entry fill price (cost basis). Display-only —
    never a sizing/settlement primitive.
    """
    mark_px: Optional[float] = None
    if book_mark_get is not None and poly_token_id:
        try:
            snap = book_mark_get(str(poly_token_id), now_ts=float(now_ts))
        except Exception:  # noqa: BLE001 — a bad/absent book degrades to entry
            snap = None
        mark_px = _top_bid(snap)
    shares: Optional[float] = None
    if live_filled_size is not None:
        try:
            shares = float(live_filled_size)
        except (TypeError, ValueError):
            shares = None
    if shares is None or shares <= 0:
        try:
            if fill_price:
                shares = float(bet_size) / float(fill_price)
        except (TypeError, ValueError, ZeroDivisionError):
            shares = None
    if mark_px is not None:
        mark_source = "book"
        mark: Optional[float] = mark_px
    else:
        mark_source = "entry"
        try:
            mark = float(fill_price) if fill_price else None
        except (TypeError, ValueError):
            mark = None
    mark_value = (
        shares * mark if shares is not None and mark is not None else None
    )
    return {
        "mark_px": _round_prob(mark_px),
        "mark_source": mark_source,
        "mark_value_usd": _round_money(mark_value),
    }


def _open_positions_mark_block(
    conn: sqlite3.Connection,
    portfolio: Optional[V6Portfolio],
    book_mark_get: Optional[Callable[..., Any]],
    account_capital_live: Optional[float],
) -> dict[str, Any]:
    """Mark every active-strategy PENDING position to the current book and sum.

    ``account_equity_incl_open`` = live cash (which excludes the cost of shares
    held) + the open positions' mark value. Uses ``_ledger_where`` so positions
    from other strategies are left out.
    """
    value_total = 0.0
    marked_n = 0
    entry_n = 0
    now_ts = time.time()
    try:
        where, params = _ledger_where(portfolio)
        pending_where = _where_plus(where, "outcome = 'PENDING'")
        rows = conn.execute(
            "SELECT poly_token_id, live_filled_size, bet_size, fill_price "
            f"FROM ledger{pending_where}",
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for r in rows:
        mark = _mark_open_position(
            book_mark_get,
            poly_token_id=r["poly_token_id"],
            now_ts=now_ts,
            live_filled_size=r["live_filled_size"],
            bet_size=r["bet_size"],
            fill_price=r["fill_price"],
        )
        if mark["mark_source"] == "book":
            marked_n += 1
        else:
            entry_n += 1
        if mark["mark_value_usd"] is not None:
            value_total += float(mark["mark_value_usd"])
    equity_incl_open = (
        _round_money(float(account_capital_live) + value_total)
        if account_capital_live is not None else None
    )
    return {
        "open_positions_value_usd": _round_money(value_total),
        "open_positions_marked_n": marked_n,
        "open_positions_entry_n": entry_n,
        "account_equity_incl_open_usd": equity_incl_open,
    }


# get_group_state and extractable_pnl_by_wallet are pure aggregates over the
# ledger, live_order_attempts and transfer_requests, and _kpis_block may be
# called more than once per publish. A short TTL cache avoids the repeat work.
# It is keyed on the ledger-content probe (capital.ledger_version), so any
# ledger change refreshes it at once; changes to the other two tables show up
# within the 1s TTL. Mode and operator state are not cached, so a stop or a
# wallet-mode change lands on the next payload. Tests clear it via
# clear_aux_cache.
_KPI_GROUP_TTL_S = 1.0
_KPI_GROUP_CACHE: dict[str, Any] = {
    # Strong identity ref, compared with `is` — an id()-keyed entry could
    # false-hit after CPython recycles a freed connection's address (the
    # _aux_cache id() note does not apply here because this cache also keys
    # on ledger content, which a fresh test DB can coincidentally repeat).
    "conn": None,
    "key": None,
    "expires_at": 0.0,
    "value": None,
}


def _group_state_cached(
    conn: sqlite3.Connection,
    portfolio: V6Portfolio,
    *,
    probe: Optional[tuple] = None,
) -> tuple[dict[str, Any], dict[str, float]]:
    """Return ``(group_state, extractable_map)``, recomputed at most once per
    TTL per unchanged (connection, portfolio, ledger-version) triple.

    ``probe`` is a ledger_version() result the caller already read this pass —
    _kpis_block shares ONE probe across this cache and the wallet-snapshot scan
    (the two used to read the full-ledger aggregate independently). Omit it and
    this reads its own (other callers)."""
    now = time.monotonic()
    if probe is None:
        try:
            probe = ledger_version(conn)
        except sqlite3.OperationalError:
            probe = None
    key = (
        tuple(sorted(str(s.label) for s in portfolio.market_sleeves)),
        probe,
    )
    cache = _KPI_GROUP_CACHE
    if (
        cache["value"] is not None
        and cache["conn"] is conn
        and cache["key"] == key
        and float(cache["expires_at"] or 0.0) > now
    ):
        return cache["value"]
    try:
        group_state = get_group_state(conn, portfolio)
    except sqlite3.OperationalError:
        group_state = {}
    # One aggregate query per table instead of two per wallet.
    try:
        extractable_map = extractable_pnl_by_wallet(conn)
    except sqlite3.OperationalError:
        extractable_map = {}
    value = (group_state, extractable_map)
    cache["conn"] = conn
    cache["key"] = key
    cache["expires_at"] = now + _KPI_GROUP_TTL_S
    cache["value"] = value
    return value


def _kpis_block(
    conn: sqlite3.Connection,
    portfolio: V6Portfolio,
    *,
    live_balance_get: Optional[Callable[[], Optional[float]]] = None,
    book_mark_get: Optional[Callable[..., Any]] = None,
) -> dict:
    # NOT cached: wallet mode + versions drive the operator surface and must
    # reflect a flip on the next payload.
    mode_state, versions = _load_wallet_mode_versions(conn)
    # ONE ledger_version() probe shared by the group-state TTL cache AND the
    # wallet-snapshot scan cache — each used to read this full-ledger aggregate
    # independently (2x per _kpis_block). The single probe still precedes both
    # scans, so the freshness contract is unchanged. None (probe read failed)
    # falls each cache back to reading its own — identical to the old path.
    try:
        ledger_probe: Optional[tuple] = ledger_version(conn)
    except sqlite3.OperationalError:
        ledger_probe = None
    group_state, extractable_map = _group_state_cached(
        conn, portfolio, probe=ledger_probe
    )
    wallet_snaps = get_wallet_snapshots(
        conn,
        portfolio,
        mode_state=mode_state or None,
        group_state=group_state or None,
        version=ledger_probe,
    )
    by_sleeve: dict[str, dict] = {}
    wallets: list[dict] = []
    for s in portfolio.market_sleeves:
        snap = wallet_snaps[s.label]
        extractable = extractable_map.get(snap.wallet_id, 0.0) if extractable_map else None
        wallet_payload = _wallet_payload(
            s,
            snap,
            portfolio,
            mode_version=versions.get(s.label),
            extractable_pnl_usd=extractable,
        )
        wallets.append(wallet_payload)
        by_sleeve[s.label] = {
            "pnl_24h": wallet_payload["pnl_24h"],
            "n_bets_24h": wallet_payload["n_bets_24h"],
            "n_topups_24h": wallet_payload["n_topups_24h"],
            "open_notional": wallet_payload["exposure_used"],
            "target_bet_size": wallet_payload["target_bet_size"],
            "ledger_sleeves": wallet_payload["ledger_sleeves"],
            "wallet_id": wallet_payload["id"],
            "realized_capital": wallet_payload["realized_capital"],
            "drawdown_pct_peak": wallet_payload["drawdown_pct_peak"],
            "backtest_max_drawdown_pct": wallet_payload["backtest_max_drawdown_pct"],
            "trading_halted": wallet_payload["trading_halted"],
            "exposure_cap_frac": wallet_payload["exposure_cap_frac"],
            "mode": wallet_payload["mode"],
            "mode_version": wallet_payload["mode_version"],
            "wallet_group": wallet_payload["wallet_group"],
        }

    # Count a shared-equity group once in the dollar equity rollups: every
    # member carries the same combined realized/peak/pending/initial values, so
    # summing members would double-count the one shared wallet. Non-shared
    # wallets are summed per wallet. Per-sleeve counts (pnl_24h, n_pending,
    # n_topups) are still summed below; those are real per-sleeve figures.
    _shared_lookup = getattr(portfolio, "shared_equity_group_for", None)
    total_initial = 0.0
    total_realized = 0.0
    total_peak = 0.0
    total_pending = 0.0
    total_cap_usd = 0.0
    # Net capital flows (deposits + rebates − withdrawals) already folded into
    # total_realized / total_peak by the capital replay. Deduped the same way,
    # since the flows are account-level and every wallet carries the same value.
    total_net_flow = 0.0
    total_pnl_drawdown = 0.0
    _counted_shared_groups: set[str] = set()
    for wallet_id, w in wallet_snaps.items():
        shared_group = None
        if callable(_shared_lookup):
            try:
                shared_group = _shared_lookup(wallet_id)
            except Exception:  # noqa: BLE001 — defensive vs fake portfolios
                shared_group = None
        if shared_group is not None:
            if shared_group.group_id in _counted_shared_groups:
                continue  # combined value already counted for this shared wallet
            _counted_shared_groups.add(shared_group.group_id)
        total_initial += w.initial_bankroll
        total_realized += w.realized_capital
        total_peak += w.peak_realized_capital
        total_pending += w.pending_exposure
        total_net_flow += w.net_flow_usd
        # PnL-only drawdown dollars, from the wallet's drawdown_ratio (=
        # (pnl_peak - pnl_realized) / initial_bankroll). Summing the
        # flow-inclusive totals instead would let a deposit move
        # ``drawdown_pct``, which must ignore funding. Same result as plain
        # subtraction when there are no flows.
        total_pnl_drawdown += w.drawdown_ratio * w.initial_bankroll
        total_cap_usd += w.realized_capital * portfolio.exposure_cap_fraction_for(wallet_id)
    total_pnl_24h = sum(w.pnl_24h for w in wallet_snaps.values())
    total_wins_24h = sum(w.wins_24h for w in wallet_snaps.values())
    total_settled_24h = sum(w.settled_24h for w in wallet_snaps.values())
    total_stops_24h = sum(w.stops_24h for w in wallet_snaps.values())
    # Stop-loss exits are realized losses. The wallet snapshot counts them
    # separately; here they are folded into losses_24h.
    _wr_denom_24h = total_settled_24h + total_stops_24h
    win_rate_24h = (
        total_wins_24h / _wr_denom_24h if _wr_denom_24h > 0 else None
    )
    drawdown_ratio = (
        max(0.0, total_pnl_drawdown / total_initial)
        if total_initial > 0 else 0.0
    )
    drawdown_pct_peak_frac = (
        max(0.0, (total_peak - total_realized) / total_peak)
        if total_peak > 0 else 0.0
    )
    halted_wallets = [w for w in wallets if w["trading_halted"]]
    account = _live_account_reconciliation(conn, portfolio)
    # Per-tick live cash equity and net (the balance sizing reads) plus the
    # open-position mark. The reconciliation ``account`` above is the audited
    # basis; these are the headline.
    account_live = _live_account_capital(conn, account, live_balance_get)
    open_marks = _open_positions_mark_block(
        conn, portfolio, book_mark_get, account_live["account_capital_live"]
    )

    return {
        "initial_bankroll": _round_money(total_initial),
        "wallet_bankroll": _round_money(config.INITIAL_BANKROLL),
        "wallet_start_at": config.WALLET_START_AT or None,
        "realized_capital": _round_money(total_realized),
        # Dashboard account view: current pUSD/CLOB capital reconciled to the
        # wallet cash basis. The LIVE ledger P&L is included separately so the
        # operator can see any cash-vs-ledger drift while strategy sizing stays
        # on realized_capital above.
        "account_capital": _round_money(account["capital"]),
        "account_initial_capital": _round_money(account["initial"]),
        "account_pnl_total": _round_money(account["pnl"]),
        "account_strategy_pnl_total": _round_money(account["strategy_pnl"]),
        "account_pnl_ledger_delta": _round_money(account["pnl_ledger_delta"]),
        "account_capital_source": account["capital_source"],
        "account_capital_ts": account["capital_ts"],
        "account_initial_capital_source": account["basis_source"],
        "account_initial_capital_ts": account["basis_ts"],
        "account_capital_comparable": account["comparable"],
        # Per-tick live CLOB cash equity and net: the same balance live sizing
        # reads (BalanceCache), not the slower reconciliation sample. Fallback
        # chain and as-of time are in _live_account_capital.
        **account_live,
        # Unrealized open-position mark-to-book (+ the honest total equity that
        # adds it to the cash that excludes the spent share cost).
        **open_marks,
        # Ledger-paper gross → fee → net decomposition over ALL settled rows
        # (Polymarket's fee is embedded in fill prices, skimmed from winning
        # payouts). DISPLAY ONLY — the Overview "P&L breakdown" panel reads
        # these; distinct from the cash-basis account_* fields above (which are
        # the real CLOB wallet), so the two bases stay visibly separate.
        # net here == ledger_rollup.pnl_net (same _ledger_where filter).
        **_ledger_pnl_decomposition(conn, portfolio),
        "peak_capital": _round_money(total_peak),
        # Net operator-recorded cash flows since the cutover, ALREADY inside
        # realized_capital + peak_capital above (a deposit raises equity at its
        # own timestamp without counting as profit). Emitted so the equity tile
        # can caption how much of the balance is funding rather than P&L — the
        # DD tile fields keep their meaning and need no client math change.
        "net_deposits": _round_money(total_net_flow),
        "drawdown_pct": round(drawdown_ratio * 100.0, 2),
        "drawdown_pct_peak": round(drawdown_pct_peak_frac * 100.0, 2),
        "max_drawdown_halt_pct": round(portfolio.max_drawdown_halt_frac * 100.0, 2),
        "trading_halted": bool(halted_wallets),
        "halt_reason": "dd_halt" if halted_wallets else None,
        "halted_wallets": [w["id"] for w in halted_wallets],
        "pnl_24h": _round_money(total_pnl_24h),
        # 24h win rate with stop-loss exits counted as losses. Per-fill counts
        # from the wallet snapshot, not trade groups.
        "win_rate_24h": round(win_rate_24h, 4) if win_rate_24h is not None else None,
        "wins_24h": total_wins_24h,
        "losses_24h": total_settled_24h - total_wins_24h + total_stops_24h,
        # Two pending counts: ``n_pending`` is the per-fill row count used by
        # the exposure bookkeeping; ``n_pending_trades`` folds those rows into
        # CLOB positions, so the Overview, sidebar and Journal all mean the
        # same thing by "open bets". None on an old schema; the client then
        # falls back to the per-fill number.
        "n_pending": sum(w.n_pending for w in wallet_snaps.values()),
        "n_pending_trades": _open_trade_group_count(conn, portfolio),
        "n_topups_24h": sum(w.n_topups_24h for w in wallet_snaps.values()),
        "n_wallets": len(wallets),
        "n_wallets_halted": len(halted_wallets),
        "exposure_used": _round_money(total_pending),
        "exposure_cap": _round_money(total_cap_usd),
        # Surface EXPOSURE_WARN_RATIO through the payload so the dashboard
        # uses one source of truth (review #22). Was previously hardcoded
        # 0.80 in StatStrip.jsx and could silently diverge from config.
        "exposure_warn_ratio": config.EXPOSURE_WARN_RATIO,
        # Tells the dashboard whether to render one tile per isolated wallet or
        # the wallet-group rollup; derived from the portfolio.
        "topology": "isolated_4" if not portfolio.wallet_groups else "wallet_groups",
        "wallets": wallets,
        "by_sleeve": by_sleeve,
    }


def _active_sleeve_filter(
    portfolio: Optional[V6Portfolio],
) -> tuple[str, tuple[str, ...]]:
    """Build a ``sleeve IN (...)`` fragment limiting the ledger to the sleeves
    the loaded portfolio trades.

    Rows from strategies that are no longer configured never show on the
    dashboard. The active set comes from the live config, so a strategy change
    needs no code change here. Returns ("", ()) when no sleeves are known (no
    filter).
    """
    labels = (
        tuple(s.label for s in portfolio.market_sleeves) if portfolio else ()
    )
    if not labels:
        return "", ()
    return f"sleeve IN ({','.join('?' * len(labels))})", labels


def _ledger_where(
    portfolio: Optional[V6Portfolio],
    *,
    last_seen_id: int = 0,
) -> tuple[str, list[Any]]:
    """Compose the shared WHERE clause used by both the streamed ledger block
    and the lifetime rollup, so the two ALWAYS agree on which rows count:
    active-strategy sleeves only, ``bet_ts >= WALLET_START_AT`` when a cutover
    is configured, and (for deltas) ``id > last_seen_id``.
    """
    # Normalize through _wallet_start_sql (as capital.py and _record_anchor_ts
    # do). An ISO cutover like '2026-07-14T12:00:00Z' compared raw against
    # SQL-text bet_ts would drop the whole cutover day ('T' > ' ').
    wallet_start_at = _wallet_start_sql()
    sleeve_sql, sleeve_params = _active_sleeve_filter(portfolio)
    conditions: list[str] = []
    params: list[Any] = []
    if last_seen_id > 0:
        conditions.append("id > ?")
        params.append(last_seen_id)
    if wallet_start_at:
        conditions.append("bet_ts >= ?")
        params.append(wallet_start_at)
    if sleeve_sql:
        conditions.append(sleeve_sql)
        params.extend(sleeve_params)
    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


# Shared win/loss predicates: the one SQL definition of the win-rate classes
# (see _WIN_RATE_CONTRACT), used by both the lifetime and the daily rollups so
# they cannot drift.
#
# A stop-loss exit is a realized loss, so it counts in ``losses``. The stop
# predicate is still needed for historical rows: their outcome is 'CANCELLED',
# so without it they would be read as voided cancels and drop out of the
# denominator. It includes the pnl_net check explicitly; that is redundant
# where the WHERE clause already requires it, but needed in the lifetime
# rollup, which also scans unsettled rows.
_STOP_BOOKED_PRED_SQL = "exit_kind='stop_loss' AND pnl_net IS NOT NULL"
# A LOSS row for counting purposes: a decided LOSS, or a booked stop exit.
_LOSS_PRED_SQL = f"(outcome='LOSS' OR ({_STOP_BOOKED_PRED_SQL}))"
_VOIDED_CANCEL_PRED_SQL = "outcome='CANCELLED' AND exit_kind IS NULL"

# ---------------------------------------------------------------------------
# Trade-group (CLOB position) contract
# ---------------------------------------------------------------------------
# The ledger writes one row per fill: an entry plus any top-ups in the same
# market window are several rows. On the venue that is one CLOB position (one
# token balance that settles once), so every count the dashboard labels
# "trades", "bets" or uses as a win-rate denominator counts groups, not rows.
# (Dollar sums are unaffected.)
#
# The grouping key is the position's identity:
#
#    venue · coin · interval_min · window_start_ts · side · wallet
#
# (coin, interval_min, window_start_ts) is the market window, ``side`` is which
# outcome token was bought (YES and NO in one window are two balances), and the
# wallet keeps separate accounts apart. ``idx_ledger_window`` on (coin,
# interval_min, window_start_ts, is_topup, outcome) indexes the same tuple.
# ``is_topup`` is not part of the key; a top-up belongs to its base bet's group
# and the flag only labels fills within a group.
#
# Rows written before wallet_id existed fall back to ``sleeve``, which is per
# interval, so the fallback can only split a group further, never merge two
# positions.
_TRADE_GROUP_KEY_SQL = (
    "coin, interval_min, window_start_ts, side, "
    "COALESCE(NULLIF(wallet_id, ''), sleeve)"
)
# Per-group outcome flags, shared by every grouped rollup query. A group is
# OPEN if any of its fills is still pending; otherwise it is a WIN if any fill
# won, a LOSS if any fill lost or stopped out, and VOIDED if it only ever
# carried never-filled cancels. (All fills of a group hold the SAME token, so
# they settle as one outcome — the win/loss flags cannot disagree in practice;
# the win-before-loss precedence below just makes the classification total.)
_GROUP_FLAGS_SQL = (
    "    MAX(CASE WHEN outcome='PENDING' THEN 1 ELSE 0 END) AS g_open, "
    "    MAX(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) AS g_win, "
    f"   MAX(CASE WHEN {_LOSS_PRED_SQL} THEN 1 ELSE 0 END) AS g_loss, "
    f"   MAX(CASE WHEN {_VOIDED_CANCEL_PRED_SQL} THEN 0 ELSE 1 END) AS g_real, "
)
# Outer-query classifiers over those flags (SQLite lets the outer SELECT read
# the subquery's aliases).
_GROUP_WIN_SQL = "CASE WHEN g_open=0 AND g_win=1 THEN 1 ELSE 0 END"
_GROUP_LOSS_SQL = "CASE WHEN g_open=0 AND g_win=0 AND g_loss=1 THEN 1 ELSE 0 END"
_GROUP_VOID_SQL = (
    "CASE WHEN g_open=0 AND g_win=0 AND g_loss=0 AND g_real=0 THEN 1 ELSE 0 END"
)
# Settled and actually opened: the one population every "trades"/"bets" count
# means. ``g_real=1`` means at least one fill that is not a voided,
# never-filled cancel. Shipped as its own value so clients do not compute
# ``total - open``, which would include voided groups.
_GROUP_SETTLED_OPENED_SQL = "CASE WHEN g_open=0 AND g_real=1 THEN 1 ELSE 0 END"


def _open_trade_group_count(
    conn: sqlite3.Connection, portfolio: Optional[V6Portfolio]
) -> Optional[int]:
    """Number of open CLOB positions: pending rows folded into trade groups
    (``_TRADE_GROUP_KEY_SQL``).

    Shipped as ``kpis.n_pending_trades`` next to the per-fill
    ``kpis.n_pending`` (capital.py's row counter) so the Overview, sidebar and
    Journal give the same answer. Returns None on an old schema so the client
    falls back.
    """
    where, params = _ledger_where(portfolio)
    cond = "outcome='PENDING'"
    sql_where = (where + " AND " + cond) if where else (" WHERE " + cond)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM ("
            f"  SELECT 1 FROM ledger{sql_where} "
            f"  GROUP BY {_TRADE_GROUP_KEY_SQL}"
            ")",
            params,
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    return int(row["n"] or 0) if row is not None else None


def _ledger_rollups(
    conn: sqlite3.Connection, portfolio: Optional[V6Portfolio]
) -> tuple[Optional[dict], dict[str, dict]]:
    """Lifetime ledger aggregates for the active strategy, over the full ledger
    (not the streamed window). Returns ``(combined, by_venue)``: the
    headline rollup plus a per-venue split with the same shape (empty/NULL
    venue counts as 'polymarket', as in ``_row_venue``).

    One GROUP BY venue scan serves both; the combined rollup sums the venue
    groups in Python (win_rate and roi are recomputed from the summed counts,
    never averaged). Uses the same active-sleeve + WALLET_START_AT filter as
    ``_ledger_block`` (via ``_ledger_where``), so rollups and streamed rows
    always agree.

    Counts are trade groups (``_TRADE_GROUP_KEY_SQL``); ``pnl_net`` and
    ``stake`` are the same sums as over rows.

    ``total`` is the audit count of every group (open + settled + voided). What
    the dashboard shows as "trades" is ``settled_opened``: settled groups with
    at least one real fill. ``total - open`` still includes voided cancels.

    Win rate = ``wins / (wins + losses)`` over groups, with stop-loss exits
    counted as losses (``_LOSS_PRED_SQL``) and voided groups (never filled or
    refunded, ``_VOIDED_CANCEL_PRED_SQL``) excluded. ``decided`` = wins +
    losses. Returns ``(None, {})`` if the ledger cannot be read (old schema).
    """
    where, params = _ledger_where(portfolio)
    try:
        rows = conn.execute(
            "SELECT venue_key, "
            "  COUNT(*) AS total, "
            f"  SUM({_GROUP_WIN_SQL}) AS wins, "
            f"  SUM({_GROUP_LOSS_SQL}) AS losses, "
            f"  SUM({_GROUP_VOID_SQL}) AS voided, "
            f"  SUM({_GROUP_SETTLED_OPENED_SQL}) AS settled_opened, "
            "  SUM(g_open) AS open_n, "
            "  COALESCE(SUM(g_pnl), 0.0) AS pnl, "
            "  COALESCE(SUM(g_stake), 0.0) AS stake, "
            "  MIN(g_first_ts) AS first_ts, "
            "  MAX(g_last_ts) AS last_ts "
            "FROM ("
            "  SELECT COALESCE(NULLIF(venue, ''), 'polymarket') AS venue_key, "
            f"{_GROUP_FLAGS_SQL}"
            "    SUM(pnl_net) AS g_pnl, "
            "    SUM(bet_size) AS g_stake, "
            "    MIN(bet_ts) AS g_first_ts, "
            "    MAX(bet_ts) AS g_last_ts "
            f"  FROM ledger{where} "
            f"  GROUP BY venue_key, {_TRADE_GROUP_KEY_SQL}"
            ") GROUP BY venue_key",
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        return None, {}

    def _entry(
        *, total: int, wins: int, losses: int,
        voided: int, settled_opened: int, open_n: int, pnl: float, stake: float,
        first_ts: Any, last_ts: Any,
    ) -> dict:
        wr_denom = wins + losses
        return {
            "total": total,
            "decided": wr_denom,
            "wins": wins,
            # Stop-loss exits are included here; they are losses.
            "losses": losses,
            "voided": voided,
            # The trade count: settled groups that actually opened (total ==
            # settled_opened + open + voided).
            "settled_opened": settled_opened,
            "open": open_n,
            "win_rate": (wins / wr_denom) if wr_denom else None,
            "pnl_net": _round_money(pnl),
            "stake": _round_money(stake),
            "roi": round(pnl / stake, 4) if stake else None,
            "first_ts": first_ts,
            "last_ts": last_ts,
        }

    by_venue: dict[str, dict] = {}
    # Combined accumulators stay RAW (pre-rounding) so the derived all-venue
    # rollup rounds the true totals, never sums of rounded per-venue values.
    counts = dict.fromkeys(
        ("total", "wins", "losses", "voided", "settled_opened", "open_n"), 0
    )
    pnl_sum = 0.0
    stake_sum = 0.0
    first_ts: Any = None
    last_ts: Any = None
    for row in rows:
        vals = {
            "total": int(row["total"] or 0),
            "wins": int(row["wins"] or 0),
            "losses": int(row["losses"] or 0),
            "voided": int(row["voided"] or 0),
            "settled_opened": int(row["settled_opened"] or 0),
            "open_n": int(row["open_n"] or 0),
        }
        pnl = float(row["pnl"] or 0.0)
        stake = float(row["stake"] or 0.0)
        by_venue[str(row["venue_key"])] = _entry(
            **vals, pnl=pnl, stake=stake,
            first_ts=row["first_ts"], last_ts=row["last_ts"],
        )
        for k in counts:
            counts[k] += vals[k]
        pnl_sum += pnl
        stake_sum += stake
        # bet_ts strings compare lexicographically == chronologically (same
        # ordering SQL MIN/MAX used), so min/max over the group extrema is the
        # global extremum.
        if row["first_ts"] is not None and (
            first_ts is None or row["first_ts"] < first_ts
        ):
            first_ts = row["first_ts"]
        if row["last_ts"] is not None and (
            last_ts is None or row["last_ts"] > last_ts
        ):
            last_ts = row["last_ts"]
    combined = _entry(
        **counts, pnl=pnl_sum, stake=stake_sum,
        first_ts=first_ts, last_ts=last_ts,
    )
    return combined, by_venue


# Degraded-read shape for _ledger_pnl_decomposition — every field None so a
# legacy-schema DB can never publish a half-filled decomposition. Returned as a
# copy so a caller mutating its dict cannot poison the constant.
_EMPTY_PNL_DECOMPOSITION: dict[str, Optional[float]] = {
    "pnl_gross_usd": None,
    "pnl_fee_usd": None,
    "pnl_net_usd": None,
    "pnl_rebate_usd": None,
}


def _ledger_pnl_decomposition(
    conn: sqlite3.Connection, portfolio: Optional[V6Portfolio]
) -> dict[str, Optional[float]]:
    """Ledger gross -> fee -> net decomposition over all settled rows.

    The fee is taken out of the fill price and realized on the winning payout.
    Per winning row:

      ``gross_row = shares * (1 - fp)``    (payout - cost, before fee)
      ``fee_row   = gross_row - pnl_net``  (the fee that reduced the payout)

    where (shares, fp) is the realized live fill (``live_filled_size`` /
    ``live_filled_price``) when present and the intended (``bet_size /
    fill_price``, ``fill_price``) otherwise; the two are never mixed. Losses
    and cancels have no payout, so ``fee_row = 0`` and ``gross_row = pnl_net``.
    Summed:

      ``pnl_net_usd``   = sum of pnl_net over settled rows (== ledger_rollup net)
      ``pnl_fee_usd``   = sum of fee_row over wins (fee paid, positive)
      ``pnl_gross_usd`` = pnl_net_usd + pnl_fee_usd

    Plus ``pnl_rebate_usd`` = sum of ``capital_flows.amount_usd`` with ``kind =
    'rebate'`` (fee rebates and rewards paid to the account). It sits outside
    the gross - fee = net identity: a rebate is an account-level cash event not
    tied to any bet, and it is already in equity through the capital-flow
    replay (so adding it here would double count against
    ``kpis.net_deposits``). ``load_capital_flows`` applies the same
    ``WALLET_START_AT`` cutover, so both cover the same period.

    Uses the same ``_ledger_where`` filter as the rollup, so this net always
    matches the headline. Display only. Returns Nones when the ledger cannot be
    read (old schema).
    """
    where, params = _ledger_where(portfolio)
    settled_cond = "pnl_net IS NOT NULL"
    sql_where = (
        (where + " AND " + settled_cond) if where else (" WHERE " + settled_cond)
    )
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(pnl_net), 0.0) AS net, "
            "  COALESCE(SUM(CASE WHEN outcome = 'WIN' THEN "
            "    CASE "
            "      WHEN COALESCE(live_filled_size, 0) > 0 "
            "           AND COALESCE(live_filled_price, 0) > 0 "
            "      THEN live_filled_size * (1.0 - live_filled_price) - pnl_net "
            "      WHEN COALESCE(fill_price, 0) > 0 "
            "      THEN bet_size * (1.0 / fill_price - 1.0) - pnl_net "
            "      ELSE 0.0 END "
            "    ELSE 0.0 END), 0.0) AS fee "
            f"FROM ledger{sql_where}",
            params,
        ).fetchone()
    except sqlite3.OperationalError:
        return dict(_EMPTY_PNL_DECOMPOSITION)
    if row is None:
        return dict(_EMPTY_PNL_DECOMPOSITION)
    net = float(row["net"] or 0.0)
    fee = float(row["fee"] or 0.0)
    # Missing capital_flows table (legacy DB) yields [] — 0.0 rebates, not None:
    # the ledger read that gates the None path succeeded.
    rebate = math.fsum(
        amount
        for _ts, _flow_id, kind, amount in load_capital_flows(
            conn, _wallet_start_sql()
        )
        if kind == "rebate"
    )
    return {
        "pnl_gross_usd": _round_money(net + fee),
        "pnl_fee_usd": _round_money(fee),
        "pnl_net_usd": _round_money(net),
        "pnl_rebate_usd": _round_money(rebate),
    }


# Day-bucket rule, shared by server and client (data.jsx settleTs mirrors it):
#
#  day = MYT (+8h) date of COALESCE(exit_ts when exit_kind='stop_loss',
#                                   NULLIF(resolution_ts,''), bet_ts)
#
# MYT (Asia/Kuala_Lumpur) is fixed UTC+8 with no DST, so the client's
# fmt.dateMyt(ts) equals date(ts_as_utc + 8h). bet_ts / resolution_ts are
# stored as UTC 'YYYY-MM-DD HH:MM:SS' strings, which SQLite's date() reads as
# the same wall clock, so ``date(ts, '+8 hours')`` gives the same day (date()
# also accepts exit_ts's ISO spelling). Stop-loss rows key on exit_ts, the time
# the loss was realized, as in the equity replay (_equity_change_order_sql).
# The final COALESCE(NULLIF(resolution_ts,''), bet_ts) matches the client's
# ``r.resolution_ts || r.bet_ts`` ('' is falsy in JS; settled rows always have
# bet_ts).
_MYT_DAY_SQL = (
    "date(COALESCE("
    "CASE WHEN exit_kind = 'stop_loss' THEN NULLIF(exit_ts, '') END, "
    "NULLIF(resolution_ts, ''), bet_ts), '+8 hours')"
)

# Human/machine-readable copies of the producer<->client contracts, emitted in
# the payload (payload["contracts"]) so the client can render/verify the exact
# rules it mirrors instead of hardcoding a prose copy that drifts.
_DAY_BUCKET_CONTRACT = (
    "day = MYT(+8h) date of COALESCE(exit_ts when exit_kind='stop_loss', "
    "NULLIF(resolution_ts,''), bet_ts)"
)
_WIN_RATE_CONTRACT = (
    "counts are TRADE GROUPS (venue·coin·interval·window_start·side·wallet — a "
    "base bet and its top-ups are ONE trade); win_rate = wins/(wins+losses); "
    "booked stop-loss exits (exit_kind='stop_loss', pnl_net NOT NULL) count as "
    "losses; voided cancels (outcome='CANCELLED', exit_kind NULL) never opened "
    "a position and are excluded from EVERY trade/bet count — win_rate, daily "
    "n, the settled/coin/slice/histogram counts and the equity curve alike; "
    "they ride only their own `voided` stat"
)
# The grouping key, in payload-readable prose (payload["contracts"]).
_TRADE_GROUP_CONTRACT = (
    "trade = one CLOB position = ledger rows sharing "
    "(venue, coin, interval_min, window_start_ts, side, wallet); top-up fills "
    "belong to their base bet's trade; $ sums are per-row and unaffected"
)


def _ledger_daily_rollups(
    conn: sqlite3.Connection, portfolio: Optional[V6Portfolio]
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    """All-time per-day P&L keyed on the MYT settlement day. Returns
    ``(combined, by_venue)``: the day array the calendar and journal read,
    plus a per-venue split with the same shape. One GROUP BY venue scan
    serves both.

    Groups every settled row (``pnl_net IS NOT NULL``) under the same
    ``_ledger_where`` filter as ``_ledger_pnl_decomposition``, so
    ``sum(day.pnl) == kpis.pnl_net_usd`` by construction. The calendar and
    journal use this array rather than the bounded client ledger, so their
    totals always match the Overview.

    Each entry is ``{date, pnl, n, wins, losses}`` (the shape the client's
    ``aggDaily`` produces), ascending by date (rule in ``_MYT_DAY_SQL``).
    ``pnl`` is the unrounded day sum, so the days add up to the headline to the
    cent; the client rounds for display.

    Counts are trade groups (``_TRADE_GROUP_KEY_SQL``). A group is filed under
    the earliest day any of its fills settles (in practice they share one), so
    every group lands on exactly one day and ``sum(day.n)`` equals the lifetime
    group count. ``wins``/``losses`` classify the group (stop-loss exits are
    losses), and ``n`` excludes groups made only of voided cancels; their $0
    stays in ``pnl``. Returns ``([], {})`` when the ledger cannot be read.
    """
    where, params = _ledger_where(portfolio)
    settled_cond = "pnl_net IS NOT NULL"
    sql_where = (
        (where + " AND " + settled_cond) if where else (" WHERE " + settled_cond)
    )
    try:
        rows = conn.execute(
            "SELECT venue_key, day, "
            "  COALESCE(SUM(g_pnl), 0.0) AS pnl, "
            "  SUM(g_real) AS n, "
            f"  SUM({_GROUP_WIN_SQL}) AS wins, "
            f"  SUM({_GROUP_LOSS_SQL}) AS losses "
            "FROM ("
            "  SELECT COALESCE(NULLIF(venue, ''), 'polymarket') AS venue_key, "
            f"    MIN({_MYT_DAY_SQL}) AS day, "
            f"{_GROUP_FLAGS_SQL}"
            "    SUM(pnl_net) AS g_pnl "
            f"  FROM ledger{sql_where} "
            f"  GROUP BY venue_key, {_TRADE_GROUP_KEY_SQL}"
            ") GROUP BY venue_key, day ORDER BY venue_key ASC, day ASC",
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        return [], {}
    by_venue: dict[str, list[dict[str, Any]]] = {}
    merged: dict[str, dict[str, Any]] = {}
    for r in rows:
        entry = {
            "date": r["day"],
            "pnl": float(r["pnl"] or 0.0),
            "n": int(r["n"] or 0),
            "wins": int(r["wins"] or 0),
            "losses": int(r["losses"] or 0),
        }
        by_venue.setdefault(str(r["venue_key"]), []).append(entry)
        day = merged.get(entry["date"])
        if day is None:
            merged[entry["date"]] = dict(entry)
        else:
            for k in ("pnl", "n", "wins", "losses"):
                day[k] += entry[k]
    # ISO 'YYYY-MM-DD' keys sort lexicographically == chronologically, matching
    # the SQL ORDER BY day ASC of the pre-consolidation combined query.
    combined = [merged[d] for d in sorted(merged)]
    return combined, by_venue


# Outcomes that count as SETTLED. Mirrors the client's `SETTLED` set and the
# `outcome IN (...)` literal in _ledger_equity_series — the three must agree.
_SETTLED_OUTCOMES = frozenset(
    ("WIN", "LOSS", "CANCELLED", "EXPIRED", "GHOST_FILL", "PUSH")
)
# Bin count of the per-bet P&L distribution. Mirrors the client aggHistogram
# default so the server block is a drop-in replacement for it.
_HISTOGRAM_BINS = 11
# Length of the recent win/loss tape the streak rail renders.
_STREAK_TAPE_N = 20
# Rolling windows the analytics block is pre-sliced into — the Topbar range
# toggle's options (shell.jsx). The client cannot slice these aggregates itself
# (they are counts, not rows), so every option it can select must ship.
_ANALYTICS_RANGE_DAYS = {"7d": 7, "30d": 30}
_ANALYTICS_ALL = "All"


def _ledger_analytics_rollups(
    conn: sqlite3.Connection, portfolio: Optional[V6Portfolio]
) -> tuple[Optional[dict], dict[str, dict]]:
    """Full-ledger analytics for the Overview and Journal (summary, per-coin,
    per-slice, P&L histogram, streak tape). Returns ``(combined, by_venue)``
    like the other rollups, each keyed by range: ``{'All': {...}, '30d':
    {...}, '7d': {...}}``.

    These panels must cover the whole history, not just the rows streamed to
    this session.

    One row scan under the same ``_ledger_where`` filter as ``_ledger_block``
    feeds every range and venue, aggregated in Python (the histogram needs the
    P&L span before binning and the streak tape needs settlement order). The
    definitions match the client's ``aggSummary`` / ``aggCoins`` /
    ``aggSlices`` / ``aggHistogram`` / ``aggStreaks`` (win rate = wins / (wins
    + losses) with stop-loss exits as losses, settled stake as the ROI
    denominator), so the client fallback and this block report the same
    numbers.

    Rows are folded into trade groups (``_trade_groups``) first, so every panel
    counts one per group, and range filtering applies to groups by settle time.
    Returns ``(None, {})`` if the ledger cannot be read.
    """
    where, params = _ledger_where(portfolio)
    try:
        rows = conn.execute(
            "SELECT "
            "  COALESCE(NULLIF(venue, ''), 'polymarket') AS venue_key, "
            "  coin, sleeve, interval_min, entry_minute, outcome, exit_kind, "
            "  pnl_net, bet_size, realized_edge, bet_ts, is_topup, "
            "  window_start_ts, side, "
            "  COALESCE(NULLIF(wallet_id, ''), sleeve) AS group_wallet, "
            "  COALESCE("
            "    CASE WHEN exit_kind = 'stop_loss' THEN NULLIF(exit_ts, '') END, "
            "    NULLIF(resolution_ts, ''), bet_ts) AS settle_ts "
            f"FROM ledger{where}",
            params,
        ).fetchall()
    except sqlite3.OperationalError:
        return None, {}

    groups = _trade_groups(rows)

    # Lexicographic cutoff against the SQL-text timestamps — the same rolling
    # -window idiom capital.py's 24h buckets use, so the ranges here and the
    # 24h tile can never disagree about what "recent" means.
    now = datetime.now(timezone.utc)
    cutoffs = {
        label: (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        for label, days in _ANALYTICS_RANGE_DAYS.items()
    }

    def _by_range(subset: list[dict]) -> dict:
        out = {_ANALYTICS_ALL: _analytics_entry(subset)}
        for label, cut in cutoffs.items():
            out[label] = _analytics_entry(
                [g for g in subset if str(g["settle_ts"] or "") >= cut]
            )
        return out

    by_venue = {
        venue: _by_range([g for g in groups if g["venue"] == venue])
        for venue in sorted({g["venue"] for g in groups})
    }
    return _by_range(groups), by_venue


def _trade_groups(rows: list) -> list[dict]:
    """Fold per-fill ledger rows into trade groups (CLOB positions).

    One group per ``_TRADE_GROUP_KEY_SQL`` tuple (venue, coin, interval, window
    start, side, wallet). Fills keep ledger order (``bet_ts``, then scan
    order), so ``fills[0]`` is the base entry whose entry_minute and
    realized_edge describe the trade.

    Per group:

      ``pnl`` / ``stake``  sums over the fills (same totals as over rows).
      ``settle_ts``        earliest over the fills (they share one resolution).
      ``open``/``win``/``loss``/``voided``/``settled``  the group's outcome class
                           (classified like ``_GROUP_FLAGS_SQL``).
      ``n_fills``          number of ledger rows in the group.
    """
    order: list[tuple] = []
    groups: dict[tuple, dict] = {}
    for row in rows:
        key = (
            str(row["venue_key"]),
            row["coin"],
            row["interval_min"],
            row["window_start_ts"],
            row["side"],
            row["group_wallet"],
        )
        g = groups.get(key)
        if g is None:
            g = {
                "venue": str(row["venue_key"]),
                "coin": row["coin"],
                "interval": _row_interval_label(row),
                "entry": None,
                "settle_ts": None,
                "pnl": 0.0,
                "stake": 0.0,
                "edge": None,
                "has_pnl": False,
                "open": False,
                "any_win": False,
                "any_loss": False,
                "any_real": False,
                "any_settled": False,
                "n_fills": 0,
                "_anchor_ts": None,
            }
            groups[key] = g
            order.append(key)
        outcome = str(row["outcome"] or "")
        is_stop = row["exit_kind"] == "stop_loss" and row["pnl_net"] is not None
        is_voided = outcome == "CANCELLED" and row["exit_kind"] is None
        g["n_fills"] += 1
        if row["pnl_net"] is not None:
            g["pnl"] += float(row["pnl_net"])
            g["has_pnl"] = True
        if row["bet_size"] is not None:
            g["stake"] += float(row["bet_size"])
        if outcome == "PENDING":
            g["open"] = True
        if outcome == "WIN":
            g["any_win"] = True
        if outcome == "LOSS" or is_stop:
            g["any_loss"] = True
        if not is_voided:
            g["any_real"] = True
        if outcome in _SETTLED_OUTCOMES:
            g["any_settled"] = True
        ts = row["settle_ts"]
        if ts is not None and (g["settle_ts"] is None or str(ts) < str(g["settle_ts"])):
            g["settle_ts"] = ts
        # ANCHOR = the earliest non-top-up fill (the base bet); a group whose
        # base row is outside the scan (or that is all top-ups) falls back to
        # its earliest fill, so `entry`/`edge` are never silently blank.
        bet_ts = str(row["bet_ts"] or "")
        # rank: base beats top-up; within the same class, earliest bet_ts wins.
        rank = (0 if row["is_topup"] else -1, bet_ts)
        cur = g["_anchor_ts"]
        if cur is None or rank < cur:
            g["_anchor_ts"] = rank
            g["entry"] = row["entry_minute"]
            g["interval"] = _row_interval_label(row)
            g["edge"] = row["realized_edge"]

    out = []
    for key in order:
        g = groups.pop(key)
        g.pop("_anchor_ts", None)
        g["settled"] = g["any_settled"] and not g["open"]
        g["win"] = (not g["open"]) and g["any_win"]
        g["loss"] = (not g["open"]) and (not g["any_win"]) and g["any_loss"]
        g["voided"] = (
            (not g["open"]) and (not g["any_win"]) and (not g["any_loss"])
            and (not g["any_real"])
        )
        out.append(g)
    return out


def _row_interval_label(row) -> str:
    """'5m' / '15m' / '?' — mirrors the client ``rowInterval`` fallback chain
    (interval_min first, then a 5m/15m substring of the sleeve label)."""
    interval_min = row["interval_min"]
    if interval_min in (5, 15):
        return f"{int(interval_min)}m"
    sleeve = str(row["sleeve"] or "")
    if "15m" in sleeve:
        return "15m"
    if "5m" in sleeve:
        return "5m"
    return "?"


def _analytics_entry(groups: list[dict]) -> dict:
    """Summary, per-coin, per-slice, histogram and streak aggregates for one
    set of trade groups (one venue, one range).

    Every count is one per group; dollar columns are the same sums over the
    fills.

    Every count labelled "trades"/"bets" (``summary.settled``, per-coin
    ``bets``/``resolved``, per-slice ``n``, histogram bins, the expectancy
    denominator) counts settled groups that actually opened. A voided,
    never-filled cancel is not a trade; it is counted only in
    ``summary.voided``, separate from ``settled`` and ``open``. This is the
    same population ``_ledger_equity_series`` plots, so the trade count and the
    equity curve's x-axis agree.
    """
    coins: dict[str, dict] = {}
    slices: dict[str, dict] = {}
    settled_pnls: list[float] = []
    outcome_rows: list[tuple[Any, bool]] = []  # (settle_ts, is_win)
    total = settled_n = wins = losses = voided_n = 0
    pnl_sum = stake_sum = stake_settled_sum = gross_win = gross_loss = 0.0
    edge_sum = 0.0
    edge_n = 0

    for g in groups:
        pnl = float(g["pnl"])
        stake = float(g["stake"])
        is_voided = bool(g["voided"])
        settled = bool(g["settled"])
        is_win = bool(g["win"])
        is_loss = bool(g["loss"])

        total += 1
        stake_sum += stake
        if settled:
            pnl_sum += pnl
            if is_voided:
                # A voided, never-filled cancel never opened a position, so it
                # is not a trade and stays out of every "trades"/"bets" count;
                # ``settled_n`` must equal the equity series' point count. It
                # has its own ``voided`` count, and its $0 stays in pnl_sum so
                # totals still reconcile.
                voided_n += 1
            else:
                settled_n += 1
                stake_settled_sum += stake
                if g["edge"] is not None:
                    edge_sum += float(g["edge"])
                    edge_n += 1
        if is_win:
            wins += 1
        elif is_loss:
            losses += 1
        if is_win or is_loss:
            if pnl >= 0:
                gross_win += pnl
            else:
                gross_loss += abs(pnl)

        coin = g["coin"]
        if coin:
            c = coins.setdefault(str(coin), {
                "coin": str(coin), "bets": 0, "resolved": 0, "wins": 0,
                "losses": 0, "pnl": 0.0, "gw": 0.0, "gl": 0.0,
                "stake": 0.0, "stakeSettled": 0.0,
            })
            # ``bets`` / ``resolved`` are the per-coin "trades" column, so
            # voided cancels count in neither. sum(coin.resolved) ==
            # summary.settled.
            if not is_voided:
                c["bets"] += 1
            c["stake"] += stake
            if settled and not is_voided:
                c["resolved"] += 1
                c["pnl"] += pnl
                c["stakeSettled"] += stake
                if is_win:
                    c["wins"] += 1
                    c["gw"] += max(0.0, pnl)
                elif is_loss:
                    c["losses"] += 1
                    c["gl"] += abs(min(0.0, pnl))

        # Per-slice ``n`` and the histogram/expectancy population. Voided
        # cancels are excluded so sum(slice.n) == summary.settled and a $0
        # refund never lands in the histogram as a break-even trade.
        if settled and not is_voided:
            interval = g["interval"]
            entry = g["entry"]
            key = f"{interval}_e{'?' if entry is None else int(entry)}"
            s = slices.setdefault(key, {
                "slice": key, "interval": interval,
                "entry": None if entry is None else int(entry),
                "n": 0, "wins": 0, "losses": 0, "pnl": 0.0,
            })
            s["n"] += 1
            s["pnl"] += pnl
            if is_win:
                s["wins"] += 1
            elif is_loss:
                s["losses"] += 1
            if g["has_pnl"]:
                settled_pnls.append(pnl)

        if is_win or is_loss:
            outcome_rows.append((g["settle_ts"], is_win))

    return {
        "summary": {
            "total": total,
            # settled = settled trade groups that actually OPENED (>=1 real
            # fill). Voided groups are neither here nor in `open` — they are
            # the third, disjoint bucket, so total == settled + open + voided.
            "settled": settled_n,
            "open": total - settled_n - voided_n,
            "wins": wins,
            "losses": losses,
            "voided": voided_n,
            "winRate": _win_rate(wins, losses),
            "pnl": _round_money(pnl_sum),
            "stake": _round_money(stake_sum),
            "stakeSettled": _round_money(stake_settled_sum),
            "roi": (round(pnl_sum / stake_settled_sum, 4)
                    if stake_settled_sum > 0 else None),
            **_profit_factor(gross_win, gross_loss),
            "avgEdge": round(edge_sum / edge_n, 6) if edge_n else None,
            # Mean realized P&L per settled, opened group that has a pnl_net
            # (voided cancels excluded), matching the client's aggRatios.
            "expectancy": (_round_money(math.fsum(settled_pnls)
                                        / len(settled_pnls))
                           if settled_pnls else None),
        },
        "coins": _analytics_coins(coins),
        "slices": _analytics_slices(slices),
        "histogram": _analytics_histogram(settled_pnls),
        "streaks": _analytics_streaks(outcome_rows),
    }


def _win_rate(wins: int, losses: int) -> Optional[float]:
    """Win rate over trade groups, shared with ``_ledger_rollups``. Stop-loss
    exits are already counted in ``losses``; voided cancels are in neither.
    """
    denom = wins + losses
    return round(wins / denom, 6) if denom else None


def _profit_factor(gross_win: float, gross_loss: float) -> dict:
    """``{'pf', 'pf_unbounded'}`` — gross win / gross loss.

    Strict JSON has no Infinity, so a LOSS-FREE book (gross_loss == 0 with a
    positive gross win — the client's ``Infinity`` sentinel) ships as
    ``pf=None`` plus ``pf_unbounded=True``; the client restores the '∞' render
    from the flag rather than showing '—' as if nothing had settled."""
    if gross_loss > 0:
        return {"pf": round(gross_win / gross_loss, 4), "pf_unbounded": False}
    return {"pf": None, "pf_unbounded": gross_win > 0}


def _analytics_coins(coins: dict[str, dict]) -> list[dict]:
    out = []
    for c in coins.values():
        out.append({
            **{k: (_round_money(v) if isinstance(v, float) else v)
               for k, v in c.items()},
            "winRate": _win_rate(c["wins"], c["losses"]),
            **_profit_factor(c["gw"], c["gl"]),
            "roi": (round(c["pnl"] / c["stakeSettled"], 4)
                    if c["stakeSettled"] > 0 else None),
        })
    out.sort(key=lambda e: -(e["pnl"] or 0.0))
    return out


def _analytics_slices(slices: dict[str, dict]) -> list[dict]:
    out = [
        {
            **{k: (_round_money(v) if isinstance(v, float) else v)
               for k, v in s.items()},
            "winRate": _win_rate(s["wins"], s["losses"]),
        }
        for s in slices.values()
    ]
    # (interval, entry) with a missing entry last — mirrors the client sort.
    out.sort(key=lambda e: (e["interval"], e["entry"] is None, e["entry"] or 0))
    return out


def _analytics_histogram(pnls: list[float]) -> list[dict]:
    """Symmetric |pnl|-spanned bins over per-TRADE (group) P&L — the client
    ``aggHistogram`` algorithm."""
    if not pnls:
        return []
    span = max(0.01, max(abs(v) for v in pnls))
    lo, width = -span, (2 * span) / _HISTOGRAM_BINS
    bins = [
        {"lo": lo + i * width, "hi": lo + (i + 1) * width,
         "mid": lo + (i + 0.5) * width, "n": 0}
        for i in range(_HISTOGRAM_BINS)
    ]
    for v in pnls:
        idx = min(_HISTOGRAM_BINS - 1, max(0, int(math.floor((v - lo) / width))))
        bins[idx]["n"] += 1
    return [
        {"lo": round(b["lo"], 4), "hi": round(b["hi"], 4),
         "mid": round(b["mid"], 4), "n": b["n"]}
        for b in bins
    ]


def _analytics_streaks(outcome_rows: list[tuple[Any, bool]]) -> dict:
    """Longest/current win-loss streaks over TRADE GROUPS in SETTLEMENT order (a
    booked stop is a loss, so it breaks a win streak like any LOSS)."""
    ordered = [
        won for _ts, won in
        sorted(outcome_rows, key=lambda e: (e[0] is None, e[0] or ""))
    ]
    longest_w = longest_l = run_w = run_l = 0
    for won in ordered:
        run_w, run_l = (run_w + 1, 0) if won else (0, run_l + 1)
        longest_w = max(longest_w, run_w)
        longest_l = max(longest_l, run_l)
    current = 0
    if ordered:
        sign = 1 if ordered[-1] else -1
        for won in reversed(ordered):
            if (1 if won else -1) != sign:
                break
            current += 1
        current *= sign
    return {
        "current": current,
        "longestW": longest_w,
        "longestL": longest_l,
        "last": ["w" if won else "l" for won in ordered[-_STREAK_TAPE_N:]],
    }


def _downsample(seq: list, max_points: int) -> list:
    """Uniform-stride downsample preserving the FIRST and LAST elements so the
    equity curve's right edge stays exactly on the supplied anchor and its left
    edge on the base. Returns ``seq`` unchanged when it already fits."""
    n = len(seq)
    if max_points < 2 or n <= max_points:
        return seq
    stride = (n - 1) / (max_points - 1)
    idxs = sorted({min(n - 1, round(i * stride)) for i in range(max_points)})
    idxs[0] = 0
    idxs[-1] = n - 1
    return [seq[i] for i in idxs]


def _ledger_equity_series(
    conn: sqlite3.Connection,
    portfolio: Optional[V6Portfolio],
    *,
    anchor: Optional[float] = None,
    max_points: Optional[int] = None,
    record_anchor: Optional[str] = None,
) -> Optional[dict]:
    """Equity curve for the active strategy.

    With ``record_anchor`` (SQL-text UTC), the curve covers only equity changes
    at or after the reset, so the chart, its peak and its max drawdown start
    from that point. The right edge still lands on ``anchor`` (current
    capital). ``None`` gives the full lifetime curve.

    Uses the same ``_ledger_where`` filter as ``_ledger_rollups`` but returns a
    cumulative series, so the chart shows the strategy's whole history rather
    than the rows streamed to the client. With ``record_anchor`` set the curve
    adds a time filter the rollup does not, so the two differ before the reset.

    One point per trade group: the x-axis is the trade number, matching the
    "trades" count. Settled fills fold under ``_TRADE_GROUP_KEY_SQL`` and
    voided cancel groups are left out. Groups replay in settlement order from a
    base chosen so the right edge equals ``anchor`` (= kpis.realized_capital,
    like the client's ``anchoredEquity``); without an anchor the portfolio
    bankroll is used. Downsampled to ``max_points`` (first and last kept).
    Returns None when the ledger cannot be read.
    """
    max_points = (
        max_points if max_points is not None else config.LEDGER_EQUITY_MAX_POINTS
    )
    where, params = _ledger_where(portfolio)
    settled_cond = (
        "outcome IN ('WIN','LOSS','CANCELLED','EXPIRED','GHOST_FILL','PUSH') "
        "AND pnl_net IS NOT NULL"
    )
    sql_where = (
        (where + " AND " + settled_cond) if where else (" WHERE " + settled_cond)
    )
    try:
        # Order and label each point by when equity actually changed, using
        # capital.py's _equity_change_order_sql, so this chart's peak and max
        # drawdown match the capital tile and the drawdown halt. Stop-loss rows
        # key on exit_ts, other rows on resolution_ts. The 'bet_ts' fallback
        # (the capital snapshot uses '') is the one intended difference.
        ts_sql = _equity_change_order_sql("bet_ts")
        rows = conn.execute(
            # The trade-group key columns ride along so the Python fold below
            # can group fills into CLOB positions; the last key column (the
            # wallet/sleeve COALESCE) needs an explicit alias to be readable.
            f"SELECT {ts_sql} AS ts, pnl_net, {_TRADE_GROUP_KEY_SQL} AS grp_wallet, "
            f"CASE WHEN {_VOIDED_CANCEL_PRED_SQL} THEN 1 ELSE 0 END AS voided "
            f"FROM ledger{sql_where} "
            "ORDER BY ts ASC, id ASC",
            params,
        ).fetchall()
        # Drop equity changes before the reset so the curve, peak and max
        # drawdown start there. Filtered in Python on the ``ts`` SQLite already
        # computed (rows are in ts order), which is simpler than putting the
        # COALESCE/CASE expression in the WHERE clause.
        if record_anchor is not None:
            rows = [
                r for r in rows
                if r["ts"] is not None and r["ts"] >= record_anchor
            ]
    except sqlite3.OperationalError:
        return None

    # Fold fills into TRADE GROUPS (one CLOB position = one point). A group's
    # ts is its LAST equity-change moment; `opened` = at least one fill that is
    # not a voided never-filled cancel — voided-only groups never plot.
    groups: dict[tuple, dict] = {}
    for r in rows:
        key = (r["coin"], r["interval_min"], r["window_start_ts"], r["side"],
               r["grp_wallet"])
        g = groups.get(key)
        if g is None:
            g = {"pnl": 0.0, "ts": r["ts"], "opened": False}
            groups[key] = g
        g["pnl"] += float(r["pnl_net"] or 0.0)
        if r["ts"] is not None and (g["ts"] is None or r["ts"] > g["ts"]):
            g["ts"] = r["ts"]
        if not r["voided"]:
            g["opened"] = True
    trades = [g for g in groups.values() if g["opened"]]
    # Rows arrived ts-ordered so insertion order ≈ settlement order already; the
    # stable re-sort on the group's final ts only matters for the rare group
    # whose fills realized at different moments (booked stop + later top-up).
    trades.sort(key=lambda g: (g["ts"] is None, g["ts"] or ""))

    total = float(math.fsum(g["pnl"] for g in trades))
    # build_payload ALWAYS supplies anchor=kpis.realized_capital (always a
    # finite float), so the right edge lands exactly on the capital tile and the
    # branch below is unreachable in practice. It exists only as a degenerate
    # last resort; portfolio.bankroll is the single-wallet base and may read low
    # for a multi-wallet topology — acceptable only because no live caller hits
    # this path. Prefer passing an anchor.
    if anchor is not None:
        base = float(anchor) - total
    elif portfolio is not None:
        base = float(getattr(portfolio, "bankroll", 0.0) or 0.0)
    else:
        base = 0.0

    eq = base
    peak = base
    max_dd = 0.0
    max_dd_abs = 0.0
    # The zero-trade point represents the accounting cutover itself, not the
    # first later settlement.  Keeping that timestamp honest lets every chart
    # consumer show the curve as beginning at WALLET_START_AT and at the
    # corresponding opening bankroll (``base``).
    base_ts = record_anchor if record_anchor is not None else (
        trades[0]["ts"] if trades else None
    )
    full: list[dict] = [
        {"i": 0, "eq": base, "dd": 0.0, "ddAbs": 0.0,
         "ts": base_ts}
    ]
    for idx, g in enumerate(trades):
        eq += g["pnl"]
        peak = max(peak, eq)
        dd_abs = eq - peak
        dd = (dd_abs / peak) if peak > 0 else 0.0
        if dd < max_dd:
            max_dd = dd
        if dd_abs < max_dd_abs:
            max_dd_abs = dd_abs
        full.append(
            {"i": idx + 1, "eq": eq, "dd": dd, "ddAbs": dd_abs, "ts": g["ts"]}
        )

    points = _downsample(full, max_points)
    return {
        "base": _round_money(base),
        "final": _round_money(eq),
        "peak": _round_money(peak),
        "maxDD": round(max_dd, 4),
        "maxDDAbs": _round_money(max_dd_abs),
        "n": len(trades),
        "points": [
            {"i": p["i"], "eq": _round_money(p["eq"]), "dd": round(p["dd"], 4),
             "ddAbs": _round_money(p["ddAbs"]), "ts": p["ts"]}
            for p in points
        ],
    }


def _cash_equity_series(
    conn: sqlite3.Connection,
    *,
    record_anchor: str,
    max_points: Optional[int] = None,
) -> Optional[dict]:
    """Server-side CASH (CLOB balance) equity curve since the record era — the
    REAL money the account holds, hourly-AVERAGED to smooth the open-position
    transit swings (a bet deploys cash out then redeems it back, so the raw
    per-minute balance saws ~25pt while true equity trends). Unlike
    ``_ledger_equity_series`` (which cumulates ledger ``pnl_net`` and diverges
    from cash by the win-redemption bridge), this plots the actual balance, so
    ``net = final − base`` is the cash-basis era PnL the operator's equity tile
    agrees with. Same point shape as the ledger series so the chart renders it
    identically. Returns None when live_readiness_reports can't be read/empty."""
    max_points = (
        max_points if max_points is not None else config.LEDGER_EQUITY_MAX_POINTS
    )
    try:
        rows = conn.execute(
            "SELECT AVG(clob_balance_usd) AS bal, MAX(created_at) AS ts "
            "FROM live_readiness_reports "
            "WHERE clob_balance_usd IS NOT NULL AND created_at >= ? "
            "GROUP BY substr(created_at, 1, 13) "  # per-hour bucket
            "ORDER BY substr(created_at, 1, 13) ASC",
            (record_anchor,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None
    rows = [r for r in rows if r["bal"] is not None]
    if not rows:
        return None
    base = float(rows[0]["bal"])
    peak = base
    max_dd = 0.0
    max_dd_abs = 0.0
    full: list[dict] = []
    eq = base
    for idx, r in enumerate(rows):
        eq = float(r["bal"])
        peak = max(peak, eq)
        dd_abs = eq - peak
        dd = (dd_abs / peak) if peak > 0 else 0.0
        if dd < max_dd:
            max_dd = dd
        if dd_abs < max_dd_abs:
            max_dd_abs = dd_abs
        full.append({"i": idx, "eq": eq, "dd": dd, "ddAbs": dd_abs, "ts": r["ts"]})
    points = _downsample(full, max_points)
    return {
        "base": _round_money(base),
        "final": _round_money(eq),
        "net": _round_money(eq - base),
        "peak": _round_money(peak),
        "maxDD": round(max_dd, 4),
        "maxDDAbs": _round_money(max_dd_abs),
        "n": len(rows),
        "points": [
            {"i": p["i"], "eq": _round_money(p["eq"]), "dd": round(p["dd"], 4),
             "ddAbs": _round_money(p["ddAbs"]), "ts": p["ts"]}
            for p in points
        ],
    }


def _ledger_block(
    conn: sqlite3.Connection,
    portfolio: Optional[V6Portfolio],
    *,
    last_seen_id: int = 0,
    limit_initial: int | None = None,
    limit_delta: int | None = None,
    book_mark_get: Optional[Callable[..., Any]] = None,
) -> tuple[list[dict], int]:
    """Return (rows, max_id_seen). With last_seen_id=0, send the most recent N
    rows; otherwise send rows with id > last_seen_id plus recently settled
    rows, so clients can replace PENDING rows they already have.

    Rows are limited to the active strategy's sleeves and to ``bet_ts >=
    WALLET_START_AT`` when a cutover is set, both via ``_ledger_where``. Both
    limits come from config.
    """
    bootstrap = limit_initial if limit_initial is not None else config.LEDGER_BOOTSTRAP_LIMIT
    delta = limit_delta if limit_delta is not None else config.LEDGER_DELTA_LIMIT
    limit = delta if last_seen_id > 0 else bootstrap
    if last_seen_id > 0:
        where, params = _ledger_where(portfolio)
        refresh_cond = "(id > ? OR outcome != 'PENDING')"
        where = f"{where} AND {refresh_cond}" if where else f" WHERE {refresh_cond}"
        params.append(last_seen_id)
    else:
        where, params = _ledger_where(portfolio)
    params.append(limit)
    rows = conn.execute(
        "SELECT id, bet_ts, coin, wallet_id, sleeve, entry_minute, side, outcome, "
        "       window_start_ts, "
        "       mode, venue, fill_price, walked_vwap, bet_size, live_filled_size, "
        "       live_filled_price, is_topup, pnl_net, "
        "       model_p, p_market, realized_edge, resolution_ts, won_up, interval_min, "
        "       pair_component, offset_sec, "
        # poly_token_id is read ONLY for the open-position book mark below — it is
        # NOT emitted to the client (no auth/token leak).
        "       exit_kind, exit_ts, poly_token_id "
        f"FROM ledger{where} ORDER BY id DESC LIMIT ?",
        params,
    ).fetchall()

    now_ts = time.time()
    max_id = last_seen_id
    out: list[dict] = []
    for r in rows:
        row_id = int(r["id"])
        if row_id > max_id:
            max_id = row_id
        mode = r["mode"] or "DRY"
        live_fill_price = r["live_filled_price"]
        live_fill_size = r["live_filled_size"]
        live_fill_value = (
            float(live_fill_size) * float(live_fill_price)
            if live_fill_size is not None and live_fill_price is not None
            else None
        )
        effective_fill_price = (
            live_fill_price
            if mode == "LIVE" and live_fill_price is not None
            else r["walked_vwap"] if r["walked_vwap"] is not None else r["fill_price"]
        )
        effective_bet_size = live_fill_value if mode == "LIVE" and live_fill_value is not None else r["bet_size"]
        row_out = {
            "id": row_id,
            "bet_ts": r["bet_ts"],
            "coin": r["coin"],
            "wallet_id": r["wallet_id"] or wallet_id_for_sleeve(
                r["sleeve"], r["pair_component"]
            ),
            "sleeve": r["sleeve"],
            "entry_minute": r["entry_minute"],
            "mode": mode,
            # Venue of the row; rows without one count as 'polymarket'.
            "venue": _row_venue(r),
            "side": r["side"],
            # Part of the trade-group key: the market window's start. The
            # client folds a position's fills into one journal row on (venue ·
            # coin · interval_min · window_start_ts · side · wallet), the same
            # tuple the server rollups group on. A row without it stays its own
            # group.
            "window_start_ts": r["window_start_ts"],
            "direction": _direction(r["side"]),
            "outcome": r["outcome"],
            "fill_price": _round_prob(r["fill_price"]),
            "walked_vwap": _round_prob(r["walked_vwap"]),
            "live_filled_size": _round_size(live_fill_size),
            "live_filled_price": _round_prob(live_fill_price),
            "live_filled_value_usd": _round_money(live_fill_value),
            "effective_fill_price": _round_prob(effective_fill_price),
            "bet_size": _round_money(r["bet_size"]),
            "intended_bet_size": _round_money(r["bet_size"]),
            "effective_bet_size": _round_money(effective_bet_size),
            "is_topup": bool(r["is_topup"]),
            "pnl_net": _round_money(r["pnl_net"]),
            # Fields the journal, calendar and analytics pages compute with
            # client-side: model probability, realized edge, market price,
            # settlement time and resolved direction. All nullable for older
            # rows.
            "model_p": _round_prob(r["model_p"]),
            "p_market": _round_prob(r["p_market"]),
            "realized_edge": _round_prob(r["realized_edge"]),
            "resolution_ts": r["resolution_ts"],
            # Fixed exit time of a stop-loss row, used by the day-bucket rule.
            "exit_ts": r["exit_ts"],
            "won_up": r["won_up"],
            "interval_min": r["interval_min"],
            # Seconds into the window at entry (the Journal's "+Ns" badge).
            "offset_sec": r["offset_sec"],
            # Historical stop-loss rows: exit_kind='stop_loss' marks an early
            # close whose pnl_net is the realized loss.
            "exit_kind": r["exit_kind"],
        }
        # Mark OPEN positions to the current book (unrealized). Zero venue calls
        # — the SAME off-tick book source the held-position pass reads. A token outside the
        # active window MISSES → mark at entry (cost basis). poly_token_id stays
        # server-side (used only for the lookup, never emitted).
        if r["outcome"] == "PENDING":
            row_out.update(_mark_open_position(
                book_mark_get,
                poly_token_id=r["poly_token_id"],
                now_ts=now_ts,
                live_filled_size=live_fill_size,
                bet_size=r["bet_size"],
                fill_price=r["fill_price"],
            ))
        out.append(row_out)
    return out, max_id


def _ledger_history_signature(
    conn: sqlite3.Connection,
    portfolio: Optional[V6Portfolio],
) -> tuple[Any, ...]:
    where, params = _ledger_where(portfolio)
    row = conn.execute(
        """
        SELECT COUNT(*) AS n,
               COALESCE(MAX(id), 0) AS max_id,
               COALESCE(SUM(CASE WHEN outcome = 'PENDING' THEN 1 ELSE 0 END), 0) AS pending_n,
               COALESCE(SUM(CASE WHEN pnl_net IS NOT NULL THEN pnl_net ELSE 0 END), 0.0) AS pnl_sum,
               COALESCE(MAX(COALESCE(resolution_ts, '')), '') AS max_resolution_ts
        """
        f"FROM ledger{where}",
        params,
    ).fetchone()
    portfolio_key = tuple(
        sorted({s.label for s in (portfolio.sleeves if portfolio is not None else [])})
    )
    return (
        portfolio_key,
        int(row["n"] or 0),
        int(row["max_id"] or 0),
        int(row["pending_n"] or 0),
        round(float(row["pnl_sum"] or 0.0), 6),
        row["max_resolution_ts"] or "",
    )


def _history_blocks_cached(
    conn: sqlite3.Connection,
    portfolio: V6Portfolio,
    *,
    anchor: Optional[float],
) -> tuple[Optional[dict], Optional[dict], list[dict], dict[str, dict],
           dict[str, list[dict]], Optional[dict], dict[str, dict]]:
    signature = (_ledger_history_signature(conn, portfolio), _round_money(anchor))
    now = time.monotonic()
    cached = _HISTORY_CACHE.get("value")
    if (
        cached is not None
        and _HISTORY_CACHE.get("key") == signature
        and float(_HISTORY_CACHE.get("expires_at") or 0.0) > now
    ):
        return cached

    # Each consolidated call runs ONE venue-keyed GROUP BY scan and returns
    # (combined, by_venue) — the H5 per-venue variants (same definitions) the
    # client prefers for venue-scoped views ride along for free.
    rollup, rollup_by_venue = _ledger_rollups(conn, portfolio)
    daily, daily_by_venue = _ledger_daily_rollups(conn, portfolio)
    analytics, analytics_by_venue = _ledger_analytics_rollups(conn, portfolio)
    value = (
        rollup,
        # Record-era PnL/equity curve: scope to the operator reset so the chart,
        # its peak and its max-drawdown all start from the reset era (not lifetime).
        _ledger_equity_series(
            conn, portfolio, anchor=anchor, record_anchor=_record_anchor_ts()
        ),
        daily,
        rollup_by_venue,
        daily_by_venue,
        analytics,
        analytics_by_venue,
    )
    _HISTORY_CACHE["key"] = signature
    _HISTORY_CACHE["expires_at"] = now + _HISTORY_CACHE_TTL_S
    _HISTORY_CACHE["value"] = value
    return value


def _live_wallet_block(conn: sqlite3.Connection) -> dict:
    """Top-level live_wallet card payload."""
    try:
        row = conn.execute(
            "SELECT sampled_at, wallet_address, source_status, clob_balance_usd, "
            "       chain_balance_usd, open_orders_count, open_positions_count, "
            "       warnings_json "
            "FROM wallet_reconciliation_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is None:
        return {
            "balance_usd": None,
            "chain_balance_usd": None,
            "refreshed_at": None,
            "source_status": None,
            "wallet_address": None,
            "open_orders_count": 0,
            "open_positions_count": 0,
            "warnings": [],
        }
    return {
        "balance_usd": _round_money(row["clob_balance_usd"]),
        "chain_balance_usd": _round_money(row["chain_balance_usd"]),
        "refreshed_at": row["sampled_at"],
        "source_status": row["source_status"],
        "wallet_address": row["wallet_address"],
        "open_orders_count": int(row["open_orders_count"] or 0),
        "open_positions_count": int(row["open_positions_count"] or 0),
        "warnings": _json_list(row["warnings_json"]),
    }


def _json_dict(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _json_list(raw: Any) -> list[Any]:
    if not raw:
        return []
    try:
        value = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError):
        return []
    return value if isinstance(value, list) else []


def _readiness_checks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    checks = payload.get("readiness_check_results")
    if not isinstance(checks, list):
        return []
    out: list[dict[str, Any]] = []
    for item in checks:
        if not isinstance(item, dict):
            continue
        out.append({
            "name": str(item.get("name") or ""),
            "passed": bool(item.get("passed")),
            "detail": str(item.get("detail") or ""),
        })
    return out


def _capital_flows_block(conn: sqlite3.Connection) -> list[dict]:
    """Recorded account cash events, oldest first.

    Rows before ``WALLET_START_AT`` are listed for history; the equity replay
    only uses rows from the cutover on. ``kpis.net_deposits`` is the current
    total, already included in the equity tile.
    """
    return [
        {
            "occurred_at": f["occurred_at"],
            "kind": f["kind"],
            "amount_usd": _round_money(f["amount_usd"]),
            "note": f["note"],
            "source": f["source"],
        }
        for f in list_capital_flows(conn)
    ]


def _operator_block(conn: sqlite3.Connection) -> dict:
    """Singleton operator_control_state + readiness card."""
    op_state: dict[str, Any] = {
        "state": "STOPPED_PROCESSING",
        "boot_dry_run": None,
        "reason": None,
        "updated_at": None,
        "version": 0,
    }
    try:
        op_row = conn.execute(
            "SELECT state, boot_dry_run, reason, updated_at, "
            "       COALESCE(version, 0) AS version "
            "FROM operator_control_state WHERE id=1"
        ).fetchone()
    except sqlite3.OperationalError:
        op_row = None
    if op_row is not None:
        op_state = {
            "state": op_row["state"],
            "boot_dry_run": (
                bool(op_row["boot_dry_run"])
                if op_row["boot_dry_run"] is not None else None
            ),
            "reason": op_row["reason"],
            "updated_at": op_row["updated_at"],
            "version": int(op_row["version"] or 0),
        }

    readiness: dict[str, Any] = {
        "status": None,
        "refreshed_at": None,
    }
    try:
        ready_row = conn.execute(
            "SELECT status, created_at, clob_balance_usd, chain_balance_usd, "
            "       report_json "
            "FROM live_readiness_reports "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        ready_row = None
    if ready_row is not None:
        report_json = _json_dict(ready_row["report_json"])
        readiness = {
            "status": ready_row["status"],
            "refreshed_at": ready_row["created_at"],
            "clob_balance_usd": _round_money(ready_row["clob_balance_usd"]),
            "chain_balance_usd": _round_money(ready_row["chain_balance_usd"]),
            "wallet_balance_freshness_s": report_json.get(
                "wallet_balance_freshness_s"
            ),
            "checks": _readiness_checks(report_json),
        }

    return {
        **op_state,
        "readiness": readiness,
    }


# ---------------------------------------------------------------------------
# C1/C2: latency + sub-minute payload blocks. Both are recomputed at most
# every _AUX_BLOCK_TTL_S (module-level cache) — the SSE loop runs at 1Hz and
# these aggregates don't need tick-level freshness.
# ---------------------------------------------------------------------------

_AUX_BLOCK_TTL_S = 5.0
# Keyed (block_name, id(conn)) so one database's cached blocks are never handed
# to another connection. id(conn) is stable while the connection lives, and the
# 5s TTL makes id reuse irrelevant.
_aux_cache: dict[tuple[str, int], tuple[float, Any]] = {}


def clear_aux_cache() -> None:
    """Test hook: drop the latency/subminute block cache, the KPI
    group-state/extractable TTL cache, AND the history-blocks TTL cache
    (see conftest). The history cache is signature-keyed, but two test DBs
    can coincidentally produce identical signatures — per-test isolation
    must never depend on that not happening."""
    _aux_cache.clear()
    _KPI_GROUP_CACHE.update(conn=None, key=None, expires_at=0.0, value=None)
    _HISTORY_CACHE.update(key=None, expires_at=0.0, value=None)
    _CHAIN_FORENSICS_CACHE.update(conn=None, key=None, value=None)


def _aux_cached(conn: sqlite3.Connection, key: str, build) -> Any:
    now = time.monotonic()
    cache_key = (key, id(conn))
    hit = _aux_cache.get(cache_key)
    if hit is not None and (now - hit[0]) < _AUX_BLOCK_TTL_S:
        return hit[1]
    value = build()
    _aux_cache[cache_key] = (now, value)
    return value


def _pctl(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round(q * (len(sorted_vals) - 1))))
    return float(sorted_vals[idx])


_LATENCY_STAGES = ("spot", "ctx", "gamma", "book", "sign", "post", "order", "total")

# Order-envelope stages whose per-tick trace value is a SUM across every order
# placed that tick (pipeline.py accumulates tick.ms_order_* += per-order ms).
_LATENCY_ORDER_STAGES = ("sign", "post", "order")

# Expected decision-to-fill floor per venue: an order POST in this range is the
# venue's floor, not slowness. Polymarket enforces a ~250ms uncancellable taker
# delay, plus ~50-80ms of network and signing, for a ~300-320ms floor. Sent in
# the payload so the latency page labels from data.
_LATENCY_VENUE_FLOOR_BY_VENUE = {
    "polymarket": {
        "taker_delay_ms": 250,
        "expected_decision_to_fill_ms_lo": 300,
        "expected_decision_to_fill_ms_hi": 320,
    },
}


def _latency_venue_floor(
    measured_post: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """The expected decision-to-fill floor for the deployed venue
    (config.VENUE).

    ``taker_delay_ms`` is the venue's documented delay (250ms on Polymarket),
    not a measurement. ``taker_delay_measured_ms`` adds the p50/p95/max/n of
    the accepted per-order POST round trip in this window (the same
    ``orders.per_order.post`` data as the accept/reject panel).

    An accepted marketable order cannot return before the delay has passed, so
    the measured value is network + signing + delay: an upper bound on the
    delay, not the delay itself. The client says so in the tooltip.

    None when the window has no accepted orders; the client then shows the
    documented value.
    """
    venue = getattr(config, "VENUE", "polymarket")
    out = dict(
        _LATENCY_VENUE_FLOOR_BY_VENUE.get(
            venue, _LATENCY_VENUE_FLOOR_BY_VENUE["polymarket"]
        )
    )
    measured = None
    if measured_post and int(measured_post.get("n", 0) or 0) > 0:
        measured = {
            k: measured_post[k]
            for k in ("p50", "p95", "max", "n")
            if k in measured_post
        }
    out["taker_delay_measured_ms"] = measured
    return out


def _latency_block(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    """Aggregate the stage='latency' pipeline_health rows (compact JSON) into
    per-stage p50/p95/max plus a list of recent ticks.

    The trace's sign/post/order values are per-tick sums over every order
    placed that tick (a tick with two orders reads ~2x one POST), and a reject
    retry is its own order. ``stages`` keeps that tick-level aggregation;
    ``orders.per_order`` is the per-order view: each tick contributes its mean
    (stage_sum / n_orders), weighted by n_orders, which is exact for the common
    one-order tick. Per-order splits within a multi-order tick are not stored,
    so the mean is the best estimate available. Reject/retry counts are in
    v2.reject_stats. ``venue_floor`` carries the expected ~300-320ms
    decision-to-fill floor so the client can label readings near it as normal.
    """
    def build() -> Optional[dict[str, Any]]:
        try:
            rows = conn.execute(
                "SELECT message, created_at FROM pipeline_health "
                "WHERE stage='latency' "
                "AND created_at >= datetime('now', '-30 minutes') "
                "ORDER BY id DESC LIMIT 200",
            ).fetchall()
        except sqlite3.OperationalError:
            return None
        samples: list[dict] = []
        for r in rows:
            try:
                d = json.loads(r["message"] or "{}")
            except (ValueError, TypeError):
                continue
            if isinstance(d, dict):
                d["ts"] = r["created_at"]
                samples.append(d)
        if not samples:
            return {
                "window_min": 30,
                "stages": {}, "recent": [],
                "orders": {
                    "n_orders_total": 0, "n_order_ticks": 0,
                    "n_multi_order_ticks": 0, "per_order": {},
                },
                "venue_floor": _latency_venue_floor(),
            }
        order_samples = [
            s for s in samples if int(s.get("n_orders", 0) or 0) > 0
        ]
        stages: dict[str, dict[str, float]] = {}
        for st in _LATENCY_STAGES:
            # sign/post/order are 0.0 on every tick without an order (most
            # ticks), so percentiles over all ticks would read ~0ms; aggregate
            # those stages over order-bearing ticks only. These are still
            # tick-level sums; the per-order view is the ``orders.per_order``
            # block below.
            src = order_samples if st in _LATENCY_ORDER_STAGES else samples
            vals = sorted(float(s.get(st, 0.0) or 0.0) for s in src)
            stages[st] = {
                "p50": round(_pctl(vals, 0.50), 1),
                "p95": round(_pctl(vals, 0.95), 1),
                "max": round(vals[-1], 1) if vals else 0.0,
                "n": len(vals),
            }
        # Per-order stats: divide each order-bearing tick's stage sum by its
        # n_orders and weight by n_orders.
        #
        # Accepted and rejected orders are aggregated separately. The trace
        # carries ``post`` (accepted orders) and ``post_rej`` (rejects) with
        # ``n_rej``. An accepted order takes ~415ms (it waits out the 250ms
        # taker delay plus matching), a reject ~286ms (a 400 skips matching),
        # and averaging the two describes neither.
        n_orders_total = 0
        n_multi_order_ticks = 0
        n_rej_total = 0
        per_order_vals: dict[str, list[float]] = {
            st: [] for st in _LATENCY_ORDER_STAGES
        }
        post_rej_vals: list[float] = []
        for s in order_samples:
            n = int(s.get("n_orders", 0) or 0)
            if n <= 0:
                continue
            n_rej = max(0, min(n, int(s.get("n_rej", 0) or 0)))
            n_acc = n - n_rej
            n_orders_total += n
            n_rej_total += n_rej
            if n > 1:
                n_multi_order_ticks += 1
            for st in _LATENCY_ORDER_STAGES:
                total = float(s.get(st, 0.0) or 0.0)
                # `post` is accept-only, so it must be divided by the ACCEPT
                # count and represented only by accepted orders. sign/order
                # still span every order on the tick.
                if st == "post":
                    if n_acc <= 0 or total <= 0.0:
                        continue
                    per_order_vals[st].extend([total / n_acc] * n_acc)
                else:
                    per_order_vals[st].extend([total / n] * n)
            rej_total = float(s.get("post_rej", 0.0) or 0.0)
            if n_rej > 0 and rej_total > 0.0:
                post_rej_vals.extend([rej_total / n_rej] * n_rej)
        per_order: dict[str, dict[str, float]] = {}
        for st, vals in per_order_vals.items():
            vals.sort()
            per_order[st] = {
                "p50": round(_pctl(vals, 0.50), 1),
                "p95": round(_pctl(vals, 0.95), 1),
                "max": round(vals[-1], 1) if vals else 0.0,
                "n": len(vals),  # = orders represented, not ticks
            }
        post_rej_vals.sort()
        per_order["post_rej"] = {
            "p50": round(_pctl(post_rej_vals, 0.50), 1),
            "p95": round(_pctl(post_rej_vals, 0.95), 1),
            "max": round(post_rej_vals[-1], 1) if post_rej_vals else 0.0,
            "n": len(post_rej_vals),
        }
        recent = [
            {
                "ts": s.get("ts"),
                **{st: s.get(st, 0.0) for st in _LATENCY_STAGES},
                "n_book": s.get("n_book", 0),
                "n_orders": s.get("n_orders", 0),
                # Absent on rows written by older versions; the page shows "—".
                "pre_post": s.get("pre_post"),
                "tail": s.get("tail"),
                "post_rej": s.get("post_rej"),
                "n_rej": s.get("n_rej"),
                "ws_age": s.get("ws_age"),
                "bk_ws": s.get("bk_ws"),
                "bk_pf": s.get("bk_pf"),
                "bk_rest": s.get("bk_rest"),
            }
            for s in samples[:60]
        ]
        # ── DECISION LATENCY ─────────────────────────────────────────────────
        # Tick start to the first order POST leaving the box: the part the bot
        # controls. Everything after it is the venue's taker delay plus
        # matching, and the ``tail`` (post-POST work) delays no fill. Reported
        # separately so the page does not present the whole tick as
        # decision-to-fill time.
        def _pstats(vals: list[float]) -> dict[str, float]:
            vals.sort()
            return {
                "p50": round(_pctl(vals, 0.50), 1),
                "p95": round(_pctl(vals, 0.95), 1),
                "max": round(vals[-1], 1) if vals else 0.0,
                "n": len(vals),
            }

        decision = {
            "pre_post": _pstats(
                [float(s.get("pre_post", 0.0) or 0.0) for s in order_samples
                 if float(s.get("pre_post", 0.0) or 0.0) > 0.0]
            ),
            "tail": _pstats(
                [float(s.get("tail", 0.0) or 0.0) for s in samples
                 if float(s.get("tail", 0.0) or 0.0) > 0.0]
            ),
        }

        # ── BOOK FRESHNESS ───────────────────────────────────────────────────
        # Staleness is a different axis from latency: a fast tick that priced a
        # seconds-old book is the worse outcome. The venue pushes ~800 msg/s,
        # so a large ws_age points at the consumer, not the feed.
        ws_ages = [
            float(s.get("ws_age", 0.0) or 0.0) for s in samples
            if int(s.get("bk_ws", 0) or 0) > 0
        ]
        n_ws_served = len(ws_ages)
        freshness = {
            "sources": {
                "ws": sum(int(s.get("bk_ws", 0) or 0) for s in samples),
                "prefetch": sum(int(s.get("bk_pf", 0) or 0) for s in samples),
                "rest": sum(int(s.get("bk_rest", 0) or 0) for s in samples),
            },
            "ws_age": _pstats(list(ws_ages)),
            "n_ws_served_ticks": n_ws_served,
            # Share of WS-served ticks whose ladder was already older than the
            # prefetch freshness bound / a full second / ten seconds.
            "ws_over_700ms": sum(1 for a in ws_ages if a > 700.0),
            "ws_over_2s": sum(1 for a in ws_ages if a > 2000.0),
            "ws_over_10s": sum(1 for a in ws_ages if a > 10000.0),
            # ⚠️ The REST prefetch cache carries NO age telemetry, so its
            # freshness is UNMEASURED — the page says so rather than implying
            # the ws_age figure covers every book.
            "prefetch_age_instrumented": False,
            # Stall watchdog counters. ws_age no longer gates anything (on
            # these books a large age usually means a quiet market), so a large
            # age is only meaningful alongside whether the REST cross-check is
            # running and agreeing. In-process counters from this process's
            # scheduler. ``armed=False`` means the watchdog was never built (WS
            # off, shadow mode, or a mirror without the methods).
            "watchdog": _watchdog_counters(),
        }

        return {
            "window_min": 30,
            "stages": stages,
            "recent": recent,
            "orders": {
                "n_orders_total": n_orders_total,
                "n_order_ticks": len(order_samples),
                "n_multi_order_ticks": n_multi_order_ticks,
                "n_rejected": n_rej_total,
                "n_accepted": max(0, n_orders_total - n_rej_total),
                "per_order": per_order,
            },
            "decision": decision,
            "freshness": freshness,
            # ACCEPT-only per-order POST = the measured wall that contains the
            # venue's uncancellable taker delay (see _latency_venue_floor).
            "venue_floor": _latency_venue_floor(per_order.get("post")),
        }

    return _aux_cached(conn, "latency", build)


# Earliest bet time counted in reject/fill accounting when no reset anchor
# applies.
_V2_GO_LIVE_TS = "2026-07-08 13:45:00"


def _record_anchor_ts(floor_ts: str = _V2_GO_LIVE_TS) -> str:
    """Anchor for the v2 record-accounting windows: the LATER of the v2
    go-live instant and the operator record cutover (WALLET_START_AT). An
    operator record reset must re-anchor every dashboard accounting surface —
    reject/fill tax, recent bets, missed-PnL rollup, cash peak — not just the
    ledger views, or the "fresh" record quotes pre-reset telemetry.
    Lexicographic max relies on _wallet_start_sql normalizing the cutover to
    SQL-text UTC (%Y-%m-%d %H:%M:%S); a malformed operator value falls through
    near-verbatim there and can over-anchor — the env example documents the
    expected format."""
    cutover = _wallet_start_sql()
    return max(floor_ts, cutover) if cutover else floor_ts


def _reject_side_dir(side: Any) -> Optional[str]:
    """Map a reject row's stored side (YES/NO or UP/DOWN) to the UP/DOWN
    settlement axis so it can be scored against ``resolved_outcome``."""
    s = str(side or "").upper()
    if s in ("YES", "UP"):
        return "UP"
    if s in ("NO", "DOWN"):
        return "DOWN"
    return None


def _sleeve_interval_tag(sleeve: Any) -> Optional[str]:
    """Interval ("5m"/"15m") from a sub-minute sleeve label (subminute_5m,
    ...).

    Used to key a resolution across sleeves and attempts: retries (or a second
    sleeve on the same interval) hit the same (coin, interval, window, side)
    resolution, and keying on the interval rather than the sleeve label counts
    a missed resolution once, not once per attempt. The interval stays in the
    key so a 5m and a 15m window starting at the same second stay separate.
    """
    m = re.search(r"(\d+)m", str(sleeve or ""))
    return f"{m.group(1)}m" if m else None


def _reject_missed_pnl(
    side: Any,
    resolved_outcome: Any,
    limit_price: Any,
    requested_usd: Any,
    theta: float,
) -> Optional[float]:
    """Would-be PnL of a rejected order had it filled at ``limit_price``.

    Wins pay ``requested*(1/p − 1 − θ·p·(1−p))``; losses forfeit the stake
    (``−requested``); unresolved (or unscoreable) rejects return None.
    """
    if resolved_outcome is None:
        return None
    req = _float_or_none(requested_usd)
    if req is None:
        return None
    direction = _reject_side_dir(side)
    resolved = str(resolved_outcome or "").upper()
    if direction is None or resolved not in ("UP", "DOWN"):
        return None
    if direction == resolved:
        p = _float_or_none(limit_price)
        if p is None or p <= 0.0:
            return None
        return req * (1.0 / p - 1.0 - theta * p * (1.0 - p))
    return -req


# ---------------------------------------------------------------------------
# The cause of a miss is the on-chain verdict and nothing else.
#
# Re-reading the book a second after a reject cannot tell an ask that traded
# away from one that was pulled, and the trade tape showed most misses were
# faster takers, not maker cancels. So ``miss_forensics`` verdicts are the only
# source of why a trade was missed.
#
# Kept alongside, as facts rather than causes:
#
#  * ``requote_ask`` / ``requote_drift``: how far the book moved after the
#    reject, shown as a price metric.
#  * MISS KIND (below): facts about our own order from our own rows
#    (ghost_fill / venue_error / book_miss). They say whether the book was
#    contested at all, i.e. which misses the chain job can classify (it
#    handles the ``no_liquidity`` rejects with no accepted fill on the
#    resolution, which are ``book_miss``).
# ---------------------------------------------------------------------------

#: A missed resolution's KIND — what OUR side did, never what the book did.
#:   ghost_fill  — the venue ACCEPTED an order for this resolution but filled
#:                 $0 (subminute accepted with no filled_usd, or a ledger
#:                 GHOST_FILL row): phantom liquidity.
#:   venue_error — every FAILED live_order_attempts row carries a NON-
#:                 no_liquidity error_kind (auth / rate / size / outage /
#:                 network): our order never reached a contest.
#:   book_miss   — the ordinary case: we asked for liquidity at our limit and
#:                 did not get it. THE cohort the chain verdict adjudicates.
MISS_KIND_GHOST = "ghost_fill"
MISS_KIND_VENUE_ERROR = "venue_error"
MISS_KIND_BOOK = "book_miss"


def _miss_res_key(
    coin: Any, sleeve: Any, window_start_ts: Any, side: Any
) -> Optional[tuple]:
    """RESOLUTION key (coin, interval-tag, window_start_ts, direction) shared
    by the reject rows, the fill/ghost context and the error-kind context so
    every surface folds repeat attempts (and YES/UP label drift) the same way.
    None when the row cannot be identified to a resolution."""
    direction = _reject_side_dir(side)
    if direction is None or window_start_ts is None:
        return None
    return (coin, _sleeve_interval_tag(sleeve), int(window_start_ts), direction)


# ── on-chain miss forensics (miss_forensics table) ─────────────────────── The
# verdicts are written by the miss_forensics scheduler job
# (scheduler/miss_forensics.py); this only reads them. They come from the trade
# tape, so FASTER_TAKER vs MAKER_CANCEL is evidence, not inference. Strongest
# evidence first. This one tuple sets both the per-resolution precedence (one
# print at our level proves the level was live, which outranks "nothing
# printed" from other attempts) and the display order. Names are imported from
# the producer, so a renamed verdict fails at import.
_CHAIN_VERDICTS = (
    VERDICT_FASTER_TAKER,
    VERDICT_REPRICED_ONLY,
    VERDICT_MAKER_CANCEL,
    VERDICT_PENDING,
    VERDICT_UNRESOLVED,
)
_CHAIN_VERDICT_RANK = {v: i for i, v in enumerate(_CHAIN_VERDICTS)}
#: Not readings at all: a retryable row has no evidence YET, UNRESOLVED never
#: will. Excluded from the classified share. Derived from the producer's own
#: retry set so adding a retryable verdict cannot leave it counted as classified.
_CHAIN_UNCLASSIFIED = (*RETRYABLE_VERDICTS, VERDICT_UNRESOLVED)
#: WINNABLE = a faster order could have taken this trade — the tape proves the
#: level was live and someone else matched it. Everything else that IS a reading
#: was unwinnable at any speed: REPRICED_ONLY never traded at our level,
#: MAKER_CANCEL never traded at all. Declared here (from the producer's names)
#: so the money split, the row flag and the panel copy all read one definition.
_CHAIN_WINNABLE = frozenset({VERDICT_FASTER_TAKER})
_CHAIN_NEVER_WINNABLE = frozenset({VERDICT_REPRICED_ONLY, VERDICT_MAKER_CANCEL})
_CHAIN_STRIP_WINDOW_S = 7 * 86400


def _order_by_verdict(d: dict[str, Any]) -> dict[str, Any]:
    """Re-key a verdict map into the canonical strongest-evidence-first order.

    Every verdict map in the payload goes through here so the dashboard can
    iterate the payload's OWN key order instead of keeping a second copy of the
    vocabulary — the reason a verdict renamed or added in ``miss_forensics``
    cannot silently vanish from a panel. An unknown verdict is APPENDED (sorted)
    rather than dropped, so it shows up as an undecorated chip instead of
    quietly leaving the counts short."""
    known = [k for k in _CHAIN_VERDICTS if k in d]
    extra = sorted(k for k in d if k not in _CHAIN_VERDICT_RANK)
    return {k: d[k] for k in (*known, *extra)}


def _chain_forensics_limit() -> int:
    """Row bound for the verdict join — the reject-list depth, doubled.

    Must comfortably exceed a WEEK of rejects so the 7-day strip window below —
    not this LIMIT — bounds the aggregate. Rejects run ~58/day (48 in the 20h
    after the 5m re-enable), i.e. ~400/week. Tied to ``V2_REJECTS_LIMIT`` (the
    same idiom the reject/fill context queries use) so raising the reject list
    cannot leave the verdict join silently truncating the counts the panel
    exists to show.
    """
    return 2 * int(config.V2_REJECTS_LIMIT)


def _chain_forensics_rows(conn: sqlite3.Connection, anchor: str) -> list:
    """Chain verdicts joined back to their reject, newest-first, anchor-bounded.

    Deliberately NOT joined to ``reject_requotes`` for the settled outcome: a
    resolution with several requote rows would multiply the verdict row and
    silently double-weight that miss in the speed-prize WR. The caller
    (``_reject_block``) already groups those rows per resolution and hands the
    outcome in as a map.
    """
    try:
        rows = conn.execute(
            "SELECT m.fill_id, m.verdict, m.beat_dt_s, m.beat_price, "
            "       m.beat_size, m.rival_addr, "
            "       f.created_at, f.coin, f.sleeve, f.window_start_ts, "
            "       f.side, f.offset_sec, f.limit_price "
            "FROM miss_forensics m "
            "JOIN subminute_fills f ON f.id = m.fill_id "
            "ORDER BY m.fill_id DESC LIMIT ?",
            (_chain_forensics_limit(),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    return [r for r in rows if str(r["created_at"] or "") >= anchor]


def _chain_forensics_cached(
    conn: sqlite3.Connection, anchor: str
) -> tuple[list, dict[tuple, dict[str, Any]], dict[tuple, dict[str, Any]]]:
    """``(rows, by_attempt, by_resolution)``, rebuilt only when the table
    changed.

    The producer runs every ``MISS_FORENSICS_SECONDS`` (300s) while the
    snapshot is rebuilt every few seconds, so most rebuilds would repeat the
    same join and indexing (~113ms). The cache key is a cheap ``(COUNT,
    MAX(fill_id), MAX(checked_at))`` probe. ``_upsert`` always updates
    ``checked_at``, so a reclassification moves the probe; only two upserts of
    the same fill_id within one second are missed, costing one stale refresh.
    """
    try:
        probe: Optional[tuple] = conn.execute(
            "SELECT COUNT(*), MAX(fill_id), MAX(checked_at) FROM miss_forensics"
        ).fetchone()
        probe = tuple(probe) if probe is not None else None
    except sqlite3.OperationalError:
        probe = None
    key = (anchor, probe)
    cache = _CHAIN_FORENSICS_CACHE
    if cache["value"] is not None and cache["conn"] is conn and cache["key"] == key:
        return cache["value"]
    rows = _chain_forensics_rows(conn, anchor)
    value = (rows, _chain_attempt_index(rows), _chain_resolution_index(rows))
    cache["conn"] = conn
    cache["key"] = key
    cache["value"] = value
    return value


def _chain_verdict_payload(row: Any) -> dict[str, Any]:
    """The per-miss chip's fields (`FASTER TAKER −1s @0.750 ×21 by 0xab…12`)."""
    rival = str(row["rival_addr"] or "") or None
    return {
        "chain_verdict": str(row["verdict"] or "") or None,
        "chain_beat_dt_s": _float_or_none(row["beat_dt_s"]),
        "chain_beat_price": _round_prob(_float_or_none(row["beat_price"])),
        "chain_beat_size": _round_size(_float_or_none(row["beat_size"])),
        "chain_rival_addr": rival,
    }


def _chain_attempt_index(rows: list) -> dict[tuple, dict[str, Any]]:
    """(coin, sleeve, window_start_ts, side, offset_sec) -> verdict payload.

    Keyed per ATTEMPT (offset included) because `subminute_fills` and
    `reject_requotes` carry the identical 5-tuple for the same order, so the
    drill-down row and its verdict can never be mismatched."""
    idx: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        if r["window_start_ts"] is None:
            continue
        key = (
            r["coin"], r["sleeve"], int(r["window_start_ts"]), r["side"],
            r["offset_sec"],
        )
        idx.setdefault(key, _chain_verdict_payload(r))
    return idx


def _chain_resolution_index(rows: list) -> dict[tuple, dict[str, Any]]:
    """RESOLUTION-level verdict (strongest evidence wins — see the rank map)."""
    idx: dict[tuple, tuple[int, dict[str, Any]]] = {}
    for r in rows:
        key = _miss_res_key(
            r["coin"], r["sleeve"], r["window_start_ts"], r["side"]
        )
        if key is None:
            continue
        rank = _CHAIN_VERDICT_RANK.get(str(r["verdict"] or ""), 9)
        cur = idx.get(key)
        if cur is None or rank < cur[0]:
            idx[key] = (rank, _chain_verdict_payload(r))
    return {k: v[1] for k, v in idx.items()}


def _chain_forensics_stats(
    rows: list,
    *,
    now_ts: float,
    resolved_by_res: dict[tuple, Any],
) -> Optional[dict[str, Any]]:
    """The summary strip: 7-day verdict counts, median beat time, and the speed
    prize.

    ``resolved_by_res`` maps ``_miss_res_key`` -> settled UP/DOWN; the caller
    already groups rejects per resolution to build it.

    The speed prize prices the upside of investing in latency. Over the
    FASTER_TAKER misses (the ones lost on speed, so the only ones a faster bot
    could have won), compare the would-be win rate with the break-even rate
    implied by our average limit price. If they are about equal, winning those
    races gains nothing and speed is not worth chasing.
    """
    if not rows:
        return None
    cutoff = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.gmtime(now_ts - _CHAIN_STRIP_WINDOW_S)
    )
    recent = [r for r in rows if str(r["created_at"] or "") >= cutoff]
    if not recent:
        return None
    counts: dict[str, int] = {}
    beat_dts: list[float] = []
    wins = 0
    limits: list[float] = []
    for r in recent:
        verdict = str(r["verdict"] or "")
        counts[verdict] = counts.get(verdict, 0) + 1
        if verdict != VERDICT_FASTER_TAKER:
            continue
        dt = _float_or_none(r["beat_dt_s"])
        if dt is not None:
            beat_dts.append(dt)
        direction = _reject_side_dir(r["side"])
        res_key = _miss_res_key(
            r["coin"], r["sleeve"], r["window_start_ts"], r["side"]
        )
        outcome = str(resolved_by_res.get(res_key) or "").upper()
        lp = _float_or_none(r["limit_price"])
        if direction is None or outcome not in ("UP", "DOWN") or lp is None:
            continue
        limits.append(lp)
        if direction == outcome:
            wins += 1
    n = len(recent)
    classified = sum(
        c for k, c in counts.items() if k not in _CHAIN_UNCLASSIFIED
    )
    # One graded resolution == one recorded limit, so len(limits) IS the resolved
    # count — a second counter could only ever disagree by being wrong.
    resolved = len(limits)
    # Under 3 resolutions a WR is noise, not a measurement — the panel renders
    # "< 3 resolved" rather than a number, same bar as reject_winrate.
    would_win_wr = (wins / resolved) if resolved >= 3 else None
    breakeven_wr = (sum(limits) / resolved) if limits else None
    return {
        "n": n,
        "n_classified": classified,
        "window_days": int(_CHAIN_STRIP_WINDOW_S // 86400),
        "counts": _order_by_verdict({k: v for k, v in counts.items() if v}),
        "share_faster_taker": (
            _round_prob(counts.get(VERDICT_FASTER_TAKER, 0) / classified)
            if classified else None
        ),
        "median_beat_dt_s": (
            round(statistics.median(beat_dts), 1) if beat_dts else None
        ),
        "speed_prize": {
            "n_resolved": resolved,
            "would_win_wr": _round_prob(would_win_wr),
            "breakeven_wr": _round_prob(breakeven_wr),
            "edge_pts": (
                _round_prob(would_win_wr - breakeven_wr)
                if would_win_wr is not None and breakeven_wr is not None
                else None
            ),
        },
    }


def _ledger_interval_tag(interval_min: Any, sleeve: Any) -> Optional[str]:
    """Interval bucket for a ledger row — interval_min when present, else the
    sleeve label (same derivation the client uses)."""
    if interval_min is not None:
        try:
            return f"{int(interval_min)}m"
        except (TypeError, ValueError):
            pass
    return _sleeve_interval_tag(sleeve)


def _reject_context_maps(
    conn: sqlite3.Connection, anchor: str
) -> tuple[set, set, dict]:
    """(filled_keys, ghost_keys, error_kinds_by_key) for the record window.

    filled_keys: resolutions with a real fill (an accepted sub-minute fill with
      filled_usd > 0, or a ledger row whose live_filled_size > 0). Their rejects
      were retries of a bet that completed, so they are never misses.
    ghost_keys: resolutions whose only accepted order filled $0.
    error_kinds_by_key: FAILED live_order_attempts error_kinds per resolution
      (for the venue_error miss kind).

    Every query is bounded (by the created_at index, or newest-N by id and then
    filtered); a missing table gives empty maps.
    """
    filled: set = set()
    ghost: set = set()
    try:
        for r in conn.execute(
            "SELECT coin, sleeve, window_start_ts, side, "
            "       COALESCE(filled_usd, 0.0) AS f "
            "FROM subminute_fills "
            "WHERE created_at >= ? AND outcome = 'accepted'",
            (anchor,),
        ):
            key = _miss_res_key(r["coin"], r["sleeve"], r["window_start_ts"], r["side"])
            if key is None:
                continue
            (filled if float(r["f"] or 0.0) > 0.0 else ghost).add(key)
    except sqlite3.OperationalError:
        pass
    try:
        rows = conn.execute(
            "SELECT bet_ts, coin, sleeve, interval_min, window_start_ts, "
            "       side, outcome, live_filled_size "
            "FROM ledger WHERE offset_sec IS NOT NULL "
            "ORDER BY id DESC LIMIT ?",
            (int(config.V2_BETS_LIMIT),),
        ).fetchall()
        for r in rows:
            if str(r["bet_ts"] or "") < anchor:
                continue
            direction = _reject_side_dir(r["side"])
            if direction is None or r["window_start_ts"] is None:
                continue
            key = (
                r["coin"],
                _ledger_interval_tag(r["interval_min"], r["sleeve"]),
                int(r["window_start_ts"]),
                direction,
            )
            is_ghost = str(r["outcome"] or "").upper() == "GHOST_FILL"
            lf = _float_or_none(r["live_filled_size"])
            if is_ghost:
                ghost.add(key)
            elif lf is not None and lf > 0.0:
                filled.add(key)
    except sqlite3.OperationalError:
        pass
    ghost -= filled  # a later real fill wins over an earlier phantom accept
    err: dict = {}
    try:
        rows = conn.execute(
            "SELECT created_at, coin, sleeve, window_start_ts, side, error_kind "
            "FROM live_order_attempts WHERE status = 'FAILED' "
            "ORDER BY id DESC LIMIT ?",
            (2 * int(config.V2_REJECTS_LIMIT),),
        ).fetchall()
        for r in rows:
            if str(r["created_at"] or "") < anchor or r["error_kind"] is None:
                continue
            key = _miss_res_key(r["coin"], r["sleeve"], r["window_start_ts"], r["side"])
            if key is None:
                continue
            err.setdefault(key, set()).add(str(r["error_kind"]))
    except sqlite3.OperationalError:
        pass
    return filled, ghost, err


def _miss_kind(*, is_ghost: bool, error_kinds: set) -> str:
    """A missed resolution's KIND — read off OUR rows, never off the book.

    ghost_fill (an accept exists, $0 behind it — the operator's named
    phenomenon) → venue_error (every failed attempt errored for a reason that
    is not no_liquidity, so our order never contended) → book_miss (the
    ordinary case, and the only kind the chain job adjudicates).

    NOT a cause: it says whether the book was contested, not what the book
    did. The cause is the on-chain verdict."""
    if is_ghost:
        return MISS_KIND_GHOST
    if error_kinds and "no_liquidity" not in error_kinds:
        return MISS_KIND_VENUE_ERROR
    return MISS_KIND_BOOK


def _reject_block(
    conn: sqlite3.Connection, theta: float, anchor: str,
) -> dict[str, Any]:
    """The "fill tax": recent rejected FAK orders (priced against what they
    would have won), a per-resolution missed-trades rollup, and aggregate
    reject/fill stats, all limited to the record window. ``anchor`` is
    ``_record_anchor_ts()`` computed once by _v2_block, so every window on
    the Subminute tab uses the same start. Every query has a LIMIT or uses
    the ``created_at`` index; empty tables give zeros/None.

    Missed trades:

      * One per resolution (coin, interval, window_start_ts, direction): several
        FAK retries are one miss, and a resolution that eventually filled is not
        a miss at all.
      * The representative attempt is the one with the largest requested size,
        and every miss is priced at our limit.
      * The cause is the chain verdict (``chain_verdict`` per row,
        ``chain_counts`` / ``chain_pnl`` in the stats). ``miss_kind`` is a fact
        about our order, not a cause (see above).
    """
    raw_rows: list = []
    try:
        # Newest-N by id (early-stops after LIMIT rows), then drop pre-anchor
        # rows in Python. Filtering in SQL would defeat the LIMIT's early
        # termination: right after a record reset few post-anchor rows exist,
        # so an id-DESC scan could not stop and re-walked the whole table on
        # every cache refresh. The anchor only ever trims the OLD prefix of
        # history, so newest-N-then-filter is exactly "newest post-anchor
        # rows, at most N". ONE fetch feeds BOTH the per-attempt display list
        # and the missed-trades rollup, so the list and the tiles can never
        # quote different windows.
        raw_rows = conn.execute(
            "SELECT id, created_at, coin, sleeve, offset_sec, "
            "       window_start_ts, side, limit_price, requested_usd, "
            "       requote_ask, resolved_outcome "
            "FROM reject_requotes ORDER BY id DESC LIMIT ?",
            (int(config.V2_REJECTS_LIMIT),),
        ).fetchall()
        raw_rows = [r for r in raw_rows if str(r["created_at"] or "") >= anchor]
    except sqlite3.OperationalError:
        raw_rows = []

    # On-chain verdicts, read once and indexed per attempt (drill-down chip)
    # and per resolution (grouped row). Empty before the job's first run, in
    # which case every lookup misses.
    chain_rows, chain_by_attempt, chain_by_resolution = (
        _chain_forensics_cached(conn, anchor) if raw_rows else ([], {}, {})
    )

    rejects: list[dict[str, Any]] = []
    for r in raw_rows:
        limit_price = _float_or_none(r["limit_price"])
        requote_ask = _float_or_none(r["requote_ask"])
        drift = (
            requote_ask - limit_price
            if requote_ask is not None and limit_price is not None
            else None
        )
        chain = chain_by_attempt.get((
            r["coin"], r["sleeve"],
            int(r["window_start_ts"]) if r["window_start_ts"] is not None else None,
            r["side"], r["offset_sec"],
        )) or {}
        rejects.append({
            **chain,
            "time": r["created_at"],
            "coin": r["coin"],
            "sleeve": r["sleeve"],
            "offset_sec": r["offset_sec"],
            "window_start_ts": r["window_start_ts"],
            "side": r["side"],
            "limit_price": _round_prob(limit_price),
            "requested_usd": _round_money(r["requested_usd"]),
            "requote_ask": _round_prob(requote_ask),
            "requote_drift": _round_prob(drift),
            "resolved_outcome": r["resolved_outcome"],
            "missed_pnl": _round_money(
                _reject_missed_pnl(
                    r["side"], r["resolved_outcome"], limit_price,
                    r["requested_usd"], theta,
                )
            ),
        })

    n_rejected = n_accepted = 0
    fill_rate_usd: Optional[float] = None
    try:
        fr = conn.execute(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN outcome='rejected' THEN 1 ELSE 0 END), 0) AS n_rej, "
            "  COALESCE(SUM(CASE WHEN outcome='accepted' THEN 1 ELSE 0 END), 0) AS n_acc, "
            "  COALESCE(SUM(filled_usd), 0.0) AS sum_filled, "
            "  COALESCE(SUM(requested_usd), 0.0) AS sum_requested "
            "FROM subminute_fills WHERE created_at >= ?",
            (anchor,),
        ).fetchone()
        if fr is not None:
            n_rejected = int(fr["n_rej"] or 0)
            n_accepted = int(fr["n_acc"] or 0)
            sum_requested = float(fr["sum_requested"] or 0.0)
            if sum_requested > 0.0:
                fill_rate_usd = float(fr["sum_filled"] or 0.0) / sum_requested
    except sqlite3.OperationalError:
        pass

    # ── missed trades: one row per resolution ──────────────────────────────
    # Retries are attempts at the same position, not separate stakes, so
    # collapse to one counterfactual per resolution. The key uses the interval
    # from the sleeve rather than the sleeve label, so every attempt on the
    # same resolution folds together. Rows without a window_start_ts are keyed
    # on id so they stay separate.
    filled_keys, ghost_keys, err_kinds = (
        _reject_context_maps(conn, anchor) if raw_rows else (set(), set(), {})
    )
    groups: dict[Any, list] = {}
    # Settled outcome per RESOLUTION, for the chain strip's speed prize. Built
    # here rather than re-read per verdict row: these rows already carry it, and
    # a JOIN back from miss_forensics would multiply a verdict whenever a
    # resolution logged several requotes (double-weighting that miss in the WR).
    resolved_by_res: dict[tuple, Any] = {}
    for r in raw_rows:
        key = _miss_res_key(r["coin"], r["sleeve"], r["window_start_ts"], r["side"])
        if key is not None and r["resolved_outcome"] is not None:
            resolved_by_res.setdefault(key, r["resolved_outcome"])
        if key is None:
            key = ("_ungrouped_", r["id"])
        groups.setdefault(key, []).append(r)

    missed_trades: list[dict[str, Any]] = []
    n_resolved = n_won = 0
    n_filled_after_reject = 0
    missed_pnl_total = 0.0
    # Per-RESOLUTION chain rollup. Deliberately separate from the strip's
    # `counts` (which is per ATTEMPT over a fixed 7d window): these are the
    # misses the table lists, so a count here always equals a row there.
    chain_counts: dict[str, int] = {}
    chain_pnl: dict[str, float] = {}
    # The money split must PARTITION the missed dollars, so the cohort with no
    # verdict row carries its own total: the table's last row would otherwise
    # print a `—` where every other row prints dollars, and those dollars would
    # appear in no surface at all.
    winnable_pnl = never_winnable_pnl = no_verdict_pnl = 0.0
    # Why a miss has NO verdict, by kind. The chain job only adjudicates
    # `no_liquidity` rejects with no accept on the resolution, so ghost/venue
    # misses can never get one — the panel says so instead of leaving a blank.
    no_verdict_kinds: dict[str, int] = {}
    for key, rows in groups.items():
        if key in filled_keys:
            # The bet COMPLETED (first-fill-wins) — its rejects are retries,
            # not a missed trade. Counted so the panel can say so.
            n_filled_after_reject += 1
            continue
        kind = _miss_kind(
            is_ghost=key in ghost_keys,
            error_kinds=err_kinds.get(key, set()),
        )
        # Representative attempt: the one carrying the full intended size (max
        # requested_usd), so the counterfactual is priced at the limit we meant
        # to buy — one rule for every miss, no cause-conditional pricing.
        rep = max(rows, key=lambda r: _float_or_none(r["requested_usd"]) or 0.0)
        direction = _reject_side_dir(rep["side"])
        resolved = str(rep["resolved_outcome"] or "").upper()
        graded = resolved in ("UP", "DOWN") and direction is not None
        won = (direction == resolved) if graded else None
        missed = _reject_missed_pnl(
            rep["side"], rep["resolved_outcome"], rep["limit_price"],
            rep["requested_usd"], theta,
        )
        if graded:
            n_resolved += 1
            if won:
                n_won += 1
        chain = chain_by_resolution.get(key) or {}
        verdict = chain.get("chain_verdict")
        if verdict:
            chain_counts[verdict] = chain_counts.get(verdict, 0) + 1
        else:
            no_verdict_kinds[kind] = no_verdict_kinds.get(kind, 0) + 1
        if missed is not None:
            missed_pnl_total += missed
            if verdict:
                chain_pnl[verdict] = chain_pnl.get(verdict, 0.0) + missed
                if verdict in _CHAIN_WINNABLE:
                    winnable_pnl += missed
                elif verdict in _CHAIN_NEVER_WINNABLE:
                    never_winnable_pnl += missed
            else:
                no_verdict_pnl += missed
        newest = max(rows, key=lambda r: str(r["created_at"] or ""))
        missed_trades.append({
            # Chain verdict for the whole resolution (strongest evidence wins);
            # absent keys leave the row exactly as it was pre-forensics.
            **chain,
            "time": newest["created_at"],
            "coin": rep["coin"],
            "interval": _sleeve_interval_tag(rep["sleeve"]),
            "window_start_ts": rep["window_start_ts"],
            "side": rep["side"],
            "attempts": len(rows),
            # What OUR order did (never what the book did) — see _miss_kind.
            "miss_kind": kind,
            # True only when the TAPE proves a faster order would have won this
            # one; False when it proves nothing would have; None = no verdict.
            "chain_winnable": (
                None if not verdict
                else True if verdict in _CHAIN_WINNABLE
                else False if verdict in _CHAIN_NEVER_WINNABLE
                else None
            ),
            "limit_price": _round_prob(_float_or_none(rep["limit_price"])),
            # PRICE metric, not a cause: the post-reject ask (how far the book
            # moved). Carried for payload inspection only — NOT what feeds the
            # dashboard's drift column (that reads `rejects[].requote_drift`) nor
            # the lag calibration (which reads the reject_requotes table).
            "requote_ask": _round_prob(_float_or_none(rep["requote_ask"])),
            "requested_usd": _round_money(rep["requested_usd"]),
            "resolved_outcome": rep["resolved_outcome"],
            "missed_pnl": _round_money(missed),
        })
    missed_trades.sort(key=lambda t: str(t["time"] or ""), reverse=True)

    reject_winrate = (n_won / n_resolved) if n_resolved >= 3 else None
    n_missed = len(missed_trades)
    return {
        "rejects": rejects,
        "missed_trades": missed_trades,
        "reject_stats": {
            # ATTEMPT counts (every FAK retry is its own order) — labeled as
            # such by the UI; the resolution counts below are the trades.
            "n_rejected": n_rejected,
            "n_accepted": n_accepted,
            "fill_rate_usd": _round_prob(fill_rate_usd),
            # Per-RESOLUTION missed rollup (no-fill resolutions only), priced
            # at OUR limit for every miss.
            "missed_pnl_total": _round_money(missed_pnl_total) if n_resolved else 0.0,
            "reject_winrate": _round_prob(reject_winrate),
            "n_missed_resolutions": n_missed,
            "n_resolved_misses": n_resolved,
            "n_reject_resolutions_filled": n_filled_after_reject,
            # CAUSE — the on-chain verdict, per missed RESOLUTION in this same
            # window, and the money split that follows from it: winnable = the
            # tape proves a faster order would have taken it; never-winnable =
            # the tape proves no speed could have.
            "chain_counts": _order_by_verdict(chain_counts),
            "chain_pnl": _order_by_verdict(
                {k: _round_money(v) for k, v in chain_pnl.items()}
            ),
            "missed_pnl_winnable": _round_money(winnable_pnl),
            "missed_pnl_never_winnable": _round_money(never_winnable_pnl),
            # The misses with NO verdict row (never adjudicable, or not reached
            # yet). Completes the partition: winnable + never-winnable +
            # no-verdict + the claimed-neither-way verdicts (PENDING /
            # UNRESOLVED, which keep their own `chain_pnl` rows) == the total.
            "missed_pnl_no_verdict": _round_money(no_verdict_pnl),
            "n_no_chain_verdict": sum(no_verdict_kinds.values()),
            "no_chain_verdict_kinds": no_verdict_kinds,
            # ON-CHAIN aggregate strip: 7d verdict counts, median beat-dt, and
            # the speed prize (the metric that decides whether latency
            # investment is ever worth it). None until the job has classified
            # something — the panel then renders nothing at all.
            "chain_forensics": _chain_forensics_stats(
                chain_rows, now_ts=time.time(), resolved_by_res=resolved_by_res,
            ),
        },
    }


def _v2_fill_rate_24h(conn: sqlite3.Connection) -> dict[str, Any]:
    """Rolling 24h fill rate (the Subminute tab's top-right KPI).

    Same definition as the breaker (fill_rate_breaker.evaluate_breaker):
    filled_usd / requested_usd summed over ``subminute_fills`` rows whose
    outcome is not 'error'. Only the window differs: orders created in the last
    24h instead of the newest ``SUBMINUTE_BREAKER_WINDOW`` orders.

    No orders in 24h (or zero requested) gives ``fill_rate_24h`` None and the
    client shows '—'. A missing table or column also gives None rather than
    breaking the payload.
    """
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(requested_usd), 0.0) AS req, "
            "       COALESCE(SUM(filled_usd), 0.0) AS fil, "
            "       COUNT(*) AS n "
            "FROM subminute_fills "
            "WHERE outcome != 'error' "
            "  AND created_at >= datetime('now', '-24 hours')"
        ).fetchone()
    except sqlite3.Error:
        logger.warning("v2 24h fill-rate query failed", exc_info=True)
        return {"fill_rate_24h": None, "n_orders_24h": None}
    n = int(row["n"] or 0)
    req = float(row["req"] or 0.0)
    fil = float(row["fil"] or 0.0)
    return {
        "fill_rate_24h": _round_prob(fil / req) if (n and req > 0) else None,
        "n_orders_24h": n,
    }


def _v2_block(
    conn: sqlite3.Connection,
    portfolio: V6Portfolio,
    kpis: dict[str, Any],
    *,
    book_mark_get: Optional[Callable[..., Any]] = None,
) -> Optional[dict[str, Any]]:
    """Sub-minute console block.

    Carries the decision spec, the sizing state (base fractions, the drawdown
    brake ``max(floor, 1 - dd/scale)`` (1.0 at the default floor) and the hard
    halt), the fill-rate breaker, and recent per-bet rows (pred_edge, stake)
    from the sub-minute ledger. None when sub-minute trading is disabled (the
    page shows a hint).

    ``equity_at_decision`` is not stored per bet, so the per-row field is None
    and the current sizing anchor (equity, peak) is in the sizing state
    instead.
    """
    if not config.SUBMINUTE_ENABLED:
        return None

    # ONE record anchor for every window on this tab (bets, rejects, fill
    # tax, cash peak) — computed once so no surface can anchor differently.
    record_anchor = _record_anchor_ts()
    consts = _v2_sizing_constants()
    kd = kpis if isinstance(kpis, dict) else {}
    _cfg_halt = float(getattr(config, "MAX_DRAWDOWN_HALT_FRAC", 0.30) or 0.30)
    halt_frac = float(
        getattr(portfolio, "max_drawdown_halt_frac", _cfg_halt) or _cfg_halt
    )
    dd_pct = float(kd.get("drawdown_pct_peak") or 0.0)
    dd_frac = dd_pct / 100.0
    scale = consts["brake_scale"] or DEFAULT_BRAKE_SCALE
    floor = consts["brake_floor"]
    brake = max(floor, 1.0 - dd_frac / scale)
    # Plain-language brake description: full size at 0% drawdown, reaching the
    # floor at dd = scale * (1 - floor), e.g. floor 0.50 / scale 0.60 is half
    # size from 30% drawdown. At floor 1.0 this is 0.0 and the brake is always
    # 1.0, so the dashboard treats floor >= 1.0 as "throttle off", not a
    # warning.
    dd_at_floor = scale * (1.0 - floor)
    # Equity: the same per-tick live CLOB balance the Overview headline and
    # live sizing use (kpis.account_capital_live), so the two tabs always
    # agree. Peak is the higher of that balance and the historical wallet peak.
    # Falls back to the ledger series only when there is no live cash basis yet
    # (DRY, unreconciled). equity_basis says which: balance_cache |
    # readiness_report | reconciliation_sample | ledger_fallback.
    live_cap = kd.get("account_capital_live")
    live_cap_source = kd.get("account_capital_live_source")
    if live_cap is not None:
        equity_val: Optional[float] = _round_money(live_cap)
        try:
            # Cash peak scoped to the SAME record anchor as the rest of the
            # tab: an operator record reset must also reset this high-water
            # mark, or the Subminute tab would keep quoting the pre-reset peak.
            _pk = conn.execute(
                "SELECT MAX(wallet_balance) AS mx FROM bankroll_peak "
                "WHERE sampled_at >= ?",
                (record_anchor,),
            ).fetchone()
            _peak_raw = float(_pk["mx"]) if _pk and _pk["mx"] is not None else None
        except Exception:  # noqa: BLE001 — telemetry table optional
            _peak_raw = None
        peak_val = (
            _round_money(max(float(live_cap), _peak_raw))
            if _peak_raw is not None else equity_val
        )
        equity_basis = live_cap_source or "reconciliation_sample"
    else:
        equity_val = kd.get("realized_capital")
        peak_val = kd.get("peak_capital")
        equity_basis = "ledger_fallback"
    sizing = {
        **consts,
        "hard_halt_frac": halt_frac,
        "dd_pct_peak": round(dd_pct, 2),
        "dd_frac": round(dd_frac, 4),
        "brake_multiplier": round(brake, 4),
        "brake_at_floor": brake <= floor + 1e-9,
        # Throttle plain-language line inputs (floor as a fraction of size, and
        # the DD at which the throttle bottoms out).
        "throttle_floor_frac": round(floor, 4),
        "throttle_dd_at_floor": round(dd_at_floor, 4),
        # Real CLOB wallet balance + peak — the series sizing/brake now use.
        "equity": equity_val,
        "peak_capital": peak_val,
        "equity_basis": equity_basis,
        # How much of that equity is operator-deposited funding rather than
        # P&L — the equity tile's sub-line. Same value as kpis.net_deposits.
        "net_deposits": kd.get("net_deposits"),
        # The ledger paper series, kept separate and labelled so the tab never
        # shows two unlabelled equity figures.
        "ledger_paper_equity": kd.get("realized_capital"),
        "ledger_paper_peak": kd.get("peak_capital"),
        "trading_halted": bool(kd.get("trading_halted")),
    }
    theta = consts["poly_fee_theta"]

    def build() -> dict[str, Any]:
        out: dict[str, Any] = {"bets": [], "fill_rate": None}
        try:
            from cryptobot.execution.fill_rate_breaker import evaluate_breaker

            br = evaluate_breaker(conn)
            out["fill_rate"] = {
                "fill_rate": br.fill_rate,
                "n_orders": br.n_orders,
                # Rolling 24h fill rate, alongside the record-window figure
                # (reject_stats.fill_rate_usd). Same definition as the breaker:
                # filled_usd / requested_usd over non-'error' sub-minute
                # orders, over the last 24h. Null when there were no orders in
                # 24h or all requested $0.
                **_v2_fill_rate_24h(conn),
                "tripped": br.tripped,
                "reason": br.reason,
                "min_orders": int(config.SUBMINUTE_BREAKER_MIN_ORDERS),
                "min_fill_rate": float(config.SUBMINUTE_BREAKER_MIN_FILL_RATE),
                "n_settled": br.n_settled,
                "settled_pnl": _round_money(br.settled_pnl),
                "settled_stake": _round_money(br.settled_stake),
            }
        except Exception:  # noqa: BLE001 — observability must never break SSE
            logger.warning("v2 fill-rate build failed", exc_info=True)
        try:
            # Limited to the record window, so bets from before a reset never
            # appear. Newest N by id and then filtered, rather than a bet_ts
            # condition in SQL: an id-DESC LIMIT stops early on the large
            # ledger table, while a filter would scan the whole table whenever
            # fewer than N bets are newer than the anchor (right after a
            # reset).
            rows = conn.execute(
                "SELECT bet_ts, coin, sleeve, interval_min, offset_sec, "
                "       window_start_ts, side, poly_token_id, is_topup, "
                "       model_p, p_market, bet_size, target_usd, fill_price, walked_vwap, outcome, "
                "       pnl_net, mode, won_up, live_filled_size, "
                "       live_filled_price, exit_kind, exit_ts, "
                "       venue, fee_rate_bps "
                "FROM ledger WHERE offset_sec IS NOT NULL "
                "ORDER BY id DESC LIMIT ?",
                (int(config.V2_BETS_LIMIT),),
            ).fetchall()
            rows = [r for r in rows if str(r["bet_ts"] or "") >= record_anchor]
            _mark_now_ts = time.time()
            bets = []
            for r in rows:
                ask = r["p_market"]
                model_p = r["model_p"]
                pred_edge = None
                if ask is not None and model_p is not None:
                    fee = fee_per_share(float(ask), theta=theta)
                    pred_edge = model_p - ask - fee
                ghost = str(r["outcome"] or "").upper() == "GHOST_FILL"
                # filled_usd is the realized fill value (size x price) once the
                # reconciler has it; the intended bet_size is only the fallback
                # for DRY or older rows.
                _lf_size = _float_or_none(r["live_filled_size"])
                _lf_price = _float_or_none(r["live_filled_price"])
                if ghost:
                    filled_usd: Optional[float] = 0.0
                elif _lf_size is not None and _lf_size > 0 and _lf_price is not None:
                    filled_usd = _round_money(_lf_size * _lf_price)
                else:
                    filled_usd = _round_money(r["bet_size"])
                bet = {
                    "time": r["bet_ts"],
                    "coin": r["coin"],
                    "sleeve": r["sleeve"],
                    "interval": (
                        f"{r['interval_min']}m"
                        if r["interval_min"] is not None else None
                    ),
                    "offset_sec": r["offset_sec"],
                    "window_start_ts": r["window_start_ts"],
                    "side": r["side"],
                    # Top-up flag. window_start_ts identifies the position;
                    # this labels the fills within it, so the Subminute tab can
                    # show one row per position and still separate the fills in
                    # the drill-down. Not part of the group key.
                    "is_topup": bool(r["is_topup"]),
                    "ask": _round_prob(ask),
                    # Same intended execution price as Trade Journal's exec
                    # column: walked VWAP, falling back to the top ask.
                    "exec_price": _round_prob(
                        r["walked_vwap"]
                        if r["walked_vwap"] is not None
                        else r["fill_price"]
                    ),
                    "model_p": _round_prob(model_p),
                    "pred_edge": _round_prob(pred_edge),
                    "stake_usd": _round_money(r["bet_size"]),
                    # The sizer's INTENT (ledger.target_usd). stake_usd /
                    # filled_usd above are realized values, so the client's
                    # PARTIAL detector was comparing filled-to-itself and
                    # could never fire on a partially filled FAK; this field
                    # is what groupResolutions' target actually needs.
                    "requested_usd": _round_money(r["target_usd"]),
                    "equity_at_decision": None,
                    "fill_price": _round_prob(r["fill_price"]),
                    # Trade Journal's realized fill source. Keep it separate
                    # from the decision-time ask/exec fields above.
                    "live_filled_price": _round_prob(r["live_filled_price"]),
                    "filled_usd": filled_usd,
                    "ghost": ghost,
                    "outcome": r["outcome"],
                    "pnl_net": _round_money(r["pnl_net"]),
                    "mode": r["mode"],
                    "exit_ts": r["exit_ts"],
                    # Historical stop-loss rows count as a realized loss.
                    "exit_kind": r["exit_kind"],
                }
                # Unrealized mark on OPEN sub-minute positions (Subminute drill-down).
                if str(r["outcome"] or "").upper() == "PENDING":
                    bet.update(_mark_open_position(
                        book_mark_get,
                        poly_token_id=r["poly_token_id"],
                        now_ts=_mark_now_ts,
                        live_filled_size=r["live_filled_size"],
                        bet_size=r["bet_size"],
                        fill_price=r["fill_price"],
                    ))
                bets.append(bet)
            out["bets"] = bets
        except Exception:  # noqa: BLE001 — observability must never break SSE
            logger.warning("v2 bets build failed", exc_info=True)
        try:
            out.update(_reject_block(conn, theta, record_anchor))
        except Exception:  # noqa: BLE001 — observability must never break SSE
            logger.warning("v2 reject block build failed", exc_info=True)
        return out

    aux = _aux_cached(conn, "v2", build) or {}
    return {
        "enabled": True,
        "kill": bool(getattr(config, "SUBMINUTE_KILL", False)),
        "spec": _v2_spec(portfolio),
        "sizing": sizing,
        "fill_rate": aux.get("fill_rate"),
        "bets": aux.get("bets", []),
        "rejects": aux.get("rejects", []),
        # ONE row per missed RESOLUTION (deduped attempts, carrying the ON-CHAIN
        # verdict) — the operator's "1 missing trade only".
        "missed_trades": aux.get("missed_trades", []),
        "reject_stats": aux.get("reject_stats"),
    }


def build_payload(
    conn: sqlite3.Connection,
    portfolio: V6Portfolio,
    *,
    last_seen_ledger_id: int = 0,
    tick_ms: Optional[int] = None,
    live_balance_get: Optional[Callable[[], Optional[float]]] = None,
    book_mark_get: Optional[Callable[..., Any]] = None,
) -> tuple[dict[str, Any], int]:
    """Build the full SSE payload; return (payload, new_max_ledger_id).

    ``live_balance_get`` is the BalanceCache reader live sizing uses
    (``balance_cache.get``) and drives the per-tick cash headline
    (kpis.account_capital_live). ``book_mark_get`` is the background book
    source (``book_prefetch.get``) used to mark open positions. Both are
    read-only.
    """
    now_ts = int(time.time())

    ledger_rows, max_id = _ledger_block(
        conn, portfolio, last_seen_id=last_seen_ledger_id,
        book_mark_get=book_mark_get,
    )
    kpis = _kpis_block(
        conn, portfolio,
        live_balance_get=live_balance_get,
        book_mark_get=book_mark_get,
    )
    realized_capital = kpis.get("realized_capital") if isinstance(kpis, dict) else None
    (
        ledger_rollup,
        ledger_equity,
        ledger_daily,
        ledger_rollup_by_venue,
        ledger_daily_by_venue,
        ledger_analytics,
        ledger_analytics_by_venue,
    ) = _history_blocks_cached(conn, portfolio, anchor=realized_capital)
    # CASH equity curve since the operator reset — the REAL account-balance
    # trajectory (green when cash is up), which the equity tile agrees with;
    # `ledger_equity` above is the paper (pnl_net) cumulation that diverges from
    # cash by the win-redemption bridge. Cheap single hourly GROUP-BY, computed
    # inline (not in the 5s history cache). net = final − base = cash-basis era P&L.
    cash_equity = _cash_equity_series(conn, record_anchor=_record_anchor_ts())
    # Put the cash net on the same period as the paper figures. The paper
    # rollup and KPIs are already limited to ``bet_ts >= WALLET_START_AT``, but
    # account_pnl_total is cash minus the lifetime audited basis.
    # ``cash_equity.net`` (cash now - cash at reset) is the cash P&L for the
    # same period, so the dashboard can show matching figures;
    # account_pnl_total is left as is for reconciliation.
    if isinstance(kpis, dict) and cash_equity is not None:
        kpis["account_pnl_era_cash"] = cash_equity.get("net")

    payload = {
        "tick_ts": tick_ms if tick_ms is not None else int(now_ts * 1000),
        # Venue seam telemetry — ALWAYS emitted (dashboard snapshot contract).
        "venue": getattr(config, "VENUE", "polymarket"),
        # Browser retention follows the largest server-side ledger window so
        # the client cap cannot silently drift when bootstrap/delta limits
        # change independently.
        "ledger_limit": max(
            int(config.LEDGER_BOOTSTRAP_LIMIT),
            int(config.LEDGER_DELTA_LIMIT),
        ),
        # Process-boot stamp: changes on every deploy/restart. live-bridge.js
        # compares it across ticks and reloads the page when it changes, so a
        # long-lived dashboard tab can never keep rendering pre-deploy JS
        # against post-deploy data (the "stale tab" class of misalignment).
        "server_boot_ts": _SERVER_BOOT_TS,
        "kpis": kpis,
        "portfolio": _portfolio_block(portfolio),
        "markets": _markets_block(),
        "ledger": ledger_rows,
        # Lifetime rollup over the active strategy's full ledger (not the
        # streamed bootstrap window) — the dashboard headline win-rate / P&L /
        # sample-size read this so they reflect the whole deployed-strategy
        # history rather than a noisy recent slice.
        "ledger_rollup": ledger_rollup,
        # Lifetime equity SERIES (downsampled, anchored to realized_capital) so
        # the equity curve shows the full active-strategy history instead of the
        # ~50-row session window — the curve-side counterpart to ledger_rollup.
        "ledger_equity": ledger_equity,
        # CASH-balance equity curve since the reset era — the operator's aligned
        # GREEN curve: net = final − base is the cash-basis era P&L the equity
        # tile agrees with (ledger_equity's paper curve diverges by transit).
        "cash_equity": cash_equity,
        # ALL-TIME per-day P&L rollup (MYT settlement day — see _MYT_DAY_SQL:
        # stop rows key on their immutable exit_ts), same population as
        # kpis.pnl_net_usd so Σ(day.pnl) == that net BY CONSTRUCTION. The
        # calendar/journal read this instead of aggregating the bounded client
        # ledger, so the Calendar headline can never disagree with the Overview
        # P&L breakdown. {date,pnl,n,wins,losses} — client aggDaily shape;
        # n/wins/losses count TRADE GROUPS (top-ups folded into their base bet),
        # pnl is the unchanged per-row Σ.
        "ledger_daily": ledger_daily,
        # Per-venue versions of the two rollups above, same definitions grouped
        # by venue. The client prefers these over re-aggregating the streamed
        # rows.
        "ledger_rollup_by_venue": ledger_rollup_by_venue,
        "ledger_daily_by_venue": ledger_daily_by_venue,
        # FULL-ledger analytics for the historical panels (headline summary ·
        # per-coin · per-slice · P&L histogram · streak tape), pre-sliced by the
        # Topbar range toggle: {'All'|'30d'|'7d': {...}}. These are COUNTS, not
        # rows, so the client cannot range-slice them itself — every option it
        # can select ships. Before this the panels aggregated the bounded
        # `ledger` window and described ~500 of ~3.5k era bets while the
        # lifetime headline beside them described all of them.
        "ledger_analytics": ledger_analytics,
        "ledger_analytics_by_venue": ledger_analytics_by_venue,
        # Producer<->client contract strings (H1 day bucket + win rate + the
        # trade-group key) so the client can render/verify the exact rules it
        # mirrors.
        "contracts": {
            "day_bucket": _DAY_BUCKET_CONTRACT,
            "win_rate": _WIN_RATE_CONTRACT,
            "trade_group": _TRADE_GROUP_CONTRACT,
        },
        # Execution settings shown on the Config tab.
        "strategy_flags": _strategy_flags_block(),
        "live_wallet": _live_wallet_block(conn),
        # Account-level cash events (deposits / withdrawals / rebates) behind
        # kpis.net_deposits and the flow-inclusive equity + peak.
        "capital_flows": _capital_flows_block(conn),
        "operator": _operator_block(conn),
        # C1 — per-stage latency decomposition (Latency page).
        "latency": _latency_block(conn),
        # v2 "step1last" console — frozen spec, equity-fraction sizing state,
        # DD-brake, fill-rate breaker, recent per-bet rows.
        "v2": _v2_block(conn, portfolio, kpis, book_mark_get=book_mark_get),
    }
    return _json_safe(payload), max_id
