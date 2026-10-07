"""Live HGB feature builder — the per-tick multi-feature contract.

This is the SOURCE-OF-TRUTH PORT of the backtest's per-row feature derivation
(`polymarket/backtest/training/modeling.py::_side_rows`). Runtime code
must NOT import the research module; the derivations are reproduced here exactly
so a live tick produces a feature vector that byte-matches the row the backtest
trained/served on. A silent divergence here destroys the exp044 edge, so the
parity test (`tests/test_hgb_features.py`) compares this builder against the
independent backtest `_side_rows` function on real decision-table rows.

Ported verbatim from the backtest (do not re-derive elsewhere):
  - COIN_MAP / INTERVAL_MAP             (ev lines ~49-50)
  - FEATURE_GROUPS / FEATURE_COLUMNS    (ev lines ~52-82)
  - _feature_columns                    (ev lines ~125-139)

Per-row feature math reproduced from `_side_rows` (ev lines ~205-266):
  - model_p_side = model_p_up if Up else 1 - model_p_up
        model_p_up is the LIVE RF predict_proba(partial_ret_z_1h), never the LUT
        hit_rate. (Backtest column model_p_v1 == that same RF probability.)
  - other_ask: if not finite -> 1 - ask                 (ev:216 fallback)
  - rf_edge = model_p_side - ask - theta*ask*(1-ask)    (ev:248, _fee at ev:142)
  - book_spread = ask + other_ask - 1.0                 (ev:249)
  - log_depth = log1p(max(0, depth))                    (ev:250)
  - depth_imbalance = log1p(max(0,depth)) - log1p(max(0,other_depth))  (ev:251)
  - partial_ret_z_side = (+1 if Up else -1) * partial_ret_z_1h  (ev:252-253)
  - abs_partial_ret_z = |partial_ret_z_1h|              (ev:254)
  - coin_id = COIN_MAP[coin.lower()] ; interval_min = INTERVAL_MAP[interval]
  - side_up = 1 if Up else 0
  - entry_minute = int entry minute
  - hour_utc / dow_utc: from window_start_ts (the window-open boundary entry_ts),
        NOT now_ts.                                     (ev:187-188)

exp044 does NOT use the depth group (log_depth/depth_imbalance) — they are
supported but unused. The "cross" group (p_mean_entry / p_std_entry /
target_vs_entry_mean / consensus_for_side) needs a tick-wide cross-coin
aggregation that is not computable from a single cell, so requesting any cross
feature raises NotImplementedError (fail-closed).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional

from cryptobot import config

# --- Ported identity maps (ev ~49-50) ---------------------------------------
COIN_MAP: dict[str, int] = {"btc": 0, "eth": 1, "sol": 2, "xrp": 3}
INTERVAL_MAP: dict[str, int] = {"5m": 5, "15m": 15}

# --- Ported feature contract (ev ~52-82) ------------------------------------
FEATURE_COLUMNS = [
    "model_p_side",
    "rf_edge",
    "ask",
    "other_ask",
    "book_spread",
    "log_depth",
    "depth_imbalance",
    "partial_ret_z_side",
    "abs_partial_ret_z",
    "coin_id",
    "interval_min",
    "entry_minute",
    "side_up",
    "hour_utc",
    "dow_utc",
    "p_mean_entry",
    "p_std_entry",
    "target_vs_entry_mean",
    "consensus_for_side",
]

FEATURE_GROUPS: dict[str, list[str]] = {
    "rf": ["model_p_side", "rf_edge"],
    "price": ["ask", "other_ask", "book_spread"],
    "depth": ["log_depth", "depth_imbalance"],
    "momentum": ["partial_ret_z_side", "abs_partial_ret_z"],
    "identity": ["coin_id", "interval_min", "side_up"],
    "time": ["entry_minute", "hour_utc", "dow_utc"],
    "cross": ["p_mean_entry", "p_std_entry", "target_vs_entry_mean", "consensus_for_side"],
}

# Features that need a tick-wide cross-coin aggregation (mean/std/votes over all
# coins entering the same (interval, entry_ts) bucket). Not computable per-cell.
_CROSS_FEATURES = frozenset(FEATURE_GROUPS["cross"])


def _feature_columns(raw_groups: str) -> tuple[str, list[str]]:
    """Ported from ev._feature_columns (lines ~125-139).

    Returns (feature_set_label, ordered_columns) for a comma-separated group
    spec. ``"all"`` (or empty) -> the full FEATURE_COLUMNS list. Order matches
    the backtest exactly so the manifest's stored column order is reproducible.
    """
    groups = [g.strip().lower() for g in raw_groups.split(",") if g.strip()]
    if not groups or groups == ["all"]:
        return "all", list(FEATURE_COLUMNS)

    unknown = sorted(set(groups) - set(FEATURE_GROUPS))
    if unknown:
        raise ValueError(f"unknown feature group(s): {', '.join(unknown)}")

    columns: list[str] = []
    for group in groups:
        for col in FEATURE_GROUPS[group]:
            if col not in columns:
                columns.append(col)
    return "+".join(groups), columns


def _fee(ask: float) -> float:
    """Ported from ev._fee (lines ~142-144): theta * ask * (1 - ask).

    Uses config.POLY_FEE_THETA, which is pinned == backtest POLY_FEE_THETA
    (test_backtest_parity asserts the equality, fail-closed on drift).
    """
    a = float(ask)
    return config.POLY_FEE_THETA * a * (1.0 - a)


def _finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def build_feature_vector(
    *,
    side: str,
    coin: str,
    interval: str,
    interval_min: int,
    entry_minute: int,
    window_start_ts: int,
    model_p_up: float,
    ask: float,
    other_ask: float = float("nan"),
    partial_ret_z_1h: float,
    depth: float = float("nan"),
    other_depth: float = float("nan"),
    feature_columns: list[str],
) -> Optional[list[float]]:
    """Reproduce `_side_rows` for one (coin, interval, entry_minute, side) cell.

    Returns the feature values in the EXACT order of ``feature_columns``
    (manifest order) as a list[float], or ``None`` if any *required* input for a
    requested column is non-finite (the caller emits a skip — this mirrors the
    backtest's NaN -> dropped-at-pick-time behaviour, and avoids feeding NaNs the
    median-imputer never saw at fit time on these always-finite columns).

    Raises:
        ValueError: unknown side / coin / interval, or an unknown feature name.
        NotImplementedError: a requested column is in the "cross" group
            (tick-wide cross-coin aggregation, not computable per-cell).
    """
    s = side.strip()
    if s not in ("Up", "Down"):
        raise ValueError(f"side must be 'Up' or 'Down', got {side!r}")
    is_up = s == "Up"

    requested = list(feature_columns)
    cross = [c for c in requested if c in _CROSS_FEATURES]
    if cross:
        raise NotImplementedError(
            "cross features require tick-wide cross-coin aggregation and are not "
            f"computable per-cell: {cross}"
        )
    unknown = [c for c in requested if c not in FEATURE_COLUMNS]
    if unknown:
        raise ValueError(f"unknown feature column(s): {unknown}")

    coin_l = coin.strip().lower()
    if coin_l not in COIN_MAP:
        raise ValueError(f"unknown coin {coin!r}; expected one of {sorted(COIN_MAP)}")
    interval_l = interval.strip().lower()
    if interval_l not in INTERVAL_MAP:
        raise ValueError(f"unknown interval {interval!r}; expected one of {sorted(INTERVAL_MAP)}")

    # --- ask: must be a valid executable price (matches ev:212 filter) -------
    a = float(ask)
    if not (math.isfinite(a) and 0.0 < a < 1.0):
        return None

    # --- other_ask fallback (ev:216): non-finite -> 1 - ask ------------------
    oa = float(other_ask)
    if not math.isfinite(oa):
        oa = 1.0 - a

    # --- model_p_side (ev:219) ----------------------------------------------
    if not _finite(model_p_up):
        return None
    mp_up = float(model_p_up)
    model_p_side = mp_up if is_up else (1.0 - mp_up)

    # --- partial_ret_z (ev:252-254) -----------------------------------------
    if not _finite(partial_ret_z_1h):
        return None
    prz = float(partial_ret_z_1h)
    sign = 1.0 if is_up else -1.0
    partial_ret_z_side = sign * prz
    abs_partial_ret_z = abs(prz)

    # --- derived price features ---------------------------------------------
    rf_edge = model_p_side - a - _fee(a)
    book_spread = a + oa - 1.0

    # --- depth features (ev:250-251): NaN-safe log1p(clip(lower=0)) ----------
    # depth/other_depth default to NaN; log1p(NaN) -> NaN, matching _side_rows
    # which keeps NaN for missing depth. exp044 does not request these columns.
    def _log_depth(d: float) -> float:
        dv = float(d)
        if not math.isfinite(dv):
            return float("nan")
        return math.log1p(max(0.0, dv))

    log_depth = _log_depth(depth)
    other_log_depth = _log_depth(other_depth)
    depth_imbalance = log_depth - other_log_depth

    # --- identity / time -----------------------------------------------------
    coin_id = float(COIN_MAP[coin_l])
    interval_min_v = float(int(interval_min))
    side_up = 1.0 if is_up else 0.0
    # 2026-07-05 parity fix: do NOT re-quantize to int. Baseline (minute) sleeves
    # pass an integer entry_minute (float(4)==4.0, unchanged); the sub-minute
    # checkpoint sleeves pass a FRACTIONAL minute (offset_sec/60, e.g. 4.833) that
    # the backtest's training.checkpoint feeds raw (checkpoint_sec/60.0).
    # The prior float(int(entry_minute)) collapsed every checkpoint in a minute to
    # one value and diverged from the model this vector is scored against.
    entry_minute_v = float(entry_minute)

    # hour_utc / dow_utc derive from window_start_ts (window-open boundary
    # entry_ts), NOT now_ts. ev uses datetime.utcfromtimestamp semantics via
    # pandas dt.hour / dt.dayofweek (Mon=0..Sun=6).
    dt = datetime.fromtimestamp(int(window_start_ts), tz=timezone.utc)
    hour_utc = float(dt.hour)
    dow_utc = float(dt.weekday())

    values: dict[str, float] = {
        "model_p_side": model_p_side,
        "rf_edge": rf_edge,
        "ask": a,
        "other_ask": oa,
        "book_spread": book_spread,
        "log_depth": log_depth,
        "depth_imbalance": depth_imbalance,
        "partial_ret_z_side": partial_ret_z_side,
        "abs_partial_ret_z": abs_partial_ret_z,
        "coin_id": coin_id,
        "interval_min": interval_min_v,
        "entry_minute": entry_minute_v,
        "side_up": side_up,
        "hour_utc": hour_utc,
        "dow_utc": dow_utc,
    }

    out: list[float] = []
    for col in requested:
        v = values[col]
        # Required-input guard: any non-finite value in a REQUESTED column means
        # the caller should skip. depth columns may legitimately be NaN, but
        # exp044 never requests them; if a config does request depth and it is
        # missing, skipping is the safe (fail-closed) choice.
        if not math.isfinite(v):
            return None
        out.append(float(v))
    return out
