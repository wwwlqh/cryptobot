"""PARITY GATE for the live HGB feature pipeline (exp044 Phase 3).

This is the highest parity-risk test in the port: it proves the LIVE per-tick
feature builder (`cryptobot.strategy.hgb_features.build_feature_vector`) produces
a vector that byte-matches the BACKTEST's own feature-frame construction for the
served checkpoint_v1 11-feature set.

The comparison is against the INDEPENDENT backtest function
`polymarket.backtest.training.checkpoint._feature_frame_for_side` — NOT a
re-derivation in this test file. A silent feature divergence destroys the edge,
so the bar is atol <= 1e-12, and column ORDER and DTYPE are checked too.

Also covers the served checkpoint_v1 pipelines' column contract and a
predict_model_prob test that matches a direct pipeline.predict_proba.

2026-07-31 (exp044 phase 3): `decision_table.parquet` was deleted with its whole
producer chain, and with it `modeling._side_rows`' substrate — the two gates that
read it went to SKIP.

2026-08-01: the gates are RESTORED on a deterministic synthetic fixture
(`ckpt_rows`) driven straight into `_feature_frame_for_side`, the construction
the live checkpoint sleeves are actually scored against. No decision_table, no
book index, no on-disk substrate of any kind — so the parity contract is enforced
on every checkout again. `decision_table` is permanently retired; do NOT
reintroduce a dependency on it.
"""

from __future__ import annotations


import numpy as np
import pytest

from cryptobot import config
from cryptobot.strategy.hgb_features import (
    COIN_MAP,
    FEATURE_GROUPS,
    _feature_columns,
    build_feature_vector,
)

# --- import the backtest source-of-truth (research code) --------------------
# The backtest package is importable via the repo root on sys.path (pytest
# pythonpath = ["src", "."]); modeling lives in the training package
# subpackage and imports its siblings by fully-qualified name.

from polymarket.backtest.training import modeling as ev  # noqa: E402

# The served checkpoint_v1 feature set, manifest order. Both intervals serve the
# same 11 columns (asserted below against the on-disk manifest).
FCOLS_5M = _feature_columns("rf,price,identity,time")[1]


# --------------------------------------------------------------------------
# LIVE builder <-> BACKTEST `_feature_frame_for_side` parity, substrate-free
# --------------------------------------------------------------------------

_INTERVAL_SECS = {"5m": 300, "15m": 900}
# The last-30s checkpoint grid the live sleeves run on (manifest step1last).
_CKPT_SECS = {"5m": (270, 284, 299), "15m": (870, 884, 899)}


class _StubRfSlice:
    """Stand-in for one per-(coin, interval, checkpoint_sec) RandomForest slice.

    `_rf_probs_checkpoint` only ever calls ``predict_proba`` on a one-column
    ``partial_ret_z_1h`` frame and reads ``[:, 1]``. A deterministic closed form
    keyed on the SLICE lets the test compute the identical P(Up) for the live
    side independently, so the grouping/index-scatter in the backtest path is
    genuinely exercised rather than read back out of its own output.
    """

    classes_ = np.array([0, 1])

    def __init__(self, coin: str, interval: str, checkpoint_sec: int) -> None:
        self.key = (coin, interval, int(checkpoint_sec))

    def predict_proba(self, frame):
        z = np.asarray(frame["partial_ret_z_1h"], dtype=np.float64)
        p = _stub_p_up(z, *self.key)
        return np.column_stack([1.0 - p, p])


def _stub_p_up(z, coin: str, interval: str, checkpoint_sec: int):
    """Deterministic P(Up) — same closed form both paths must land on."""
    bias = 0.01 * (COIN_MAP[coin] + 1) + 0.0001 * int(checkpoint_sec)
    bias += 0.02 if interval == "15m" else 0.0
    return 1.0 / (1.0 + np.exp(-(0.35 * np.asarray(z, dtype=np.float64) + bias)))


def _ckpt_sleeve(interval: str, feature_columns):
    from polymarket.backtest.training import pmdata_target as etgt

    return etgt.ManifestSleeve(
        label=f"ckpt_{interval}",
        interval=interval,
        threshold=0.03,
        stake_frac=0.10,
        book_consumption_max=0.3,
        ask_bucket_raw="band:0.65:0.85",
        side_filter="both",
        feature_columns=list(feature_columns),
    )


def _ckpt_rows(interval: str):
    """Deterministic synthetic checkpoint rows — every column
    ``_feature_frame_for_side`` touches, spread over coins / asks / z / clocks."""
    import pandas as pd

    secs = _INTERVAL_SECS[interval]
    coins = sorted(COIN_MAP)
    asks = (0.34, 0.51, 0.66, 0.72, 0.79, 0.88)
    zs = (-2.25, -0.8, 0.0, 0.4, 1.7)
    # 2025-05-04T00:00:00Z is a Sunday; stepping the window start by whole
    # windows walks hour_utc and (further out) dow_utc.
    base_ts = 1_746_316_800

    records = []
    i = 0
    for coin in coins:
        for ckpt_sec in _CKPT_SECS[interval]:
            for ask in asks:
                for z in zs:
                    entry_ts = base_ts + (i * 37) * secs
                    records.append(
                        {
                            "coin": coin,
                            "interval": interval,
                            "checkpoint_sec": int(ckpt_sec),
                            "entry_ts": int(entry_ts),
                            "entry_minute": ckpt_sec / 60.0,
                            "ask": float(ask),
                            # every 4th row leaves other_ask non-finite so the
                            # `1 - ask` fallback is exercised on BOTH paths
                            "other_ask": (
                                float("nan") if i % 4 == 3 else float(round(1.02 - ask, 4))
                            ),
                            "partial_ret_z_1h": float(z),
                            "up_depth": float(100 + 7 * i),
                            "down_depth": float(60 + 3 * i),
                            "won_up": int(i % 2),
                        }
                    )
                    i += 1
    return pd.DataFrame.from_records(records)


def _stub_estimators(rows):
    return {
        (str(r.coin).lower(), str(r.interval).lower(), int(r.checkpoint_sec)): _StubRfSlice(
            str(r.coin).lower(), str(r.interval).lower(), int(r.checkpoint_sec)
        )
        for r in rows.itertuples(index=False)
    }


@pytest.mark.parametrize("interval,n_cols", [("5m", 11), ("15m", 11)])
@pytest.mark.parametrize("side", ["Up", "Down"])
def test_live_features_match_backtest_checkpoint_frame(interval, n_cols, side):
    """THE GATE: the live vector == the backtest checkpoint feature frame.

    Column ORDER, DTYPE and VALUES (atol 1e-12) on the served 11-feature set,
    both sides, no on-disk substrate.
    """
    from polymarket.backtest.training import checkpoint as ckpt

    feature_columns = FCOLS_5M
    assert len(feature_columns) == n_cols

    rows = _ckpt_rows(interval)
    sleeve = _ckpt_sleeve(interval, feature_columns)
    sub, feats, ask, other = ckpt._feature_frame_for_side(
        rows, sleeve, side, _stub_estimators(rows)
    )
    assert not sub.empty, f"{interval}/{side}: backtest frame produced no rows"

    # (a) column order is the manifest order, (b) every column is float64.
    assert list(feats.columns) == list(feature_columns)
    assert all(str(dt) == "float64" for dt in feats.dtypes), dict(feats.dtypes)

    # The non-finite `other_ask` rows are the fallback probe. On Up they survive
    # and both builders must apply `1 - ask`; on Down that column IS the ask, so
    # the backtest's validity filter drops them — assert the shape either way so
    # a fixture that quietly stopped probing the fallback fails here.
    n_nan_other = int(rows["other_ask"].isna().sum())
    assert n_nan_other > 0
    expected_rows = len(rows) if side == "Up" else len(rows) - n_nan_other
    assert len(sub) == expected_rows

    # Feed the LIVE builder the RAW other-side price so its own non-finite
    # fallback runs independently of the backtest's.
    raw_other_col = "other_ask" if side == "Up" else "ask"

    n_compared = 0
    for idx in sub.index:
        row = sub.loc[idx]
        p_up = float(
            _stub_p_up(
                float(row["partial_ret_z_1h"]),
                str(row["coin"]).lower(),
                str(row["interval"]).lower(),
                int(row["checkpoint_sec"]),
            )
        )
        live = build_feature_vector(
            side=side,
            coin=str(row["coin"]),
            interval=interval,
            interval_min=int(ev.INTERVAL_MAP[interval]),
            entry_minute=float(row["entry_minute"]),
            window_start_ts=int(row["entry_ts"]),
            model_p_up=p_up,
            ask=float(ask.loc[idx]),
            other_ask=float(rows.loc[idx, raw_other_col]),
            partial_ret_z_1h=float(row["partial_ret_z_1h"]),
            feature_columns=feature_columns,
        )
        assert live is not None, (
            f"{interval}/{side}: live builder returned None on a finite backtest "
            f"row (coin={row['coin']} sec={row['checkpoint_sec']})"
        )
        bt = [float(v) for v in feats.loc[idx].to_numpy()]
        assert len(live) == len(bt) == len(feature_columns)
        for col, lv, bv in zip(feature_columns, live, bt):
            assert lv == pytest.approx(bv, abs=1e-12), (
                f"FEATURE MISMATCH {interval}.{col} side={side}: live={lv!r} "
                f"bt={bv!r} (coin={row['coin']} sec={row['checkpoint_sec']} "
                f"ask={row['ask']} entry_ts={row['entry_ts']})"
            )
        n_compared += 1
    assert n_compared == expected_rows


def test_window_start_ts_drives_hour_dow():
    """hour_utc/dow_utc come from window_start_ts, NOT now. 2025-05-04 12:34:56
    UTC is a Sunday (pandas dayofweek/Python weekday Mon=0..Sun=6 -> 6), hour 12.
    Cross-checked against pandas dt.dayofweek to pin the same convention as
    ev._prepare_base_frame (entry.dt.dayofweek)."""
    import pandas as pd

    ts = 1_746_362_096  # 2025-05-04T12:34:56Z (Sunday)
    expected_hour = float(pd.Timestamp(ts, unit="s", tz="UTC").hour)
    expected_dow = float(pd.Timestamp(ts, unit="s", tz="UTC").dayofweek)
    vec = build_feature_vector(
        side="Up",
        coin="btc",
        interval="5m",
        interval_min=5,
        entry_minute=3,
        window_start_ts=ts,
        model_p_up=0.6,
        ask=0.5,
        other_ask=0.5,
        partial_ret_z_1h=0.0,
        feature_columns=["hour_utc", "dow_utc"],
    )
    assert vec == [expected_hour, expected_dow] == [12.0, 6.0]


def test_other_ask_fallback_to_one_minus_ask():
    """Non-finite other_ask -> 1 - ask (ev:216 fallback)."""
    vec = build_feature_vector(
        side="Up", coin="eth", interval="15m", interval_min=15, entry_minute=7,
        window_start_ts=1_746_362_096, model_p_up=0.55, ask=0.42,
        other_ask=float("nan"), partial_ret_z_1h=0.3,
        feature_columns=["ask", "other_ask", "book_spread"],
    )
    ask, other_ask, book_spread = vec
    assert other_ask == pytest.approx(1.0 - 0.42)
    assert book_spread == pytest.approx(0.42 + (1.0 - 0.42) - 1.0)


def test_down_side_flips():
    """Down side flips, with known inputs independent of any data row:
    model_p_side = 1 - model_p_up, partial_ret_z_side = -z, side_up = 0,
    rf_edge = model_p_side - ask - theta*ask*(1-ask)."""
    cols = ["model_p_side", "partial_ret_z_side", "abs_partial_ret_z", "side_up", "rf_edge"]
    vec = build_feature_vector(
        side="Down", coin="eth", interval="15m", interval_min=15, entry_minute=7,
        window_start_ts=1_746_362_096, model_p_up=0.65, ask=0.40, other_ask=0.55,
        partial_ret_z_1h=0.4, feature_columns=cols,
    )
    mp_side, prz_side, abs_prz, side_up, rf_edge = vec
    assert mp_side == pytest.approx(1.0 - 0.65)
    assert prz_side == pytest.approx(-0.4)
    assert abs_prz == pytest.approx(0.4)
    assert side_up == 0.0
    theta = config.POLY_FEE_THETA
    assert rf_edge == pytest.approx((1.0 - 0.65) - 0.40 - theta * 0.40 * (1.0 - 0.40))


def test_invalid_ask_returns_none():
    for bad in (0.0, 1.0, 1.5, float("nan"), float("inf")):
        assert build_feature_vector(
            side="Up", coin="btc", interval="5m", interval_min=5, entry_minute=1,
            window_start_ts=1_746_362_096, model_p_up=0.6, ask=bad, other_ask=0.5,
            partial_ret_z_1h=0.0, feature_columns=["ask"],
        ) is None


def test_cross_feature_raises_not_implemented():
    for col in FEATURE_GROUPS["cross"]:
        with pytest.raises(NotImplementedError):
            build_feature_vector(
                side="Up", coin="btc", interval="5m", interval_min=5, entry_minute=1,
                window_start_ts=1_746_362_096, model_p_up=0.6, ask=0.5, other_ask=0.5,
                partial_ret_z_1h=0.0, feature_columns=[col],
            )


def test_unknown_coin_and_interval_raise():
    with pytest.raises(ValueError, match="coin"):
        build_feature_vector(
            side="Up", coin="bnb", interval="5m", interval_min=5, entry_minute=1,
            window_start_ts=1_746_362_096, model_p_up=0.6, ask=0.5, other_ask=0.5,
            partial_ret_z_1h=0.0, feature_columns=["coin_id"],
        )
    with pytest.raises(ValueError, match="interval"):
        build_feature_vector(
            side="Up", coin="btc", interval="1h", interval_min=60, entry_minute=1,
            window_start_ts=1_746_362_096, model_p_up=0.6, ask=0.5, other_ask=0.5,
            partial_ret_z_1h=0.0, feature_columns=["interval_min"],
        )


# --------------------------------------------------------------------------
# hgb_loader serve contract
#
# The hgb_v1 manifest-shape tests (intervals / feature_columns-vs-groups / the
# rf_v1 sha drift check) lived here and went with the 2026-07-31 exp044
# retirement together with ``load_hgb``. checkpoint_loader owns those checks
# now (see test_checkpoint_parity.py); what survives here is the parity gate
# between the live feature builder and the served pipeline.
# --------------------------------------------------------------------------


def test_coin_map_matches_backtest():
    assert COIN_MAP == ev.COIN_MAP
    assert dict(zip(FEATURE_GROUPS, FEATURE_GROUPS.values())) and (
        FEATURE_GROUPS == ev.FEATURE_GROUPS
    )
