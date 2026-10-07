"""Exact-era review of the live TWAP feature contract.

The frozen HGB was trained on source-relative Binance movement plus a small
point-to-TWAP opening adjustment.  A raw ``Binance_now / Chainlink_TWAP_open``
ratio leaks the persistent USDT-vs-USD venue basis into the directional
feature.  This review compares the directly live-servable Binance point return
with a source-corrected opening adjustment whose centre is fitted on Aug 7-10
only and then frozen for the Aug 11 check.
"""
from __future__ import annotations

import json
import math
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.lib import pool_study as ps
from polymarket.backtest.studies.tick_latency import run_twap_exact_aug7 as exact
from polymarket.backtest.training.export_twap_symmetric import FEATURE_COLUMNS


HOLDOUT_START = 1_786_406_400  # 2026-08-11 00:00:00 UTC
BASIS_RESIDUAL_CLIP_LOG = 0.0001  # one basis point
OUT = Path(tempfile.gettempdir()) / "twap_feature_contract_review_20260812.json"


def _development_basis_centres(
    rows: pd.DataFrame, *, open_col: str
) -> dict[tuple[str, str], float]:
    markets = rows.drop_duplicates("market_slug").copy()
    markets["basis"] = np.log(
        markets[open_col].to_numpy(float)
        / markets["chainlink_open"].to_numpy(float)
    )
    dev = markets[markets["slug_ts"] < HOLDOUT_START]
    return {
        (str(coin), str(interval)): float(group["basis"].median())
        for (coin, interval), group in dev.groupby(["coin", "interval"])
    }


def _score(
    rows: pd.DataFrame,
    model,
    *,
    centres: dict[tuple[str, str], float],
    open_col: str,
    corrected: bool,
) -> pd.DataFrame:
    out = rows.copy()
    point_ret = np.log(
        out["spot"].to_numpy(float) / out[open_col].to_numpy(float)
    )
    if corrected:
        centre = np.asarray(
            [centres[(str(c), str(i))] for c, i in zip(out["coin"], out["interval"])],
            dtype=float,
        )
        basis = np.log(
            out[open_col].to_numpy(float)
            / out["chainlink_open"].to_numpy(float)
        )
        point_ret += np.clip(
            basis - centre,
            -BASIS_RESIDUAL_CLIP_LOG,
            BASIS_RESIDUAL_CLIP_LOG,
        )
    z_up = point_ret / (
        out["rv_open"].to_numpy(float)
        * np.sqrt(out["entry_minute"].to_numpy(float))
    )
    out["partial_ret_z_side"] = np.where(out["side_up"].to_numpy(bool), z_up, -z_up)
    out["abs_partial_ret_z"] = np.abs(z_up)
    out["model_p"] = model.predict_proba(out[FEATURE_COLUMNS])[:, 1]
    out["pred_edge"] = (
        out["model_p"] - out["ask"] - ps.fee(out["ask"].to_numpy(float))
    )
    return out


def _breakdown(replayed: pd.DataFrame) -> dict:
    dated = replayed.copy()
    dated["date"] = pd.to_datetime(dated["entry_ts"], unit="s", utc=True).dt.strftime(
        "%Y-%m-%d"
    )
    report: dict[str, object] = {
        "full": exact._metrics(dated),
        "holdout": exact._metrics(dated[dated["entry_ts"] >= HOLDOUT_START]),
        "per_coin": {},
        "per_day": {},
    }
    for coin, group in dated.groupby("coin"):
        report["per_coin"][str(coin)] = {
            "full": exact._metrics(group),
            "holdout": exact._metrics(group[group["entry_ts"] >= HOLDOUT_START]),
        }
    for day, group in dated.groupby("date"):
        report["per_day"][str(day)] = exact._metrics(group)
    return report


def main() -> None:
    print("loading exact-era rows", flush=True)
    rows, paths, _spots = exact._feature_rows()
    print(f"fitting frozen model rows={len(rows):,}", flush=True)
    model = exact._fit_frozen_model()
    point_centres = _development_basis_centres(rows, open_col="binance_open")
    twap_centres = _development_basis_centres(rows, open_col="binance_twap_open")

    report: dict[str, object] = {
        "basis_residual_clip_log": BASIS_RESIDUAL_CLIP_LOG,
        "basis_centres": {
            "binance_point": {
                f"{coin}:{interval}": value
                for (coin, interval), value in sorted(point_centres.items())
            },
            "binance_twap": {
                f"{coin}:{interval}": value
                for (coin, interval), value in sorted(twap_centres.items())
            },
        },
        "centre_fit_period": "2026-08-07/2026-08-10",
        "holdout_period": "2026-08-11",
        "universe": ["btc", "sol"],
        "variants": {},
    }
    variants = (
        ("binance_point_centred_clipped", "binance_open", point_centres),
        ("binance_twap_centred_clipped", "binance_twap_open", twap_centres),
    )
    for name, open_col, centres in variants:
        print(f"scoring {name}", flush=True)
        scored = _score(
            rows,
            model,
            centres=centres,
            open_col=open_col,
            corrected=True,
        )
        replayed = exact._replay(
            scored,
            paths,
            tier_mult=1.0,
            t3=True,
            coin_filter={"btc", "sol"},
            compound_available_cash=True,
        )
        report["variants"][name] = _breakdown(replayed)

    OUT.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
