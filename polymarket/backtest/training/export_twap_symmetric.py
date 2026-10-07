"""Compatibility entry point for TWAP export and pre-cutover replay helpers.

Model selection used expanding B/C/D windows with a one-hour embargo; native
fold F was evaluated once and never used to tune the feature set, band, gate,
or hyperparameters. After that evaluation is recorded, this production export
fits the frozen model on every labeled row available through fold F. Production
export now delegates to :mod:`export_twap_symmetric_60s`; ``_production_frame``
remains here solely for the retained August 7 pre-cutover replay.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from polymarket.backtest import REPO_ROOT
from polymarket.backtest.studies.tick_latency.run_twap_wfo_full import (
    _load_side_frame,
)

OUT_DEFAULT = REPO_ROOT / "data" / "models" / "twap_symmetric_v1"
FEATURE_COLUMNS = [
    "ask",
    "other_ask",
    "book_spread",
    "coin_id",
    "interval_min",
    "side_up",
    "entry_minute",
    "hour_utc",
    "dow_utc",
    "partial_ret_z_side",
    "abs_partial_ret_z",
]


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _production_frame() -> pd.DataFrame:
    frame = _load_side_frame().copy()
    frame["partial_ret_z_side"] = frame["sym_z"].astype(float)
    frame["abs_partial_ret_z"] = frame["sym_z"].abs().astype(float)
    return frame.dropna(subset=FEATURE_COLUMNS + ["win_twap_s"]).copy()


from polymarket.backtest.training.export_twap_symmetric_60s import (  # noqa: E402
    build_production_model,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT_DEFAULT)
    args = parser.parse_args()
    manifest = build_production_model(args.out)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
