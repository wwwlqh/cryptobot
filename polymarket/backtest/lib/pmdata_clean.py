"""pmdata cleaning and loading helpers.

Every pipeline loads data through here, so it is never cleaned two different
ways.

Raw-data problems handled:

- placeholder strings ("-", "--", em-dash, "") in object/string columns -> NaN;
- schema drift between snapshot files: some lacked ``winning_outcome`` (fixed
with ``pmdata_download.py --backfill-outcome``), timestamps are ms in some
files and us in others, and book arrays are strings in some and
``large_list<double>`` in others; - labels: ``won_up`` is Polymarket's official
resolution from Gamma, fetched by
``polymarket/backtest/fetch/fetch_gamma_resolutions.py`` and stored in
``pmdata_outcomes.parquet`` (``label_source == 'gamma'``). The ``cl_won_up``
column is unused. Rows with status 'ambiguous' are unlabeled and excluded; -
degenerate books: ask <= 0, ask >= 1, and crossed books (spread < -0.02).
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

from polymarket.backtest import BT_DATA_DIR

# Absolute path, so it works from any working directory.
PMDATA_ROOT = str(BT_DATA_DIR / "pmdata")
OUTCOMES_PATH = os.path.join(PMDATA_ROOT, "pmdata_outcomes.parquet")

COINS = ("btc", "eth", "sol", "xrp")
INTERVALS = ("5m", "15m")

PLACEHOLDER_STRINGS = {"-", "--", "—", "–", "", "None", "nan", "<NA>", "N/A", "null"}

EXPECTED_SNAPSHOT_COLUMNS = (
    "timestamp",
    "event_type",
    "ask_prices",
    "ask_sizes",
    "bid_prices",
    "bid_sizes",
    "winning_outcome",
)


def normalize_placeholders(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """Replace placeholder strings with NaN in every object/string column.

    Returns the cleaned frame and a per-column count of replaced cells.
    """
    df = df.copy()
    replaced: dict[str, int] = {}
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype).startswith(("str", "string")):
            as_str = df[col].astype("string")
            mask = as_str.str.strip().isin(PLACEHOLDER_STRINGS) & df[col].notna()
            if mask.any():
                replaced[col] = int(mask.sum())
                df.loc[mask, col] = pd.NA
    return df, replaced


def load_outcomes(path: str = OUTCOMES_PATH, ok_only: bool = True) -> pd.DataFrame:
    """Load the settlement labels; by default only status == 'ok' rows.

    won_up is 1.0/0.0 (Polymarket's resolution; see ``label_source``).
    Ambiguous markets have NaN and are dropped when ok_only.

    ``ok_only=False`` keeps every row with ``won_up`` as float (NaN where
    ambiguous), safe for a left join: filter with
    ``.dropna(subset=['won_up'])`` rather than counting unlabeled rows as
    misses. ``status != 'ok'`` exactly when ``won_up`` is NaN, because
    ``fetch_gamma_resolutions.assemble_outcomes`` derives status from the
    label.
    """
    out = pd.read_parquet(path)
    out, _ = normalize_placeholders(out)
    if ok_only:
        out = out[(out["status"] == "ok") & out["won_up"].notna()].copy()
        out["won_up"] = out["won_up"].astype(np.int8)
    return out


