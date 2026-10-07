"""Canonical pmdata cleaning + audit helpers for the notebook pipeline.

Single source of truth: every notebook and the option-3 pool builder import from
here, so the data is never cleaned twice with different rules.

Known raw-data defects handled (found in the 2026-07-07 audit):
- placeholder strings ("-", "--", em-dash, "") in any object/string column -> NaN
- schema drift across snapshot parquets: 3,466 old-schema files (all Jun 10-13,
  from a 2026-06-14 download run) lacked ``winning_outcome`` -> FIXED 2026-07-07
  via ``pmdata_download.py --backfill-outcome``; timestamp dtype is ms in some
  files, us in others; book arrays are object-of-str in some,
  large_list<double> in others
- labels: ``won_up`` = the Gamma Chainlink resolution (venue-official, FULL
  history via Gamma ``/markets?slug=``), fetched by
  ``polymarket/backtest/fetch/fetch_gamma_resolutions.py`` -> ``gamma_resolutions/`` and
  joined into ``pmdata_outcomes.parquet`` (``label_source == 'gamma'``). The old
  pmdata Chainlink-STREAM path (``build_chainlink_outcomes.py`` +
  ``chainlink_streams/``) was retired 2026-07-12 — the stream held only ~1 month
  of coverage, so its ``cl_won_up`` column is now vestigial.
  Unlabeled: status == 'ambiguous' -> excluded from any pool
- degenerate books: ask <= 0, ask >= 1, and crossed books (spread < -0.02)
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

from polymarket.backtest import BT_DATA_DIR

# Absolute (was CWD-relative "backtest/data/pmdata", which silently resolved  (legacy-path-ok: describes the OLD bug)
# to nothing whenever a driver ran from anywhere but the repo root).
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

    won_up is 1.0/0.0 (Gamma Chainlink resolution -- see ``label_source``);
    ambiguous markets carry NaN and are dropped when ok_only.

    ``ok_only=False`` is the NA-PRESERVING, left-join-safe form: every row
    survives with ``won_up`` as float (NaN where ambiguous), so a downstream
    ``.dropna(subset=['won_up'])`` filters instead of the caller inflating
    denominators with zero-hit rows. Status and label are not independent —
    ``fetch_gamma_resolutions.assemble_outcomes``, the SOLE writer of
    pmdata_outcomes.parquet, derives ``status`` FROM the label
    (``won_up.notna().map({True: 'ok', False: 'ambiguous'})``), so
    ``status != 'ok'`` <=> ``won_up`` is NaN, always. That equivalence is why
    ``lib/outcome_labels.load_outcome_labels`` — a second loader that existed
    only to re-apply the NA mask under the decision_table's
    ``won_up_polymarket``/``poly_settle_status`` column names — was retired
    2026-08-01 (phase 6) rather than kept alongside this one.
    """
    out = pd.read_parquet(path)
    out, _ = normalize_placeholders(out)
    if ok_only:
        out = out[(out["status"] == "ok") & out["won_up"].notna()].copy()
        out["won_up"] = out["won_up"].astype(np.int8)
    return out


def _coerce_price_array(value) -> np.ndarray | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, np.ndarray):
        return value.astype(np.float64, copy=False)
    if isinstance(value, (list, tuple)):
        return np.asarray(value, dtype=np.float64)
    if isinstance(value, str):
        # object-of-str variant: "[0.54 0.55 ...]" possibly with newlines
        return np.fromstring(value.strip("[]"), sep=" ", dtype=np.float64)
    return np.asarray(value, dtype=np.float64)


# RETIRED 2026-08-03: ``flag_degenerate_books`` (QA flags for a pool builder),
# ``SnapshotAudit`` / ``audit_snapshot_dir`` / ``audit_report_frame`` (the raw
# ``poly_snapshot/`` tree audit) and ``SNAPSHOT_ROOT``. That tree was deleted
# 2026-07-31 with no rebuild path, so every entry point could only fail on a
# missing directory, and none of the four had a Python caller — only prose
# references in ``cryptobot_polymarket/book_recorder.py`` docstrings, which
# describe the per-second layout those audits used to check. The
# ``qa_book_crossed`` column the pool filter still reads was baked into the
# option3 pool parquets at build time and does not come from here. Recover with
# ``git show <this commit>^:polymarket/backtest/lib/pmdata_clean.py``.
