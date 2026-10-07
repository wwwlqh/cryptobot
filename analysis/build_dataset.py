"""Export the live trade ledger to an anonymised CSV for Power BI.

Usage:
    python analysis/build_dataset.py path/to/ledger_snapshot.db

Reads the bot's SQLite `ledger` table and writes analysis/data/trades.csv.
Wallet IDs, order IDs, market/token IDs and config hashes are dropped.
"""
import sqlite3
import sys
from pathlib import Path

import pandas as pd

OUT = Path(__file__).parent / "data" / "trades.csv"

STRATEGY = {
    "subminute_5m": ("Prediction model", "5m"),
    "subminute_15m": ("Prediction model", "15m"),
    "nl_5m": ("Prediction model (v1)", "5m"),
    "nl_15m": ("Prediction model (v1)", "15m"),
    "preopen_5m": ("Pre-open", "5m"),
}
OUTCOME = {"WIN": "Win", "LOSS": "Loss", "CANCELLED": "Stop-loss exit", "PENDING": "Pending"}


def build(db_path: str) -> pd.DataFrame:
    with sqlite3.connect(db_path) as con:
        raw = pd.read_sql("SELECT * FROM ledger ORDER BY bet_ts, id", con)

    df = pd.DataFrame()
    df["trade_id"] = range(1, len(raw) + 1)
    ts = pd.to_datetime(raw["bet_ts"])
    df["bet_time_utc"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    df["bet_date"] = ts.dt.strftime("%Y-%m-%d")
    df["bet_hour_utc"] = ts.dt.hour
    df["coin"] = raw["coin"]
    df["strategy"] = raw["sleeve"].map(lambda s: STRATEGY[s][0])
    df["window"] = raw["sleeve"].map(lambda s: STRATEGY[s][1])
    df["side"] = raw["side"].map({"YES": "Up", "NO": "Down"})
    df["model_prob"] = raw["model_p"].round(4)
    df["market_price"] = raw["p_market"].round(4)
    df["model_edge"] = (raw["model_p"] - raw["p_market"]).round(4)
    # Pre-open logs a different score in model_p, so it is not a win probability.
    preopen = df["strategy"].eq("Pre-open")
    df.loc[preopen, ["model_prob", "model_edge"]] = None
    df["fill_price"] = raw["live_filled_price"].fillna(raw["fill_price"]).round(4)
    df["stake_usd"] = raw["bet_size"].round(4)
    df["is_top_up"] = raw["is_topup"].astype(int)
    df["outcome"] = raw["outcome"].map(OUTCOME)
    df["market_went_up"] = raw["won_up"].astype("Int64")
    # The model called the direction right if its side matches how the window resolved.
    up = raw["side"].eq("YES")
    resolved = raw["won_up"].notna()
    df["model_correct"] = (up == raw["won_up"].eq(1)).astype("Int64").where(resolved)
    df["pnl_usd"] = raw["pnl_net"].round(4)
    df["seconds_into_window"] = raw["offset_sec"]
    # Account-level equity curve, in trade order.
    cum = df["pnl_usd"].fillna(0).cumsum()
    df["cum_pnl_usd"] = cum.round(4)
    df["drawdown_usd"] = (cum - cum.cummax().clip(lower=0)).round(4)
    return df


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    trades = build(sys.argv[1])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    trades.to_csv(OUT, index=False)
    print(f"wrote {len(trades):,} trades -> {OUT}")
