"""Render the README charts from analysis/data/trades.csv.

Usage:
    python analysis/make_charts.py
"""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).parent
OUT = HERE / "charts"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "text.color": INK,
    "xtick.color": INK_2, "ytick.color": INK_2, "font.size": 10,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
})


def usd(v: float) -> str:
    return f"-${-v:,.2f}" if v < 0 else f"${v:,.2f}"


def load() -> pd.DataFrame:
    df = pd.read_csv(HERE / "data" / "trades.csv", parse_dates=["bet_time_utc"])
    return df[df["outcome"] != "Pending"]


def cumulative_pnl(df: pd.DataFrame) -> None:
    cum = df["pnl_usd"].cumsum()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(df["bet_time_utc"], cum, color=BLUE, linewidth=2)
    ax.axhline(0, color=INK_2, linewidth=0.8)
    end = cum.iloc[-1]
    ax.annotate(usd(end), (df["bet_time_utc"].iloc[-1], end),
                xytext=(6, 0), textcoords="offset points", va="center", color=INK)
    ax.set_title("Cumulative net P&L, 5,384 live trades", loc="left", fontweight="bold")
    ax.set_ylabel("USD")
    ax.grid(axis="x", visible=False)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(OUT / "cumulative_pnl.png", dpi=160)


def pnl_by_coin(df: pd.DataFrame) -> None:
    main = ["BTC", "ETH", "SOL", "XRP"]
    coin = df["coin"].where(df["coin"].isin(main), "Other")
    g = df.groupby(coin).agg(pnl=("pnl_usd", "sum"), n=("pnl_usd", "size"))
    g = g.reindex(main + ["Other"]).iloc[::-1]
    fig, ax = plt.subplots(figsize=(7, 3.4))
    ax.barh(g.index, g["pnl"], color=BLUE, height=0.55)
    ax.axvline(0, color=INK_2, linewidth=0.8)
    for y, (pnl, n) in enumerate(zip(g["pnl"], g["n"])):
        ha, dx = ("left", 4) if pnl >= 0 else ("right", -4)
        ax.annotate(f"{usd(pnl)}  ({n:,} trades)", (pnl, y), xytext=(dx, 0),
                    textcoords="offset points", ha=ha, va="center", color=INK)
    lo, hi = g["pnl"].min(), g["pnl"].max()
    ax.set_xlim(lo * 1.9, max(hi, 0) * 3 + 10)
    ax.set_title("Net P&L by coin", loc="left", fontweight="bold")
    ax.set_xlabel("USD")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    fig.savefig(OUT / "pnl_by_coin.png", dpi=160)


def calibration(df: pd.DataFrame) -> None:
    m = df[df["model_prob"].notna()].copy()
    m["bucket"] = pd.cut(m["model_prob"], [0.6, 0.7, 0.8, 0.9, 1.0],
                         labels=["60-70%", "70-80%", "80-90%", "90-100%"])
    g = m.groupby("bucket", observed=True).agg(
        model=("model_prob", "mean"), price=("market_price", "mean"),
        actual=("model_correct", "mean"), n=("model_prob", "size"))
    x = range(len(g))
    fig, ax = plt.subplots(figsize=(8, 4))
    series = [("model", "Model's predicted win chance", BLUE),
              ("price", "Market price paid (break-even)", ORANGE),
              ("actual", "Actual hit rate", AQUA)]
    for key, label, color in series:
        ax.plot(x, g[key] * 100, color=color, linewidth=2, marker="o", markersize=8,
                markeredgecolor=SURFACE, markeredgewidth=2, label=label)
    for i, v in enumerate(g["actual"] * 100):
        ax.annotate(f"{v:.0f}%", (i, v), xytext=(0, -16), textcoords="offset points",
                    ha="center", color=INK)
    ax.set_xticks(list(x), [f"{b}\n{n:,} trades" for b, n in zip(g.index, g["n"])])
    ax.set_ylim(50, 96)
    ax.set_ylabel("%")
    ax.set_title("The model was overconfident: actual hit rate sat below the price paid",
                 loc="left", fontweight="bold")
    ax.legend(frameon=False, loc="upper left")
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(OUT / "calibration.png", dpi=160)


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    trades = load()
    cumulative_pnl(trades)
    pnl_by_coin(trades)
    calibration(trades)
    print(f"charts -> {OUT}")
