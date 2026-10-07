"""Export the v2 "step1last" PRODUCTION models in ``checkpoint_v1`` format.

The live sub-minute scorer loads ``checkpoint_v1`` (see
``src/cryptobot/strategy/checkpoint_loader.py``). The retired checkpoint
exporter had two blockers for the v2 promotion:

  * GAP A — it stages only folds A..D and drops fold E + any fresher rows, so
    the frozen model never sees the most recent regime.
  * GAP B — its 15m sleeve carries the 5-feature exp044 manifest, not the
    11-feature set the v2 spec bets on.

This script sidesteps both by re-using the CANONICAL research trainer
(``polymarket/backtest/lib/pool_study.py`` — the same ``fit_rf_features`` /
``fit_hgb`` that produced ``scored_11set_truth.parquet``) and writing the
``checkpoint_v1`` artifacts directly:

  data/models/checkpoint_v1/checkpoint_rf_v1.joblib    {(coin,interval,sec): RandomForestClassifier}
  data/models/checkpoint_v1/checkpoint_hgb_v1.joblib   {interval: sklearn Pipeline}  (both carry FEATURES_11)
  data/models/checkpoint_v1/checkpoint_v1_manifest.json

Design decisions (see W1 of the promotion plan):
  * FULL expanding window through today: RF slices + HGB are fit on ALL folds
    (A..E and any fresher rows), no A..D stop. A freshness gate asserts the
    newest training row is within ``--fresh-window-hours`` of build time.
  * 11-feature set on BOTH intervals (``FEATURES_11``), fixing GAP B.
  * By default a SINGLE cross-interval HGB pipeline (exactly what
    ``pool_study.score_wfo`` fits — ``interval_min`` is one of the 11 features)
    is stored under BOTH the "5m" and "15m" keys. This preserves bet-for-bet
    parity with ``scored_11set_truth.parquet`` (the artifact the edge was
    validated on). ``--per-interval-hgb`` instead fits two interval-scoped
    pipelines (structurally distinct models; breaks that parity — opt-in only).
  * Memory-bounded per feedback_wfo_memory_budget: ``load_pool`` reads only
    LIGHT_COLUMNS (no fill ladders); the HGB fit is capped to a representative
    subsample (RF stays on the full staged set — 2 columns/slice, low RAM).

HONEST LIMITS (unchanged from the 2026-07-05 audit): a single frozen model is
NOT the per-WFO-window OOS ensemble and cannot reproduce the archived $170k; it
restores DECISION parity, not FILL parity (the 250ms Polymarket taker delay is
irreducible). Sub-minute remains adversely selected/unfillable per the standing
research verdict — the live fill-rate breaker is the arbiter.

Run (full production build — heavy, the supervisor runs this):
    python -m polymarket.backtest.training.export_models

Validate the plumbing end-to-end quickly:
    python -m polymarket.backtest.training.export_models --smoke
"""
from __future__ import annotations

import argparse
import glob as _glob
import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]  # repo root (legacy-path-ok: pre-import sys.path bootstrap)
for _p in (ROOT, ROOT / "core" / "src"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
from polymarket.backtest import BT_RUNS_DIR, REPO_ROOT  # noqa: E402

from polymarket.backtest.lib import pmdata_clean as pc  # noqa: E402  fresh outcomes (mirror load_pool)
from polymarket.backtest.lib import pool_study as ps  # noqa: E402  canonical research trainer/helpers
from polymarket.backtest.training import checkpoint as cg  # noqa: E402

GRIDS = ("step1last",)
FEATURES = ps.FEATURES_11
# Repo-root ``data/models/`` — the LIVE model tree (config.DATA_ROOT/models),
# NOT the research substrate. deploy.sh ships $REPO_ROOT/data/models; writing
# these under the BT_DATA_DIR shim would put them where no reader looks.
OUT_DEFAULT = REPO_ROOT / "data" / "models" / "checkpoint_v1"
POOL_DIR = BT_RUNS_DIR / "option3_pool"
EMBARGO_S = ps.EMBARGO_S

# Production caps inherited from the retired exporter. RF is fit on the full
# staged set (cheap, 2 cols/slice); the HGB side-expansion is what needs a cap.
HGB_SUBSAMPLE_ROWS = 6_000_000
FRESH_WINDOW_HOURS_DEFAULT = 24.0


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #
def _pool_part_files(grid: str = "step1last", convention: str = "option3") -> list[str]:
    part_dir = POOL_DIR / f"pool_{grid}_{convention}"
    paths = sorted(_glob.glob(str(part_dir / "part_*.parquet")))
    legacy = POOL_DIR / f"pool_{grid}_{convention}.parquet"
    if not paths and legacy.exists():
        paths = [str(legacy)]
    if not paths:
        raise FileNotFoundError(f"no pool parts under {part_dir}")
    return paths


def _limited_pool(part_files: list[str], *, holdout_visible: bool) -> pd.DataFrame:
    """Memory-bounded load of an EXPLICIT part-file list, mirroring
    ``pool_study.load_pool``'s dedup + fresh-outcome re-merge + E-hide. Used only
    by ``--smoke`` and the reduced equivalence test; the full production build
    goes through ``ps.load_pool`` verbatim for exact parity."""
    frames = [pd.read_parquet(p, columns=ps.LIGHT_COLUMNS) for p in part_files]
    pool = pd.concat(frames, ignore_index=True)
    if "offset_sec" in pool.columns:
        pool = pool.drop_duplicates(subset=["market_slug", "offset_sec"], keep="last").copy()
    if "qa_book_crossed" in pool.columns:
        pool = pool[~pool["qa_book_crossed"]].copy()
    fresh = pc.load_outcomes(pc.OUTCOMES_PATH, ok_only=True)
    pool["won_up"] = pool["market_slug"].map(fresh.set_index("market_slug")["won_up"])
    pool = pool.dropna(subset=["won_up"]).copy()
    pool["won_up"] = pool["won_up"].astype(np.int8)
    if not holdout_visible:
        pool = pool[pool["fold"] != ps.HOLDOUT_FOLD].copy()
    return pool


def load_side_frame(*, part_files: list[str] | None = None,
                    holdout_visible: bool = True) -> pd.DataFrame:
    """One-row-per-(candidate, side) frame. ``part_files=None`` => full
    production load via ``ps.load_pool``; else the limited loader above."""
    if part_files is None:
        pool = ps.load_pool(grids=GRIDS, holdout_visible=holdout_visible)
    else:
        pool = _limited_pool(part_files, holdout_visible=holdout_visible)
    return ps.side_frame(pool)


# --------------------------------------------------------------------------- #
# Fitting (shared by production + equivalence paths)
# --------------------------------------------------------------------------- #
def _attach_rf(sf: pd.DataFrame, *, seed: int, rf_n_jobs: int
               ) -> tuple[dict, pd.DataFrame, dict]:
    """Fit per-(coin, interval, sec) RF slices on side_up==1 rows and attach
    ``model_p_side`` / ``rf_edge`` — the EXACT semantics of
    ``pool_study.fit_rf_features``, but also returning the estimators dict
    (which ``fit_rf_features`` discards) so we can serialize it."""
    args = SimpleNamespace(rf_n_estimators=200, rf_n_jobs=rf_n_jobs, seed=seed)
    params = cg._rf_params(args)
    estimators, rf_diag = cg._fit_rf_slices(sf[sf["side_up"] == 1], params)
    probs = np.asarray(cg._rf_probs_checkpoint(sf, estimators), dtype=float)
    out = sf.copy()
    out["model_p_side"] = np.where(out["side_up"] == 1, probs, 1.0 - probs)
    out["rf_edge"] = out["model_p_side"] - out["ask"] - ps.fee(out["ask"].to_numpy())
    return estimators, out, rf_diag


def _fit_hgb_pipelines(train: pd.DataFrame, *, seed: int,
                       hgb_subsample_rows: int | None, per_interval: bool
                       ) -> tuple[dict, dict]:
    """Fit the checkpoint_v1 HGB dict {interval: Pipeline}. Default: one
    cross-interval pipeline (== pool_study.score_wfo) shared under both keys."""
    train = train.dropna(subset=["model_p_side"]).copy()
    n_full = int(len(train))
    if hgb_subsample_rows and n_full > hgb_subsample_rows:
        train = train.sample(n=hgb_subsample_rows, random_state=seed).reset_index(drop=True)
    if per_interval:
        pipelines: dict[str, object] = {}
        for iv in ("5m", "15m"):
            sub = train[train["interval"] == iv]
            if sub.empty:
                raise ValueError(f"no {iv} rows to fit the HGB — check the pool span")
            pipelines[iv] = ps.fit_hgb(sub, FEATURES, seed=seed)
    else:
        pipe = ps.fit_hgb(train, FEATURES, seed=seed)
        pipelines = {"5m": pipe, "15m": pipe}
    diag = {"hgb_train_rows_full": n_full, "hgb_subsample_rows": int(len(train)),
            "per_interval_hgb": bool(per_interval)}
    return pipelines, diag


def _sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# Public builders
# --------------------------------------------------------------------------- #
def build_production_models(
    out_dir: Path | str = OUT_DEFAULT,
    *,
    part_files: list[str] | None = None,
    seed: int = 42,
    rf_n_jobs: int = 4,
    hgb_subsample_rows: int | None = HGB_SUBSAMPLE_ROWS,
    per_interval_hgb: bool = False,
    fresh_window_hours: float = FRESH_WINDOW_HOURS_DEFAULT,
    require_fresh: bool = True,
) -> dict:
    """Fit RF slices + HGB on the FULL expanding window and write the three
    ``checkpoint_v1`` artifacts. Returns the manifest dict."""
    t0 = time.time()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    sf = load_side_frame(part_files=part_files, holdout_visible=True)
    if sf.empty:
        raise SystemExit("[export] loaded 0 rows — aborting")
    build_ts = int(time.time())
    max_entry_ts = int(sf["entry_ts"].max())
    age_h = (build_ts - max_entry_ts) / 3600.0

    # Acceptance #3: production must train through ~today.
    fresh_ok = age_h <= fresh_window_hours
    if require_fresh and not fresh_ok:
        raise SystemExit(
            f"[export] STALE POOL: newest training entry_ts {max_entry_ts} "
            f"({pd.to_datetime(max_entry_ts, unit='s', utc=True)}) is {age_h:.1f}h "
            f"before build time, > --fresh-window-hours={fresh_window_hours}. "
            "Rebuild the option3_pool through today, or pass --no-fresh-check."
        )

    estimators, train, rf_diag = _attach_rf(sf, seed=seed, rf_n_jobs=rf_n_jobs)
    if not estimators:
        raise SystemExit("[export] fit 0 RF slices — aborting")
    pipelines, hgb_diag = _fit_hgb_pipelines(
        train, seed=seed, hgb_subsample_rows=hgb_subsample_rows, per_interval=per_interval_hgb
    )

    rf_path = out_dir / "checkpoint_rf_v1.joblib"
    hgb_path = out_dir / "checkpoint_hgb_v1.joblib"
    joblib.dump(estimators, rf_path)
    joblib.dump(pipelines, hgb_path)

    ckpt_avail: dict[str, list[int]] = {}
    for (coin, interval, sec) in estimators:
        ckpt_avail.setdefault(f"{coin}:{interval}", []).append(int(sec))
    for k in ckpt_avail:
        ckpt_avail[k] = sorted(ckpt_avail[k])

    manifest = {
        "artifact": "checkpoint_v1",
        "built_from_grids": list(GRIDS),
        "feature_key": "11set",
        "coins": sorted({k.split(":")[0] for k in ckpt_avail}),
        "intervals": sorted(pipelines.keys()),
        "n_rf_slices": len(estimators),
        # Both intervals carry the 11-set (fixes GAP B). Loader cross-checks
        # each pipeline's n_features_in_ == len(feature_columns[iv]).
        "feature_columns": {"5m": list(FEATURES), "15m": list(FEATURES)},
        # Serve-time bet-selection contract per sleeve. main.py validates the
        # runtime v2 sleeves against THIS block (not hgb_v1's baseline entries —
        # checkpoint sleeves are served by checkpoint_v1). Values mirror
        # v2_step1last_live.json; a config/manifest mismatch fails boot closed.
        "sleeves": {
            "5m": {"threshold": 0.03, "ask_bucket": "band:0.65:0.85",
                   "book_consumption_max": 0.3, "side_filter": "both",
                   "feature_groups": "rf,price,identity,time"},
            "15m": {"threshold": 0.03, "ask_bucket": "band:0.65:0.85",
                    "book_consumption_max": 0.06, "side_filter": "both",
                    "feature_groups": "rf,price,identity,time"},
            "nl_5m": {"threshold": 0.01, "ask_bucket": "band:0.85:0.92",
                      "book_consumption_max": 0.3, "side_filter": "both",
                      "feature_groups": "rf,price,identity,time"},
            "nl_15m": {"threshold": 0.01, "ask_bucket": "band:0.85:0.92",
                       "book_consumption_max": 0.06, "side_filter": "both",
                       "feature_groups": "rf,price,identity,time"},
        },
        "sleeves_note": (
            "Generated by polymarket.backtest.training.export_models. Label-keyed contracts "
            "cover MID (5m/15m) and NL (nl_5m/nl_15m); runtime validates by "
            "sleeve label first and interval fallback second."
        ),
        "checkpoint_secs_by_coin_interval": ckpt_avail,
        "staged_rows": int((sf["side_up"] == 1).sum()),
        "hgb_train_rows_full": hgb_diag["hgb_train_rows_full"],
        "hgb_subsample_rows": hgb_diag["hgb_subsample_rows"],
        "per_interval_hgb": hgb_diag["per_interval_hgb"],
        "rf_hyperparams": cg._rf_params(
            SimpleNamespace(rf_n_estimators=200, rf_n_jobs=rf_n_jobs, seed=seed)
        ),
        "rf_diag_n_slices": rf_diag["n_slices"],
        "rf_skipped_slices": len(rf_diag.get("skipped_slices", [])),
        "sample_weight_mode": "recent_chunk_boost",
        "seed": int(seed),
        "build_ts": build_ts,
        "build_dt_utc": pd.to_datetime(build_ts, unit="s", utc=True).isoformat(),
        "max_entry_ts": max_entry_ts,
        "max_entry_dt_utc": pd.to_datetime(max_entry_ts, unit="s", utc=True).isoformat(),
        "train_age_hours": round(age_h, 2),
        "fresh_window_hours": float(fresh_window_hours),
        "fresh_ok": bool(fresh_ok),
        "model_sha1": _sha1(hgb_path),
        "rf_v1_model_sha1": _sha1(rf_path),
        "frozen_note": (
            "v2 step1last production model. Single frozen model (NOT the per-WFO "
            "OOS ensemble): RF slices on the FULL staged step1last set, HGB on a "
            f"{hgb_diag['hgb_subsample_rows']:,}-row representative subsample, "
            "11-feature set on BOTH intervals (fixes GAP B), trained through the "
            "newest available fold (fixes GAP A). Restores DECISION parity with "
            "scored_11set_truth.parquet, not FILL parity (250ms taker delay is "
            "irreducible; the live fill-rate breaker is the arbiter)."
        ),
        "elapsed_s": round(time.time() - t0, 1),
    }
    (out_dir / "checkpoint_v1_manifest.json").write_text(
        json.dumps(cg._json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )
    print(
        f"[export] DONE in {time.time() - t0:.0f}s -> {out_dir}\n"
        f"[export]   rf_slices={len(estimators)} hgb_intervals={sorted(pipelines)} "
        f"per_interval={per_interval_hgb}\n"
        f"[export]   staged_rows={manifest['staged_rows']:,} "
        f"hgb_subsample={hgb_diag['hgb_subsample_rows']:,}/{hgb_diag['hgb_train_rows_full']:,} "
        f"train_age={age_h:.1f}h fresh_ok={fresh_ok}",
        file=sys.stderr, flush=True,
    )
    return manifest


def build_research_equivalence_models(
    *,
    part_files: list[str] | None = None,
    seed: int = 42,
    feature_cols: list[str] | None = None,
    hgb_train_cap: int | None = None,
) -> pd.DataFrame:
    """Reproduce ``pool_study.score_wfo("11set")``'s E-fold scoring (train A..D
    embargoed, score E) using the SAME research primitives. Returns the scored
    E frame with the canonical ``keep`` columns + ``model_p``.

    This is the guard that the export machinery == the research trainer (feature
    construction, ``em_value``, ``sample_weight_mode`` all aligned). Used ONLY by
    the equivalence test — it is never serialized. ``part_files`` / ``hgb_train_cap``
    enable a fast REDUCED run; the supervisor's full run passes neither.
    """
    feature_cols = feature_cols or FEATURES
    sf = load_side_frame(part_files=part_files, holdout_visible=True)
    e_rows = sf[sf["fold"] == ps.HOLDOUT_FOLD]
    if e_rows.empty:
        raise ValueError("no fold-E rows loaded — pass part_files covering the E span")
    e0 = e_rows["entry_ts"].min()
    tr = sf[(sf["fold"].isin(["A", "B", "C", "D"])) & (sf["entry_ts"] < e0 - EMBARGO_S)]
    te = sf[sf["fold"] == ps.HOLDOUT_FOLD]
    if tr.empty:
        raise ValueError("no A..D training rows loaded — pass part_files covering early folds")

    both = ps.fit_rf_features(tr, pd.concat([tr, te], ignore_index=True), seed=seed)
    trx = both[both["fold"] != ps.HOLDOUT_FOLD].dropna(subset=["model_p_side"])
    tex = both[both["fold"] == ps.HOLDOUT_FOLD].dropna(subset=["model_p_side"]).copy()
    if hgb_train_cap and len(trx) > hgb_train_cap:
        trx = trx.sample(n=hgb_train_cap, random_state=seed).reset_index(drop=True)

    pipe = ps.fit_hgb(trx, feature_cols, seed=seed)
    tex["model_p"] = ps.score_hgb(pipe, tex, feature_cols)
    tex["pred_edge"] = tex["model_p"] - tex["ask"] - ps.fee(tex["ask"].to_numpy())
    tex["win_true"] = tex["win"].astype(np.int8)
    keep = [
        "fold", "market_slug", "entry_ts", "interval", "coin", "ask", "offset_sec",
        "range_z", "partial_ret_z_side", "reachable", "pred_edge", "side_fill_ask",
        "win_true", "model_p",
    ]
    return tex[keep].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--smoke", action="store_true",
                    help="tiny subsample end-to-end plumbing check (no full train)")
    ap.add_argument("--out", default=None,
                    help="output dir (default: data/models/checkpoint_v1; "
                         "smoke defaults to a scratch tempdir)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--rf-n-jobs", type=int, default=4)
    ap.add_argument("--hgb-subsample-rows", type=int, default=HGB_SUBSAMPLE_ROWS)
    ap.add_argument("--per-interval-hgb", action="store_true",
                    help="fit two interval-scoped HGBs (breaks truth parity; opt-in)")
    ap.add_argument("--fresh-window-hours", type=float, default=FRESH_WINDOW_HOURS_DEFAULT)
    ap.add_argument("--no-fresh-check", action="store_true",
                    help="skip the 'trained through ~today' staleness assertion")
    # smoke knobs
    ap.add_argument("--smoke-parts", type=int, default=6,
                    help="number of leading pool part-files to load in --smoke")
    ap.add_argument("--smoke-hgb-rows", type=int, default=60_000,
                    help="HGB subsample cap in --smoke")
    return ap.parse_args(argv)


def _run_smoke(args: argparse.Namespace) -> int:
    parts = _pool_part_files()[: args.smoke_parts]
    if not parts:
        print("[smoke] no pool parts found", file=sys.stderr)
        return 1
    out = Path(args.out) if args.out else Path(tempfile.mkdtemp(prefix="ckpt_v2_smoke_"))
    print(f"[smoke] parts={len(parts)} out={out}", file=sys.stderr, flush=True)
    manifest = build_production_models(
        out_dir=out,
        part_files=parts,
        seed=args.seed,
        rf_n_jobs=args.rf_n_jobs,
        hgb_subsample_rows=args.smoke_hgb_rows,
        per_interval_hgb=args.per_interval_hgb,
        require_fresh=False,  # smoke rides old parts on purpose
    )

    # Prove the artifacts satisfy every checkpoint_loader assertion.
    from cryptobot.strategy import checkpoint_loader as cl

    rf, hgb = cl.load_checkpoint_models(
        rf_path=out / "checkpoint_rf_v1.joblib",
        hgb_path=out / "checkpoint_hgb_v1.joblib",
        manifest_path=out / "checkpoint_v1_manifest.json",
    )
    assert set(hgb.feature_columns) == {"5m", "15m"}, hgb.feature_columns
    assert len(hgb.feature_columns["15m"]) == 11, "15m must carry the 11-set (GAP B)"
    assert len(hgb.feature_columns["5m"]) == 11
    assert manifest["n_rf_slices"] == len(rf.estimators)
    for iv in ("5m", "15m"):
        pipe = hgb.pipelines[iv]
        est = pipe.steps[-1][1] if hasattr(pipe, "steps") else pipe
        assert int(est.n_features_in_) == 11
    print(
        f"[smoke] OK — checkpoint_loader accepted the artifacts: "
        f"{len(rf.estimators)} RF slices, HGB intervals={sorted(hgb.pipelines)}, "
        f"11 features/interval. out={out}",
        file=sys.stderr, flush=True,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.smoke:
        return _run_smoke(args)
    build_production_models(
        out_dir=Path(args.out) if args.out else OUT_DEFAULT,
        seed=args.seed,
        rf_n_jobs=args.rf_n_jobs,
        hgb_subsample_rows=args.hgb_subsample_rows,
        per_interval_hgb=args.per_interval_hgb,
        fresh_window_hours=args.fresh_window_hours,
        require_fresh=not args.no_fresh_check,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
