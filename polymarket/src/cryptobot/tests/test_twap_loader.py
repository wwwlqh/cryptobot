from __future__ import annotations

import json
import hashlib
from dataclasses import replace

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier

from cryptobot.strategy.twap_loader import (
    FEATURE_COLUMNS,
    load_twap_symmetric_model,
    validate_twap_coin_universe,
    validate_twap_sleeves,
)
from cryptobot.strategy.portfolio_loader import load_sleeves
from cryptobot import config


def _artifacts(tmp_path):
    x = pd.DataFrame(
        [
            [0.70, 0.31, 0.01, 0, 5, 1, 4.5, 1, 2, 1.0, 1.0],
            [0.69, 0.32, 0.01, 0, 5, 0, 4.5, 1, 2, -1.0, 1.0],
            [0.75, 0.26, 0.01, 1, 15, 1, 14.5, 1, 2, 2.0, 2.0],
            [0.74, 0.27, 0.01, 1, 15, 0, 14.5, 1, 2, -2.0, 2.0],
        ],
        columns=FEATURE_COLUMNS,
    )
    model = HistGradientBoostingClassifier(
        max_iter=2,
        random_state=42,
        categorical_features=["coin_id"],
    ).fit(
        x, np.array([1, 0, 1, 0])
    )
    model_path = tmp_path / "model.joblib"
    manifest_path = tmp_path / "manifest.json"
    joblib.dump({"5m": model, "15m": model}, model_path)
    manifest = {
        "artifact": "twap_symmetric_v1",
        "contract_version": 3,
        "target": "win_twap_s",
        "feature_columns": {"5m": FEATURE_COLUMNS, "15m": FEATURE_COLUMNS},
        "chainlink_window_seconds": {"5m": 60, "15m": 60},
        "decision_window_seconds": {"5m": 150, "15m": 150},
        "model_count": 2,
        "model_routing": {"5m": "pooled_all4", "15m": "pooled_all4"},
        "categorical_features": {"5m": ["coin_id"], "15m": ["coin_id"]},
        "live_coins": ["btc", "eth", "sol", "xrp"],
        "live_feature_contract": {
            "name": "binance_trailing_twap_centered_chainlink_basis_v2",
            "binance_window_seconds": {"5m": 60, "15m": 60},
            "basis_residual_clip_log": 0.0001,
            "basis_center_log": {
                "btc": {"5m": 0.0007, "15m": 0.0007},
                "eth": {"5m": 0.0007, "15m": 0.0007},
                "sol": {"5m": 0.0007, "15m": 0.0007},
                "xrp": {"5m": 0.0007, "15m": 0.0007},
            },
        },
        "execution_by_interval": {
            interval: {
                "ask_lo": 0.55,
                "ask_hi": 0.85,
                "gate": 0.10,
                "book_consumption_max": 0.06,
            }
            for interval in ("5m", "15m")
        },
        "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return model_path, manifest_path, manifest


def test_load_twap_symmetric_model_validates_contract(tmp_path):
    model_path, manifest_path, _ = _artifacts(tmp_path)

    artifact = load_twap_symmetric_model(
        model_path=model_path, manifest_path=manifest_path
    )

    assert set(artifact.pipelines) == {"5m", "15m"}
    assert artifact.columns_for("5m") == FEATURE_COLUMNS


def test_load_twap_symmetric_model_rejects_wrong_chainlink_window_map(tmp_path):
    model_path, manifest_path, manifest = _artifacts(tmp_path)
    manifest["chainlink_window_seconds"]["5m"] = 30
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="60s Chainlink"):
        load_twap_symmetric_model(
            model_path=model_path, manifest_path=manifest_path
        )


def test_load_twap_symmetric_model_rejects_wrong_decision_window_map(tmp_path):
    model_path, manifest_path, manifest = _artifacts(tmp_path)
    manifest["decision_window_seconds"]["5m"] = 160
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="T-150 windows for both intervals"):
        load_twap_symmetric_model(
            model_path=model_path, manifest_path=manifest_path
        )


def test_load_twap_symmetric_model_rejects_missing_live_basis_centre(tmp_path):
    model_path, manifest_path, manifest = _artifacts(tmp_path)
    del manifest["live_feature_contract"]["basis_center_log"]["sol"]["15m"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="incomplete for sol"):
        load_twap_symmetric_model(
            model_path=model_path, manifest_path=manifest_path
        )


def test_production_twap_config_matches_frozen_contract(monkeypatch, tmp_path):
    repo = config._REPO_ROOT
    monkeypatch.setattr(config, "SUBMINUTE_ENABLED", True)
    monkeypatch.setattr(config, "MAX_DRAWDOWN_HALT_FRAC", 0.40)
    config_path = (
        repo / "polymarket" / "live" / "config" / "twap_symmetric_live.json"
    )
    portfolio = load_sleeves(config_path)
    _, _, manifest = _artifacts(tmp_path)
    manifest["sleeves"] = {
        "5m": {
            "threshold": 0.10, "ask_bucket": "band:0.55:0.85",
            "side_filter": "both", "book_consumption_max": 0.06,
        },
        "15m": {
            "threshold": 0.10, "ask_bucket": "band:0.55:0.85",
            "side_filter": "both", "book_consumption_max": 0.06,
        },
    }

    validate_twap_sleeves(portfolio.sleeves, manifest)
    assert {s.interval for s in portfolio.sleeves} == {"5m", "15m"}
    assert {s.interval: s.threshold for s in portfolio.sleeves} == pytest.approx(
        {"5m": 0.10, "15m": 0.10}
    )
    assert {
        s.interval: s.book_consumption_max for s in portfolio.sleeves
    } == pytest.approx({"5m": 0.06, "15m": 0.06})
    raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    assert raw_config["coin_universe"] == ["BTC", "ETH", "SOL", "XRP"]
    assert {
        float(sleeve["config"]["capital_frac"])
        for sleeve in raw_config["sleeves"]
    } == {0.01}
    assert {
        float(sleeve["config"]["stake_frac"])
        for sleeve in raw_config["sleeves"]
    } == {0.01}
    offsets = {s.interval: s.checkpoint_offset_set for s in portfolio.sleeves}
    assert offsets["5m"] == tuple(range(150, 300))
    assert offsets["15m"] == tuple(range(750, 900))
    assert {s.capital_frac for s in portfolio.sleeves} == {0.01}
    assert portfolio.max_drawdown_halt_frac == pytest.approx(0.40)

    drifted = [
        replace(
            sleeve,
            book_consumption_max=float(sleeve.book_consumption_max) + 0.01,
        )
        for sleeve in portfolio.sleeves
    ]
    with pytest.raises(ValueError, match="book_consumption_max"):
        validate_twap_sleeves(drifted, manifest)


def test_twap_coin_universe_fails_closed_on_extra_or_missing_coin():
    manifest = {"live_coins": ["btc", "eth"]}
    validate_twap_coin_universe(("BTC", "ETH"), ("eth", "btc"), manifest)

    with pytest.raises(ValueError, match="SOL"):
        validate_twap_coin_universe(
            ("BTC", "ETH", "SOL"), ("BTC", "ETH"), manifest
        )
    with pytest.raises(ValueError, match=r"live contract \[\]"):
        validate_twap_coin_universe(("BTC", "ETH"), (), manifest)

    with pytest.raises(ValueError, match="manifest"):
        validate_twap_coin_universe(
            ("BTC", "ETH"), ("BTC", "ETH"), {"live_coins": ["BTC"]}
        )


def test_pooled_classifier_accepts_all_coin_categories():
    x = pd.DataFrame(
        [
            [0.70, 0.31, 0.01, 0, 15, 1, 14.0, 1, 2, 1.0, 1.0],
            [0.69, 0.32, 0.01, 1, 15, 0, 14.0, 1, 2, -1.0, 1.0],
            [0.75, 0.26, 0.01, 0, 15, 1, 14.5, 1, 2, 2.0, 2.0],
            [0.74, 0.27, 0.01, 1, 15, 0, 14.5, 1, 2, -2.0, 2.0],
        ],
        columns=FEATURE_COLUMNS,
    )
    model = HistGradientBoostingClassifier(
        max_iter=2,
        random_state=42,
        categorical_features=["coin_id"],
    ).fit(
        x, np.array([1, 0, 1, 0]),
    )
    probabilities = model.predict_proba(x)
    assert probabilities.shape == (4, 2)
    assert np.isfinite(probabilities).all()
    assert bool(model.is_categorical_[FEATURE_COLUMNS.index("coin_id")])
