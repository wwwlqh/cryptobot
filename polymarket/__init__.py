"""Polymarket package root.

``backtest`` is the research code, ``src/`` holds the live bot (``cryptobot``)
and the Polymarket adapter (``cryptobot_polymarket``), ``live/`` the deployed
strategy config and systemd units, and ``infra/`` the deploy scripts and env
template.

Only ``backtest`` is imported through this namespace; the bot packages are
installed from ``src`` and imported as top-level ``cryptobot`` and
``cryptobot_polymarket``.

    from polymarket.backtest import BT_DATA_DIR
    from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA

The repo root must be on ``sys.path``: pytest adds it via ``pythonpath =
["polymarket/src", "."]`` in pyproject, and command-line use is ``python -m
polymarket.backtest.<subpackage>.<module>`` from the repo root.
"""
