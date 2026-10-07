"""Polymarket venue namespace.

Everything Polymarket-specific lives under this package: ``backtest`` (the
research tree), ``live/`` (the ``cryptobot_polymarket`` adapter package under
``live/src/``, plus the deployed config contract and systemd units),
``infra/`` (deploy drivers, the env contract, cron). Recorder code and the
laptop-side pull scripts were retired; their historical paths remain in git
history and provenance notes only.

``backtest`` is the only subpackage imported *through this namespace*. The
adapter package is installed from ``live/src`` (pyproject
``package-dir``/``pythonpath``) and imported as the TOP-LEVEL
``cryptobot_polymarket``, never as ``polymarket.live.src.…``.

Import surface::

    from polymarket.backtest import BT_DATA_DIR
    from polymarket.backtest.engine.sweep_lib import POLY_FEE_THETA

The repo root must be on ``sys.path`` for that to resolve: pytest gets it via
``pythonpath = ["polymarket/src", "."]`` (pyproject), and CLI usage via
``python -m polymarket.backtest.<subpackage>.<module>`` run from the repo root.
"""
