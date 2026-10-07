"""Canonical research evaluators — the single source of truth the live decision
core is pinned to.

Moved here from ``research/notebooks/lib/`` (2026-07-12) to break the
research<->backtest import cycle: ``polymarket.backtest.scripts`` and the
``polymarket/src/cryptobot`` parity tests import these, so they belong under
``backtest``, not ``research``.

  - ``pool_study``          — the canonical bet builder / grader / trainer
                              (``FEATURES_11``, ``build_bets``, ``grade``,
                              ``fit_hgb``, ``SIZING_SPEC``/``stake_weights``).
  - ``pmdata_clean``        — canonical pmdata book cleaner + outcome loader.
  - ``nb_common``           — shared fee / P&L / interval maths (``THETA``,
                              ``fee``, ``breakeven_wr``, ``flat_pnl``,
                              ``wilson``). Deliberately sklearn-free.
  - ``forward_oos_regrade`` — the forward-OOS arbiter (frozen ``CONFIGS``); the
                              only figure that decides HOLD / RE-WFO / KILL.
  - ``tick_store``          — SQLite reader for the consolidated last-30s tick
                              substrate (``poly_tick_last30.db``) that replaced
                              the per-window parquet tree.

Retired 2026-08-03, all by literal caller census (`git grep -nw` per public
symbol, then per-module import check):

* ``recorder_store`` (+ ``nb_common.recorder_slug_index``) —
  ``data/recorder_snapshot/`` was deleted 2026-08-01 and the operator declined
  to restore it; the book recorder that fed it was retired 2026-08-02 with both
  venues' remote history deleted. No data, no writer.
* ``raw_replay`` (875 lines) — the raw-L2 counterfactual arms. Written for
  ``{5m,15m}/notebooks/04_live_parity.ipynb``; the repo has never contained a
  single ``.ipynb``. Its only importer was ``tests/test_raw_replay.py``.
* Most of ``nb_common`` (~590 lines) — same cause, same day. It survived only
  because ``test_raw_replay.py`` imported it. Four helpers had real callers and
  stayed.
* ``pool_study``'s sizing block and ``FEATURES_5`` / ``FEATURES_ASK_ONLY`` —
  superseded by the live config; see the tombstones in that file.

Recover any of it with ``git show <this commit>^:<path>``.
"""
