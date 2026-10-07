"""Research training pipeline: pool building, walk-forward validation and model
export.

Precondition: rebuilding the pool needs ``pmdata/poly_snapshot/``. Without it
``build_pool.build_grid_pool`` and ``checkpoint._build_pool_for_grid`` raise
FileNotFoundError via ``pmdata_target.require_snapshot_dir()``; download it
with ``python -m polymarket.backtest.fetch.pmdata_download`` first.

* ``export_twap_symmetric_60s``: trains and exports the live TWAP model. *
``offset_wfo``: per-second feature and book substrate, and the fold calendar. *
``offset_validation``: the fixed-offset gate/sizing sweep (``_fixed_set_wfo``)
and wallet simulator that ``checkpoint`` uses; library only. * ``checkpoint``:
walk-forward harness for sub-minute decision grids. * ``pmdata_target``: sleeve
and ask-bucket types, fee math, and the pmdata raw-book reader behind
``require_snapshot_dir``. * ``book_depth``: book-depth index walker (needs a
prebuilt index). * ``modeling`` / ``portfolio``: fill and wallet primitives.

Tick-level simulation commands live in polymarket.backtest.scripts.
"""
