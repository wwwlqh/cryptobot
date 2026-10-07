"""Reproducible 11-feature checkpoint model training pipeline.

PRECONDITION (2026-07-31): ``pmdata/poly_snapshot/`` was deleted in the data
consolidation, so ``build_pool.build_grid_pool`` and
``checkpoint._build_pool_for_grid`` now raise FileNotFoundError at entry via
``pmdata_target.require_snapshot_dir()``. Re-download it with
``python -m polymarket.backtest.fetch.pmdata_download`` before any pool rebuild;
see SOURCES.md.

Modules here rebuild the option-3 pool, train/export checkpoint_v1, and retain
the substrate the sub-minute pipeline needs for reruns — named, not adjectival:

* ``offset_wfo`` — per-second feature/book substrate + the fold calendar.
* ``offset_validation`` — the fixed-set gate/sizing sweep (``_fixed_set_wfo``)
  and the wallet simulator ``checkpoint`` drives; library-only since af3c124.
* ``pmdata_target`` — manifest sleeves, fee/ask-bucket math, ORB columns and
  the pmdata raw-book reader behind ``require_snapshot_dir``.
* ``book_depth`` — the depth helper. Its index cache died 2026-07-31, so the
  no-arg loader now always raises (bf80d8b).
* ``modeling`` / ``portfolio`` — the execution-fill and wallet primitives.

Operational tick simulation commands live in polymarket.backtest.scripts.
"""
