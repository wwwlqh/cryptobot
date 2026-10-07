"""Canonical research evaluators that the live decision code is checked
against.

  - ``pool_study``    the pool loader and canonical bet spec
                      (``load_pool``, ``side_frame``, ``fee``,
                      ``BET_SPEC_DEFAULT``).
  - ``pmdata_clean``  pmdata book cleaning and the outcome loader.
  - ``tick_store``    SQLite reader for the last-30-second tick books
                      (``poly_tick_last30.db``).
"""
