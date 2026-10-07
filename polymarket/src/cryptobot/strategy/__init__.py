"""Strategy scoring layer: sleeve loading + feature build + edge computation.

`portfolio_loader.load_sleeves()` returns the sleeves configured in the live
artifact; `partial_ret` computes `partial_ret_z_1h`; `score.score_cell_hgb()`
serves the checkpoint model and picks the best-edge side. The LUT lookup that
used to sit between them was retired with the pair sleeves on 2026-07-31.
"""

from cryptobot.strategy.portfolio_loader import Sleeve, V6Portfolio, load_sleeves

__all__ = ["Sleeve", "V6Portfolio", "load_sleeves"]
