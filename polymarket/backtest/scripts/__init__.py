"""polymarket.backtest.scripts — research / ops CLIs.

Not part of the core pipeline import graph; these are hand-run tools
(`python -m polymarket.backtest.scripts.<name>`). They import the engine via
`from polymarket.backtest.engine.… import …` and each other via
`from polymarket.backtest.scripts.… import …`.
"""
