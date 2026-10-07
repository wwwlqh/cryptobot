"""Live scanner for Polymarket 15M/5M crypto markets.

Single-process Python service that ticks at 1 Hz, runs the v2 step1last
checkpoint HGB portfolio against live Binance + Polymarket data, and
broadcasts a live snapshot to the dashboard over Server-Sent Events.
"""

__version__ = "0.1.0"
