"""Venue-agnostic live data adapters. Today: Binance spot (`binance.py`).

Each adapter exposes a single `fetch_*` function that returns typed
results. All HTTP boundaries are mockable via httpx.MockTransport so the
test suite doesn't hit the network. Venue-specific feeds (Polymarket
Gamma / CLOB, predict.fun) live in their venue package, not here.
"""
