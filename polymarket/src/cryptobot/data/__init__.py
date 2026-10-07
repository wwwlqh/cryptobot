"""Venue-independent live data adapters (Binance spot in ``binance.py``).

Each adapter exposes a ``fetch_*`` function returning typed results. HTTP calls
can be mocked with httpx.MockTransport, so tests never touch the network.
Polymarket's own feeds (Gamma, CLOB) live in ``cryptobot_polymarket``.
"""
