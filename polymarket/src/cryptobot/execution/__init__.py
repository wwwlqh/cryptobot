"""Order execution + ledger writes + gate stack for the live scanner.

`walk_book.walk_book_edge_preserving` — ported from hightempbot's edge-preserving
walker with cryptobot's 5% absolute walk cap as the default. Other modules
(ledger, gates, capital, pipeline, resolution) land in subsequent units.
"""
