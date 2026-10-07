"""Shared exit-code constants for cryptobot CLI entry points."""

from __future__ import annotations


OK: int = 0
USAGE: int = 1
NOGO: int = 2
UNEXPECTED: int = 3

__all__ = [
    "OK",
    "USAGE",
    "NOGO",
    "UNEXPECTED",
]
