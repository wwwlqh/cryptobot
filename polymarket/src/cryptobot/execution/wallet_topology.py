"""Polymarket signature-type / funder validation.

Lifted out of ``execution/walker.py`` (now ``cryptobot_polymarket.walker``) in
Phase 2. It validates fields of ``cryptobot.runtime_secrets.Config``
(``POLY_SIGNATURE_TYPE`` / ``POLY_FUNDER``), and core's live-readiness gate
calls it on EVERY venue — so it stayed with the config it validates rather than
travelling into the venue package and forcing core to import it.

``cryptobot_polymarket.walker`` re-exports both names, so its own public surface
and ``__all__`` are unchanged.
"""

from __future__ import annotations

# D-LIVE-7: Polymarket V2 signature topology labels. The literal map is
# copied verbatim from hightempbot/execution/walker.py:46 so error messages
# remain identical across the two bots — the operator's mental model and
# any incident search log indexing carry over.
_SIGNATURE_TYPE_LABELS: dict[int, str] = {
    0: "EOA",
    1: "POLY_PROXY",
    2: "POLY_GNOSIS_SAFE",
    3: "POLY_1271",
}


def _validate_wallet_config(
    signature_type: int | None, funder: str | None
) -> tuple[int, str]:
    """Normalize Polymarket wallet topology and fail closed on impossible config.

    Returns the normalized (signature_type, funder) tuple after asserting:

      * `signature_type` is one of the four known values.
      * SIG=3 (POLY_1271 deposit wallet) requires a non-empty `POLY_FUNDER`.

    Copied verbatim (sans imports) from hightempbot/execution/walker.py:97 so
    the failure modes — and any operator-side fix runbooks — stay identical.
    """
    sig_type = int(signature_type or 0)
    funder = (funder or "").strip()
    if sig_type not in _SIGNATURE_TYPE_LABELS:
        allowed = ", ".join(
            f"{k}={v}" for k, v in sorted(_SIGNATURE_TYPE_LABELS.items())
        )
        raise ValueError(
            f"POLY_SIGNATURE_TYPE must be one of {allowed}; got {sig_type}"
        )
    if sig_type == 3 and not funder:
        raise ValueError(
            "POLY_SIGNATURE_TYPE=3 (POLY_1271 deposit wallet) requires "
            "POLY_FUNDER=<deposit wallet address>"
        )
    return sig_type, funder


__all__ = ["_SIGNATURE_TYPE_LABELS", "_validate_wallet_config"]
