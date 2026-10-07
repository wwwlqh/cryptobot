"""Polymarket signature-type and funder validation.

Validates ``POLY_SIGNATURE_TYPE`` / ``POLY_FUNDER`` from
``cryptobot.runtime_secrets.Config``. The live-readiness gate calls it, so it
lives next to the config it checks; ``cryptobot_polymarket.walker`` re-exports
both names.
"""

from __future__ import annotations

# Polymarket V2 signature types, used in error messages.
_SIGNATURE_TYPE_LABELS: dict[int, str] = {
    0: "EOA",
    1: "POLY_PROXY",
    2: "POLY_GNOSIS_SAFE",
    3: "POLY_1271",
}


def _validate_wallet_config(
    signature_type: int | None, funder: str | None
) -> tuple[int, str]:
    """Normalize the Polymarket wallet setup and fail closed on an impossible
    one.

    Returns the normalized (signature_type, funder) after checking that:

      * ``signature_type`` is one of the four known values;
      * type 3 (POLY_1271 deposit wallet) has a non-empty ``POLY_FUNDER``.
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
