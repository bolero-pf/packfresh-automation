"""
price_synthesis.py — Last-resort condition-price estimation.

Only called when BOTH Scrydex and PPT have no data for a specific condition.
This module owns FALLBACK_MULTIPLIERS — the source-specific clients must not
synthesize prices internally anymore; they return None for missing conditions
and let callers decide whether to fall back here.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

# Raw-card condition multipliers relative to NM.
# Kept as Decimal for exact arithmetic with Decimal market prices.
FALLBACK_MULTIPLIERS = {
    "NM":  Decimal("1.00"),
    "LP":  Decimal("0.80"),
    "MP":  Decimal("0.65"),
    "HP":  Decimal("0.45"),
    "DMG": Decimal("0.25"),
}


def synthesize_from_nm(nm_price, target_condition: str) -> Optional[Decimal]:
    """Estimate target_condition price by multiplying an NM price.
    Returns None when nm_price is None or target_condition is unknown."""
    if nm_price is None:
        return None
    mult = FALLBACK_MULTIPLIERS.get((target_condition or "").upper())
    if mult is None:
        return None
    return (Decimal(str(nm_price)) * mult).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def retarget_condition(current_price, from_condition: str,
                       to_condition: str) -> Optional[Decimal]:
    """Given a price for from_condition, estimate price for to_condition.

    Used during intake when a deal-time market_price is stamped at one condition
    and later adjusted to another without re-querying any data source.
    """
    if current_price is None:
        return None
    from_mult = FALLBACK_MULTIPLIERS.get((from_condition or "").upper())
    to_mult = FALLBACK_MULTIPLIERS.get((to_condition or "").upper())
    if from_mult is None or to_mult is None or from_mult <= 0:
        return None
    nm = Decimal(str(current_price)) / from_mult
    return (nm * to_mult).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
