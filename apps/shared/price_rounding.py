"""Charm-ceil rounding for raw card prices — shared so kiosk/card_manager/
price_updater all agree on what 'the kiosk price' for a given market is.

Tier scheme (denser at low prices since raw cards span $0.50-$2000+):
  <  $100   -> nearest $1   ending in .99 ($1.99, $19.99, $87.99, $99.99)
  <  $500   -> nearest $5   ending in .99 ($104.99, $124.99, $499.99)
  <  $2000  -> nearest $25  ending in .99 ($524.99, $1224.99)
  >= $2000  -> nearest $50  ending in .99 ($2049.99, $2099.99)

Always ceils so we never undercut the market.

Keep this in sync with price_updater/raw_card_updater.py::charm_ceil_raw
(intentional duplicate — price_updater doesn't import shared/ at module load).
"""
import math


def charm_ceil_raw(price) -> float:
    try:
        p = float(price or 0)
    except (TypeError, ValueError):
        return 0.0
    if p <= 0:
        return 0.0
    if p < 100:
        increment = 1
    elif p < 500:
        increment = 5
    elif p < 2000:
        increment = 25
    else:
        increment = 50
    next_step = math.ceil(p / increment) * increment
    candidate = next_step - 0.01
    if candidate < p:
        candidate = next_step + increment - 0.01
    return round(candidate, 2)
