"""Charm-ceil rounding for raw card prices — shared so kiosk/card_manager/
price_updater all agree on what 'the kiosk price' for a given market is.

Tier scheme (denser at low prices since raw cards span $0.50-$500+):
  <  $20   -> nearest $1   ending in .99 ($1.99, $2.99, ..., $19.99)
  <  $100  -> nearest $5   ending in .99 ($24.99, $29.99, ..., $99.99)
  <  $500  -> nearest $10  ending in .99 ($109.99, $119.99, ...)
  >= $500  -> nearest $25  ending in .99 ($524.99, $549.99, ...)

Always ceils so we never undercut the market.
"""
import math


def charm_ceil_raw(price) -> float:
    try:
        p = float(price or 0)
    except (TypeError, ValueError):
        return 0.0
    if p <= 0:
        return 0.0
    if p < 20:
        increment = 1
    elif p < 100:
        increment = 5
    elif p < 500:
        increment = 10
    else:
        increment = 25
    next_step = math.ceil(p / increment) * increment
    candidate = next_step - 0.01
    if candidate < p:
        candidate = next_step + increment - 0.01
    return round(candidate, 2)
