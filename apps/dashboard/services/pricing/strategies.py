# apps/dashboard/services/pricing/strategies.py
import math
from typing import List, Dict, Tuple, Optional

def ema_price(sales: List[Dict], half_life_days: float = 7.0) -> Optional[float]:
    """
    Expects sales sorted oldest -> newest:
      [{"date":"YYYY-MM-DD","price": float}, ...]
    Returns EMA with a half-life bias toward recent points.
    """
    if not sales:
        return None
    alpha = 1 - math.exp(math.log(0.5) / max(half_life_days, 0.1))  # guard
    ema = float(sales[0]["price"])
    for pt in sales[1:]:
        ema = alpha * float(pt["price"]) + (1 - alpha) * ema
    return ema

def psychological_round(price: float) -> float:
    """
    Round to your store's style (… .99).
    """
    p = round(price) - 0.01
    return float(f"{max(p, 0.99):.2f}")

def smart_price(
    sales: List[Dict],
    *,
    half_life_days: float = 7.0,
    min_samples: int = 2,
    rounding: bool = True,
    floor: Optional[float] = None,
    cap: Optional[float] = None
) -> Optional[float]:
    """
    Convert raw sales history into a target price.
    - EMA with recency weighting
    - Optional min_samples gate
    - Optional floor/cap
    - Optional .99 rounding
    """
    if len(sales) < min_samples:
        return None
    ema = ema_price(sales, half_life_days=half_life_days)
    if ema is None:
        return None
    price = float(ema)
    if floor is not None:
        price = max(price, float(floor))
    if cap is not None:
        price = min(price, float(cap))
    return psychological_round(price) if rounding else float(f"{price:.2f}")

def decide_update(
    current_price: Optional[float],
    target_price: Optional[float],
    *,
    max_auto_down_pct: float = 2.0
) -> Tuple[str, Optional[float]]:
    """
    Your policy:
      - Upward changes: always apply.
      - Downward: auto-apply if |Δ| ≤ 2%, else flag.
      - None values → noop.

    Returns one of:
      ("update", new_price) | ("flag_down", target_price) | ("noop", current_price)
    """
    if current_price is None or target_price is None:
        return ("noop", current_price)

    cur = float(current_price)
    tgt = float(target_price)

    if cur == 0:
        # Treat as new listing or zeroed price → set it
        return ("update", tgt)

    delta = tgt - cur
    pct = (delta / cur) * 100.0

    if pct >= 0:
        return ("update", tgt)

    if abs(pct) <= max_auto_down_pct:
        return ("update", tgt)

    return ("flag_down", tgt)
