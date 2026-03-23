"""
Shared SKU analytics read helpers.

Used by intake, ingest, inventory to look up velocity data.
The analytics service writes to sku_analytics; these functions read it.
"""

import logging

logger = logging.getLogger(__name__)


def get_analytics_for_tcgplayer_ids(tcgplayer_ids: list, db) -> dict:
    """
    Batch lookup analytics by tcgplayer_id.
    Returns {tcgplayer_id: {velocity_score, units_sold_90d, ...}}
    """
    if not tcgplayer_ids:
        return {}
    try:
        ph = ",".join(["%s"] * len(tcgplayer_ids))
        rows = db.query(
            f"SELECT * FROM sku_analytics WHERE tcgplayer_id IN ({ph})",
            tuple(int(t) for t in tcgplayer_ids)
        )
        return {r["tcgplayer_id"]: dict(r) for r in rows}
    except Exception as e:
        logger.warning(f"sku_analytics lookup failed: {e}")
        return {}


def get_analytics_for_variant_ids(variant_ids: list, db) -> dict:
    """
    Batch lookup analytics by shopify_variant_id.
    Returns {shopify_variant_id: {velocity_score, units_sold_90d, ...}}
    """
    if not variant_ids:
        return {}
    try:
        ph = ",".join(["%s"] * len(variant_ids))
        rows = db.query(
            f"SELECT * FROM sku_analytics WHERE shopify_variant_id IN ({ph})",
            tuple(int(v) for v in variant_ids)
        )
        return {r["shopify_variant_id"]: dict(r) for r in rows}
    except Exception as e:
        logger.warning(f"sku_analytics lookup failed: {e}")
        return {}


def compute_offer_adjustment(analytics_data: dict, target_pct: float = 80.0) -> dict:
    """
    Compute collection-level offer adjustment from per-item analytics.

    Args:
        analytics_data: {tcgplayer_id: {item_value: float, analytics: dict}}
            where analytics is a row from sku_analytics (or None)
        target_pct: base offer percentage (e.g., 80.0)

    Returns:
        {
            "suggested_pct": float,
            "adjustment": float,
            "per_item": [{tcgplayer_id, adjustment, reasons}],
            "summary": str
        }
    """
    items = []
    total_value = 0.0

    for tcg_id, info in analytics_data.items():
        item_value = float(info.get("item_value", 0))
        a = info.get("analytics")  # sku_analytics row or None
        total_value += item_value

        adj = 0.0
        reasons = []

        if not a or a.get("units_sold_90d") is None:
            reasons.append("no data")
            items.append({"tcgplayer_id": tcg_id, "adjustment": 0, "reasons": reasons, "value": item_value})
            continue

        velocity = float(a.get("velocity_score") or 0)
        units_90d = int(a.get("units_sold_90d") or 0)
        oos_days = int(a.get("out_of_stock_days") or 0)
        current_qty = int(a.get("current_qty") or 0)
        price_trend = float(a.get("price_trend_pct") or 0)
        avg_days = float(a.get("avg_days_to_sell") or 999)

        # Velocity adjustment
        daily_rate = units_90d / 90.0
        if daily_rate > 1.0:
            adj += 5; reasons.append("very fast seller")
        elif daily_rate > 0.5:
            adj += 3; reasons.append("fast seller")
        elif daily_rate > 0.15:
            pass  # baseline
        elif daily_rate > 0.05:
            adj -= 3; reasons.append("slow seller")
        else:
            adj -= 5; reasons.append("very slow seller")

        # Out of stock bonus
        if oos_days > 30:
            adj += 3; reasons.append("frequently OOS")
        elif current_qty == 0:
            adj += 2; reasons.append("currently OOS")

        # Overstock penalty
        if current_qty > 5:
            adj -= 2; reasons.append(f"overstocked ({current_qty})")

        # Price trend
        if price_trend > 5:
            adj += 2; reasons.append("price rising")
        elif price_trend < -10:
            adj -= 3; reasons.append("price declining")

        items.append({
            "tcgplayer_id": tcg_id,
            "adjustment": round(adj, 1),
            "reasons": reasons,
            "value": item_value,
            "velocity": velocity,
            "units_90d": units_90d,
            "avg_days": avg_days,
        })

    # Weighted average adjustment
    if total_value > 0:
        weighted_adj = sum(i["adjustment"] * i["value"] for i in items) / total_value
    else:
        weighted_adj = 0

    suggested = max(70.0, min(87.0, target_pct + weighted_adj))

    # Summary text
    fast = sum(1 for i in items if i["adjustment"] > 0)
    slow = sum(1 for i in items if i["adjustment"] < 0)
    no_data = sum(1 for i in items if "no data" in i.get("reasons", []))

    summary_parts = []
    if fast:
        summary_parts.append(f"{fast} fast movers")
    if slow:
        summary_parts.append(f"{slow} slow movers")
    if no_data:
        summary_parts.append(f"{no_data} no data")

    return {
        "suggested_pct": round(suggested, 1),
        "adjustment": round(weighted_adj, 1),
        "target_pct": target_pct,
        "per_item": items,
        "summary": " · ".join(summary_parts) if summary_parts else "average collection",
    }
