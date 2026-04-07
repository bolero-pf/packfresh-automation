"""
Shared breakdown helpers — JIT refresh of component market prices.

When breakdown recipes are loaded for display, this module checks if
component market prices are stale and refreshes them from the PPT API.
"""

import logging
import time
from datetime import datetime, timedelta
from decimal import Decimal

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_HOURS = 4


def refresh_stale_component_prices(variant_ids, db, ppt, max_age_hours=DEFAULT_MAX_AGE_HOURS):
    """
    Check if any components in the given variants have stale market prices.
    If so, fetch fresh prices from PPT API and update the DB.

    Args:
        variant_ids: list of variant UUIDs to check
        db: database module with query() and execute() functions
        ppt: PPTClient instance (or None to skip refresh)
        max_age_hours: consider prices stale after this many hours

    Returns:
        int: number of components updated
    """
    if not variant_ids or not ppt:
        return 0

    ph = ",".join(["%s"] * len(variant_ids))
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

    # Find stale components: no timestamp or older than cutoff
    stale = db.query(f"""
        SELECT id, variant_id, tcgplayer_id,
               COALESCE(component_type, 'sealed') AS component_type,
               product_name, market_price
        FROM sealed_breakdown_components
        WHERE variant_id IN ({ph})
          AND tcgplayer_id IS NOT NULL
          AND (market_price_updated_at IS NULL OR market_price_updated_at < %s)
    """, tuple(variant_ids) + (cutoff,))

    if not stale:
        return 0

    # Deduplicate by tcgplayer_id — same component in multiple variants only needs one PPT call
    unique_components = {}
    for row in stale:
        tcg_id = int(row["tcgplayer_id"])
        if tcg_id not in unique_components:
            unique_components[tcg_id] = {
                "type": row["component_type"],
                "name": row.get("product_name") or "",
            }

    # Fetch fresh prices from PPT
    fresh_prices = {}
    for tcg_id, comp in unique_components.items():
        if ppt.should_throttle():
            logger.warning("PPT rate limit reached — stopping component price refresh")
            break
        try:
            if comp["type"] == "promo":
                data = ppt.get_card_by_tcgplayer_id(tcg_id)
            else:
                data = ppt.get_sealed_product_by_tcgplayer_id(tcg_id, product_name=comp["name"])
            price = ppt.extract_market_price(data)
            if price is not None:
                fresh_prices[tcg_id] = price
        except Exception as e:
            logger.warning(f"Failed to fetch price for component TCG#{tcg_id}: {e}")

    if not fresh_prices:
        return 0

    # Update components with fresh prices
    updated_count = 0
    affected_variant_ids = set()
    now = datetime.utcnow()

    for row in stale:
        tcg_id = int(row["tcgplayer_id"])
        if tcg_id in fresh_prices:
            db.execute("""
                UPDATE sealed_breakdown_components
                SET market_price = %s, market_price_updated_at = %s
                WHERE id = %s
            """, (fresh_prices[tcg_id], now, row["id"]))
            updated_count += 1
            affected_variant_ids.add(row["variant_id"])

    # Recompute denormalized totals for affected variants
    if affected_variant_ids:
        _recompute_denormalized_totals(db, list(affected_variant_ids))

    logger.info(f"Refreshed {updated_count} component prices ({len(fresh_prices)} unique from PPT)")
    return updated_count


def _recompute_denormalized_totals(db, variant_ids):
    """Recompute total_component_market and best_variant_market after price updates."""
    for vid in variant_ids:
        # Update variant total
        db.execute("""
            UPDATE sealed_breakdown_variants SET
                total_component_market = COALESCE((
                    SELECT SUM(market_price * quantity_per_parent)
                    FROM sealed_breakdown_components WHERE variant_id = %s
                ), 0),
                last_updated = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (vid, vid))

    # Update parent cache best_variant_market for all affected breakdown_ids
    db.execute("""
        UPDATE sealed_breakdown_cache sbc SET
            best_variant_market = COALESCE((
                SELECT MAX(total_component_market)
                FROM sealed_breakdown_variants WHERE breakdown_id = sbc.id
            ), 0),
            last_updated = CURRENT_TIMESTAMP
        WHERE sbc.id IN (
            SELECT DISTINCT breakdown_id FROM sealed_breakdown_variants
            WHERE id IN ({})
        )
    """.format(",".join(["%s"] * len(variant_ids))), tuple(variant_ids))
