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
        ppt: PriceProvider instance (or None to skip refresh)
        max_age_hours: consider prices stale after this many hours

    Returns:
        int: number of components updated
    """
    if not variant_ids or not ppt:
        return 0

    _t0 = time.perf_counter()
    ph = ",".join(["%s"] * len(variant_ids))
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

    # Find stale components: no timestamp or older than cutoff
    _ts = time.perf_counter()
    stale = db.query(f"""
        SELECT id, variant_id, tcgplayer_id,
               COALESCE(component_type, 'sealed') AS component_type,
               market_price
        FROM sealed_breakdown_components
        WHERE variant_id IN ({ph})
          AND tcgplayer_id IS NOT NULL
          AND (market_price_updated_at IS NULL OR market_price_updated_at < %s)
    """, tuple(variant_ids) + (cutoff,))
    _t_stale_q = time.perf_counter() - _ts

    if not stale:
        return 0

    # Deduplicate by tcgplayer_id — same component in multiple variants only needs one PPT call
    unique_components = {}
    for row in stale:
        tcg_id = int(row["tcgplayer_id"])
        if tcg_id not in unique_components:
            unique_components[tcg_id] = row["component_type"]

    # Fetch fresh prices from PPT (cache-first; PPT only on miss)
    fresh_prices = {}
    _ts = time.perf_counter()
    for tcg_id, comp_type in unique_components.items():
        if ppt.should_throttle():
            logger.warning("PPT rate limit reached — stopping component price refresh")
            break
        try:
            # Scalar API — cache-first (USD-converted), PPT fallback. Promo
            # cards ask for NM primary-variant raw; sealed get unopened price.
            if comp_type == "promo":
                price = ppt.get_raw_condition_price(
                    tcgplayer_id=tcg_id, condition="NM",
                )
            else:
                price = ppt.get_sealed_market_price(tcg_id)
            if price is not None:
                fresh_prices[tcg_id] = price
        except Exception as e:
            logger.warning(f"Failed to fetch price for component TCG#{tcg_id}: {e}")
    _t_fetch = time.perf_counter() - _ts

    if not fresh_prices:
        _t_total = time.perf_counter() - _t0
        if _t_total > 0.5:
            logger.warning(
                "refresh_stale_component_prices empty: variants=%d stale=%d unique=%d total=%.2fs "
                "[stale_q=%.2fs fetch=%.2fs]",
                len(variant_ids), len(stale), len(unique_components),
                _t_total, _t_stale_q, _t_fetch,
            )
        return 0

    # Update components with fresh prices
    updated_count = 0
    affected_variant_ids = set()
    now = datetime.utcnow()

    _ts = time.perf_counter()
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
    _t_update = time.perf_counter() - _ts

    # Recompute denormalized totals for affected variants
    _ts = time.perf_counter()
    if affected_variant_ids:
        _recompute_denormalized_totals(db, list(affected_variant_ids))
    _t_recompute = time.perf_counter() - _ts

    _t_total = time.perf_counter() - _t0
    if _t_total > 0.5:
        logger.warning(
            "refresh_stale_component_prices: variants=%d stale=%d unique=%d updated=%d total=%.2fs "
            "[stale_q=%.2fs fetch=%.2fs update=%.2fs recompute=%.2fs]",
            len(variant_ids), len(stale), len(unique_components), updated_count,
            _t_total, _t_stale_q, _t_fetch, _t_update, _t_recompute,
        )
    else:
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
