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


def refresh_stale_component_prices(variant_ids, db, ppt, max_age_hours=DEFAULT_MAX_AGE_HOURS,
                                    cache_only=False):
    """
    Check if any components in the given variants have stale market prices.
    If so, fetch fresh prices from cache (and optionally PPT) and update the DB.

    Args:
        variant_ids: list of variant UUIDs to check
        db: database module with query() and execute() functions
        ppt: PriceProvider instance (or None to skip refresh)
        max_age_hours: consider prices stale after this many hours
        cache_only: when True, only read prices from the local cache (no
            PPT/Scrydex network calls). Use this from read endpoints like
            Collection Summary — the JIT refresh used to burn 12+ seconds
            on a few cache-miss components hitting PPT serially. Cache-only
            misses just leave the existing market_price as-is.

    Returns:
        int: number of components updated
    """
    if not variant_ids or not ppt:
        return 0
    # Cache-only mode needs the underlying PriceCache. Skip cleanly if the
    # provider was constructed without one (shouldn't happen in practice).
    cache = getattr(ppt, "cache", None) if cache_only else None
    if cache_only and cache is None:
        return 0

    _t0 = time.perf_counter()
    ph = ",".join(["%s"] * len(variant_ids))
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)

    # Find stale components: no timestamp or older than cutoff. Joining out
    # to the parent so cache-miss logs say "component X for parent Y" — Sean
    # asked for that so he can see which breakdown is forcing PPT calls.
    _ts = time.perf_counter()
    stale = db.query(f"""
        SELECT sbco.id, sbco.variant_id, sbco.tcgplayer_id,
               COALESCE(sbco.component_type, 'sealed') AS component_type,
               sbco.market_price,
               sbc.tcgplayer_id AS parent_tcg_id
        FROM sealed_breakdown_components sbco
        JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
        JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
        WHERE sbco.variant_id IN ({ph})
          AND sbco.tcgplayer_id IS NOT NULL
          AND (sbco.market_price_updated_at IS NULL OR sbco.market_price_updated_at < %s)
    """, tuple(variant_ids) + (cutoff,))
    _t_stale_q = time.perf_counter() - _ts

    if not stale:
        return 0

    # Deduplicate by tcgplayer_id — same component in multiple variants only
    # needs one lookup. Track parents per component so cache-miss logs name
    # the breakdowns this component is wired into.
    unique_components = {}
    parents_per_component: dict[int, set] = {}
    for row in stale:
        tcg_id = int(row["tcgplayer_id"])
        if tcg_id not in unique_components:
            unique_components[tcg_id] = row["component_type"]
        parents_per_component.setdefault(tcg_id, set()).add(int(row["parent_tcg_id"]))

    # Fetch fresh prices. We always check the local cache first ourselves
    # (rather than relying on the provider's internal cache-first behavior)
    # so we can log cache misses with parent context — that tells Sean
    # exactly which components aren't in cache. cache_only mode then skips
    # the network entirely for the misses; the legacy mode falls through
    # to PPT/Scrydex for callers that need fresh data (inventory editor,
    # ingestion verify).
    fresh_prices = {}
    cache_misses: list[tuple[int, str, set]] = []  # (tcg_id, comp_type, parent_set)
    cache_for_lookup = getattr(ppt, "cache", None)
    _ts = time.perf_counter()
    for tcg_id, comp_type in unique_components.items():
        price = None
        if cache_for_lookup is not None:
            try:
                if comp_type == "promo":
                    price = cache_for_lookup.get_raw_condition_price(
                        tcgplayer_id=tcg_id, condition="NM",
                    )
                else:
                    price = cache_for_lookup.get_sealed_market_price(tcg_id)
            except Exception as e:
                logger.warning(f"Cache lookup for component TCG#{tcg_id}: {e}")
        if price is not None:
            fresh_prices[tcg_id] = price
            continue
        cache_misses.append((tcg_id, comp_type, parents_per_component.get(tcg_id, set())))

    # Log every cache miss with parent context. One line per miss so Sean
    # can grep for "breakdown cache miss" in Railway and see which
    # component+breakdown pairs need attention (probably a Scrydex sync gap).
    for tcg_id, comp_type, parents in cache_misses:
        parents_str = ",".join(str(p) for p in sorted(parents)) or "?"
        logger.warning(
            "breakdown cache miss: comp_tcg=%d type=%s parents=[%s]%s",
            tcg_id, comp_type, parents_str,
            "" if cache_only else " — falling through to network",
        )

    # Network fallback for the misses (only when cache_only=False).
    if cache_misses and not cache_only:
        for tcg_id, comp_type, _parents in cache_misses:
            if ppt.should_throttle():
                logger.warning("PPT rate limit reached — stopping component price refresh")
                break
            try:
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
                "refresh_stale_component_prices empty: variants=%d stale=%d unique=%d misses=%d "
                "cache_only=%s total=%.2fs [stale_q=%.2fs fetch=%.2fs]",
                len(variant_ids), len(stale), len(unique_components),
                len(cache_misses), cache_only,
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
            "refresh_stale_component_prices: variants=%d stale=%d unique=%d misses=%d "
            "updated=%d cache_only=%s total=%.2fs "
            "[stale_q=%.2fs fetch=%.2fs update=%.2fs recompute=%.2fs]",
            len(variant_ids), len(stale), len(unique_components), len(cache_misses),
            updated_count, cache_only,
            _t_total, _t_stale_q, _t_fetch, _t_update, _t_recompute,
        )
    else:
        logger.info(f"Refreshed {updated_count} component prices ({len(fresh_prices)} unique from cache/PPT)")
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
