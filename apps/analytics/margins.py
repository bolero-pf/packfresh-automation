"""
Realized margin computation.

Joins sku_daily_sales against COGS data (inventory_product_cache.unit_cost,
maintained by ingestion's weighted-average write to Shopify cost_per_item) and
market prices (scrydex_price_history) to compute per-variant per-day realized
margins.

Only processes sale days that don't already have a realized_margin row,
so it's safe to run incrementally.
"""

import logging
from datetime import date, timedelta

import db

logger = logging.getLogger(__name__)

# We only began capturing real COGS on ingestion ~2026-03-23 (when this analytics
# service launched). Before that there is no trustworthy cost, so we do NOT compute
# margin for older sales — retroactively pricing them against today's (appreciated)
# cost turns real profits into fake losses (a $31 distro ETB sold at $50 read as a
# $93 loss at today's ~$143 cost). Margin exists only for the COGS-tracking era.
COGS_TRACKING_START = date(2026, 3, 23)


def compute_realized_margins():
    """
    Compute realized_margin for all sale days not yet computed, on/after
    COGS_TRACKING_START. Joins:
      - sku_daily_sales (revenue net of discounts, units)
      - inventory_product_cache (unit_cost — weighted-average COGS from ingestion)
      - scrydex_price_history (market_price at time of sale)
    """
    logger.info("Computing realized margins...")

    # Find sale records that don't have margin computed yet (tracked era only)
    rows = db.query("""
        SELECT
            s.sale_date,
            s.shopify_variant_id,
            s.units_sold,
            s.revenue,
            ipc.shopify_product_id,
            s.unit_cost,
            ipc.tcgplayer_id
        FROM sku_daily_sales s
        LEFT JOIN inventory_product_cache ipc
            ON ipc.shopify_variant_id = s.shopify_variant_id
        WHERE s.sale_date >= %s
          AND NOT EXISTS (
            SELECT 1 FROM realized_margin rm
            WHERE rm.sale_date = s.sale_date
              AND rm.shopify_variant_id = s.shopify_variant_id
        )
        ORDER BY s.sale_date
    """, (COGS_TRACKING_START,))

    if not rows:
        logger.info("No new sale days to compute margins for")
        return {"computed": 0}

    computed = 0
    for row in rows:
        units = row["units_sold"]
        revenue = float(row["revenue"] or 0)

        unit_cost = float(row["unit_cost"]) if row["unit_cost"] else None
        cogs_at_sale = unit_cost  # per-unit COGS
        total_cogs = (unit_cost * units) if unit_cost else None

        # Suspect-cost guard. We store only the CURRENT unit_cost, which for appreciating
        # stock (and items hit by the old breakdown-COGS bug) can be far above the actual
        # cost at sale time — e.g. a $31 distro ETB sold at $50 MSRP now reads cost ~$143.
        # If recorded cost >= what it actually sold for, the cost is almost certainly stale/
        # polluted rather than a real below-cost sale, so treat it as COST-UNKNOWN (NULL
        # margin) — it then flows into the category-based estimate instead of a fake loss.
        if total_cogs is not None and revenue > 0 and total_cogs >= revenue:
            total_cogs = None
            cogs_at_sale = None

        gross_margin = (revenue - total_cogs) if total_cogs is not None else None
        margin_pct = round((gross_margin / revenue) * 100, 2) if gross_margin is not None and revenue > 0 else None

        # Market price at time of sale from price history
        market_price = None
        if row["tcgplayer_id"]:
            mp_row = db.query_one("""
                SELECT market_price FROM scrydex_price_history
                WHERE tcgplayer_id = %s
                  AND snapshot_date <= %s
                  AND condition = 'NM'
                  AND price_type = 'raw'
                ORDER BY snapshot_date DESC
                LIMIT 1
            """, (row["tcgplayer_id"], row["sale_date"]))
            if mp_row:
                market_price = float(mp_row["market_price"])

        # Effective margin: how we did vs market expectation
        effective_margin_pct = None
        if gross_margin is not None and market_price and market_price > 0 and units > 0:
            market_revenue = market_price * units
            effective_margin_pct = round((gross_margin / market_revenue) * 100, 2)

        db.execute("""
            INSERT INTO realized_margin (
                sale_date, shopify_variant_id, shopify_product_id,
                units_sold, revenue, cogs_at_sale, market_price_at_sale,
                gross_margin, margin_pct, effective_margin_pct
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (sale_date, shopify_variant_id) DO UPDATE SET
                units_sold = EXCLUDED.units_sold,
                revenue = EXCLUDED.revenue,
                cogs_at_sale = EXCLUDED.cogs_at_sale,
                market_price_at_sale = EXCLUDED.market_price_at_sale,
                gross_margin = EXCLUDED.gross_margin,
                margin_pct = EXCLUDED.margin_pct,
                effective_margin_pct = EXCLUDED.effective_margin_pct
        """, (
            row["sale_date"], row["shopify_variant_id"], row["shopify_product_id"],
            units, revenue, cogs_at_sale, market_price,
            gross_margin, margin_pct, effective_margin_pct,
        ))
        computed += 1

    logger.info(f"Computed realized margins for {computed} sale-day records")
    return {"computed": computed}
