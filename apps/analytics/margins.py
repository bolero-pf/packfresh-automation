"""
Realized margin computation.

Joins sku_daily_sales against COGS data (inventory_product_cache.unit_cost
or sealed_cogs.avg_cogs) and market prices (scrydex_price_history) to compute
per-variant per-day realized margins.

Only processes sale days that don't already have a realized_margin row,
so it's safe to run incrementally.
"""

import logging
from datetime import date, timedelta

import db

logger = logging.getLogger(__name__)


def compute_realized_margins():
    """
    Compute realized_margin for all sale days not yet computed.
    Joins:
      - sku_daily_sales (revenue, units)
      - inventory_product_cache (unit_cost — primary COGS source)
      - sealed_cogs (avg_cogs — fallback for sealed products)
      - scrydex_price_history (market_price at time of sale)
    """
    logger.info("Computing realized margins...")

    # Find sale records that don't have margin computed yet
    rows = db.query("""
        SELECT
            s.sale_date,
            s.shopify_variant_id,
            s.units_sold,
            s.revenue,
            ipc.shopify_product_id,
            ipc.unit_cost,
            ipc.tcgplayer_id,
            sc.avg_cogs
        FROM sku_daily_sales s
        JOIN inventory_product_cache ipc
            ON ipc.shopify_variant_id = s.shopify_variant_id
        LEFT JOIN sealed_cogs sc
            ON sc.shopify_variant_id = s.shopify_variant_id
        WHERE NOT EXISTS (
            SELECT 1 FROM realized_margin rm
            WHERE rm.sale_date = s.sale_date
              AND rm.shopify_variant_id = s.shopify_variant_id
        )
        ORDER BY s.sale_date
    """)

    if not rows:
        logger.info("No new sale days to compute margins for")
        return {"computed": 0}

    computed = 0
    for row in rows:
        units = row["units_sold"]
        revenue = float(row["revenue"] or 0)

        # COGS: prefer unit_cost (covers all variants), fallback to sealed avg_cogs
        unit_cost = float(row["unit_cost"]) if row["unit_cost"] else None
        if unit_cost is None and row["avg_cogs"]:
            unit_cost = float(row["avg_cogs"])

        cogs_at_sale = unit_cost  # per-unit COGS
        total_cogs = (unit_cost * units) if unit_cost else None
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
