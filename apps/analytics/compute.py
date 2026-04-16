"""
SKU analytics computation engine.

Pulls Shopify order line items, writes daily sales snapshots,
and recomputes per-variant velocity metrics.
"""

import os
import logging
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal

from shopify_graphql import shopify_gql, gid_numeric
import db

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# GraphQL
# ═══════════════════════════════════════════════════════════════════════════════

ORDERS_QUERY = """
query Orders($first:Int!, $after:String, $query:String!) {
  orders(first:$first, after:$after, query:$query, sortKey:CREATED_AT) {
    edges {
      cursor
      node {
        id
        createdAt
        displayFinancialStatus
        lineItems(first:100) {
          edges {
            node {
              variant { id }
              quantity
              originalTotalSet { shopMoney { amount } }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""



# ═══════════════════════════════════════════════════════════════════════════════
# Order Ingestion
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_orders(since_date: str = None, full_backfill: bool = False):
    """
    Pull Shopify orders and write daily sales snapshots.

    Args:
        since_date: ISO date string to pull from (e.g., '2025-12-23')
        full_backfill: if True, pull 90 days regardless of last run
    """
    if full_backfill or not since_date:
        # Default: 90 days back
        since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    else:
        since = since_date

    logger.info(f"Ingesting orders since {since}")

    query_filter = f'financial_status:paid created_at:>="{since}"'
    cursor = None
    total_orders = 0
    daily_sales = {}  # (date_str, variant_id) -> {units, revenue}

    while True:
        variables = {"first": 50, "query": query_filter}
        if cursor:
            variables["after"] = cursor

        try:
            data = shopify_gql(ORDERS_QUERY, variables)
        except Exception as e:
            logger.error(f"GraphQL error fetching orders: {e}")
            break

        edges = data.get("data", {}).get("orders", {}).get("edges", [])
        if not edges:
            break

        for edge in edges:
            node = edge["node"]
            order_date = node["createdAt"][:10]  # YYYY-MM-DD
            total_orders += 1

            for li_edge in node.get("lineItems", {}).get("edges", []):
                li = li_edge["node"]
                variant = li.get("variant")
                if not variant or not variant.get("id"):
                    continue

                variant_id = int(gid_numeric(variant["id"]))
                qty = li.get("quantity", 0) or 0
                revenue = float(li.get("originalTotalSet", {}).get("shopMoney", {}).get("amount", 0))

                key = (order_date, variant_id)
                if key not in daily_sales:
                    daily_sales[key] = {"units": 0, "revenue": 0.0}
                daily_sales[key]["units"] += qty
                daily_sales[key]["revenue"] += revenue

        page_info = data.get("data", {}).get("orders", {}).get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    # Write to sku_daily_sales (upsert)
    written = 0
    for (sale_date, variant_id), vals in daily_sales.items():
        db.execute("""
            INSERT INTO sku_daily_sales (sale_date, shopify_variant_id, units_sold, revenue)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (sale_date, shopify_variant_id) DO UPDATE SET
                units_sold = EXCLUDED.units_sold,
                revenue = EXCLUDED.revenue
        """, (sale_date, variant_id, vals["units"], vals["revenue"]))
        written += 1

    # Update last run timestamp
    now = datetime.now(timezone.utc).isoformat()
    db.execute("""
        INSERT INTO analytics_meta (key, value, updated_at)
        VALUES ('last_order_ingest', %s, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
    """, (now,))

    logger.info(f"Ingested {total_orders} orders → {written} daily sales records")
    return {"orders": total_orders, "records": written}


# ═══════════════════════════════════════════════════════════════════════════════
# Metric Computation
# ═══════════════════════════════════════════════════════════════════════════════

def recompute_analytics():
    """
    Recompute sku_analytics from sku_daily_sales + inventory_product_cache.
    Call after order ingestion.
    """
    today = date.today()
    d90 = today - timedelta(days=90)
    d30 = today - timedelta(days=30)
    d7 = today - timedelta(days=7)

    logger.info("Recomputing SKU analytics...")

    # Aggregate sales by variant, excluding drop event days
    # drop_events table records which variants had drops on which dates
    # (populated by the future drop planner — empty until then)
    rows = db.query("""
        SELECT
            s.shopify_variant_id,
            SUM(CASE WHEN s.sale_date >= %s THEN s.units_sold ELSE 0 END) AS units_90d,
            SUM(CASE WHEN s.sale_date >= %s THEN s.units_sold ELSE 0 END) AS units_30d,
            SUM(CASE WHEN s.sale_date >= %s THEN s.units_sold ELSE 0 END) AS units_7d,
            SUM(CASE WHEN s.sale_date >= %s THEN s.revenue ELSE 0 END) AS revenue_90d,
            SUM(s.units_sold) AS total_sold_all_time,
            MIN(s.sale_date) AS first_sale_date,
            MAX(s.sale_date) AS last_sale_date
        FROM sku_daily_sales s
        WHERE NOT EXISTS (
              SELECT 1 FROM drop_events de
              WHERE de.shopify_variant_id = s.shopify_variant_id
                AND de.drop_date = s.sale_date
          )
        GROUP BY s.shopify_variant_id
    """, (d90, d30, d7, d90))

    if not rows:
        logger.info("No sales data found")
        return {"updated": 0}

    # Get current inventory state
    cache_rows = db.query("""
        SELECT shopify_variant_id, shopify_product_id, tcgplayer_id,
               title, shopify_price, shopify_qty
        FROM inventory_product_cache
    """)
    cache_map = {r["shopify_variant_id"]: r for r in cache_rows}

    updated = 0
    for row in rows:
        vid = row["shopify_variant_id"]
        cache = cache_map.get(vid, {})

        units_90d = int(row["units_90d"] or 0)
        units_30d = int(row["units_30d"] or 0)
        units_7d = int(row["units_7d"] or 0)
        revenue_90d = float(row["revenue_90d"] or 0)
        total_all_time = int(row["total_sold_all_time"] or 0)
        first_sale = row["first_sale_date"]
        last_sale = row["last_sale_date"]

        first_sale = row["first_sale_date"]
        current_qty = int(cache.get("shopify_qty") or 0)
        current_price = float(cache.get("shopify_price") or 0)

        # Average sale price
        avg_sale_price = revenue_90d / units_90d if units_90d > 0 else current_price

        # Out of stock days: best available estimate
        # 1. Use daily inventory snapshots if we have them (most accurate)
        # 2. Proxy: if current qty=0 and we have a last sale date, days since last sale = OOS days
        oos_row = db.query_one("""
            SELECT COUNT(*) AS oos_days
            FROM sku_daily_inventory
            WHERE shopify_variant_id = %s AND snapshot_date >= %s AND qty = 0
        """, (vid, d90))
        oos_days = int(oos_row["oos_days"]) if oos_row else 0

        # Proxy for items with no/limited snapshot data
        if oos_days == 0 and current_qty == 0 and last_sale:
            # Currently OOS: days since last sale = minimum OOS estimate
            days_since_last_sale = max(0, (today - last_sale).days)
            oos_days = min(days_since_last_sale, 90)

        # Days active: how long has this item been selling? (capped at 90)
        if first_sale:
            days_active = max(1, (today - first_sale).days)
        else:
            days_active = 90
        days_active = min(days_active, 90)

        # OOS days can't exceed days_active (snapshot data could span before first_sale)
        oos_days = min(oos_days, days_active - 1)

        # Selling days: subtract OOS days to get TRUE rate when in stock
        # If OOS data hasn't accumulated yet (oos_days=0), use days_active as-is
        selling_days = max(1, days_active - oos_days)

        # Actual daily sell rate based on days we had stock, not calendar days
        daily_rate = units_90d / selling_days if selling_days > 0 else 0

        # Days of inventory: how long until current stock sells out at this rate?
        days_of_inventory = current_qty / daily_rate if daily_rate > 0 else 9999

        # avg_days_to_sell: average interval between sales (when in stock)
        avg_days = selling_days / units_90d if units_90d > 0 else None

        # Velocity score = days of inventory (lower = faster)
        velocity = round(days_of_inventory, 1)

        # Price trend: compare avg sale price to current price
        price_trend = 0.0
        if avg_sale_price > 0 and current_price > 0:
            price_trend = round((current_price - avg_sale_price) / avg_sale_price * 100, 2)

        db.execute("""
            INSERT INTO sku_analytics (
                shopify_variant_id, shopify_product_id, tcgplayer_id, title,
                units_sold_90d, units_sold_30d, units_sold_7d, total_sold_all_time,
                avg_days_to_sell, out_of_stock_days, first_seen_date,
                current_qty, current_price, avg_sale_price,
                price_trend_pct, last_sale_at, velocity_score, computed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (shopify_variant_id) DO UPDATE SET
                shopify_product_id = EXCLUDED.shopify_product_id,
                tcgplayer_id = EXCLUDED.tcgplayer_id,
                title = EXCLUDED.title,
                units_sold_90d = EXCLUDED.units_sold_90d,
                units_sold_30d = EXCLUDED.units_sold_30d,
                units_sold_7d = EXCLUDED.units_sold_7d,
                total_sold_all_time = EXCLUDED.total_sold_all_time,
                avg_days_to_sell = EXCLUDED.avg_days_to_sell,
                out_of_stock_days = EXCLUDED.out_of_stock_days,
                first_seen_date = EXCLUDED.first_seen_date,
                current_qty = EXCLUDED.current_qty,
                current_price = EXCLUDED.current_price,
                avg_sale_price = EXCLUDED.avg_sale_price,
                price_trend_pct = EXCLUDED.price_trend_pct,
                last_sale_at = EXCLUDED.last_sale_at,
                velocity_score = EXCLUDED.velocity_score,
                computed_at = CURRENT_TIMESTAMP
        """, (
            vid, cache.get("shopify_product_id"), cache.get("tcgplayer_id"),
            cache.get("title", ""),
            units_90d, units_30d, units_7d, total_all_time,
            avg_days, oos_days, first_sale,
            current_qty, current_price, round(avg_sale_price, 2),
            price_trend, last_sale, velocity,
        ))
        updated += 1

    # Zero out SKUs that had all their sales excluded (e.g., drop-only items)
    # These variants exist in sku_analytics but weren't in the query results
    updated_vids = {row["shopify_variant_id"] for row in rows}
    zeroed = 0
    stale_rows = db.query("""
        SELECT shopify_variant_id FROM sku_analytics
        WHERE units_sold_90d > 0 AND shopify_variant_id NOT IN (
            SELECT DISTINCT shopify_variant_id FROM sku_daily_sales s
            WHERE s.sale_date >= %s
              AND NOT EXISTS (
                  SELECT 1 FROM drop_events de
                  WHERE de.shopify_variant_id = s.shopify_variant_id AND de.drop_date = s.sale_date
              )
        )
    """, (d90,))
    for sr in stale_rows:
        db.execute("""
            UPDATE sku_analytics SET
                units_sold_90d = 0, units_sold_30d = 0, units_sold_7d = 0,
                avg_days_to_sell = NULL, velocity_score = 0, avg_sale_price = NULL,
                computed_at = CURRENT_TIMESTAMP
            WHERE shopify_variant_id = %s
        """, (sr["shopify_variant_id"],))
        zeroed += 1

    logger.info(f"Updated {updated} SKU analytics records, zeroed {zeroed} drop-only SKUs")
    return {"updated": updated, "zeroed": zeroed}


def snapshot_inventory():
    """
    Take a daily snapshot of inventory levels from inventory_product_cache.
    Used to compute accurate out-of-stock days.
    """
    today = date.today()

    # Check if we already have a snapshot for today
    existing = db.query_one(
        "SELECT 1 FROM sku_daily_inventory WHERE snapshot_date = %s LIMIT 1",
        (today,)
    )
    if existing:
        logger.info(f"Inventory snapshot for {today} already exists, skipping")
        return {"date": str(today), "skipped": True}

    # Snapshot current quantities from the product cache
    rows = db.query("""
        SELECT shopify_variant_id, shopify_qty
        FROM inventory_product_cache
        WHERE shopify_variant_id IS NOT NULL
    """)

    count = 0
    for r in rows:
        db.execute("""
            INSERT INTO sku_daily_inventory (snapshot_date, shopify_variant_id, qty)
            VALUES (%s, %s, %s)
            ON CONFLICT (snapshot_date, shopify_variant_id) DO NOTHING
        """, (today, r["shopify_variant_id"], int(r["shopify_qty"] or 0)))
        count += 1

    logger.info(f"Inventory snapshot: {count} variants captured for {today}")
    return {"date": str(today), "variants": count}


def run_full_pipeline():
    """
    Run the complete analytics pipeline:
      1. Snapshot market prices (before nightly scrydex sync overwrites)
      2. Snapshot inventory levels (for OOS tracking)
      3. Ingest orders from Shopify
      4. Recompute velocity metrics
      5. Classify product taxonomy
      6. Sync customer orders + summaries
      7. Compute daily business summary
      8. Compute realized margins
    """
    results = {}

    # 1. Snapshot scrydex prices to history
    try:
        from price_history import snapshot_scrydex_prices
        results["price_history"] = snapshot_scrydex_prices()
    except Exception as e:
        logger.exception(f"Price history snapshot failed: {e}")

    # 2. Snapshot today's inventory levels (for OOS tracking)
    snapshot_inventory()

    # 3. Ingest orders
    meta = db.query_one("SELECT value FROM analytics_meta WHERE key = 'last_order_ingest'")
    if meta and meta["value"]:
        # Incremental: pull from day before last run to catch any stragglers
        last_run = meta["value"][:10]  # YYYY-MM-DD
        since = (datetime.fromisoformat(last_run) - timedelta(days=1)).strftime("%Y-%m-%d")
        ingest_result = ingest_orders(since_date=since)
    else:
        # First run: full 90-day backfill
        ingest_result = ingest_orders(full_backfill=True)
    results.update(ingest_result)

    # 4. Recompute velocity metrics
    compute_result = recompute_analytics()
    results.update(compute_result)

    # 5. Classify product taxonomy
    try:
        from taxonomy import classify_taxonomy
        results["taxonomy"] = classify_taxonomy()
    except Exception as e:
        logger.exception(f"Taxonomy classification failed: {e}")

    # 6. Sync customer orders + summaries
    try:
        from customers import sync_customer_orders, recompute_customer_summaries, compute_daily_business_summary
        results["customer_orders"] = sync_customer_orders()
        results["customer_summaries"] = recompute_customer_summaries()
        # 7. Daily business summary for today
        results["daily_summary"] = compute_daily_business_summary(date.today())
    except Exception as e:
        logger.exception(f"Customer pipeline failed: {e}")

    # 8. Compute realized margins
    try:
        from margins import compute_realized_margins
        results["margins"] = compute_realized_margins()
    except Exception as e:
        logger.exception(f"Margin computation failed: {e}")

    return results
