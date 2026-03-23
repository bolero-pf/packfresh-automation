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

# Spike detection: if a variant sells this many multiples of its median daily rate
# on a single day, that day is flagged as a spike (drop day) and excluded from velocity
SPIKE_MULTIPLIER = 10


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

    # Step 1: Detect spike days per variant
    # A spike day = units_sold > SPIKE_MULTIPLIER * median_daily_units for that variant
    all_daily = db.query("""
        SELECT shopify_variant_id, sale_date, units_sold
        FROM sku_daily_sales WHERE sale_date >= %s
        ORDER BY shopify_variant_id, units_sold
    """, (d90,))

    # Group by variant, compute median, flag spikes
    from collections import defaultdict
    variant_days = defaultdict(list)
    for r in all_daily:
        variant_days[r["shopify_variant_id"]].append((r["sale_date"], int(r["units_sold"])))

    spike_days = set()  # (variant_id, sale_date) pairs to exclude
    for vid, days in variant_days.items():
        units = sorted(d[1] for d in days)
        if len(units) < 3:
            continue  # too few data points to detect spikes
        median = units[len(units) // 2]
        threshold = max(median * SPIKE_MULTIPLIER, 20)  # at least 20 units to be a spike
        for sale_date, u in days:
            if u >= threshold:
                spike_days.add((vid, sale_date))

    if spike_days:
        logger.info(f"Detected {len(spike_days)} spike days (drops) — excluding from velocity")

    # Step 2: Aggregate sales excluding spike days
    rows = db.query("""
        SELECT
            s.shopify_variant_id,
            SUM(CASE WHEN s.sale_date >= %s THEN s.units_sold ELSE 0 END) AS units_90d,
            SUM(CASE WHEN s.sale_date >= %s THEN s.units_sold ELSE 0 END) AS units_30d,
            SUM(CASE WHEN s.sale_date >= %s THEN s.units_sold ELSE 0 END) AS units_7d,
            SUM(CASE WHEN s.sale_date >= %s THEN s.revenue ELSE 0 END) AS revenue_90d,
            MAX(s.sale_date) AS last_sale_date
        FROM sku_daily_sales s
        WHERE s.sale_date >= %s
        GROUP BY s.shopify_variant_id
    """, (d90, d30, d7, d90, d90))

    # We'll subtract spike-day numbers in the per-variant loop below

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

    # Pre-compute spike deductions per variant per window
    spike_deductions = defaultdict(lambda: {"u90": 0, "u30": 0, "u7": 0, "rev90": 0})
    for vid, days in variant_days.items():
        for sale_date, units in days:
            if (vid, sale_date) in spike_days:
                spike_deductions[vid]["u90"] += units
                if sale_date >= d30:
                    spike_deductions[vid]["u30"] += units
                if sale_date >= d7:
                    spike_deductions[vid]["u7"] += units
                # Revenue deduction: lookup from DB
                rev_row = db.query_one(
                    "SELECT revenue FROM sku_daily_sales WHERE shopify_variant_id=%s AND sale_date=%s",
                    (vid, sale_date))
                if rev_row:
                    spike_deductions[vid]["rev90"] += float(rev_row["revenue"] or 0)

    updated = 0
    for row in rows:
        vid = row["shopify_variant_id"]
        cache = cache_map.get(vid, {})
        sd = spike_deductions.get(vid, {"u90": 0, "u30": 0, "u7": 0, "rev90": 0})

        units_90d = max(0, int(row["units_90d"] or 0) - sd["u90"])
        units_30d = max(0, int(row["units_30d"] or 0) - sd["u30"])
        units_7d = max(0, int(row["units_7d"] or 0) - sd["u7"])
        revenue_90d = max(0, float(row["revenue_90d"] or 0) - sd["rev90"])
        last_sale = row["last_sale_date"]

        current_qty = int(cache.get("shopify_qty") or 0)
        current_price = float(cache.get("shopify_price") or 0)

        # Average sale price
        avg_sale_price = revenue_90d / units_90d if units_90d > 0 else current_price

        # Average days to sell (rough: 90 / units_sold gives avg interval between sales)
        avg_days = 90.0 / units_90d if units_90d > 0 else None

        # Out of stock days: count days at qty=0 from daily inventory snapshots
        oos_row = db.query_one("""
            SELECT COUNT(*) AS oos_days
            FROM sku_daily_inventory
            WHERE shopify_variant_id = %s AND snapshot_date >= %s AND qty = 0
        """, (vid, d90))
        oos_days = int(oos_row["oos_days"]) if oos_row else 0

        # Velocity score
        daily_rate = units_30d / 30.0
        demand_bonus = min(oos_days / 90.0, 1.0) * 2.0
        stock_penalty = min(current_qty / 10.0, 1.0) * -0.5
        velocity = round(daily_rate + demand_bonus + stock_penalty, 2)

        # Price trend: compare avg sale price to current price
        price_trend = 0.0
        if avg_sale_price > 0 and current_price > 0:
            price_trend = round((current_price - avg_sale_price) / avg_sale_price * 100, 2)

        db.execute("""
            INSERT INTO sku_analytics (
                shopify_variant_id, shopify_product_id, tcgplayer_id, title,
                units_sold_90d, units_sold_30d, units_sold_7d,
                avg_days_to_sell, out_of_stock_days,
                current_qty, current_price, avg_sale_price,
                price_trend_pct, last_sale_at, velocity_score, computed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
            ON CONFLICT (shopify_variant_id) DO UPDATE SET
                shopify_product_id = EXCLUDED.shopify_product_id,
                tcgplayer_id = EXCLUDED.tcgplayer_id,
                title = EXCLUDED.title,
                units_sold_90d = EXCLUDED.units_sold_90d,
                units_sold_30d = EXCLUDED.units_sold_30d,
                units_sold_7d = EXCLUDED.units_sold_7d,
                avg_days_to_sell = EXCLUDED.avg_days_to_sell,
                out_of_stock_days = EXCLUDED.out_of_stock_days,
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
            units_90d, units_30d, units_7d,
            avg_days, oos_days,
            current_qty, current_price, round(avg_sale_price, 2),
            price_trend, last_sale, velocity,
        ))
        updated += 1

    logger.info(f"Updated {updated} SKU analytics records")
    return {"updated": updated}


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
    """Run the complete pipeline: snapshot inventory + ingest orders + recompute metrics."""
    # 1. Snapshot today's inventory levels (for OOS tracking)
    snapshot_inventory()

    # 2. Ingest orders
    meta = db.query_one("SELECT value FROM analytics_meta WHERE key = 'last_order_ingest'")
    if meta and meta["value"]:
        # Incremental: pull from day before last run to catch any stragglers
        last_run = meta["value"][:10]  # YYYY-MM-DD
        since = (datetime.fromisoformat(last_run) - timedelta(days=1)).strftime("%Y-%m-%d")
        ingest_result = ingest_orders(since_date=since)
    else:
        # First run: full 90-day backfill
        ingest_result = ingest_orders(full_backfill=True)

    # 3. Recompute velocity metrics
    compute_result = recompute_analytics()
    return {**ingest_result, **compute_result}
