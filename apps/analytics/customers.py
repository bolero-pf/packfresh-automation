"""
Customer analytics pipeline.

Syncs Shopify order data into customer_orders, then computes
customer_summary rollups and daily_business_summary aggregates.

Runs nightly as part of the analytics pipeline.
"""

import json
import logging
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal

from shopify_graphql import shopify_gql, gid_numeric
import db

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# GraphQL — richer than the velocity query, includes customer + fulfillment
# ═══════════════════════════════════════════════════════════════════════════════

CUSTOMER_ORDERS_QUERY = """
query CustomerOrders($first:Int!, $after:String, $query:String!) {
  orders(first:$first, after:$after, query:$query, sortKey:CREATED_AT) {
    edges {
      cursor
      node {
        id
        name
        createdAt
        displayFinancialStatus
        displayFulfillmentStatus
        customer { id email firstName lastName }
        currentTotalPriceSet { shopMoney { amount } }
        totalRefundedSet { shopMoney { amount } }
        lineItems(first:100) {
          edges { node {
            variant { id }
            title
            quantity
            originalTotalSet { shopMoney { amount } }
          }}
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Order sync
# ═══════════════════════════════════════════════════════════════════════════════

def sync_customer_orders(full_backfill: bool = False):
    """
    Pull Shopify orders and upsert into customer_orders.

    Args:
        full_backfill: if True, pull 365 days. Otherwise incremental from last sync.
    """
    if full_backfill:
        since = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")
    else:
        meta = db.query_one("SELECT value FROM analytics_meta WHERE key = 'last_customer_sync'")
        if meta and meta["value"]:
            last_run = meta["value"][:10]
            since = (datetime.fromisoformat(last_run) - timedelta(days=2)).strftime("%Y-%m-%d")
        else:
            since = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

    logger.info(f"Syncing customer orders since {since}")

    query_filter = f'financial_status:paid created_at:>="{since}"'
    cursor = None
    total_orders = 0
    skipped_no_customer = 0

    while True:
        variables = {"first": 50, "query": query_filter}
        if cursor:
            variables["after"] = cursor

        try:
            data = shopify_gql(CUSTOMER_ORDERS_QUERY, variables)
        except Exception as e:
            logger.error(f"GraphQL error fetching customer orders: {e}")
            break

        edges = data.get("data", {}).get("orders", {}).get("edges", [])
        if not edges:
            break

        for edge in edges:
            node = edge["node"]
            customer = node.get("customer")
            if not customer or not customer.get("id"):
                skipped_no_customer += 1
                continue

            customer_id = int(gid_numeric(customer["id"]))
            order_id = int(gid_numeric(node["id"]))
            order_date = node["createdAt"][:10]
            order_total = float(node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
            refund_amount = float(node.get("totalRefundedSet", {}).get("shopMoney", {}).get("amount", 0))
            net_amount = round(order_total - refund_amount, 2)

            fulfillment_status = node.get("displayFulfillmentStatus")

            # Extract line items summary
            items = []
            item_count = 0
            for li_edge in node.get("lineItems", {}).get("edges", []):
                li = li_edge["node"]
                qty = li.get("quantity", 0) or 0
                item_count += qty
                variant = li.get("variant")
                items.append({
                    "variant_id": int(gid_numeric(variant["id"])) if variant and variant.get("id") else None,
                    "title": li.get("title", ""),
                    "qty": qty,
                    "price": float(li.get("originalTotalSet", {}).get("shopMoney", {}).get("amount", 0)),
                })

            db.execute("""
                INSERT INTO customer_orders (
                    customer_id, order_id, order_gid, order_name,
                    order_date, order_total, refund_amount, net_amount,
                    channel, fulfillment_status, created_at_ts,
                    item_count, items
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (customer_id, order_id) DO UPDATE SET
                    refund_amount = EXCLUDED.refund_amount,
                    net_amount = EXCLUDED.net_amount,
                    fulfillment_status = EXCLUDED.fulfillment_status,
                    item_count = EXCLUDED.item_count,
                    items = EXCLUDED.items
            """, (
                customer_id, order_id, node["id"], node.get("name"),
                order_date, order_total, refund_amount, net_amount,
                "online",  # default — POS will show as "pos" when brick-and-mortar is live
                fulfillment_status,
                node["createdAt"],
                item_count,
                json.dumps(items),
            ))
            total_orders += 1

            # Also upsert minimal customer info for the summary
            _upsert_customer_stub(customer_id, customer)

        page_info = data.get("data", {}).get("orders", {}).get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    # Update last sync timestamp
    now = datetime.now(timezone.utc).isoformat()
    db.execute("""
        INSERT INTO analytics_meta (key, value, updated_at)
        VALUES ('last_customer_sync', %s, CURRENT_TIMESTAMP)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
    """, (now,))

    logger.info(f"Synced {total_orders} customer orders ({skipped_no_customer} skipped, no customer)")
    return {"orders": total_orders, "skipped": skipped_no_customer}


def _upsert_customer_stub(customer_id: int, customer: dict):
    """Upsert basic customer identity into customer_summary if not exists."""
    db.execute("""
        INSERT INTO customer_summary (customer_id, customer_gid, email, first_name, last_name)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            email = COALESCE(EXCLUDED.email, customer_summary.email),
            first_name = COALESCE(EXCLUDED.first_name, customer_summary.first_name),
            last_name = COALESCE(EXCLUDED.last_name, customer_summary.last_name)
    """, (
        customer_id,
        customer.get("id", ""),
        customer.get("email"),
        customer.get("firstName"),
        customer.get("lastName"),
    ))


# ═══════════════════════════════════════════════════════════════════════════════
# Customer summary rollup
# ═══════════════════════════════════════════════════════════════════════════════

def recompute_customer_summaries():
    """
    Recompute customer_summary from customer_orders.
    Pure SQL rollup — fast.
    """
    logger.info("Recomputing customer summaries...")

    today = date.today()
    d30 = today - timedelta(days=30)
    d90 = today - timedelta(days=90)

    result = db.execute("""
        UPDATE customer_summary cs SET
            first_order_date = sub.first_order_date,
            first_order_amount = sub.first_order_amount,
            cohort_month = TO_CHAR(sub.first_order_date, 'YYYY-MM'),
            total_orders = sub.total_orders,
            total_spend = sub.total_spend,
            total_refunds = sub.total_refunds,
            net_spend = sub.net_spend,
            avg_order_value = sub.avg_order_value,
            max_order_value = sub.max_order_value,
            last_order_date = sub.last_order_date,
            last_order_amount = sub.last_order_amount,
            order_frequency_30d = sub.freq_30d,
            order_frequency_90d = sub.freq_90d,
            updated_at = NOW()
        FROM (
            SELECT
                co.customer_id,
                MIN(co.order_date) AS first_order_date,
                (ARRAY_AGG(co.order_total ORDER BY co.order_date ASC))[1] AS first_order_amount,
                COUNT(*) AS total_orders,
                SUM(co.order_total) AS total_spend,
                SUM(co.refund_amount) AS total_refunds,
                SUM(co.net_amount) AS net_spend,
                ROUND(AVG(co.net_amount), 2) AS avg_order_value,
                MAX(co.order_total) AS max_order_value,
                MAX(co.order_date) AS last_order_date,
                (ARRAY_AGG(co.order_total ORDER BY co.order_date DESC))[1] AS last_order_amount,
                COUNT(*) FILTER (WHERE co.order_date >= %s) AS freq_30d,
                COUNT(*) FILTER (WHERE co.order_date >= %s) AS freq_90d
            FROM customer_orders co
            GROUP BY co.customer_id
        ) sub
        WHERE cs.customer_id = sub.customer_id
    """, (d30, d90))

    # Compute days_between_orders for customers with 2+ orders
    db.execute("""
        UPDATE customer_summary cs SET
            days_between_orders = sub.avg_gap
        FROM (
            SELECT
                customer_id,
                ROUND(AVG(gap)::numeric, 1) AS avg_gap
            FROM (
                SELECT
                    customer_id,
                    order_date - LAG(order_date) OVER (
                        PARTITION BY customer_id ORDER BY order_date
                    ) AS gap
                FROM customer_orders
            ) gaps
            WHERE gap IS NOT NULL
            GROUP BY customer_id
        ) sub
        WHERE cs.customer_id = sub.customer_id
    """)

    # Detect VIP tier from customer tags (if we have it in orders)
    # For now, derive from spend thresholds matching VIP service logic
    db.execute("""
        UPDATE customer_summary SET
            vip_tier = CASE
                WHEN order_frequency_90d > 0 AND (
                    SELECT SUM(net_amount) FROM customer_orders co
                    WHERE co.customer_id = customer_summary.customer_id
                      AND co.order_date >= %s
                ) >= 2500 THEN 'VIP3'
                WHEN order_frequency_90d > 0 AND (
                    SELECT SUM(net_amount) FROM customer_orders co
                    WHERE co.customer_id = customer_summary.customer_id
                      AND co.order_date >= %s
                ) >= 1250 THEN 'VIP2'
                WHEN order_frequency_90d > 0 AND (
                    SELECT SUM(net_amount) FROM customer_orders co
                    WHERE co.customer_id = customer_summary.customer_id
                      AND co.order_date >= %s
                ) >= 500 THEN 'VIP1'
                ELSE 'VIP0'
            END
    """, (d90, d90, d90))

    updated = result if isinstance(result, int) else 0
    logger.info(f"Recomputed {updated} customer summaries")
    return {"updated": updated}


# ═══════════════════════════════════════════════════════════════════════════════
# Daily business summary
# ═══════════════════════════════════════════════════════════════════════════════

def compute_daily_business_summary(target_date: date = None):
    """
    Aggregate a single day's KPIs into daily_business_summary.
    If target_date is None, computes for today.
    """
    target = target_date or date.today()
    logger.info(f"Computing daily business summary for {target}")

    # Order metrics from customer_orders
    order_stats = db.query_one("""
        SELECT
            COUNT(*) AS total_orders,
            COALESCE(SUM(order_total), 0) AS total_revenue,
            COALESCE(SUM(refund_amount), 0) AS total_refunds,
            COALESCE(SUM(net_amount), 0) AS net_revenue,
            COUNT(DISTINCT customer_id) AS unique_customers,
            ROUND(AVG(net_amount), 2) AS avg_order_value,
            COALESCE(SUM(item_count), 0) AS total_units_sold,
            COUNT(*) FILTER (WHERE channel = 'online') AS orders_online,
            COUNT(*) FILTER (WHERE channel = 'pos') AS orders_pos,
            COALESCE(SUM(net_amount) FILTER (WHERE channel = 'online'), 0) AS revenue_online,
            COALESCE(SUM(net_amount) FILTER (WHERE channel = 'pos'), 0) AS revenue_pos
        FROM customer_orders
        WHERE order_date = %s
    """, (target,))

    # New vs returning customers
    new_customers = 0
    if order_stats and order_stats["unique_customers"]:
        new_row = db.query_one("""
            SELECT COUNT(*) AS new_custs
            FROM customer_summary
            WHERE first_order_date = %s
        """, (target,))
        new_customers = new_row["new_custs"] if new_row else 0

    returning = (order_stats["unique_customers"] or 0) - new_customers

    # Intake metrics
    intake_stats = db.query_one("""
        SELECT
            COUNT(*) AS sessions,
            COALESCE(SUM(total_offer_amount), 0) AS total_cost,
            COALESCE(SUM(
                (SELECT COALESCE(SUM(quantity), 0)
                 FROM intake_items WHERE session_id = s.id)
            ), 0) AS total_items
        FROM intake_sessions s
        WHERE s.created_at::date = %s
          AND s.status NOT IN ('cancelled', 'in_progress')
    """, (target,))

    db.execute("""
        INSERT INTO daily_business_summary (
            summary_date, total_orders, total_revenue, total_refunds, net_revenue,
            unique_customers, new_customers, returning_customers, avg_order_value,
            total_units_sold, orders_online, orders_pos, revenue_online, revenue_pos,
            intake_sessions, intake_total_cost, intake_total_items, updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (summary_date) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            total_revenue = EXCLUDED.total_revenue,
            total_refunds = EXCLUDED.total_refunds,
            net_revenue = EXCLUDED.net_revenue,
            unique_customers = EXCLUDED.unique_customers,
            new_customers = EXCLUDED.new_customers,
            returning_customers = EXCLUDED.returning_customers,
            avg_order_value = EXCLUDED.avg_order_value,
            total_units_sold = EXCLUDED.total_units_sold,
            orders_online = EXCLUDED.orders_online,
            orders_pos = EXCLUDED.orders_pos,
            revenue_online = EXCLUDED.revenue_online,
            revenue_pos = EXCLUDED.revenue_pos,
            intake_sessions = EXCLUDED.intake_sessions,
            intake_total_cost = EXCLUDED.intake_total_cost,
            intake_total_items = EXCLUDED.intake_total_items,
            updated_at = NOW()
    """, (
        target,
        order_stats["total_orders"] or 0,
        order_stats["total_revenue"] or 0,
        order_stats["total_refunds"] or 0,
        order_stats["net_revenue"] or 0,
        order_stats["unique_customers"] or 0,
        new_customers,
        max(0, returning),
        order_stats["avg_order_value"],
        order_stats["total_units_sold"] or 0,
        order_stats["orders_online"] or 0,
        order_stats["orders_pos"] or 0,
        order_stats["revenue_online"] or 0,
        order_stats["revenue_pos"] or 0,
        intake_stats["sessions"] if intake_stats else 0,
        intake_stats["total_cost"] if intake_stats else 0,
        intake_stats["total_items"] if intake_stats else 0,
    ))

    logger.info(f"Daily summary for {target}: {order_stats['total_orders'] or 0} orders, "
                f"${order_stats['net_revenue'] or 0} net revenue")
    return {"date": str(target), "orders": order_stats["total_orders"] or 0}


def backfill_daily_summaries(days: int = 365):
    """Backfill daily_business_summary for the last N days."""
    today = date.today()
    for i in range(days, -1, -1):
        target = today - timedelta(days=i)
        compute_daily_business_summary(target)
    logger.info(f"Backfilled {days + 1} daily summaries")
