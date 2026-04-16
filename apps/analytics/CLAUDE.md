# Analytics Service (analytics/)
> Executive analytics engine (analytics.pack-fresh.com)

## Key Files
- **app.py** — Flask app: dashboard UI, /run pipeline trigger, /api/* endpoints
- **compute.py** — Pipeline orchestrator: calls all steps in sequence
- **price_history.py** — Daily scrydex_price_cache → scrydex_price_history snapshot
- **taxonomy.py** — Product classification: IP, form_factor, set, era → product_taxonomy
- **customers.py** — Shopify orders → customer_orders + customer_summary + daily_business_summary
- **margins.py** — Daily sales × COGS × market price → realized_margin
- **migrate_sku_analytics.py** — v1 tables (sku_analytics, sku_daily_sales, sku_daily_inventory, drop_events, analytics_meta)
- **migrate_analytics_v2.py** — v2 tables (scrydex_price_history, product_taxonomy, customer_orders, customer_summary, daily_business_summary, realized_margin)
- DB via shared/db.py (no local db.py)

## Daily Pipeline (triggered by Flow POST to /run)
1. Snapshot scrydex prices → scrydex_price_history (before nightly sync overwrites cache)
2. Snapshot inventory levels → sku_daily_inventory
3. Pull paid orders → sku_daily_sales
4. Recompute sku_analytics rollups with OOS-adjusted velocity
5. Classify product taxonomy (IP, form_factor, set, era)
6. Sync customer orders → customer_orders
7. Recompute customer_summary rollups
8. Compute daily_business_summary
9. Compute realized_margin (sales × COGS × market price)

Each step is independent — if one fails, the rest still run.

## Velocity Formula
```
selling_days = max(1, days_active - oos_days)
daily_rate = units_sold_90d / selling_days
days_of_inventory = current_qty / daily_rate
```
- days_active: first_sale_date to today (capped at 90)
- oos_days: from sku_daily_inventory snapshots (accumulates over time), capped at days_active - 1
- Sell rate labels: Very Fast (5+/day), Fast (1+), Medium (0.3+), Slow (0.1+), Very Slow

## Taxonomy Classification
- **IP detection**: scrydex expansion data (Pokemon-only for now), title keyword fallback for MTG/Yu-Gi-Oh/One Piece/Lorcana
- **Form factor**: reuses TYPE_RULES regex from shared/product_enrichment.py
- **Expansion/era**: scrydex_price_cache lookup by tcgplayer_id, fallback to ERA_SETS from product_enrichment.py
- **manual_override=TRUE** on product_taxonomy skips auto-classification

## Known Issues
- OOS data only from 2026-03-23 onward — velocity accuracy improves over time
- Price history starts from deploy date (no backfill possible for historical prices)
- Damaged variants can collide on tcgplayer_id — consumers must JOIN with is_damaged=FALSE
- Drop exclusion requires drop_events records — backfill past drops via drops service
- Scrydex currently Pokemon-only — MTG/Yu-Gi-Oh taxonomy relies on title parsing

## Database Tables

### v1 (PK: shopify_variant_id, NOT tcgplayer_id)
- **sku_analytics** — per-variant velocity metrics (recomputed daily)
- **sku_daily_sales** — daily sales snapshots per variant
- **sku_daily_inventory** — daily qty snapshots for OOS tracking
- **drop_events** — drop dates to exclude from velocity (populated by drops service)
- **analytics_meta** — last run tracking (keys: last_order_ingest, last_customer_sync)

### v2
- **scrydex_price_history** — daily market price snapshots (expansion_id, tcgplayer_id, market_price, low_price)
- **product_taxonomy** — dimensional classification (ip, form_factor, expansion_id, set_name, era, product_type)
- **customer_orders** — per-customer order log (order_total, refund_amount, fulfillment_status, items JSONB)
- **customer_summary** — rolled-up customer dimension (cohort_month, total_orders, net_spend, avg_order_value, vip_tier, days_between_orders)
- **daily_business_summary** — pre-aggregated daily KPIs (revenue, orders, AOV, new/returning customers, intake spend)
- **realized_margin** — per-variant per-day margin (cogs_at_sale, market_price_at_sale, gross_margin, margin_pct)

## Auth
- JWT cookie (owner only) for browser UI
- /run, /run/backfill, /api/* endpoints open for webhook + service-to-service
- /run/migrate requires owner JWT
