# Analytics Service (analytics/)
> SKU sell-through analytics (analytics.pack-fresh.com)

## Key Files
- **app.py** — Flask app: data browser UI, /run endpoint, /api/analytics, /api/browse
- **compute.py** — Core engine: order ingestion, metric computation, inventory snapshots
- **migrate_sku_analytics.py** — Create tables (sku_analytics, sku_daily_sales, sku_daily_inventory, drop_events, analytics_meta)
- **db.py** — Database connection pool

## Daily Pipeline (triggered by Flow POST to /run)
1. Snapshot inventory levels from inventory_product_cache → sku_daily_inventory
2. Pull paid orders since last run via Shopify GraphQL
3. Extract line items → write to sku_daily_sales (excluding drop_events dates)
4. Recompute sku_analytics rollups with OOS-adjusted velocity
5. Zero out SKUs whose only sales were on drop days

## Velocity Formula
```
selling_days = max(1, days_active - oos_days)
daily_rate = units_sold_90d / selling_days
days_of_inventory = current_qty / daily_rate
```
- days_active: first_sale_date to today (capped at 90)
- oos_days: from sku_daily_inventory snapshots (accumulates over time), capped at days_active - 1
- Sell rate labels: Very Fast (5+/day), Fast (1+), Medium (0.3+), Slow (0.1+), Very Slow

## Known Issues
- OOS data only from 2026-03-23 onward — velocity accuracy improves over time
- Damaged variants can collide on tcgplayer_id — consumers must JOIN with is_damaged=FALSE
- Drop exclusion requires drop_events records — backfill past drops via drops service

## Database Tables (PK: shopify_variant_id, NOT tcgplayer_id)
- **sku_analytics** — per-variant velocity metrics (recomputed daily)
- **sku_daily_sales** — daily sales snapshots per variant
- **sku_daily_inventory** — daily qty snapshots for OOS tracking
- **drop_events** — drop dates to exclude from velocity (populated by drops service)
- **analytics_meta** — last run tracking

## Auth
- JWT cookie (owner only) for browser UI
- /run, /api/* endpoints open for webhook + service-to-service
