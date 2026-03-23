# Analytics Service (analytics/)
> SKU sell-through analytics (analytics.pack-fresh.com or internal)

## Key Files
- **app.py** — Flask app: /run (daily trigger), /run/backfill, /api/analytics (batch lookup)
- **compute.py** — Core engine: order ingestion from Shopify GraphQL, metric computation
- **migrate_sku_analytics.py** — Create sku_analytics + sku_daily_sales + analytics_meta tables
- **db.py** — Database connection pool

## Daily Pipeline
1. Triggered via Shopify Flow POST to /run (same pattern as VIP sweep_kick)
2. Pulls paid orders since last run via GraphQL
3. Extracts line items → writes to sku_daily_sales (date, variant, units, revenue)
4. Recomputes sku_analytics rollups (90d/30d/7d windows, velocity score, OOS days)
5. First run does full 90-day backfill; subsequent runs are incremental

## Database Tables
- **sku_analytics** — per-variant velocity metrics (PK: shopify_variant_id)
- **sku_daily_sales** — daily sales snapshots per variant
- **analytics_meta** — key-value for tracking last run timestamp

## Key Metrics
- units_sold_90d/30d/7d, avg_days_to_sell, out_of_stock_days
- velocity_score (composite: daily rate + demand bonus + stock penalty)
- price_trend_pct (current price vs avg sale price)

## Shared Helpers
- **shared/sku_analytics.py** — read functions for other services:
  - get_analytics_for_tcgplayer_ids()
  - get_analytics_for_variant_ids()
  - compute_offer_adjustment() — collection-level offer scoring (70-87% bounds)
