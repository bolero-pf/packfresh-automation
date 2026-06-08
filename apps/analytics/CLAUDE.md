# Analytics Service (analytics/)
> Executive analytics engine (analytics.pack-fresh.com)

## Key Files
- **app.py** — Flask app: tabbed dashboard UI (SKUs + Inventory Flow), /run pipeline trigger, /api/* endpoints
  - Inventory Flow tab. TWO streams, reconciles to the inventory page (~$485k):
    - Sealed/catalog: value/qty from `inventory_product_cache` (shopify_qty × shopify_price) — the SAME source the inventory page sums. Velocity is LEFT-joined from `sku_analytics`; a missing row = never sold = 0 sales (so we MUST base on the cache, not sku_analytics, or never-sold deadstock vanishes — that was the original $288k-vs-$400k bug).
    - Raw singles: `raw_cards` in STORED/DISPLAY (= on hand / available to buy; BARCODED is excluded bulk). Velocity from state='SOLD' + removal_date.
  - Velocity uses the TRUE daily rate = 1/`avg_days_to_sell` (first-seen + OOS adjusted), NOT units_sold_90d/90 — the naive denominator understates recent fast movers (a 6-week-old pack that sold 31 looked half as fast). days_of_inventory = qty × avg_days_to_sell.
  - Dead capital = in stock, status='ACTIVE', **zero sales in 90d**, AND its CURRENT stock has sat continuously **≥ DEAD_MIN_AGE_DAYS (45)**. "In stock since" = day after the most recent `qty=0` snapshot (current streak), NOT `MIN(snapshot_date)` — else a sold-out-then-restocked item reads as old when its lot just arrived. Does NOT extrapolate "months of supply" from a few sales (that mislabeled deliberately-stocked slow movers like a UPC that sold 1 in 90d). KPI uses the SAME predicate as the Job A list. ~160 SKUs / ~$63k.
  - Dead capital is a working tool, not a chart: group-by pivot (`/api/inventory/dead-by?dim=era|set_name|product_type|ip`) shows WHERE it sits (mostly older-era Pokémon), each item carries a suggested action, and `dead.csv` exports the lot for a spreadsheet pass. The roll-up "Dead $" column, the KPI, the list, and the pivot all use the one `_dead_where` predicate so the numbers agree.
  - **Break-down decision (`_dead_action`)**: for dead SEALED it's a LIQUIDITY call, judged against COST basis, NOT the inflated sealed price that's keeping it dead. Cost source: prefer `inventory_product_cache.unit_cost`, fall back to `intake_items.unit_cost_basis` (avg by tcgplayer_id) for older stock predating cost tracking — lifts dead-sealed cost coverage ~42%→~74%. Fallback costs flagged `cost_is_estimate` (`~` in UI, "Cost Estimated?" col in CSV). Per-unit break value = `sealed_breakdown_cache.best_variant_market` (component market by tcgplayer_id; analytics reads it, doesn't refresh it). parts≥cost → "break down, free +$X over cost"; parts≥0.8×cost → "break to free cash (small loss OK for velocity)"; else markdown/hold. Comparing parts vs *price* is misleading (almost nothing clears it); vs *cost*, ~26 items recover ~$7.9k (+$2.5k over cost). List/CSV show cost · sealed · parts. Non-sealed: vintage/high-value→reprice-hold, <$15→bundle, else markdown.
  - **Component velocity (`_PARTS_VEL`)**: per parent, picks the best variant (max total_component_market), sums its components' 90d sales — catalog packs via `sku_analytics`, singles via SOLD `raw_cards` — both by component tcgplayer_id, weighted by quantity_per_parent. Chain: components.variant_id → sealed_breakdown_variants.id; variants.breakdown_id → sealed_breakdown_cache.id (NOT components.breakdown_id, which is null). Shown as "parts ~N sold/90d" (store-wide demand signal, not per-box). Reveals dead sealed that breaks into HOT singles even when parts $ < sealed $ (e.g. Crown Zenith/Prismatic tins). ~55% of components resolve to a sales signal.
  - **No-recipe gap**: sealed with no `sealed_breakdown_cache` row = `needs_recipe` (amber "no recipe" tag + summary stat). ~82 dead sealed / ~$33k have no recipe, so can't be assessed — biggest lever is building recipes for these (done in the inventory/ingest breakdown UI, not analytics).
  - Endpoints: `/api/inventory/flow` (combined KPIs + sealed roll-up), `/api/inventory/raw` (raw roll-up), `/api/inventory/raw-aging` (raw dead capital), `/api/inventory/dead` (Job A list + total, top 300, each w/ action), `/api/inventory/dead-by` (dead grouped by dimension), `/api/inventory/dead.csv` (full export), `/api/inventory/restock` (Job B: ≤30 days left at true rate).
  - NOTE: product_type `card` = catalog singles + graded slabs listed as normal Shopify products (~$25k slabs / ~$21k singles in stock). This is SEPARATE from "Raw Singles" (the `raw_cards` barcoded binder/display inventory, ~$81k) — disjoint tables, not double-counted.
- **compute.py** — Pipeline orchestrator: calls all steps in sequence
- **price_history.py** — Daily scrydex_price_cache → scrydex_price_history snapshot (NM/raw only; auto-creates monthly partition + drops partitions >90d)
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
- **Non-TCG retail**: board games / supplies / apparel are classified from Shopify product-type tags (`board game`, `accessories`/`sleeve`, `apparel`) via TAG_PRODUCT_RULES, checked BEFORE the title regex. Without this they fall through to card/single_card. The Shopify product_type column is NOT cached locally — only `tags` are, so tags are the signal. (shared/product_enrichment.py mislabels these too for intake/ingest, but is out of scope unless those services need it.)
- **Sealed missed by the regex**: the catalog holds no raw singles (those live in `raw_cards`), so a non-graded item the form-factor regex defaulted to `single_card` but that's tagged `sealed` is really sealed (odd-named starter decks, mini tins, exclusives, bundles) → product_type forced to `sealed`. Runs AFTER the board-game/accessory tag rules (those tags also include `sealed`), and leaves `slab` alone. This reclaimed ~$18k that was inflating the `card` bucket; the residual `card` in-stock value (~$25k) is graded slabs listed as catalog products (slabs are NOT barcoded raw_cards).
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
- **scrydex_price_history** — daily NM/raw market price snapshots. RANGE-partitioned by snapshot_date (one partition/month, e.g. scrydex_price_history_202606), 90-day retention via partition DROP. Only condition='NM'/price_type='raw' stored (the sole consumer, margins.py, reads nothing else). Two indexes: idx_sph_unique_p (dedup/ON CONFLICT), idx_sph_tcg_date_p (margins lookup). Migrated from a flat 50M-row/12GB heap by migrate_price_history_partition.py — the pre-migration heap is preserved as `scrydex_price_history_old` (DROP after verifying margins).
- **product_taxonomy** — dimensional classification (ip, form_factor, expansion_id, set_name, era, product_type)
- **customer_orders** — per-customer order log (order_total, refund_amount, fulfillment_status, items JSONB)
- **customer_summary** — rolled-up customer dimension (cohort_month, total_orders, net_spend, avg_order_value, vip_tier, days_between_orders)
- **daily_business_summary** — pre-aggregated daily KPIs (revenue, orders, AOV, new/returning customers, intake spend)
- **realized_margin** — per-variant per-day margin (cogs_at_sale, market_price_at_sale, gross_margin, margin_pct)

## Auth
- JWT cookie (owner only) for browser UI
- /run, /run/backfill, /api/* endpoints open for webhook + service-to-service
- /run/migrate requires owner JWT
