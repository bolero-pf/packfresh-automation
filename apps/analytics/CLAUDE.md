# Analytics Service (analytics/)
> Executive analytics engine (analytics.pack-fresh.com)

## Key Files
- **app.py** — Flask app: tabbed dashboard UI (SKUs + Inventory Flow), /run pipeline trigger, /api/* endpoints
  - Inventory Flow tab. TWO streams, reconciles to the inventory page (~$485k):
    - Sealed/catalog: value/qty from `inventory_product_cache` (shopify_qty × shopify_price) — the SAME source the inventory page sums. Velocity is LEFT-joined from `sku_analytics`; a missing row = never sold = 0 sales (so we MUST base on the cache, not sku_analytics, or never-sold deadstock vanishes — that was the original $288k-vs-$400k bug).
    - Raw singles: `raw_cards` in STORED/DISPLAY (= on hand / available to buy; BARCODED is excluded bulk). Velocity from state='SOLD' + removal_date.
  - Velocity uses the TRUE daily rate = 1/`avg_days_to_sell` (first-seen + OOS adjusted), NOT units_sold_90d/90 — the naive denominator understates recent fast movers (a 6-week-old pack that sold 31 looked half as fast). days_of_inventory = qty × avg_days_to_sell.
  - **Damaged variants are excluded everywhere** (`_NOT_DAMAGED` = `is_damaged=FALSE`) — they share a tcgplayer_id with their non-damaged twin and are a separate disposition; never a dead/rollup/restock/needs-era line. Applied to the KPI, rollup, dead, restock, needs-era queries.
  - **Velocity is only as fresh as `sku_daily_sales`** — that table is rebuilt by `ingest_orders` in the daily `/run`. If the pipeline stops firing, velocity goes stale and items get FALSELY flagged dead (a UPC that sold 6 in 90d read as 0). Symptom: dead count too high / known sellers flagged. Fix: re-run the pipeline; root-fix: ensure the Shopify Flow trigger to `/run` is actually firing daily.
  - Dead capital = in stock, status='ACTIVE', not damaged, **zero sales in 90d**, AND its CURRENT stock has sat continuously **≥ DEAD_MIN_AGE_DAYS (45)**. "In stock since" = day after the most recent `qty=0` snapshot (current streak), NOT `MIN(snapshot_date)` — else a sold-out-then-restocked item reads as old when its lot just arrived. Does NOT extrapolate "months of supply" from a few sales (that mislabeled deliberately-stocked slow movers like a UPC that sold 1 in 90d). KPI uses the SAME predicate as the Job A list. ~160 SKUs / ~$63k.
  - Dead capital is a working tool, not a chart: group-by pivot (`/api/inventory/dead-by?dim=era|set_name|product_type|ip`) shows WHERE it sits (mostly older-era Pokémon), each item carries a suggested action, and `dead.csv` exports the lot for a spreadsheet pass. The roll-up "Dead $" column, the KPI, the list, and the pivot all use the one `_dead_where` predicate so the numbers agree.
  - **Break-down decision (`_dead_action`)**: for dead SEALED it's a LIQUIDITY call, judged against COST basis, NOT the inflated sealed price that's keeping it dead. Cost source: prefer `inventory_product_cache.unit_cost`, fall back to `intake_items.unit_cost_basis` (avg by tcgplayer_id) for older stock predating cost tracking — lifts dead-sealed cost coverage ~42%→~74%. Fallback costs flagged `cost_is_estimate` (`~` in UI, "Cost Estimated?" col in CSV). Per-unit break value = `sealed_breakdown_cache.best_variant_market` (component market by tcgplayer_id; analytics reads it, doesn't refresh it). parts≥cost → "break down, free +$X over cost"; parts≥0.8×cost → "break to free cash (small loss OK for velocity)"; else markdown/hold. Comparing parts vs *price* is misleading (almost nothing clears it); vs *cost*, ~26 items recover ~$7.9k (+$2.5k over cost). List/CSV show cost · sealed · parts. Non-sealed: vintage/high-value→reprice-hold, <$15→bundle, else markdown.
  - **Component velocity (`_PARTS_VEL`)**: per parent, picks the best variant (max total_component_market), sums its components' 90d sales — catalog packs via `sku_analytics`, singles via SOLD `raw_cards` — both by component tcgplayer_id, weighted by quantity_per_parent. Chain: components.variant_id → sealed_breakdown_variants.id; variants.breakdown_id → sealed_breakdown_cache.id (NOT components.breakdown_id, which is null). Shown as "parts ~N sold/90d" (store-wide demand signal, not per-box). Reveals dead sealed that breaks into HOT singles even when parts $ < sealed $ (e.g. Crown Zenith/Prismatic tins). ~55% of components resolve to a sales signal.
  - **No-recipe gap**: sealed with no `sealed_breakdown_cache` row = `needs_recipe` (amber "no recipe" tag + summary stat). ~82 dead sealed / ~$33k have no recipe, so can't be assessed — biggest lever is building recipes for these (done in the inventory/ingest breakdown UI, not analytics).
  - Endpoints: `/api/inventory/flow` (combined KPIs + sealed roll-up), `/api/inventory/raw` (raw roll-up), `/api/inventory/raw-aging` (raw dead capital), `/api/inventory/dead` (Job A list + total, top 300, each w/ action), `/api/inventory/dead-by` (dead grouped by dimension), `/api/inventory/dead.csv` (full export), `/api/inventory/restock` (Job B: ≤30 days left at true rate).
  - NOTE: product_type `card` = catalog singles + graded slabs listed as normal Shopify products (~$25k slabs / ~$21k singles in stock). This is SEPARATE from "Raw Singles" (the `raw_cards` barcoded binder/display inventory, ~$81k) — disjoint tables, not double-counted.
  - **Sales tab** — sub-tabs Overview / Channel / Buy-vs-Sell / Product-Mix, with a 7d/30d/90d/1yr window selector. Reads LIVE off source tables, NOT `daily_business_summary` (that pre-agg drifted — see below); `customer_orders` is ~10k rows so live roll-up is cheap and always correct. Charts are hand-rolled SVG (`buildChart()` — area/line/bar + hover tooltip, no CDN).
  - **Adaptive granularity** (`_granularity(days)`): day ≤31d, week ≤100d, else month — applied to EVERY sales series (overview/channel/buysell/mix) so each X tick is meaningful (no weekly buckets on a 7-day window). `_bucket_seq` fills zero-activity buckets; payloads carry `granularity` and the front-end formats X labels + the 7-day moving average (daily only) accordingly.
  - **Margin honesty**: cost-less catalog sales (`gross_margin` NULL, ~20-25% of catalog rev — older pre-cost-tracking stock) book $0 margin, never full. Margin **%** is computed on COST-KNOWN revenue (`_kpi_block`), not all revenue, so the no-cost tail doesn't dilute the rate; `cost_coverage` is surfaced. **Est. total margin** (`_nocost_projection`) projects each product_type's no-cost revenue at THAT type's own known margin rate (blended fallback) — labeled a CONSERVATIVE FLOOR: older stock runs higher-margin, AND recorded COGS is itself often overstated (ingestion stamps the most-recent weighted-avg cost across pre-tracking quantity; only resets at qty=0), so true margin exceeds both the proven and the estimated figure. Endpoints: `/api/sales/overview` (KPIs + prior-period deltas + daily series: net/orders/units/in-store/shipped/margin), `/api/sales/buysell` (weekly intake spend by `ingested_at` vs net sales), `/api/sales/mix` (weekly stacked revenue by stream or game). **Margin** = catalog `realized_margin.gross_margin` + raw singles (`sale_price-cost_basis`); margin% is vs merch revenue (realized_margin.revenue + raw sale_price), not order totals. **Raw singles** are categorized straight from `raw_cards` by barcode (game/set/graded) — their revenue is ALREADY in `customer_orders` (barcode line items), so they're a reclassification, never added on top.
  - **In-store vs shipped = `customer_orders.delivery_method`** (Shopify `fulfillmentOrders.deliveryMethod.methodType`: SHIPPING=shipped, RETAIL=walk-in POS, PICK_UP=hold pickup; RETAIL+PICK_UP = in-store). The pos/online `channel` field is WRONG for this — in-store RETAIL orders carry a customer record so the sync labels them 'online', which hid the entire in-store channel (often the LARGER one: ~87 in-store vs 54 shipped on a Saturday). Validated against a store-closed Tuesday (in-store craters to ~4) and the April-2026 store opening (pre-open is ~all shipped).
- **compute.py** — Pipeline orchestrator: calls all steps in sequence
- **price_history.py** — Daily scrydex_price_cache → scrydex_price_history snapshot (NM/raw only; auto-creates monthly partition + drops partitions >90d)
- **taxonomy.py** — Product classification: IP, form_factor, set, era → product_taxonomy
- **customers.py** — Shopify orders → customer_orders + customer_summary + daily_business_summary
- **margins.py** — Daily sales × COGS × market price → realized_margin. Revenue here is `sku_daily_sales.revenue`, written by `compute.ingest_orders` as **line `originalTotal` minus `discountAllocations`** (net of ALL discounts — line + allocated order/cart codes). NOT list price: ~63% of orders carry a discount and most are cart-level, so using originalTotal alone overstated margin ~3-4%. `discountedTotalSet` is insufficient (line-level discounts only). This revenue is already ex-tax/ex-shipping (line totals), so it's the correct margin base and is unaffected by the customer_orders Net Sales change.
  - **Margin only exists for the COGS-tracking era — `COGS_TRACKING_START = 2026-03-23`.** Before that, COGS wasn't captured, and `unit_cost` is the CURRENT (single point-in-time) value — for appreciating stock that's far above the real cost at sale time (a $31 distro ETB sold at $50 MSRP reads ~$143 cost today → fake loss). So we do NOT compute/retroactively price any sale before the cutoff. NEVER widen this or DELETE+recompute the whole table — recomputing re-prices old rows at today's cost. The incremental `WHERE NOT EXISTS` (each day computed once, near sale time) is the point; leave it.
  - **Suspect-cost guard**: within the era, if recorded `total_cogs >= revenue` (sale "lost money" on paper) the cost is treated as polluted/stale and set NULL → flows into the category-based estimate (`_nocost_projection`) instead of booking a fake loss. Residual breakdown-bug pollution that lands cost *below* sale price stays as conservatively-low margin.
  - Raw-card margin is separate and correct by construction: `raw_cards.cost_basis` is frozen at intake (true acquisition cost), so `sale_price - cost_basis` needs no era cutoff or guard.
- **migrate_sku_analytics.py** — v1 tables (sku_analytics, sku_daily_sales, sku_daily_inventory, drop_events, analytics_meta)
- **migrate_analytics_v2.py** — v2 tables (scrydex_price_history, product_taxonomy, customer_orders, customer_summary, daily_business_summary, realized_margin)
- DB via shared/db.py (no local db.py)

## Revenue definition (customer_orders.net_amount = NET SALES)
- `net_amount` = **net sales: ex-tax, shipping kept, net of refunds.** Sales tax is a pass-through (remitted to the state), so it's stripped; shipping charged is kept. ~2.2% below the orders-page total (tax), which itself sits ~2% above pure merchandise (shipping).
- Computed as `kept - currentTotalTax`, where `kept = currentTotalPrice` for not-yet-captured AUTHORIZED/PARTIALLY_PAID orders (netPayment is $0 until capture) else `netPayment`.
- **Why not `currentTotalPrice - totalRefunded`** (the old formula): `currentTotalPrice` ALREADY nets return-based refunds, so subtracting the refund again double-counts — refunded orders went negative (a fully-refunded $1.5k order recorded −$1,500). `netPayment` is Shopify's true kept amount and handles both return refunds and money-back refunds correctly.
- `order_total` column = `currentTotalPrice` (gross, incl tax+shipping) kept for reference.
- **Voids never count**: VOIDED/EXPIRED are excluded by the financial-status gate (verified 0 of 99 voided orders in a 90d window leaked in). A void releases the authorization — `netPayment`/`currentTotalPrice` go to $0 and the row is deleted on re-sync.

## Order capture (financial status)
- A sale is counted when the order is **placed**, attributed by `createdAt` date — NOT fulfillment, NOT payment capture.
- **Both** order syncs (`compute.ingest_orders` → sku_daily_sales, `customers.sync_customer_orders` → customer_orders) count `COUNTED_FINANCIAL_STATUSES` = PAID / AUTHORIZED / PARTIALLY_PAID / PARTIALLY_REFUNDED / REFUNDED, and skip VOIDED / EXPIRED / PENDING. Defined in `customers.py`, imported by `compute.py`.
- **Why AUTHORIZED matters**: this store authorizes payment at checkout and captures (→PAID) later, around fulfillment. Filtering on `financial_status:paid` (the old behavior) blanked the most recent 1-3 days — e.g. June 9 was 100% AUTHORIZED / $27k and showed as ~$0. Capture flips authorized→paid within ~1-3 days.
- **Void handling**: `sync_customer_orders` DELETEs a row whose status falls out of the counted set (an order counted while AUTHORIZED that later voids). `ingest_orders` re-sums + overwrites each day, so excluded voids drop out on the next re-sync of that day's window.

## Daily Pipeline (triggered by Flow POST to /run)
1. Snapshot scrydex prices → scrydex_price_history (before nightly sync overwrites cache)
2. Snapshot inventory levels → sku_daily_inventory
3. Pull paid orders → sku_daily_sales
4. Recompute sku_analytics rollups with OOS-adjusted velocity
5. Classify product taxonomy (IP, form_factor, set, era)
6. Sync customer orders → customer_orders
7. Recompute customer_summary rollups
8. Compute daily_business_summary — recomputes a TRAILING 14-DAY WINDOW (`backfill_daily_summaries(days=14)`), not just today
9. Compute realized_margin (sales × COGS × market price)

Each step is independent — if one fails, the rest still run.

**daily_business_summary trailing-window fix**: step 8 used to compute only `date.today()` once per run. Orders keep landing all day after the morning `/run`, so each day froze at its pre-run slice — recent months collapsed (May 2026 logged $6.7k vs the real ~$368k) while pre-backfill history stayed correct. Now it re-runs the last 14 days so each day finalizes as its orders complete. The Sales tab sidesteps this entirely by reading `customer_orders` live; the pre-agg is kept correct mainly for the future Morning Coffee dashboard.

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
- **Era**: AUTHORITATIVE — read from the `custom.era` Shopify metafield, mirrored into `inventory_product_cache.era` by the shared cache sync (shopify_client fetches it alongside the `tcg` namespace; cache_manager stores it). NEVER inferred. The old ERA_SETS guessing is dead for era (still used for set_name fallback only). Non-Pokemon have no era by design — the dashboards label them by game, not "unclassified"; a Pokemon with no era = the metafield isn't set on that product ("(needs era)").
- **Expansion/set**: scrydex_price_cache lookup by tcgplayer_id, fallback to ERA_SETS title match (set_name only)
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
- **customer_orders** — per-customer order log (order_total, refund_amount, fulfillment_status, **delivery_method** [SHIPPING/RETAIL/PICK_UP — the real in-store vs shipped signal], items JSONB)
- **customer_summary** — rolled-up customer dimension (cohort_month, total_orders, net_spend, avg_order_value, vip_tier, days_between_orders)
- **daily_business_summary** — pre-aggregated daily KPIs (revenue, orders, AOV, new/returning customers, intake spend, **revenue_instore/revenue_shipped + orders_instore/orders_shipped** from delivery_method). Recomputed over a trailing 14-day window each run (see fix above).
- **realized_margin** — per-variant per-day margin (cogs_at_sale, market_price_at_sale, gross_margin, margin_pct)

## Auth
- JWT cookie (owner only) for browser UI
- /run, /run/backfill, /api/* endpoints open for webhook + service-to-service
- /run/migrate requires owner JWT
