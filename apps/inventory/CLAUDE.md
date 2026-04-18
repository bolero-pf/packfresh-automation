# Inventory + Breakdown Engine (inventory/)

## Key Files
- **routes/breakdown.py** — Breakdown recommendations engine + execute; recipe CRUD/search moved to shared/breakdown_routes.py blueprint
- **routes/inventory.py** — Core inventory routes, Shopify client, auth
- **routes/ai_enrichment.py** — AI Enrichment backfill console (agentic metafields, GTINs via Claude API)
- **app.py** — Flask app, cache manager initialization, registers shared breakdown blueprint + AI enrichment blueprint
- DB via shared/db.py (no local db.py)

## Breakdown Engine
The breakdown system spans 3 services (inventory, ingest-service, ingestion) sharing the same DB tables:

### Tables
- `sealed_breakdown_cache` — One row per parent product (tcgplayer_id). Denormalized `best_variant_market`.
- `sealed_breakdown_variants` — Named configs per product (e.g., "Open + Sell Singles")
- `sealed_breakdown_components` — Individual components per variant (booster packs, promos, etc.)
- `inventory_product_cache` — Shopify store snapshot (prices, qty). Refreshed by CacheManager.

### Price Sources
- **Store prices**: Live from `inventory_product_cache` via JOIN at read time. Already fresh.
- **Market prices**: Stored on `sealed_breakdown_components.market_price`, JIT-refreshed from PPT API when stale (>4 hours). Tracked via `market_price_updated_at` column.
- Denormalized totals (`total_component_market`, `best_variant_market`) recomputed after refresh.

### Key Functions
- `_build_recommendations()` in breakdown.py — Joins inventory + recipes, computes scores
- `refresh_stale_component_prices()` in shared/breakdown_helpers.py — JIT market price refresh
- PPT client via `_get_ppt_client()` from routes/inventory.py

## AI Enrichment (temporary backfill tool)
- Page at `/inventory/ai-enrichment` for batch-generating agentic metafields + GTINs
- Uses `shared/ai_enrichment.py` (Claude Haiku 4.5 API)
- Workflow: Scan Shopify → Generate → Review/Edit → Approve → Push
- Table: `ai_enrichment_queue` tracks status per product
- Will be deprecated once all existing products are enriched (new products get it automatically via product_enrichment.py step 8)
- Requires `ANTHROPIC_API_KEY` env var

## Key Patterns
- Inventory service proxies to ingest-service via `INGEST_INTERNAL_URL` for some operations
- Auth via `requires_auth` decorator (checks ADMIN_KEY)
- CacheManager handles Shopify product cache staleness detection and background refresh

## Pricing: ALWAYS Scrydex-first, PPT fallback
PPT graded data is unreliable (often 3× off from market). Scrydex has holes (Japanese,
Scrydex-only cards) so PPT stays as a fallback — **never** as the primary source.

- **Raw per-condition:** `PriceCache.get_card_by_tcgplayer_id(tcg_id)` →
  `ScrydexClient.extract_condition_price(card_data, condition, variant=...)`. PPT fallback
  only on cache miss.
- **Graded per-grade:** `get_live_graded_comps(tcg_id, company, grade, db, ...)` from
  `shared/graded_pricing.py`. PPT fallback via `PriceProvider.get_graded_price()` only on miss.
- For sealed-breakdown component pricing, `refresh_stale_component_prices()` in
  `shared/breakdown_helpers.py` already routes through the correct hierarchy — use it,
  don't hand-roll a PPT call.
- **Self-check:** grep any pricing diff for `ppt_client.get_card_by_tcgplayer_id` —
  if it's not preceded by a `PriceCache` / `get_live_graded_comps` call in the same
  function, the change is wrong. Rewrite before committing.
