# Inventory + Breakdown Engine (inventory/)

## Key Files
- **routes/breakdown.py** — Breakdown recommendations engine + execute; recipe CRUD/search moved to shared/breakdown_routes.py blueprint
- **routes/inventory.py** — Core inventory routes, Shopify client, auth
- **app.py** — Flask app, cache manager initialization, registers shared breakdown blueprint
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

## Key Patterns
- Inventory service proxies to ingest-service via `INGEST_INTERNAL_URL` for some operations
- Auth via `requires_auth` decorator (checks ADMIN_KEY)
- CacheManager handles Shopify product cache staleness detection and background refresh
