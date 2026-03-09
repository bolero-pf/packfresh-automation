# inventory.pack-fresh.com

Standalone inventory management service for Pack Fresh.  
Extracted from `price_updater`'s inventory blueprint and reimplemented as a  
fully independent Flask app backed by the shared Postgres database.

## What changed vs the old blueprint

| Feature | Old (price_updater blueprint) | New (standalone) |
|---------|-------------------------------|-----------------|
| URL | `prices.pack-fresh.com/inventory` | `inventory.pack-fresh.com` |
| DB | SQLite (`inventory.db`, per-volume) | Postgres (shared with intake/ingestion) |
| Shopify sync | `python-shopify` REST, 5-min TTL | `ShopifyClient` GraphQL + `CacheManager` (smart staleness) |
| Non-TCG items | ✗ (only items with TCGPlayer ID cached by intake) | ✓ all products cached in `inventory_product_cache` |
| Add item | "Create new listing" (name only, basic draft) | PPT search → full enrichment **or** stub for slabs/accessories |
| Physical count column | `total amount (4/1)` | `physical_count` (renamed) |

## Architecture

```
inventory_product_cache   — all Shopify variants (incl. those without TCGPlayer ID)
inventory_overrides       — physical_count + notes (persisted per variant_id)
inventory_cache_meta      — last sync time, order/product staleness signals
```

`inventory_product_cache` is separate from intake's `shopify_product_cache` — the  
two services refresh independently without contention.

## Deploy to Railway

### 1. Add as a new service in your existing Railway project

```bash
# In your railway project root
railway service create inventory
```

### 2. Set environment variables

Copy `.env.example` and fill in values. Use the **same** `DATABASE_URL` as  
intake/ingestion (they share the Postgres instance).

### 3. Run the migration (once)

```bash
railway run python migrate.py

# If migrating physical_count + notes from the old SQLite:
railway run python migrate.py --from-sqlite /path/to/inventory.db
```

### 4. Configure custom domain

Set `inventory.pack-fresh.com` to point at the new Railway service.

### 5. First load

On first request, `CacheManager` will detect an empty cache and trigger a  
background Shopify sync. The page will load immediately with 0 rows and  
refresh automatically once the sync completes (just reload).

## Add Item flow

**With TCGPlayer ID / PPT search:**
1. Go to `/inventory/add`
2. Search by product name (searches PPT) **or** enter a TCGPlayer ID directly
3. Preview shows inferred tags, era, weight, image preview
4. Click "Create Draft Listing" → full enrichment runs (image bg removal,  
   tags, metafields, channels, weight, COGS)

**Without TCGPlayer ID (slabs, accessories):**
1. Go to `/inventory/add`
2. Scroll to "Create Stub Listing"
3. Enter name + optional quantity → creates minimal Shopify draft

## Local development

```bash
cp .env.example .env   # fill in values
pip install -r requirements.txt
python migrate.py      # bootstrap tables
python app.py          # runs on :5002
```
