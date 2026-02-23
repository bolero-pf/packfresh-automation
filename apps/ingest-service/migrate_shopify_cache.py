"""
Migration: Create shopify_product_cache table.
Run this via: railway run python migrate_shopify_cache.py
Or locally: DATABASE_URL=... python migrate_shopify_cache.py
"""

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Running migration: shopify_product_cache...")

cur.execute("""
    CREATE TABLE IF NOT EXISTS shopify_product_cache (
        id SERIAL PRIMARY KEY,
        tcgplayer_id BIGINT NOT NULL,
        shopify_product_id BIGINT NOT NULL,
        shopify_variant_id BIGINT NOT NULL,
        title VARCHAR(500),
        handle VARCHAR(500),
        sku VARCHAR(100),
        shopify_price DECIMAL(10, 2),
        shopify_qty INTEGER DEFAULT 0,
        status VARCHAR(50) DEFAULT 'ACTIVE',
        last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(tcgplayer_id, shopify_variant_id)
    )
""")
print("  ✓ Table created (or already exists)")

cur.execute("CREATE INDEX IF NOT EXISTS idx_shopify_cache_tcg ON shopify_product_cache(tcgplayer_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_shopify_cache_product ON shopify_product_cache(shopify_product_id)")
print("  ✓ Indexes created")

# Add shopify_variant_id to sealed_cogs if missing
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'sealed_cogs' AND column_name = 'shopify_variant_id'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE sealed_cogs ADD COLUMN shopify_variant_id BIGINT")
    print("  ✓ Added shopify_variant_id to sealed_cogs")
else:
    print("  ✓ sealed_cogs.shopify_variant_id already exists")

conn.commit()
cur.close()
conn.close()
print("\nMigration complete!")
