"""
Migration: Create shopify_product_cache table.
Run this via: railway run python migrate_shopify_cache.py
Or locally: DATABASE_URL=... python migrate_shopify_cache.py
"""

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env"]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    DATABASE_URL = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if DATABASE_URL:
            break
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set and not found in .env")
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

# Add is_damaged to shopify_product_cache if missing
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'shopify_product_cache' AND column_name = 'is_damaged'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE shopify_product_cache ADD COLUMN is_damaged BOOLEAN DEFAULT FALSE")
    print("  ✓ Added is_damaged to shopify_product_cache")
else:
    print("  ✓ shopify_product_cache.is_damaged already exists")

# Add status transition timestamps to intake_sessions if missing
for col in ['offered_at', 'accepted_at', 'received_at', 'ingested_at', 'rejected_at']:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'intake_sessions' AND column_name = %s
    """, (col,))
    if not cur.fetchone():
        cur.execute(f"ALTER TABLE intake_sessions ADD COLUMN {col} TIMESTAMP")
        print(f"  ✓ Added {col} to intake_sessions")
    else:
        print(f"  ✓ intake_sessions.{col} already exists")

# Add fulfillment_method, tracking_number, is_distribution to intake_sessions
for col, coltype in [('fulfillment_method', "VARCHAR(20) DEFAULT 'pickup'"),
                     ('tracking_number', 'VARCHAR(500)'),
                     ('is_distribution', 'BOOLEAN DEFAULT FALSE')]:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'intake_sessions' AND column_name = %s
    """, (col,))
    if not cur.fetchone():
        cur.execute(f"ALTER TABLE intake_sessions ADD COLUMN {col} {coltype}")
        print(f"  ✓ Added {col} to intake_sessions")
    else:
        print(f"  ✓ intake_sessions.{col} already exists")

conn.commit()

# Recreate the intake_session_summary view to include new columns
print("Updating intake_session_summary view...")
cur.execute("DROP VIEW IF EXISTS intake_session_summary")
cur.execute("""
    CREATE VIEW intake_session_summary AS
    SELECT 
        s.id,
        s.customer_name,
        s.session_type,
        s.status,
        s.total_market_value,
        s.offer_percentage,
        s.total_offer_amount,
        s.created_at,
        s.finalized_at,
        s.offered_at,
        s.accepted_at,
        s.received_at,
        s.ingested_at,
        s.rejected_at,
        COUNT(i.id) as item_count,
        SUM(i.quantity) as total_quantity,
        COUNT(*) FILTER (WHERE i.is_mapped = FALSE) as unmapped_count
    FROM intake_sessions s
    LEFT JOIN intake_items i ON s.id = i.session_id
    GROUP BY s.id
""")
print("  ✓ View updated")

conn.commit()
cur.close()
conn.close()
print("\nMigration complete!")
