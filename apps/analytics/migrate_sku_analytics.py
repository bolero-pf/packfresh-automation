"""
Migration: Create sku_analytics + sku_daily_sales tables.

sku_analytics: per-variant velocity metrics (recomputed daily)
sku_daily_sales: daily sales snapshots for incremental updates

Run once against the shared database (safe to re-run).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env", "../ingestion/.env", "../ingest-service/.env"]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    DATABASE_URL = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if DATABASE_URL:
            break
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Running SKU analytics migration...")


def table_exists(name):
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s", (name,))
    return bool(cur.fetchone())


if not table_exists("sku_analytics"):
    cur.execute("""
        CREATE TABLE sku_analytics (
            shopify_variant_id  BIGINT PRIMARY KEY,
            shopify_product_id  BIGINT,
            tcgplayer_id        BIGINT,
            title               TEXT,
            units_sold_90d      INTEGER DEFAULT 0,
            units_sold_30d      INTEGER DEFAULT 0,
            units_sold_7d       INTEGER DEFAULT 0,
            avg_days_to_sell    DECIMAL(10,2),
            out_of_stock_days   INTEGER DEFAULT 0,
            current_qty         INTEGER DEFAULT 0,
            current_price       DECIMAL(10,2),
            avg_sale_price      DECIMAL(10,2),
            price_trend_pct     DECIMAL(10,4),
            last_sale_at        TIMESTAMP,
            velocity_score      DECIMAL(10,2),
            computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE INDEX idx_sku_analytics_tcg ON sku_analytics(tcgplayer_id)
        WHERE tcgplayer_id IS NOT NULL
    """)
    cur.execute("""
        CREATE INDEX idx_sku_analytics_velocity ON sku_analytics(velocity_score DESC NULLS LAST)
    """)
    print("  Created sku_analytics table")
else:
    print("  sku_analytics already exists")

if not table_exists("sku_daily_sales"):
    cur.execute("""
        CREATE TABLE sku_daily_sales (
            id                  SERIAL PRIMARY KEY,
            sale_date           DATE NOT NULL,
            shopify_variant_id  BIGINT NOT NULL,
            units_sold          INTEGER DEFAULT 0,
            revenue             DECIMAL(10,2) DEFAULT 0,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(sale_date, shopify_variant_id)
        )
    """)
    cur.execute("""
        CREATE INDEX idx_daily_sales_date ON sku_daily_sales(sale_date)
    """)
    cur.execute("""
        CREATE INDEX idx_daily_sales_variant ON sku_daily_sales(shopify_variant_id)
    """)
    print("  Created sku_daily_sales table")
else:
    print("  sku_daily_sales already exists")

# Daily inventory snapshots for OOS tracking
if not table_exists("sku_daily_inventory"):
    cur.execute("""
        CREATE TABLE sku_daily_inventory (
            id                  SERIAL PRIMARY KEY,
            snapshot_date       DATE NOT NULL,
            shopify_variant_id  BIGINT NOT NULL,
            qty                 INTEGER DEFAULT 0,
            UNIQUE(snapshot_date, shopify_variant_id)
        )
    """)
    cur.execute("CREATE INDEX idx_daily_inv_variant ON sku_daily_inventory(shopify_variant_id)")
    cur.execute("CREATE INDEX idx_daily_inv_date ON sku_daily_inventory(snapshot_date)")
    print("  Created sku_daily_inventory table")
else:
    print("  sku_daily_inventory already exists")

# Drop events — populated by future drop planner, used to exclude drop-day sales from velocity
if not table_exists("drop_events"):
    cur.execute("""
        CREATE TABLE drop_events (
            id                  SERIAL PRIMARY KEY,
            shopify_variant_id  BIGINT NOT NULL,
            drop_date           DATE NOT NULL,
            qty_offered         INTEGER,
            drop_name           TEXT,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shopify_variant_id, drop_date)
        )
    """)
    cur.execute("CREATE INDEX idx_drop_events_variant ON drop_events(shopify_variant_id)")
    cur.execute("CREATE INDEX idx_drop_events_date ON drop_events(drop_date)")
    print("  Created drop_events table")
else:
    print("  drop_events already exists")

# Metadata table for tracking last run
if not table_exists("analytics_meta"):
    cur.execute("""
        CREATE TABLE analytics_meta (
            key     VARCHAR(100) PRIMARY KEY,
            value   TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  Created analytics_meta table")
else:
    print("  analytics_meta already exists")

conn.commit()
cur.close()
conn.close()
print("Done.")
