"""
Migration: Extend drop_events table with columns for the drop planner.

The base table was created by analytics/migrate_sku_analytics.py.
This adds: status, prices, title, product_id, drop_type, units_sold.

Run once (safe to re-run).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env", "../ingestion/.env", "../analytics/.env"]:
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

print("Running drops migration...")


def column_exists(table, col):
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (table, col))
    return bool(cur.fetchone())


cols = {
    "status":             "VARCHAR(20) DEFAULT 'scheduled'",
    "drop_type":          "VARCHAR(20) DEFAULT 'weekly'",    # weekly or vip
    "original_price":     "DECIMAL(10,2)",
    "drop_price":         "DECIMAL(10,2)",
    "units_sold":         "INTEGER",
    "shopify_product_id": "BIGINT",
    "title":              "TEXT",
    "release_time":       "TIME DEFAULT '11:00:00'",
    "limit_qty":          "INTEGER",                         # limit-X tag value
    "revenue":            "DECIMAL(10,2)",                   # total revenue from the drop
}

for col, typedef in cols.items():
    if not column_exists("drop_events", col):
        cur.execute(f"ALTER TABLE drop_events ADD COLUMN {col} {typedef}")
        print(f"  Added {col} to drop_events")
    else:
        print(f"  {col} already exists")

conn.commit()
cur.close()
conn.close()
print("Done.")
