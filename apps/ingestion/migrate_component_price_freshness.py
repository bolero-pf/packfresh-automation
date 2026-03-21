"""
Migration: Add market_price_updated_at timestamp to sealed_breakdown_components.

This supports JIT refresh of component market prices — when a recipe is loaded,
stale components get fresh prices from PPT API. The timestamp tracks when
the market_price was last refreshed.

Run once against the shared database (safe to re-run).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env", "../intake/ingest-service/.env", "../../intake/ingest-service/.env"]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    DATABASE_URL = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if DATABASE_URL:
            break
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — set it as an env var or place a .env file here")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Running component price freshness migration...")


def column_exists(table, col):
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (table, col))
    return bool(cur.fetchone())


if not column_exists("sealed_breakdown_components", "market_price_updated_at"):
    cur.execute("ALTER TABLE sealed_breakdown_components ADD COLUMN market_price_updated_at TIMESTAMP")
    print("  Added market_price_updated_at column to sealed_breakdown_components")
else:
    print("  market_price_updated_at column already exists — skipping")

conn.commit()
cur.close()
conn.close()
print("Done.")
