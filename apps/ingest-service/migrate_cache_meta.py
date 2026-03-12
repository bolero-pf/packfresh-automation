"""
Migration: Create cache_meta table for self-aware cache staleness tracking.
Run: python migrate_cache_meta.py
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
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

print("Running migration: cache_meta...")

cur.execute("""
    CREATE TABLE IF NOT EXISTS cache_meta (
        id INTEGER PRIMARY KEY DEFAULT 1,
        last_refreshed_at TIMESTAMP NOT NULL DEFAULT '1970-01-01',
        last_refreshed_reason VARCHAR(100),
        last_order_number INTEGER,
        last_product_updated_at TIMESTAMP,
        CONSTRAINT single_row CHECK (id = 1)
    )
""")
print("  ✓ cache_meta table created (or already exists)")

# Add INTAKE_INTERNAL_URL reminder
print("\n  ℹ  Remember to set INTAKE_INTERNAL_URL env var in ingestion Railway service")
print("     e.g. INTAKE_INTERNAL_URL=https://your-intake-service.railway.app")
print("     This allows ingestion to notify intake cache after push-live.\n")

conn.commit()
cur.close()
conn.close()
print("Migration complete!")
