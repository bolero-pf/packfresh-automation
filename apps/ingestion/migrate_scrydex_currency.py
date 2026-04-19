"""Add currency column to scrydex_price_cache.

Scrydex sends a per-price `currency` field (confirmed via
/api/ingest/debug/scrydex/<id> — raw rows on JP cards come back with
currency=JPY, while graded rows scraped from eBay are USD). The nightly
sync was storing `p.get("market")` as-is, which meant JP-marketplace
prices (¥8000 for adv3_ja-25 Ampharos ex) landed in the cache as if
they were $8000 USD.

This migration just adds the column. The sync update + query
conversions are in the service code. Existing rows get NULL for
currency — downstream queries treat NULL and 'USD' the same (no
conversion), so this is safe to deploy before re-syncing.

Run: python ingestion/migrate_scrydex_currency.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import psycopg2
from psycopg2.extras import DictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor(cursor_factory=DictCursor)


def col_exists(table: str, col: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, col))
    return cur.fetchone() is not None


print("Running migration: scrydex_price_cache.currency ...")

if not col_exists("scrydex_price_cache", "currency"):
    cur.execute("ALTER TABLE scrydex_price_cache ADD COLUMN currency TEXT")
    print("  [+] Added currency column")
else:
    print("  [=] currency column already exists")

cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_scrydex_cache_currency
    ON scrydex_price_cache(currency)
    WHERE currency IS NOT NULL AND currency <> 'USD'
""")
print("  [+] Ensured partial index on non-USD rows")

print("Done. Now re-sync Japanese expansions so their rows get the correct currency:")
print("  python shared/scrydex_nightly.py --sets <jp_expansion_ids>")
