"""Add pg_trgm + GIN trigram indexes to scrydex_price_cache for fast substring
search.

The /api/ingest/search-cards endpoint does `ILIKE '%term%'` across 4 name
columns for the relink autocomplete. Without a trigram index that's a
sequential scan over every card × variant × condition × grade row — enough
to make typing in the relink box lag 2–3 seconds on a fat cache.

CREATE INDEX CONCURRENTLY runs without locking writes, so this is safe to
run against prod while staff are working. Indexes may take ~30s–2min each
to build depending on cache size.

Run: python ingestion/migrate_search_indexes.py
"""
import os
import sys
import time

import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

# autocommit=True is required for CREATE INDEX CONCURRENTLY — it can't run
# inside a transaction.
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def run(sql: str, label: str):
    print(f"→ {label}")
    t0 = time.time()
    try:
        cur.execute(sql)
        print(f"  done in {time.time()-t0:.1f}s")
    except psycopg2.errors.DuplicateTable:
        print("  already exists, skipping")
    except Exception as e:
        # CREATE INDEX IF NOT EXISTS CONCURRENTLY doesn't always suppress
        # the "already exists" in old PG versions — check message.
        if "already exists" in str(e).lower():
            print("  already exists, skipping")
        else:
            print(f"  FAILED: {e}")
            raise


# 1. Enable the trigram extension. No-op if already enabled.
run("CREATE EXTENSION IF NOT EXISTS pg_trgm", "enabling pg_trgm extension")

# 2. GIN trigram indexes on every column the search endpoint ILIKEs against.
# The predicate filter (price_type='raw' AND condition='NM') matches what
# search-cards does — narrows the index to ~1/5 the rows and keeps it small.
index_specs = [
    ("idx_scrydex_cache_product_name_trgm",       "product_name"),
    ("idx_scrydex_cache_product_name_en_trgm",    "product_name_en"),
    ("idx_scrydex_cache_expansion_name_trgm",     "expansion_name"),
    ("idx_scrydex_cache_expansion_name_en_trgm",  "expansion_name_en"),
]
for idx_name, col in index_specs:
    run(
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
        f"ON scrydex_price_cache USING gin ({col} gin_trgm_ops) "
        f"WHERE price_type = 'raw' AND condition = 'NM'",
        f"building {idx_name} on {col}",
    )

# 3. Plain btree on card_number (exact/prefix matches) — much smaller, cheap.
run(
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_cache_card_number "
    "ON scrydex_price_cache (card_number) "
    "WHERE price_type = 'raw' AND condition = 'NM'",
    "building idx_scrydex_cache_card_number",
)

print()
print("Done. Run this on the ingest autocomplete next — should drop from 2-3s to <100ms.")
