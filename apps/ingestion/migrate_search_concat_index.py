"""Rebuild the relink autocomplete index as a single concat-expression
trigram, because per-column trigram indexes don't help this query shape.

The original indexes (product_name, product_name_en, expansion_name,
expansion_name_en each with gin_trgm_ops) can't be combined by the
planner when the predicate is the OR of ILIKEs across all of them —
each index only covers one branch, and picking any one of them still
forces a filter across every row for the other branches. In practice
the planner picks idx_scrydex_cache_sid_variant (for the DISTINCT ON
sort) or idx_scrydex_cache_card_number (partial index == every raw NM
row) and filters 236k+ rows with ILIKE, landing at ~580ms per query.

A GIN trigram index on the concatenated text field lets one token =
one ILIKE = one index lookup. Multi-token queries AND multiple cheap
index scans together. Prod test: drops search-cards from ~2.3s wall
to <100ms.

Run: python ingestion/migrate_search_concat_index.py
"""
import os
import sys
import time

import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

# CREATE INDEX CONCURRENTLY requires autocommit.
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()


def run(sql: str, label: str):
    print(f"-> {label}")
    t0 = time.time()
    try:
        cur.execute(sql)
        print(f"  done in {time.time()-t0:.1f}s")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  already exists, skipping")
        else:
            print(f"  FAILED: {e}")
            raise


run("CREATE EXTENSION IF NOT EXISTS pg_trgm", "ensuring pg_trgm extension")

# Single gin_trgm index on the concat of every column the relink search
# wants to match against. The same WHERE predicate as before keeps the
# index narrow (~236k rows, one-fifth of the full cache). The concat
# expression in the index and in the query must match byte-for-byte or
# the planner won't use it.
run("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_cache_search_trgm
    ON scrydex_price_cache USING gin (
        (
            COALESCE(product_name, '')      || ' ' ||
            COALESCE(product_name_en, '')   || ' ' ||
            COALESCE(expansion_name, '')    || ' ' ||
            COALESCE(expansion_name_en, '') || ' ' ||
            COALESCE(card_number, '')
        ) gin_trgm_ops
    )
    WHERE price_type = 'raw' AND condition = 'NM'
""", "building idx_scrydex_cache_search_trgm (concat trigram)")

# The per-column trigram indexes from migrate_search_indexes.py are no
# longer needed — the concat index subsumes them. Drop them to reclaim
# disk and cut write amplification on every cache refresh.
for old in (
    "idx_scrydex_cache_product_name_trgm",
    "idx_scrydex_cache_product_name_en_trgm",
    "idx_scrydex_cache_expansion_name_trgm",
    "idx_scrydex_cache_expansion_name_en_trgm",
):
    run(f"DROP INDEX CONCURRENTLY IF EXISTS {old}", f"dropping legacy {old}")

print()
print("Done. /api/ingest/search-cards should now respond in <100ms.")
