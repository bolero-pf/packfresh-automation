"""Drop the sealed_cogs and cogs_history tables.

Sealed COGS is now sourced from Shopify's variant cost_per_item field,
maintained by ingestion's weighted-average write at push-live and mirrored
locally as inventory_product_cache.unit_cost. The sealed_cogs table is a
vestigial parallel mirror that nothing has written to since the
intake.finalize_session() chain was orphaned during the blueprint refactor.
"""
import os
import sys
import psycopg2

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

with psycopg2.connect(DB_URL) as conn:
    conn.autocommit = False
    with conn.cursor() as cur:
        # Snapshot current row counts so we know what we're dropping
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM sealed_cogs)   AS sealed_cogs_rows,
                (SELECT COUNT(*) FROM cogs_history)  AS cogs_history_rows
        """)
        sealed_n, history_n = cur.fetchone()
        print(f"sealed_cogs:  {sealed_n} rows")
        print(f"cogs_history: {history_n} rows")

        # cogs_history.sealed_cogs_id → sealed_cogs.id (CASCADE).
        # Drop history first to avoid relying on cascade semantics.
        cur.execute("DROP TABLE IF EXISTS cogs_history CASCADE")
        cur.execute("DROP TABLE IF EXISTS sealed_cogs CASCADE")
        conn.commit()
        print("dropped sealed_cogs and cogs_history")
