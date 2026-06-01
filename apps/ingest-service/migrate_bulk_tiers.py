"""
Migration: per-session bulk pricing tiers.

Replaces the hardcoded "raw card under $2 → flat 25%" rule with a
JSONB column that holds up to 3 ascending tiers, e.g.:

    [{"max": 1, "pct": 0}, {"max": 2, "pct": 25}, {"max": 5, "pct": 50}]

The default value matches the legacy hardcode so existing in-flight
sessions and any callers that don't yet specify tiers keep paying the
same numbers.

Run once: python migrate_bulk_tiers.py
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

DEFAULT_TIERS_JSON = '[{"max": 2, "pct": 25}]'

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

print("Running bulk_tiers migration...")


def has_column(table, col):
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, col))
    return cur.fetchone() is not None


if not has_column("intake_sessions", "bulk_tiers"):
    cur.execute(
        f"ALTER TABLE intake_sessions ADD COLUMN bulk_tiers JSONB DEFAULT '{DEFAULT_TIERS_JSON}'::jsonb"
    )
    print("  Added intake_sessions.bulk_tiers")
else:
    print("  intake_sessions.bulk_tiers already exists")

# Backfill any pre-existing rows that have NULL (the DEFAULT only fires on
# new INSERTs).
cur.execute(
    f"UPDATE intake_sessions SET bulk_tiers = '{DEFAULT_TIERS_JSON}'::jsonb WHERE bulk_tiers IS NULL"
)
print(f"  Backfilled bulk_tiers on {cur.rowcount} rows")

# Recreate the summary view so the new column is selectable.
cur.execute("DROP VIEW IF EXISTS intake_session_summary CASCADE")
cur.execute("""
    CREATE VIEW intake_session_summary AS
    SELECT
        s.id,
        s.customer_name,
        s.session_type,
        s.status,
        s.total_market_value,
        s.offer_percentage,
        s.cash_percentage,
        s.credit_percentage,
        s.accepted_offer_type,
        s.is_walk_in,
        s.total_offer_amount,
        s.bulk_tiers,
        s.created_at,
        s.finalized_at,
        s.is_distribution,
        s.fulfillment_method,
        s.tracking_number,
        s.pickup_date,
        COUNT(i.id) as item_count,
        SUM(i.quantity) as total_quantity,
        COUNT(*) FILTER (WHERE i.is_mapped = FALSE) as unmapped_count
    FROM intake_sessions s
    LEFT JOIN intake_items i ON s.id = i.session_id
    GROUP BY s.id
""")
print("  Recreated intake_session_summary view with bulk_tiers")

cur.close()
conn.close()
print("Done.")
