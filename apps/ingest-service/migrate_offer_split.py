"""
Migration: Split offer_percentage into cash_percentage + credit_percentage,
plus add the walk-in session flag.

Pre-Phase-2 migration so the ingest-service AND ingestion agents can both
read the new columns in parallel without coordinating a schema change.
This migration is ADDITIVE — it does not drop offer_percentage. Existing
code that still reads offer_percentage keeps working until the agent
removes those references.

What this does:
  1. Adds intake_sessions.cash_percentage     DECIMAL(5,2) NULL
  2. Adds intake_sessions.credit_percentage   DECIMAL(5,2) NULL
  3. Adds intake_sessions.accepted_offer_type VARCHAR(10)  NULL
       (will be set to 'cash' or 'credit' once a customer accepts)
  4. Adds intake_sessions.is_walk_in          BOOLEAN DEFAULT FALSE
       (TRUE = customer is physically at the counter; accepting jumps
        the session straight to 'received' with no pickup/mail step)
  5. Backfills cash_percentage from offer_percentage on rows where the
     legacy column has a value — that preserves all existing pricing.
  6. Recreates intake_session_summary view to expose the new columns.

The Phase-2 ingest-service agent owns:
  - Removing offer_percentage references from app.py / intake.py
  - UI that captures both percentages and the accepted type
  - Role caps (associate locked at defaults, manager 0-80%, owner uncapped)
  - Walk-in flag on manual entry (defaults TRUE) and the short-circuit
    accept flow that skips pickup/mail.

Run once: python migrate_offer_split.py
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
conn.autocommit = True
cur = conn.cursor()

print("Running offer-split migration...")


def has_column(table, col):
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, col))
    return cur.fetchone() is not None


def add_column(col, ddl_type):
    if has_column("intake_sessions", col):
        print(f"  intake_sessions.{col} already exists")
        return False
    cur.execute(f"ALTER TABLE intake_sessions ADD COLUMN {col} {ddl_type}")
    print(f"  Added intake_sessions.{col}")
    return True


add_column("cash_percentage",     "DECIMAL(5, 2)")
add_column("credit_percentage",   "DECIMAL(5, 2)")
add_column("accepted_offer_type", "VARCHAR(10)")
add_column("is_walk_in",          "BOOLEAN DEFAULT FALSE")

# Backfill cash_percentage from the legacy single column. credit_percentage
# stays NULL on legacy rows — historically only one offer existed, and we
# don't want to invent a credit number we never actually quoted.
cur.execute("""
    UPDATE intake_sessions
       SET cash_percentage = offer_percentage
     WHERE cash_percentage IS NULL
       AND offer_percentage IS NOT NULL
""")
print(f"  Backfilled cash_percentage from offer_percentage on {cur.rowcount} rows")

# Recreate the summary view to expose the new columns. CASCADE drops any
# dependent objects; this view has no dependents in the current codebase.
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
print("  Recreated intake_session_summary view with offer-split columns")

cur.close()
conn.close()
print("Done.")
