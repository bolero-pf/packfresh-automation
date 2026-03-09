"""
Migration: Add pickup_date column and update intake_session_summary view.
Run once: python migrate_pickup_date.py
"""
import os
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

print("Running pickup_date migration...")

# Add pickup_date column
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_sessions' AND column_name = 'pickup_date'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_sessions ADD COLUMN pickup_date DATE")
    print("  Added pickup_date column")
else:
    print("  pickup_date already exists")

# Recreate view with pickup_date
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
print("  Recreated intake_session_summary view with pickup_date")

cur.close()
conn.close()
print("Done!")
