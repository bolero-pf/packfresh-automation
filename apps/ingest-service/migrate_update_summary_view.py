"""
Migration: Update intake_session_summary view to include
is_distribution, fulfillment_method, and tracking_number columns.

Run once: python migrate_update_summary_view.py
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

print("Updating intake_session_summary view...")

cur.execute("DROP VIEW IF EXISTS intake_session_summary CASCADE")
print("  Dropped old view")

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
        COUNT(i.id) as item_count,
        SUM(i.quantity) as total_quantity,
        COUNT(*) FILTER (WHERE i.is_mapped = FALSE) as unmapped_count
    FROM intake_sessions s
    LEFT JOIN intake_items i ON s.id = i.session_id
    GROUP BY s.id
""")

print("  View recreated with is_distribution, fulfillment_method, tracking_number")

cur.close()
conn.close()
print("Done!")
