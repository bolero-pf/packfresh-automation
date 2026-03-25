"""
Migration: Add original_offer_amount and received_items_snapshot to intake_sessions.

These columns enable the ingest service to show an "Offer Adjustment" summary
comparing the original offer (at receive time) with the current state after
damage, qty changes, adds, deletes, and relinks.

Run once against the shared database.
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to .env or set it in the environment")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Running offer snapshot migrations...")

# original_offer_amount on intake_sessions
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_sessions' AND column_name = 'original_offer_amount'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_sessions ADD COLUMN original_offer_amount DECIMAL(10,2)")
    print("  + Added original_offer_amount to intake_sessions")

    # Backfill: set original_offer_amount = total_offer_amount for existing received/ingested sessions
    cur.execute("""
        UPDATE intake_sessions
        SET original_offer_amount = total_offer_amount
        WHERE status IN ('received', 'partially_ingested', 'ingested', 'finalized')
          AND original_offer_amount IS NULL
    """)
    print(f"  + Backfilled original_offer_amount for {cur.rowcount} existing sessions")
else:
    print("  - original_offer_amount already exists")

# received_items_snapshot on intake_sessions
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_sessions' AND column_name = 'received_items_snapshot'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_sessions ADD COLUMN received_items_snapshot JSONB")
    print("  + Added received_items_snapshot to intake_sessions")
else:
    print("  - received_items_snapshot already exists")

conn.commit()
cur.close()
conn.close()
print("Done.")
