"""
Migration: Add parent_item_id to intake_items (for break-down tracking)
           Add ingested_at to intake_sessions
           Add 'broken_down' as valid item_status

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

print("Running ingest migrations...")

# parent_item_id on intake_items
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_items' AND column_name = 'parent_item_id'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_items ADD COLUMN parent_item_id UUID REFERENCES intake_items(id)")
    print("  ✓ Added parent_item_id to intake_items")
else:
    print("  ✓ parent_item_id already exists")

# ingested_at on intake_sessions
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_sessions' AND column_name = 'ingested_at'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_sessions ADD COLUMN ingested_at TIMESTAMP")
    print("  ✓ Added ingested_at to intake_sessions")
else:
    print("  ✓ ingested_at already exists")

# pushed_at on intake_items (tracks which items were successfully pushed to Shopify)
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_items' AND column_name = 'pushed_at'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_items ADD COLUMN pushed_at TIMESTAMP")
    print("  ✓ Added pushed_at to intake_items")
else:
    print("  ✓ pushed_at already exists")

conn.commit()
cur.close()
conn.close()
print("Done!")
