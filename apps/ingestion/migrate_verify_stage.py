"""
Migration: Add verified_at to intake_items (for verify stage tracking)

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

print("Running verify-stage migration...")

# verified_at on intake_items
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_items' AND column_name = 'verified_at'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_items ADD COLUMN verified_at TIMESTAMP")
    print("  ✓ Added verified_at to intake_items")
else:
    print("  ✓ verified_at already exists")

conn.commit()
cur.close()
conn.close()
print("Done.")
