"""
Migration: Add `game` column to intake_items.

Lets manual card entries (where Scrydex/PPT can't be looked up by tcgplayer_id)
persist their game so downstream bin assignment routes Magic cards to Magic
bins instead of silently falling back to Pokemon.

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

print("Running intake_items.game migration...")

cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'intake_items' AND column_name = 'game'
""")
if not cur.fetchone():
    cur.execute("ALTER TABLE intake_items ADD COLUMN game VARCHAR(32)")
    print("  + Added game to intake_items")
else:
    print("  . game already exists")

conn.commit()
cur.close()
conn.close()
print("Done.")
