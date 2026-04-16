"""
Migration: Allow NULL tcgplayer_id on raw_cards.

Japanese cards and Scrydex-only cards often don't have TCGplayer IDs,
but the original schema marked raw_cards.tcgplayer_id NOT NULL. Drop that
constraint so the push pipeline doesn't explode when a JP card comes through.

Safe to re-run - checks the current column definition before altering.

"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set -add it to .env or set it in the environment")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

print("Running migration: drop NOT NULL on raw_cards.tcgplayer_id...")

cur.execute("""
    SELECT is_nullable
    FROM information_schema.columns
    WHERE table_name = 'raw_cards' AND column_name = 'tcgplayer_id'
""")
row = cur.fetchone()
if not row:
    print("  - raw_cards.tcgplayer_id column not found -nothing to do")
elif row[0] == 'YES':
    print("  - Already nullable -nothing to do")
else:
    cur.execute("ALTER TABLE raw_cards ALTER COLUMN tcgplayer_id DROP NOT NULL")
    print("  [OK] Dropped NOT NULL on raw_cards.tcgplayer_id")

conn.commit()
cur.close()
conn.close()
print("\nDone.")
