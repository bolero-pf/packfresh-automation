"""Migration: split push into separate barcoding + placement stages.

Adds:
- intake_items.barcoded_at — set when raw_cards rows are created (state='BARCODED_*')
- raw_cards.intake_item_id — link back to the intake_item so push can find which
  pre-barcoded rows to place. Nullable for legacy rows + grade/bulk paths.

New raw_cards.state values (no schema change — state is VARCHAR):
- BARCODED_STORAGE — barcoded, no bin yet, will go to a storage bin at push
- BARCODED_DISPLAY — barcoded, no bin yet, will go to a binder at push

Customer-facing queries (kiosk, card_browser, card_manager) already filter on
state='STORED', so BARCODED_* rows are invisible to customers automatically.

Safe to re-run.
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to .env")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

print("Running migration: barcoding stage...")

cur.execute("ALTER TABLE intake_items ADD COLUMN IF NOT EXISTS barcoded_at TIMESTAMP")
print("  + intake_items.barcoded_at")

cur.execute("ALTER TABLE raw_cards   ADD COLUMN IF NOT EXISTS intake_item_id UUID")
print("  + raw_cards.intake_item_id")

cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_cards_intake_item ON raw_cards(intake_item_id)")
print("  + idx_raw_cards_intake_item")

conn.commit()
cur.close()
conn.close()
print("Done.")
