"""
Migration: Display Phase 1 — Front Glass display case + featured cards table.

Adds:
  - 'display_case' as a valid storage_rows.location_type
  - "Front Glass" storage row + a single FG-1 location with capacity 50
  - featured_cards table for the Set Out scoring algorithm

Safe to re-run.
"""

import os
from dotenv import load_dotenv
# .env lives in the parent service dir; works when run from card_manager/
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to card_manager/.env or set it in the environment")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

# ── 1. Front Glass storage row + location ────────────────────────────────────
print("[1/2] Front Glass display case row...")
cur.execute("""
    INSERT INTO storage_rows (row_label, card_type, location_type, description)
    VALUES ('FG', 'mixed', 'display_case', 'Front Glass display case (customer-facing)')
    ON CONFLICT (row_label) DO UPDATE
        SET location_type = 'display_case',
            description = EXCLUDED.description
""")
cur.execute("SELECT id FROM storage_rows WHERE row_label = 'FG'")
fg_row_id = cur.fetchone()[0]
print(f"  [OK] Front Glass row id: {fg_row_id}")

cur.execute("""
    INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type, capacity)
    VALUES ('FG-1', %s, 1, 'mixed', 50)
    ON CONFLICT (bin_label) DO NOTHING
""", (fg_row_id,))
print(f"  [OK] FG-1 location seeded (capacity 50, editable later)")

# ── 2. featured_cards table ─────────────────────────────────────────────────
print("\n[2/2] featured_cards table...")
cur.execute("""
    CREATE TABLE IF NOT EXISTS featured_cards (
        id            SERIAL PRIMARY KEY,
        name_pattern  TEXT NOT NULL,
        game          TEXT NOT NULL DEFAULT '*',
        weight        INTEGER NOT NULL DEFAULT 50,
        notes         TEXT,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_featured_cards_game ON featured_cards(game)
""")
print("  [OK] featured_cards table ready")

conn.commit()
cur.close()
conn.close()
print("\nDone.")
