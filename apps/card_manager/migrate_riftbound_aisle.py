"""
Migration: Riftbound storage aisle E.

Riftbound is a first-class routable game (shared/storage.py CARD_TYPE_MAP +
card_manager SINGLE_GAME_TYPES already know it), but it had no physical bin row,
so assign_bins('riftbound') raised "No available bins". This seeds aisle E:

  - storage_rows 'E' (card_type='riftbound', location_type='bin', active)
  - 50 storage_locations E-1 .. E-50, capacity 50 each (2500 cards)

Mirrors One Piece's aisle C. Auto-expands further on demand via
_auto_expand_bins once the first batch of singles arrives.

Safe to re-run.
"""

import os
from dotenv import load_dotenv
# card_manager/.env may lack DATABASE_URL; fall back to admin/.env.
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
if not os.getenv("DATABASE_URL"):
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "admin", ".env"))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to card_manager/.env or admin/.env")

ROW_LABEL   = "E"
CARD_TYPE   = "riftbound"
NUM_BINS    = 50
CAPACITY    = 50

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

print(f"[1/2] Riftbound bin row '{ROW_LABEL}'...")
cur.execute("""
    INSERT INTO storage_rows (row_label, card_type, location_type, description, active)
    VALUES (%s, %s, 'bin', 'Riftbound Row E', TRUE)
    ON CONFLICT (row_label) DO UPDATE
        SET card_type = EXCLUDED.card_type,
            location_type = EXCLUDED.location_type,
            active = TRUE
    RETURNING id
""", (ROW_LABEL, CARD_TYPE))
row_id = cur.fetchone()[0]
print(f"      row id = {row_id}")

print(f"[2/2] {NUM_BINS} bins {ROW_LABEL}-1 .. {ROW_LABEL}-{NUM_BINS} (cap {CAPACITY})...")
created = 0
for n in range(1, NUM_BINS + 1):
    cur.execute("""
        INSERT INTO storage_locations
            (bin_label, row_id, partition_num, card_type, capacity, current_count)
        VALUES (%s, %s, %s, %s, %s, 0)
        ON CONFLICT (bin_label) DO NOTHING
    """, (f"{ROW_LABEL}-{n}", row_id, n, CARD_TYPE, CAPACITY))
    created += cur.rowcount

conn.commit()
print(f"      +{created} new bin(s) (existing left untouched)")

cur.execute("""
    SELECT COUNT(*) AS bins, SUM(capacity) AS cap, SUM(current_count) AS used
    FROM storage_locations sl JOIN storage_rows sr ON sr.id = sl.row_id
    WHERE sr.card_type = %s AND sr.location_type = 'bin'
""", (CARD_TYPE,))
bins, cap, used = cur.fetchone()
print(f"\nRiftbound bin capacity now: {bins} bins, {cap} cards, {used} used.")
print("Committed.")

cur.close()
conn.close()
