"""
Migration: Display Phase 2 — segmented binders + FG-2, capacity reset, game backfill.

Splits storage by TCG to match the physical layout:
  - FG-1 (Front Glass) is now Pokemon-only at capacity 100.
  - FG-2 is a new partition under the FG row for non-Pokemon TCGs (capacity 100).
  - Binder-1 and Binder-2 are Pokemon binders, capacity 360 each (real binder size).
  - Binder-3 is the Magic binder, capacity 360.

Also backfills `raw_cards.game` for legacy NULL rows in Binder-1 by joining on
scrydex_price_cache (via scrydex_id, then tcgplayer_id), and normalizes
'magicthegathering' to 'magic' to match shared/storage.py canonical types.

Then moves the magic cards (in Binder-1 and Binder-2) into Binder-3, matching
the physical sort done in-store.

Safe to re-run.
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to card_manager/.env or set it in the environment")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()


def fetchone(sql, params=()):
    cur.execute(sql, params)
    return cur.fetchone()


def fetchall(sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


# ── 1. Backfill raw_cards.game from scrydex_price_cache ─────────────────────
# 479 cards in Binder-1 carry game=NULL (legacy data from before the column
# was tracked). Cross-reference scrydex_id -> game, then tcgplayer_id -> game.
print("[1/6] Backfilling raw_cards.game for NULL rows...")
cur.execute("""
    UPDATE raw_cards rc
    SET game = sub.game
    FROM (
        SELECT rc.id,
               COALESCE(
                 (SELECT spc.game FROM scrydex_price_cache spc
                  WHERE spc.scrydex_id = rc.scrydex_id LIMIT 1),
                 (SELECT spc.game FROM scrydex_price_cache spc
                  WHERE spc.tcgplayer_id = rc.tcgplayer_id LIMIT 1)
               ) AS game
        FROM raw_cards rc
        WHERE rc.game IS NULL
    ) sub
    WHERE rc.id = sub.id AND sub.game IS NOT NULL
""")
print(f"  [OK] Backfilled {cur.rowcount} rows from scrydex/tcgplayer cache")

# Normalize 'magicthegathering' -> 'magic' so suggest filters don't have to
# handle both (matches CARD_TYPE_MAP in shared/storage.py).
cur.execute("UPDATE raw_cards SET game = 'magic' WHERE game = 'magicthegathering'")
print(f"  [OK] Normalized {cur.rowcount} 'magicthegathering' rows to 'magic'")


# ── 2. FG-1 -> pokemon, capacity 100 ─────────────────────────────────────────
print("\n[2/6] FG-1 -> pokemon, capacity 100...")
cur.execute("""
    UPDATE storage_locations SET capacity = 100, card_type = 'pokemon'
    WHERE bin_label = 'FG-1'
""")
print("  [OK] FG-1 updated")


# ── 3. FG-2 -> new partition under FG row, card_type='other', capacity 100 ──
print("\n[3/6] FG-2 -> new non-Pokemon partition...")
fg_row = fetchone("SELECT id FROM storage_rows WHERE row_label = 'FG'")
if not fg_row:
    raise RuntimeError("FG row missing — run migrate_display_phase1.py first")
fg_row_id = fg_row[0]

cur.execute("""
    INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type, capacity)
    VALUES ('FG-2', %s, 2, 'other', 100)
    ON CONFLICT (bin_label) DO UPDATE
        SET card_type = 'other', capacity = 100
""", (fg_row_id,))
print("  [OK] FG-2 seeded (capacity 100, card_type='other' = every non-Pokemon TCG)")


# ── 4. Binders typed + capacity 360 ─────────────────────────────────────────
# Real binders hold 360 slots, not 480 — the old number was over-optimistic.
# Each binder gets a card_type that the suggest endpoint will filter on.
print("\n[4/6] Binder typing + capacity 360...")
binder_types = {'Binder-1': 'pokemon', 'Binder-2': 'pokemon', 'Binder-3': 'magic'}
for bin_label, ctype in binder_types.items():
    cur.execute("""
        UPDATE storage_locations SET capacity = 360, card_type = %s
        WHERE bin_label = %s
    """, (ctype, bin_label))
    cur.execute("""
        UPDATE storage_rows SET card_type = %s
        WHERE row_label = %s
    """, (ctype, bin_label))
    print(f"  [OK] {bin_label} -> card_type={ctype}, capacity=360")


# ── 5. Move magic cards out of pokemon binders -> Binder-3 ───────────────────
print("\n[5/6] Relocating magic cards to Binder-3...")
binder3 = fetchone("SELECT id FROM storage_locations WHERE bin_label = 'Binder-3'")
binder1 = fetchone("SELECT id FROM storage_locations WHERE bin_label = 'Binder-1'")
binder2 = fetchone("SELECT id FROM storage_locations WHERE bin_label = 'Binder-2'")
binder3_id, binder1_id, binder2_id = binder3[0], binder1[0], binder2[0]

cur.execute("""
    UPDATE raw_cards
    SET bin_id = %s, updated_at = CURRENT_TIMESTAMP
    WHERE state = 'DISPLAY'
      AND bin_id IN (%s, %s)
      AND game = 'magic'
""", (binder3_id, binder1_id, binder2_id))
print(f"  [OK] Moved {cur.rowcount} magic cards into Binder-3")


# ── 6. Show resulting layout ────────────────────────────────────────────────
print("\n[6/6] Final layout:")
rows = fetchall("""
    SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count
    FROM storage_locations sl
    JOIN storage_rows sr ON sl.row_id = sr.id
    WHERE sr.location_type IN ('binder', 'display_case')
    ORDER BY sr.location_type, sl.bin_label
""")
for r in rows:
    bin_label, ctype, cap, cnt = r
    flag = '  (over)' if cnt > cap else ''
    print(f"  {bin_label:<10} type={ctype:<10} {cnt:>4}/{cap}{flag}")

conn.commit()
cur.close()
conn.close()
print("\nDone.")
