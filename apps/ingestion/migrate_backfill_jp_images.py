"""
Migration: Backfill raw_cards.scrydex_id and image_url for JP / Scrydex-only cards.

Old push code dropped scrydex_id on the floor and only fetched images via PPT,
so JP cards (no tcgplayer_id) ended up with both scrydex_id and image_url NULL.
Kiosk's image fallback then had no key to look up against scrydex_price_cache.

Two phases:
  1. Fill raw_cards.scrydex_id from the matching intake_items row (same session,
     name, set, condition, variant) whenever raw_cards.scrydex_id IS NULL.
  2. Fill raw_cards.image_url from scrydex_price_cache via scrydex_id (or
     tcgplayer_id as fallback) whenever image_url IS NULL.

Safe to re-run.
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to .env or set it in the environment")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

print("Phase 1: backfill raw_cards.scrydex_id from intake_items...")
cur.execute("""
    UPDATE raw_cards rc
    SET scrydex_id = ii.scrydex_id
    FROM intake_items ii
    WHERE rc.scrydex_id IS NULL
      AND ii.scrydex_id IS NOT NULL
      AND rc.intake_session_id = ii.session_id
      AND rc.card_name = ii.product_name
      AND COALESCE(rc.set_name, '') = COALESCE(ii.set_name, '')
      AND rc.condition = ii.condition
      AND COALESCE(rc.variant, '') = COALESCE(ii.variance, ii.variant, '')
""")
print(f"  [OK] Filled scrydex_id on {cur.rowcount} raw_cards rows")

print("Phase 2a: backfill image_url via scrydex_id...")
cur.execute("""
    UPDATE raw_cards rc
    SET image_url = sub.img
    FROM (
        SELECT scrydex_id,
               COALESCE(MAX(image_large), MAX(image_medium), MAX(image_small)) AS img
        FROM scrydex_price_cache
        WHERE scrydex_id IS NOT NULL
        GROUP BY scrydex_id
    ) sub
    WHERE rc.image_url IS NULL
      AND rc.scrydex_id IS NOT NULL
      AND rc.scrydex_id = sub.scrydex_id
      AND sub.img IS NOT NULL
""")
print(f"  [OK] Filled image_url on {cur.rowcount} raw_cards rows (via scrydex_id)")

print("Phase 2b: backfill image_url via tcgplayer_id (for rows still missing)...")
cur.execute("""
    UPDATE raw_cards rc
    SET image_url = sub.img
    FROM (
        SELECT tcgplayer_id,
               COALESCE(MAX(image_large), MAX(image_medium), MAX(image_small)) AS img
        FROM scrydex_price_cache
        WHERE tcgplayer_id IS NOT NULL
        GROUP BY tcgplayer_id
    ) sub
    WHERE rc.image_url IS NULL
      AND rc.tcgplayer_id IS NOT NULL
      AND rc.tcgplayer_id = sub.tcgplayer_id
      AND sub.img IS NOT NULL
""")
print(f"  [OK] Filled image_url on {cur.rowcount} raw_cards rows (via tcgplayer_id)")

conn.commit()

cur.execute("""
    SELECT
      COUNT(*) FILTER (WHERE image_url IS NULL AND state = 'STORED' AND current_hold_id IS NULL) AS still_missing,
      COUNT(*) FILTER (WHERE state = 'STORED' AND current_hold_id IS NULL) AS total_kiosk_visible
    FROM raw_cards
""")
row = cur.fetchone()
print(f"\nKiosk-visible raw cards: {row[1]}, still missing image: {row[0]}")

cur.close()
conn.close()
print("\nDone.")
