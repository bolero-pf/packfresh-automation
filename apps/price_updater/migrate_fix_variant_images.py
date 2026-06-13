"""One-time backfill: heal raw_cards.image_url to the correct per-variant art.

Root cause: in One Piece (and any game where alt arts share a card number /
scrydex_id), every variant shares ONE scrydex_id but has its own tcgplayer_id
and its own image. Past intakes / rebinds left some live cards pointing at the
wrong variant's image (e.g. a base foil OP13-118 carrying the alt-art
OP13-118A art). The kiosk renders raw_cards.image_url directly, so the wrong
art showed on the floor.

tcgplayer_id is the variant-unique key and maps 1:1 to the correct image in
scrydex_price_cache, so we re-derive image_url by joining on tcgplayer_id.
Only rows whose current image disagrees with the canonical one are touched.

Run:  python price_updater/migrate_fix_variant_images.py          # dry-run
      python price_updater/migrate_fix_variant_images.py --apply  # write
"""
import os
import sys
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

APPLY = "--apply" in sys.argv

PREVIEW_SQL = """
SELECT rc.game, COUNT(*) AS n
FROM raw_cards rc
JOIN LATERAL (
    SELECT image_large FROM scrydex_price_cache
    WHERE tcgplayer_id = rc.tcgplayer_id AND image_large IS NOT NULL
    LIMIT 1
) c ON true
WHERE rc.tcgplayer_id IS NOT NULL
  AND rc.state IN ('STORED','DISPLAY')
  AND rc.image_url IS DISTINCT FROM c.image_large
GROUP BY rc.game
ORDER BY n DESC;
"""

UPDATE_SQL = """
UPDATE raw_cards rc
SET image_url = c.image_large
FROM (
    SELECT DISTINCT ON (tcgplayer_id) tcgplayer_id, image_large
    FROM scrydex_price_cache
    WHERE tcgplayer_id IS NOT NULL AND image_large IS NOT NULL
    ORDER BY tcgplayer_id, image_large
) c
WHERE rc.tcgplayer_id = c.tcgplayer_id
  AND rc.state IN ('STORED','DISPLAY')
  AND rc.image_url IS DISTINCT FROM c.image_large;
"""

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
try:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(PREVIEW_SQL)
        rows = cur.fetchall()
        total = sum(r["n"] for r in rows)
        print(f"Rows to fix (STORED/DISPLAY): {total}")
        for r in rows:
            print(f"  {r['game']:<12} {r['n']}")

        if not APPLY:
            print("\nDry-run only. Re-run with --apply to write.")
            conn.rollback()
        else:
            cur.execute(UPDATE_SQL)
            print(f"\nUpdated {cur.rowcount} rows.")
            conn.commit()
            print("Committed.")
finally:
    conn.close()
