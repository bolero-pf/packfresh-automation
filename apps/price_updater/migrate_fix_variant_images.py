"""One-time backfill: heal raw_cards.image_url to the correct per-variant art.

Root cause: in One Piece (and any game where alt arts share a card number /
scrydex_id), every variant shares ONE scrydex_id but has its own tcgplayer_id
and its own image. Past intakes / rebinds left some live cards pointing at the
wrong variant's image (e.g. a base foil OP13-118 carrying the alt-art
OP13-118A art). The kiosk renders raw_cards.image_url directly, so the wrong
art showed on the floor.

The correct key is (tcgplayer_id, variant): for most cards a tcgplayer_id maps
1:1 to an image (One Piece alt arts each get their own tcg_id), but for ~1,500
cards ONE tcgplayer_id holds multiple variants with different art (Pokemon Base
Set 1st-Edition vs Unlimited Shadowless, MTG prerelease-stamped promos…). There
the variant is the disambiguator, so we:
  1. prefer the image for the row's exact (tcgplayer_id, normalized variant),
  2. fall back to the tcg-level image only when that tcg_id has a single image.
Variant strings are normalized (lowercase, strip non-alphanumerics) so raw_cards
"Unlimitedshadowless" lines up with cache "unlimitedShadowless".
Only rows whose current image disagrees with the resolved one are touched; a
row we can't disambiguate (multi-image tcg, no variant match) is left as-is.

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

# Resolved image per row: variant-specific image first, tcg-level only when the
# tcg_id is unambiguous (single image). Shared by preview + update.
RESOLVED_CTE = """
WITH variant_img AS (
    SELECT tcgplayer_id, nvar, MIN(image_large) AS image_large
    FROM (
        SELECT tcgplayer_id,
               regexp_replace(lower(variant), '[^a-z0-9]', '', 'g') AS nvar,
               image_large
        FROM scrydex_price_cache
        WHERE tcgplayer_id IS NOT NULL AND image_large IS NOT NULL
    ) z
    GROUP BY tcgplayer_id, nvar
    HAVING COUNT(DISTINCT image_large) = 1
),
tcg_img AS (
    SELECT tcgplayer_id, MIN(image_large) AS image_large
    FROM scrydex_price_cache
    WHERE tcgplayer_id IS NOT NULL AND image_large IS NOT NULL
    GROUP BY tcgplayer_id
    HAVING COUNT(DISTINCT image_large) = 1
),
resolved AS (
    SELECT rc.id, rc.game,
           COALESCE(vi.image_large, ti.image_large) AS image_large
    FROM raw_cards rc
    LEFT JOIN variant_img vi
      ON vi.tcgplayer_id = rc.tcgplayer_id
     AND vi.nvar = regexp_replace(lower(coalesce(rc.variant,'')), '[^a-z0-9]', '', 'g')
    LEFT JOIN tcg_img ti
      ON ti.tcgplayer_id = rc.tcgplayer_id
    WHERE rc.tcgplayer_id IS NOT NULL
      AND rc.state IN ('STORED','DISPLAY')
)
"""

PREVIEW_SQL = RESOLVED_CTE + """
SELECT rc.game, COUNT(*) AS n
FROM raw_cards rc JOIN resolved r ON r.id = rc.id
WHERE r.image_large IS NOT NULL
  AND rc.image_url IS DISTINCT FROM r.image_large
GROUP BY rc.game
ORDER BY n DESC;
"""

UPDATE_SQL = RESOLVED_CTE + """
UPDATE raw_cards rc
SET image_url = r.image_large
FROM resolved r
WHERE rc.id = r.id
  AND r.image_large IS NOT NULL
  AND rc.image_url IS DISTINCT FROM r.image_large;
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
