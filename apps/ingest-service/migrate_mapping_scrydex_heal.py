"""
Migration: heal raw-card re-link matching.

Three parts:
  1. Add product_mappings.scrydex_id so Scrydex-only links (JP / no-TCG cards)
     round-trip through the cache like TCG-linked cards do.
  2. Add a functional index for the set-insensitive Tier-2 lookup
     (name + normalized number + normalized variance).
  3. Backfill the cache from intake_items linking history — every raw card you
     ever linked, keyed on the Collectr identity it was imported under, so old
     cards light up on the next import instead of needing a re-link.

The backfill is ADDITIVE and idempotent: ON CONFLICT it only fills NULL fields,
never overwrites an existing tcgplayer_id/scrydex_id. Re-runnable safely.

Run once: python migrate_mapping_scrydex_heal.py
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env", "../admin/.env"]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    DATABASE_URL = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if DATABASE_URL:
            break

if not DATABASE_URL:
    print("ERROR: DATABASE_URL not found")
    raise SystemExit(1)

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur = conn.cursor()


def step(label, sql, params=None):
    print(f"... {label}")
    cur.execute(sql, params or ())


# 1. scrydex_id column
step("add product_mappings.scrydex_id",
     "ALTER TABLE product_mappings ADD COLUMN IF NOT EXISTS scrydex_id VARCHAR(100)")

# 2. Tier-2 functional index (set-insensitive: name + number + variance)
step("add Tier-2 lookup index", """
    CREATE INDEX IF NOT EXISTS idx_product_mappings_numvar
    ON product_mappings (
        collectr_name, product_type,
        upper(replace(COALESCE(card_number, ''), ' ', '')),
        lower(COALESCE(NULLIF(variance, ''), 'normal'))
    )
""")

# 2b. Tier-3 functional index (name-insensitive: set + number + variance)
step("add Tier-3 lookup index", """
    CREATE INDEX IF NOT EXISTS idx_product_mappings_setnumvar
    ON product_mappings (
        product_type,
        COALESCE(set_name, ''),
        upper(replace(COALESCE(card_number, ''), ' ', '')),
        lower(COALESCE(NULLIF(variance, ''), 'normal'))
    )
""")

cur.execute("SELECT COUNT(*) FROM product_mappings WHERE product_type='raw'")
before = cur.fetchone()[0]

# 3. Backfill from linking history. Per (name, set, number, variance) take the
#    most-recent non-null tcgplayer_id and scrydex_id. ON CONFLICT only fills
#    gaps; it never changes an existing link.
step("backfill cache from intake_items history", """
    INSERT INTO product_mappings
        (collectr_name, product_type, set_name, card_number, variance,
         tcgplayer_id, scrydex_id, use_count, created_at, last_used)
    SELECT g.n, 'raw', g.s, g.c, g.v, g.tcg, g.sx, 0,
           CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    FROM (
        SELECT product_name n, set_name s, card_number c, COALESCE(variance, '') v,
               (array_agg(tcgplayer_id ORDER BY created_at DESC)
                    FILTER (WHERE tcgplayer_id IS NOT NULL))[1] tcg,
               (array_agg(scrydex_id   ORDER BY created_at DESC)
                    FILTER (WHERE scrydex_id   IS NOT NULL))[1] sx
        FROM intake_items
        WHERE product_type = 'raw' AND is_mapped
          AND (tcgplayer_id IS NOT NULL OR scrydex_id IS NOT NULL)
          AND product_name IS NOT NULL AND product_name <> ''
        GROUP BY product_name, set_name, card_number, COALESCE(variance, '')
    ) g
    WHERE g.tcg IS NOT NULL OR g.sx IS NOT NULL
    ON CONFLICT (collectr_name, product_type,
                 COALESCE(set_name, ''), COALESCE(card_number, ''), COALESCE(variance, ''))
    DO UPDATE SET
        scrydex_id   = COALESCE(product_mappings.scrydex_id, EXCLUDED.scrydex_id),
        tcgplayer_id = COALESCE(product_mappings.tcgplayer_id, EXCLUDED.tcgplayer_id)
""")

cur.execute("SELECT COUNT(*) FROM product_mappings WHERE product_type='raw'")
after = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM product_mappings WHERE product_type='raw' AND scrydex_id IS NOT NULL")
with_sx = cur.fetchone()[0]

conn.commit()
print(f"\nDone. raw mappings: {before} -> {after} (+{after - before} new), "
      f"{with_sx} now carry a scrydex_id.")
cur.close()
conn.close()
