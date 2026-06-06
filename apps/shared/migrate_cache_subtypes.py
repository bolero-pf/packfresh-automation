"""One-time migration: denormalize scrydex_card_meta.subtypes onto
scrydex_price_cache so card search can match subtypes without a cross-table JOIN.

Why:
  shared/price_cache.py card search OR'd `m.subtypes::text ILIKE %s` (from a JOINed
  scrydex_card_meta) into a WHERE that otherwise hits trigram indexes on c.*.
  Because that one OR branch lived on a different table, Postgres couldn't build a
  BitmapOr over the trigram indexes and fell back to a full seq scan of all ~1.07M
  card rows + a seq scan of scrydex_card_meta — ~1.2s warm / ~2.5s cold per relink.

  Moving subtypes onto the cache row (single-table OR) lets the trigram BitmapOr
  fire. Measured: ~1195ms -> ~412ms for a representative query, and no meta scan.

After: scrydex_nightly.py keeps the column in sync (added to the cache upsert).

Run:  python shared/migrate_cache_subtypes.py
Idempotent: ADD COLUMN IF NOT EXISTS, re-runnable backfill, CREATE INDEX IF NOT EXISTS.
"""
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def load_env(p):
    for line in pathlib.Path(p).read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env(ROOT / "admin" / ".env")
sys.stdout.reconfigure(encoding="utf-8")

import psycopg2

conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = True  # DDL + CREATE INDEX CONCURRENTLY can't run in a txn block
cur = conn.cursor()

# ── 1. column ────────────────────────────────────────────────────────────────
print("[1/3] Adding scrydex_price_cache.subtypes ...")
cur.execute("ALTER TABLE scrydex_price_cache ADD COLUMN IF NOT EXISTS subtypes JSONB")

# ── 2. backfill from scrydex_card_meta, batched by expansion ─────────────────
print("[2/3] Backfilling subtypes from scrydex_card_meta (per expansion) ...")
cur.execute("""
    SELECT DISTINCT expansion_id FROM scrydex_price_cache
    WHERE product_type = 'card' AND expansion_id IS NOT NULL
    ORDER BY expansion_id
""")
expansions = [r[0] for r in cur.fetchall()]
total = 0
for i, exp in enumerate(expansions, 1):
    cur.execute("""
        UPDATE scrydex_price_cache c
        SET subtypes = m.subtypes
        FROM scrydex_card_meta m
        WHERE m.game = c.game AND m.scrydex_id = c.scrydex_id
          AND c.product_type = 'card'
          AND c.expansion_id = %s
          AND c.subtypes IS DISTINCT FROM m.subtypes
    """, (exp,))
    total += cur.rowcount
    if i % 25 == 0 or i == len(expansions):
        print(f"   {i}/{len(expansions)} expansions, {total} rows updated so far")
print(f"   backfill complete: {total} rows")

# ── 3. trigram index (partial: cards only) ───────────────────────────────────
print("[3/3] Building trigram index on subtypes (CONCURRENTLY) ...")
cur.execute("""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_spc_subtypes_trgm
    ON scrydex_price_cache USING gin ((subtypes::text) gin_trgm_ops)
    WHERE product_type = 'card'
""")

cur.execute("SELECT count(*) FILTER (WHERE subtypes IS NOT NULL) AS with_sub, count(*) AS cards FROM scrydex_price_cache WHERE product_type='card'")
with_sub, cards = cur.fetchone()
print(f"\nDone. {with_sub:,}/{cards:,} card rows have subtypes populated.")

cur.close()
conn.close()
