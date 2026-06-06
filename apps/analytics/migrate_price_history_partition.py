"""One-time migration: convert scrydex_price_history to a monthly range-partitioned
table, narrowed to the rows that are actually read (condition='NM', price_type='raw').

Why:
  - The old table was a flat 50M-row / 12GB heap, growing ~980k rows/night (a full
    copy of every priced cache row). The sole consumer (analytics/margins.py) only
    ever reads condition='NM' AND price_type='raw' by tcgplayer_id, so ~80% of every
    nightly snapshot was written, indexed, and never queried.
  - The nightly INSERT averaged 137s because ON CONFLICT + 5 indexes were maintained
    against the full 50M-row table.

After this migration:
  - scrydex_price_history is RANGE-partitioned by snapshot_date (one partition/month).
  - Only NM/raw rows are stored (backfilled from the old table for every existing day,
    so margins keeps full lookback).
  - Two indexes instead of five: a unique dedup index (for ON CONFLICT) and
    (tcgplayer_id, snapshot_date) for the margins lookup.
  - The nightly job (price_history.py) creates the current month's partition on the
    fly and drops partitions older than the retention window.

Safety:
  - The old table is renamed to scrydex_price_history_old and LEFT IN PLACE. Verify
    margins still computes, then drop it manually: DROP TABLE scrydex_price_history_old;
  - Idempotent: if scrydex_price_history is already partitioned, it does nothing.

Run:  python analytics/migrate_price_history_partition.py
"""
import os
import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def load_env(p):
    for line in pathlib.Path(p).read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env(ROOT / "admin" / ".env")
sys.stdout.reconfigure(encoding="utf-8")

import psycopg2

COLS = (
    "snapshot_date, scrydex_id, tcgplayer_id, expansion_id, expansion_name, "
    "product_type, product_name, variant, condition, price_type, "
    "grade_company, grade_value, market_price, low_price"
)


def month_floor(d):
    return d.replace(day=1)


def next_month(d):
    d = month_floor(d)
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = False
cur = conn.cursor()

# ── idempotency guard ────────────────────────────────────────────────────────
cur.execute("""
    SELECT EXISTS (
        SELECT 1 FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        WHERE c.relname = 'scrydex_price_history'
    )
""")
if cur.fetchone()[0]:
    print("scrydex_price_history is already partitioned — nothing to do.")
    sys.exit(0)

# ── data range to cover ──────────────────────────────────────────────────────
cur.execute("SELECT min(snapshot_date), max(snapshot_date) FROM scrydex_price_history")
lo, hi = cur.fetchone()
print(f"Existing history spans {lo} .. {hi}")

# ── 1. new partitioned table (no surrogate id — nothing references it) ────────
print("[1/5] Creating partitioned table scrydex_price_history_new ...")
cur.execute("""
    CREATE TABLE scrydex_price_history_new (
        snapshot_date       DATE NOT NULL,
        scrydex_id          TEXT NOT NULL,
        tcgplayer_id        INTEGER,
        expansion_id        TEXT NOT NULL,
        expansion_name      TEXT,
        product_type        TEXT NOT NULL DEFAULT 'card',
        product_name        TEXT,
        variant             TEXT NOT NULL DEFAULT 'normal',
        condition           TEXT NOT NULL DEFAULT 'NM',
        price_type          TEXT NOT NULL DEFAULT 'raw',
        grade_company       TEXT,
        grade_value         TEXT,
        market_price        NUMERIC(10,2),
        low_price           NUMERIC(10,2)
    ) PARTITION BY RANGE (snapshot_date)
""")

# ── 2. monthly partitions covering existing data + a buffer month ─────────────
print("[2/5] Creating monthly partitions ...")
m = month_floor(lo)
end = next_month(hi)  # exclusive upper bound, gives us one buffer month
while m < end:
    nxt = next_month(m)
    pname = f"scrydex_price_history_{m:%Y%m}"
    cur.execute(
        f"CREATE TABLE {pname} PARTITION OF scrydex_price_history_new "
        f"FOR VALUES FROM ('{m:%Y-%m-%d}') TO ('{nxt:%Y-%m-%d}')"
    )
    print(f"   + {pname}  [{m} .. {nxt})")
    m = nxt

# ── 3. backfill NM/raw rows (build indexes AFTER load for speed) ──────────────
print("[3/5] Backfilling NM/raw rows (this is the slow part) ...")
cur.execute(f"""
    INSERT INTO scrydex_price_history_new ({COLS})
    SELECT {COLS}
    FROM scrydex_price_history
    WHERE market_price IS NOT NULL
      AND condition = 'NM'
      AND price_type = 'raw'
""")
print(f"   backfilled {cur.rowcount} rows")

# ── 4. indexes (unique for ON CONFLICT dedup + tcg/date for margins) ──────────
print("[4/5] Building indexes ...")
cur.execute("""
    CREATE UNIQUE INDEX idx_sph_unique_p ON scrydex_price_history_new (
        snapshot_date, scrydex_id, variant, condition, price_type,
        COALESCE(grade_company, ''), COALESCE(grade_value, '')
    )
""")
cur.execute("CREATE INDEX idx_sph_tcg_date_p ON scrydex_price_history_new (tcgplayer_id, snapshot_date)")

# ── 5. atomic swap; keep old table for verification ──────────────────────────
print("[5/5] Swapping tables ...")
cur.execute("ALTER TABLE scrydex_price_history RENAME TO scrydex_price_history_old")
cur.execute("ALTER TABLE scrydex_price_history_new RENAME TO scrydex_price_history")

conn.commit()

# ── report ───────────────────────────────────────────────────────────────────
cur.execute("SELECT count(*) FROM scrydex_price_history")
new_rows = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM scrydex_price_history_old")
old_rows = cur.fetchone()[0]
cur.execute("""
    SELECT pg_size_pretty(pg_total_relation_size('scrydex_price_history')),
           pg_size_pretty(pg_total_relation_size('scrydex_price_history_old'))
""")
new_sz, old_sz = cur.fetchone()
print("\nDone.")
print(f"  new scrydex_price_history : {new_rows:,} rows, {new_sz}")
print(f"  scrydex_price_history_old : {old_rows:,} rows, {old_sz} (verify margins, then DROP)")

cur.close()
conn.close()
