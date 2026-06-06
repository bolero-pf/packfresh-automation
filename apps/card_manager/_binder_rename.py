"""One-shot binder rename + add for the Pokemon-vs-Magic split.

Renames:
  Binder-1 -> Pokemon Binder 1
  Binder-2 -> Pokemon Binder 2
  Binder-3 -> Magic Binder 1

Adds:
  Pokemon Binder 3 (pokemon, 340)
  Magic Binder 2 (magic, 340)
  Magic Binder 3 (magic, 340)

bin_label is updated on storage_locations AND row_label is updated on
storage_rows so both surfaces (which sometimes display row_label) stay
consistent.

Idempotent: safe to re-run. Skips rows that are already at the target.
"""
import os, sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
def load_env(p):
    for line in pathlib.Path(p).read_text().splitlines():
        if not line or line.startswith("#") or "=" not in line: continue
        k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
load_env(ROOT / "admin" / ".env")
sys.stdout.reconfigure(encoding="utf-8")

import psycopg2, psycopg2.extras
conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = False
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

RENAMES = [
    ("Binder-1", "Pokemon Binder 1"),
    ("Binder-2", "Pokemon Binder 2"),
    ("Binder-3", "Magic Binder 1"),
]
NEW_BINDERS = [
    ("Pokemon Binder 3", "pokemon", 340),
    ("Magic Binder 2",    "magic",   340),
    ("Magic Binder 3",    "magic",   340),
]

print("=" * 60)
print("RENAMING")
print("=" * 60)
for old, new in RENAMES:
    cur.execute("""
        SELECT sl.id AS loc_id, sl.bin_label, sr.id AS row_id, sr.row_label
        FROM storage_locations sl
        JOIN storage_rows sr ON sr.id = sl.row_id
        WHERE sr.location_type = 'binder'
          AND (sl.bin_label = %s OR sr.row_label = %s OR sl.bin_label = %s)
    """, (old, old, new))
    rows = cur.fetchall()
    if not rows:
        print(f"  -- {old}: not found, skipping")
        continue
    for r in rows:
        if r["bin_label"] == new and r["row_label"] == new:
            print(f"  -- {r['bin_label']}: already renamed")
            continue
        cur.execute("UPDATE storage_locations SET bin_label = %s WHERE id = %s",
                    (new, r["loc_id"]))
        cur.execute("UPDATE storage_rows SET row_label = %s WHERE id = %s",
                    (new, r["row_id"]))
        print(f"  OK  {r['bin_label']} -> {new}")

print("\n" + "=" * 60)
print("ADDING")
print("=" * 60)
for label, ct, cap in NEW_BINDERS:
    cur.execute("""
        SELECT sl.id FROM storage_locations sl
        JOIN storage_rows sr ON sr.id = sl.row_id
        WHERE sl.bin_label = %s AND sr.location_type = 'binder'
    """, (label,))
    if cur.fetchone():
        print(f"  -- {label}: already exists")
        continue
    # Create a row + a location at partition 1. storage_rows.card_type is
    # NOT NULL (separate from storage_locations.card_type), so we set it
    # to the same game value on both rows.
    cur.execute("""
        INSERT INTO storage_rows (row_label, card_type, location_type, description)
        VALUES (%s, %s, 'binder', %s) RETURNING id
    """, (label, ct, f"Customer Browse {label}"))
    row_id = cur.fetchone()["id"]
    cur.execute("""
        INSERT INTO storage_locations
            (bin_label, row_id, partition_num, card_type, capacity, current_count)
        VALUES (%s, %s, 1, %s, %s, 0)
    """, (label, row_id, ct, cap))
    print(f"  OK  {label} ({ct}, cap={cap})")

conn.commit()
print("\nCommitted.")

# Verify final state
cur.execute("""
    SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count
    FROM storage_locations sl JOIN storage_rows sr ON sr.id = sl.row_id
    WHERE sr.location_type = 'binder'
    ORDER BY sl.bin_label
""")
print("\nFinal state:")
for r in cur.fetchall():
    print(f"  {r['bin_label']:<22}  {r['card_type']:<10}  {r['current_count']}/{r['capacity']}")
