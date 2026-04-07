"""
Migration: Raw card routing system (005)

Adds to intake_items:
  - routing_destination (storage/display/grade/bulk)
  - display_location_id (FK to storage_locations for binder assignment)

Adds to storage_rows:
  - location_type (bin/binder/bulk)

Seeds binder rows (Binder-1/2/3, 480 capacity) and bulk bins per TCG.

Safe to re-run — every step checks before altering.
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

def col_exists(table, col):
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, col))
    return bool(cur.fetchone())

def table_exists(table):
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_name = %s
    """, (table,))
    return bool(cur.fetchone())

print("Running migration 005: raw card routing system...")

# ── 1. intake_items columns ──────────────────────────────────────────────────
print("\n[1/3] intake_items routing columns")

new_cols = [
    ("routing_destination",  "VARCHAR(20) DEFAULT 'storage'"),
    ("display_location_id",  "UUID"),
]
for col, defn in new_cols:
    if not col_exists("intake_items", col):
        cur.execute(f"ALTER TABLE intake_items ADD COLUMN {col} {defn}")
        print(f"  + Added {col}")
    else:
        print(f"  . {col} already exists")

# Add FK for display_location_id
cur.execute("""
    SELECT 1 FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
    WHERE tc.table_name = 'intake_items'
      AND tc.constraint_type = 'FOREIGN KEY'
      AND ccu.column_name = 'display_location_id'
""")
if not cur.fetchone() and table_exists("storage_locations"):
    try:
        cur.execute("""
            ALTER TABLE intake_items
            ADD CONSTRAINT fk_intake_items_display_location
            FOREIGN KEY (display_location_id) REFERENCES storage_locations(id)
        """)
        print("  + Added FK intake_items.display_location_id -> storage_locations")
    except Exception as e:
        print(f"  ! FK failed (non-fatal): {e}")
        conn.rollback()

# ── 2. storage_rows location_type column ─────────────────────────────────────
print("\n[2/3] storage_rows.location_type column")

if not col_exists("storage_rows", "location_type"):
    cur.execute("ALTER TABLE storage_rows ADD COLUMN location_type VARCHAR(20) DEFAULT 'bin'")
    print("  + Added location_type to storage_rows")
else:
    print("  . location_type already exists")

# ── 3. Seed binder rows + bulk bins ──────────────────────────────────────────
print("\n[3/3] Seeding binder and bulk locations")

# Binder rows
binders = [
    ("Binder-1", "display", "binder", "Customer Browse Binder 1"),
    ("Binder-2", "display", "binder", "Customer Browse Binder 2"),
    ("Binder-3", "display", "binder", "Customer Browse Binder 3"),
]
for label, ctype, loc_type, desc in binders:
    cur.execute("SELECT id FROM storage_rows WHERE row_label = %s", (label,))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            INSERT INTO storage_rows (row_label, card_type, location_type, description)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, (label, ctype, loc_type, desc))
        row_id = cur.fetchone()[0]
        print(f"  + Created binder row {label}")
    else:
        row_id = row[0]
        # Ensure location_type is set
        cur.execute("UPDATE storage_rows SET location_type = %s WHERE id = %s", (loc_type, row_id))
        print(f"  . Binder row {label} already exists")

    # Seed binder location (single slot per binder, 480 capacity)
    cur.execute("SELECT 1 FROM storage_locations WHERE bin_label = %s", (label,))
    if not cur.fetchone():
        cur.execute("""
            INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type, capacity)
            VALUES (%s, %s, 1, %s, 480)
        """, (label, row_id, ctype))
        print(f"    + Created binder location {label} (capacity 480)")
    else:
        # Update capacity to 480 if it was set differently
        cur.execute("UPDATE storage_locations SET capacity = 480 WHERE bin_label = %s", (label,))
        print(f"    . Binder location {label} already exists")

# Bulk bins per TCG
bulk_bins = [
    ("Bulk-Pokemon", "pokemon", "bulk", "Pokemon bulk bin"),
    ("Bulk-Magic",   "magic",   "bulk", "Magic bulk bin"),
    ("Bulk-Yugioh",  "yugioh",  "bulk", "Yu-Gi-Oh bulk bin"),
    ("Bulk-Other",   "other",   "bulk", "Other TCG bulk bin"),
]
for label, ctype, loc_type, desc in bulk_bins:
    cur.execute("SELECT id FROM storage_rows WHERE row_label = %s", (label,))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            INSERT INTO storage_rows (row_label, card_type, location_type, description)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, (label, ctype, loc_type, desc))
        print(f"  + Created bulk row {label}")
    else:
        cur.execute("UPDATE storage_rows SET location_type = %s WHERE id = %s", (loc_type, row[0]))
        print(f"  . Bulk row {label} already exists")

conn.commit()
cur.close()
conn.close()
print("\n Done — migration 005 complete.")
