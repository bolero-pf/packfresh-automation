"""
Migration: Graded slab fields + raw card storage system (004)

Adds to intake_items:
  - is_graded, grade_company, grade_value, cert_number, variant, language
  - item_status (if missing), pushed_at (if missing)

Replaces boxes with:
  - storage_rows  (rows/shelves with card_type affinity)
  - storage_locations  (individual 100-card bins)

Adds to raw_cards:
  - bin_id (replaces box_id), image_url, is_graded, grade_company,
    grade_value, variant, language

Seeds default rows A/B/C (Pokemon) and D (Magic) with 50 bins each.

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

def index_exists(index):
    cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (index,))
    return bool(cur.fetchone())

print("Running migration 004: graded slab fields + storage system...")

# ── 1. intake_items columns ──────────────────────────────────────────────────
print("\n[1/4] intake_items columns")

new_cols = [
    ("is_graded",      "BOOLEAN DEFAULT FALSE"),
    ("grade_company",  "VARCHAR(20)"),
    ("grade_value",    "VARCHAR(10)"),
    ("cert_number",    "VARCHAR(50)"),
    ("variant",        "VARCHAR(100)"),
    ("language",       "VARCHAR(20) DEFAULT 'EN'"),
    ("item_status",    "VARCHAR(30) DEFAULT 'good'"),
    ("pushed_at",      "TIMESTAMP"),
]
for col, defn in new_cols:
    if not col_exists("intake_items", col):
        cur.execute(f"ALTER TABLE intake_items ADD COLUMN {col} {defn}")
        print(f"  ✓ Added {col} to intake_items")
    else:
        print(f"  · {col} already exists")

# ── 2. storage_rows table ────────────────────────────────────────────────────
print("\n[2/4] storage_rows + storage_locations tables")

if not table_exists("storage_rows"):
    cur.execute("""
        CREATE TABLE storage_rows (
            id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            row_label   VARCHAR(20)  NOT NULL UNIQUE,
            card_type   VARCHAR(50)  NOT NULL,
            description VARCHAR(255),
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("  ✓ Created storage_rows")
else:
    print("  · storage_rows already exists")

if not table_exists("storage_locations"):
    cur.execute("""
        CREATE TABLE storage_locations (
            id            UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
            bin_label     VARCHAR(20)  NOT NULL UNIQUE,
            row_id        UUID         NOT NULL REFERENCES storage_rows(id),
            partition_num INTEGER      NOT NULL,
            card_type     VARCHAR(50)  NOT NULL,
            capacity      INTEGER      NOT NULL DEFAULT 50,
            current_count INTEGER      NOT NULL DEFAULT 0,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (row_id, partition_num)
        )
    """)
    print("  ✓ Created storage_locations")
    if not index_exists("idx_storage_locations_card_type"):
        cur.execute("""
            CREATE INDEX idx_storage_locations_card_type
            ON storage_locations(card_type, current_count)
        """)
        print("  ✓ Created index idx_storage_locations_card_type")
else:
    print("  · storage_locations already exists")

# ── 3. Seed default rows + bins ──────────────────────────────────────────────
print("\n[3/4] Seeding default storage rows and bins")

default_rows = [
    ("A", "pokemon", "Pokemon Row A"),
    ("B", "pokemon", "Pokemon Row B"),
    ("C", "pokemon", "Pokemon Row C"),
    ("D", "magic",   "Magic: The Gathering Row D"),
]

for label, ctype, desc in default_rows:
    cur.execute("SELECT id FROM storage_rows WHERE row_label = %s", (label,))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            INSERT INTO storage_rows (row_label, card_type, description)
            VALUES (%s, %s, %s) RETURNING id
        """, (label, ctype, desc))
        row_id = cur.fetchone()[0]
        print(f"  ✓ Created row {label} ({ctype})")
    else:
        row_id = row[0]
        print(f"  · Row {label} already exists")

    # Seed 50 bins for this row
    cur.execute("SELECT COUNT(*) FROM storage_locations WHERE row_id = %s", (row_id,))
    existing_bins = cur.fetchone()[0]
    if existing_bins < 50:
        for n in range(existing_bins + 1, 51):
            bin_label = f"{label}-{n}"
            cur.execute("""
                INSERT INTO storage_locations
                    (bin_label, row_id, partition_num, card_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (bin_label) DO NOTHING
            """, (bin_label, row_id, n, ctype))
        added = 50 - existing_bins
        print(f"    ✓ Added {added} bins for row {label}")
    else:
        print(f"    · Bins for row {label} already seeded")

# ── 4. raw_cards columns ─────────────────────────────────────────────────────
print("\n[4/4] raw_cards columns")

if table_exists("raw_cards"):
    raw_cols = [
        ("image_url",     "VARCHAR(1000)"),
        ("is_graded",     "BOOLEAN DEFAULT FALSE"),
        ("grade_company", "VARCHAR(20)"),
        ("grade_value",   "VARCHAR(10)"),
        ("variant",       "VARCHAR(100)"),
        ("language",      "VARCHAR(20) DEFAULT 'EN'"),
        ("bin_id",        "UUID"),  # FK added separately below
    ]
    for col, defn in raw_cols:
        if not col_exists("raw_cards", col):
            cur.execute(f"ALTER TABLE raw_cards ADD COLUMN {col} {defn}")
            print(f"  ✓ Added {col} to raw_cards")
        else:
            print(f"  · {col} already exists")

    # Add FK on bin_id if storage_locations now exists
    cur.execute("""
        SELECT 1 FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
          ON tc.constraint_name = ccu.constraint_name
        WHERE tc.table_name = 'raw_cards'
          AND tc.constraint_type = 'FOREIGN KEY'
          AND ccu.column_name = 'bin_id'
    """)
    if not cur.fetchone():
        cur.execute("""
            ALTER TABLE raw_cards
            ADD CONSTRAINT fk_raw_cards_bin_id
            FOREIGN KEY (bin_id) REFERENCES storage_locations(id)
        """)
        print("  ✓ Added FK raw_cards.bin_id → storage_locations")

    if not index_exists("idx_raw_cards_bin"):
        cur.execute("CREATE INDEX idx_raw_cards_bin ON raw_cards(bin_id)")
        print("  ✓ Created index idx_raw_cards_bin")

    # Drop old box_id if it exists
    if col_exists("raw_cards", "box_id"):
        cur.execute("ALTER TABLE raw_cards DROP COLUMN box_id")
        print("  ✓ Dropped legacy box_id from raw_cards")
    else:
        print("  · box_id already absent")
else:
    print("  · raw_cards table not found — skipping (run schema.sql first)")

conn.commit()
cur.close()
conn.close()
print("\n✅ Migration 004 complete.")
