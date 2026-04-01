"""
Migration: Change product_mappings unique constraint to include set_name and card_number.

Old key: (collectr_name, product_type) — two cards with the same name but different sets
          would overwrite each other's mapping.

New key: (collectr_name, product_type, set_name_key, card_number_key) — uses COALESCE
         to treat NULL/empty as '' so sealed products (where these are always empty)
         keep the same effective uniqueness.

Run once: python migrate_mapping_key.py
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env"]:
        if os.path.exists(_p):
            for _line in open(_p):
                if _line.strip().startswith("DATABASE_URL="):
                    DATABASE_URL = _line.strip().split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if DATABASE_URL:
            break

if not DATABASE_URL:
    print("ERROR: DATABASE_URL not found")
    exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

STEPS = [
    # 1. Drop the old unique constraint
    (
        "Drop old unique constraint on product_mappings",
        """
        ALTER TABLE product_mappings
        DROP CONSTRAINT IF EXISTS product_mappings_collectr_name_product_type_key;
        """,
    ),
    # Also try the alternate constraint name format
    (
        "Drop alternate constraint name (if exists)",
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'product_mappings_collectr_name_product_type_key'
                  AND conrelid = 'product_mappings'::regclass
            ) THEN
                ALTER TABLE product_mappings
                DROP CONSTRAINT product_mappings_collectr_name_product_type_key;
            END IF;
        EXCEPTION WHEN undefined_object THEN
            NULL;
        END $$;
        """,
    ),
    # 2. Normalize existing NULLs to empty strings
    (
        "Normalize NULL set_name to empty string",
        """
        UPDATE product_mappings
        SET set_name = ''
        WHERE set_name IS NULL;
        """,
    ),
    (
        "Normalize NULL card_number to empty string",
        """
        UPDATE product_mappings
        SET card_number = ''
        WHERE card_number IS NULL;
        """,
    ),
    # 3. De-duplicate before adding new constraint
    # Keep the row with the highest use_count for each (name, type, set, number) combo
    (
        "De-duplicate existing mappings",
        """
        DELETE FROM product_mappings a
        USING product_mappings b
        WHERE a.collectr_name = b.collectr_name
          AND a.product_type = b.product_type
          AND COALESCE(a.set_name, '') = COALESCE(b.set_name, '')
          AND COALESCE(a.card_number, '') = COALESCE(b.card_number, '')
          AND a.id < b.id;
        """,
    ),
    # 4. Create new unique index with COALESCE
    (
        "Create new unique index with set_name + card_number",
        """
        CREATE UNIQUE INDEX IF NOT EXISTS product_mappings_full_key
        ON product_mappings(collectr_name, product_type, COALESCE(set_name, ''), COALESCE(card_number, ''));
        """,
    ),
]

print(f"Running migration on {DATABASE_URL[:40]}...")
for desc, sql in STEPS:
    try:
        print(f"  {desc}...", end=" ")
        cur.execute(sql)
        conn.commit()
        print("OK")
    except Exception as e:
        conn.rollback()
        print(f"SKIP ({e})")

cur.close()
conn.close()
print("Done.")
