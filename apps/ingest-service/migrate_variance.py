"""
Migration: Add variance (printing) column to intake_items and product_mappings.

Variance distinguishes printings like "1st Edition Holofoil" vs "Unlimited Holofoil"
or "Normal" vs "Reverse Holofoil". Without it, two copies of the same card with
different printings get falsely auto-linked.

Run once: python migrate_variance.py
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
    # 1. Add variance column to intake_items
    (
        "Add variance column to intake_items",
        """
        ALTER TABLE intake_items
        ADD COLUMN IF NOT EXISTS variance VARCHAR(100) DEFAULT '';
        """,
    ),
    # 2. Add variance column to product_mappings
    (
        "Add variance column to product_mappings",
        """
        ALTER TABLE product_mappings
        ADD COLUMN IF NOT EXISTS variance VARCHAR(100) DEFAULT '';
        """,
    ),
    # 3. Drop old unique index and create new one with variance
    (
        "Drop old unique index on product_mappings",
        """
        DROP INDEX IF EXISTS product_mappings_full_key;
        """,
    ),
    (
        "Create new unique index with variance",
        """
        CREATE UNIQUE INDEX IF NOT EXISTS product_mappings_full_key
        ON product_mappings(
            collectr_name, product_type,
            COALESCE(set_name, ''), COALESCE(card_number, ''), COALESCE(variance, '')
        );
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
