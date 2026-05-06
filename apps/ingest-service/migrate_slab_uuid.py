"""
Migration: Slab UUID tracking for graded Collectr pastes.

Collectr's graded HTML carries the grade info only in the slab graphic image
(no text). The slab image URL is stable per (company, grade) — e.g. every PSA
10 row uses the same UUID. We extract that UUID at parse time and look it up
in slab_grade_lookup. Unknown UUIDs land on intake_items.slab_uuid with NULL
grade_company/grade_value; the operator identifies it once in the UI, we
write the lookup, and every future paste with that UUID auto-fills.

Adds:
  - slab_grade_lookup            (UUID PK -> company + grade)
  - intake_items.slab_uuid       VARCHAR(64)    nullable

Run once: python migrate_slab_uuid.py
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
    raise RuntimeError("DATABASE_URL not set and not found in .env")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

print("Running slab UUID migration...")

steps = [
    ("slab_grade_lookup table", """
        CREATE TABLE IF NOT EXISTS slab_grade_lookup (
            slab_uuid       VARCHAR(64) PRIMARY KEY,
            grade_company   VARCHAR(20) NOT NULL,
            grade_value     VARCHAR(10) NOT NULL,
            sample_image_url TEXT,
            identified_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            identified_by   VARCHAR(100)
        )
    """),
    ("intake_items.slab_uuid",
     "ALTER TABLE intake_items ADD COLUMN IF NOT EXISTS slab_uuid VARCHAR(64)"),
    ("idx intake_items.slab_uuid (partial)",
     "CREATE INDEX IF NOT EXISTS idx_intake_items_slab_uuid ON intake_items(slab_uuid) WHERE slab_uuid IS NOT NULL"),
]

for name, sql in steps:
    cur.execute(sql)
    print(f"  ✓ {name}")

cur.close()
conn.close()
print("Migration complete.")
