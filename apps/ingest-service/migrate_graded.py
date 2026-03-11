"""
Migration: Add graded card fields to intake_items and raw_cards.

Adds:
  - intake_items.is_graded       BOOLEAN DEFAULT FALSE
  - intake_items.grade_company   VARCHAR(20)   -- PSA, BGS, CGC, SGC
  - intake_items.grade_value     VARCHAR(10)   -- 10, 9.5, 9, 8, ...

  - raw_cards.is_graded          BOOLEAN DEFAULT FALSE
  - raw_cards.grade_company      VARCHAR(20)
  - raw_cards.grade_value        VARCHAR(10)

Run once: python migrate_graded.py
"""

import os
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

print("Running graded fields migration...")

steps = [
    ("intake_items.is_graded",
     "ALTER TABLE intake_items ADD COLUMN IF NOT EXISTS is_graded BOOLEAN NOT NULL DEFAULT FALSE"),
    ("intake_items.grade_company",
     "ALTER TABLE intake_items ADD COLUMN IF NOT EXISTS grade_company VARCHAR(20)"),
    ("intake_items.grade_value",
     "ALTER TABLE intake_items ADD COLUMN IF NOT EXISTS grade_value VARCHAR(10)"),
    ("raw_cards.is_graded",
     "ALTER TABLE raw_cards ADD COLUMN IF NOT EXISTS is_graded BOOLEAN NOT NULL DEFAULT FALSE"),
    ("raw_cards.grade_company",
     "ALTER TABLE raw_cards ADD COLUMN IF NOT EXISTS grade_company VARCHAR(20)"),
    ("raw_cards.grade_value",
     "ALTER TABLE raw_cards ADD COLUMN IF NOT EXISTS grade_value VARCHAR(10)"),
]

for name, sql in steps:
    cur.execute(sql)
    print(f"  ✓ {name}")

cur.close()
conn.close()
print("Migration complete.")
