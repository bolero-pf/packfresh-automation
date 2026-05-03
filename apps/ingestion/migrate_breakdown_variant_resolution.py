"""
Migration: Variant claim tracking on intake rows.

Adds:
  intake_items.claimed_variant_id  -- seller's claim at intake (rare; locks offer math to a specific variant)
  intake_items.actual_variant_id   -- variant chosen during ingest break-down

Any recipe with >1 variant is implicitly probabilistic at intake time — the
operator only knows the actual variant when cracking it open in ingest. Intake
offer math uses the avg across all variants unless the seller asserted a
specific variant ("it's the Kanto one") and the operator locked it via
claimed_variant_id. A later mismatch between claimed_variant_id and
actual_variant_id is a misrepresentation flag.

Run once against the shared database (safe to re-run).
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — add it to .env or set it in the environment")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Running variant claim/actual tracking migration...")


def _has_column(table: str, column: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, column))
    return cur.fetchone() is not None


# ── intake_items.claimed_variant_id ─────────────────────────────────
if not _has_column("intake_items", "claimed_variant_id"):
    cur.execute("""
        ALTER TABLE intake_items
            ADD COLUMN claimed_variant_id UUID
            REFERENCES sealed_breakdown_variants(id) ON DELETE SET NULL
    """)
    print("  + Added claimed_variant_id to intake_items")
else:
    print("  - claimed_variant_id already exists on intake_items")


# ── intake_items.actual_variant_id ──────────────────────────────────
if not _has_column("intake_items", "actual_variant_id"):
    cur.execute("""
        ALTER TABLE intake_items
            ADD COLUMN actual_variant_id UUID
            REFERENCES sealed_breakdown_variants(id) ON DELETE SET NULL
    """)
    print("  + Added actual_variant_id to intake_items")
else:
    print("  - actual_variant_id already exists on intake_items")


conn.commit()
cur.close()
conn.close()
print("Done.")
