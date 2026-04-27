"""
Migration: Pipeline stage tracking (006)

Adds pipeline_stage + user-stamp columns + void columns to intake_items.
Backfills pipeline_stage from existing timestamps (pushed > barcoded > routed > verified).

Phase 1 of the Verify -> Barcode -> Route -> Push reorder. Additive only —
existing code ignores the new column. Safe to re-run.
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

def index_exists(name):
    cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (name,))
    return bool(cur.fetchone())

print("Running migration 006: pipeline_stage tracking...")

# ── 1. New columns on intake_items ────────────────────────────────────────────
print("\n[1/3] intake_items: pipeline_stage + user stamps + void columns")

new_cols = [
    ("pipeline_stage",  "VARCHAR(20) NOT NULL DEFAULT 'received'"),
    ("verified_by",     "VARCHAR(100)"),
    ("barcoded_by",     "VARCHAR(100)"),
    ("routed_by",       "VARCHAR(100)"),
    ("pushed_by",       "VARCHAR(100)"),
    ("voided_at",       "TIMESTAMP"),
    ("voided_by",       "VARCHAR(100)"),
    ("voided_reason",   "TEXT"),
]
for col, defn in new_cols:
    if not col_exists("intake_items", col):
        cur.execute(f"ALTER TABLE intake_items ADD COLUMN {col} {defn}")
        print(f"  + Added {col}")
    else:
        print(f"  . {col} already exists")

# ── 2. Backfill pipeline_stage from existing timestamps ───────────────────────
print("\n[2/3] Backfill pipeline_stage from existing timestamps")

# Most-advanced wins. routing_reviewed_at is today's "routed" timestamp.
cur.execute("""
    UPDATE intake_items
       SET pipeline_stage = CASE
           WHEN pushed_at           IS NOT NULL THEN 'pushed'
           WHEN barcoded_at         IS NOT NULL THEN 'barcoded'
           WHEN routing_reviewed_at IS NOT NULL THEN 'routed'
           WHEN verified_at         IS NOT NULL THEN 'verified'
           ELSE 'received'
       END
     WHERE pipeline_stage = 'received'
""")
print(f"  + Backfilled {cur.rowcount} intake_items rows")

# ── 3. Index for dashboard rollups ────────────────────────────────────────────
print("\n[3/3] Index on (session_id, pipeline_stage)")

if not index_exists("idx_intake_items_session_stage"):
    cur.execute("""
        CREATE INDEX idx_intake_items_session_stage
        ON intake_items (session_id, pipeline_stage)
    """)
    print("  + Created idx_intake_items_session_stage")
else:
    print("  . Index already exists")

conn.commit()
cur.close()
conn.close()
print("\n Done — migration 006 complete.")
