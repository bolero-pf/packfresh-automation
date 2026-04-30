"""
Migration: inventory_adjustments audit table.

Logs every barcode-bind inventory adjustment Sean (or staff) makes during
aisle walks — variant, who, when, the delta, before/after quantities, and
an optional free-text note. No reason dropdown by design — the audit
work happens later, this table just gives you something to grep when a
discrepancy turns up.

Idempotent — safe to re-run.
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
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

print("Running inventory_adjustments migration...")

cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_adjustments (
        id BIGSERIAL PRIMARY KEY,
        variant_id        BIGINT NOT NULL,
        product_id        BIGINT,
        inventory_item_id BIGINT,
        product_title     VARCHAR(500),
        variant_title     VARCHAR(500),
        user_id           UUID,
        user_name         VARCHAR(255),
        delta             INTEGER NOT NULL,
        qty_before        INTEGER,
        qty_after         INTEGER,
        note              TEXT,
        created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
print("  Ensured inventory_adjustments table")

cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_adj_variant ON inventory_adjustments(variant_id)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_adj_created ON inventory_adjustments(created_at DESC)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_inv_adj_user    ON inventory_adjustments(user_id)")
print("  Ensured indexes")

cur.close()
conn.close()
print("Done.")
