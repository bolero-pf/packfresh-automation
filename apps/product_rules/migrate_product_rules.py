"""
Migration: create tables for the product_rules service.

- preorder_overrides: per-tag custom messaging (optional, defaults synthesized from the date)
- product_rule_state: which rule tags are currently on each product (webhook-maintained, drives dashboard counts)

Run once (safe to re-run).
"""

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    for _p in [".env", "../.env", "../admin/.env", "../drops/.env"]:
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
cur = conn.cursor()

print("Running product_rules migration...")

cur.execute("""
    CREATE TABLE IF NOT EXISTS preorder_overrides (
        tag           TEXT PRIMARY KEY,
        display_name  TEXT,
        button_text   TEXT,
        pdp_message   TEXT,
        cart_message  TEXT,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
""")
print("  preorder_overrides ready")

cur.execute("""
    CREATE TABLE IF NOT EXISTS product_rule_state (
        shopify_product_id  TEXT PRIMARY KEY,
        rule_tags           TEXT[] NOT NULL DEFAULT '{}',
        last_synced_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_product_rule_state_tags
    ON product_rule_state USING GIN (rule_tags)
""")
print("  product_rule_state ready")

conn.commit()
cur.close()
conn.close()
print("Done.")
