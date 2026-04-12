"""
Migration: Kiosk Champion checkout fields on holds table (005)

Adds to holds:
  - cohort          (guest | champion)
  - customer_email
  - shopify_customer_gid
  - checkout_url    (Shopify checkoutUrl from Storefront API)
  - checkout_status (pending | completed | abandoned)

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

# ── holds table: champion checkout columns ───────────────────────────────────

cols = {
    "cohort":               "VARCHAR(20) DEFAULT 'guest'",
    "customer_email":       "VARCHAR(255)",
    "shopify_customer_gid": "VARCHAR(100)",
    "checkout_url":         "TEXT",
    "checkout_status":      "VARCHAR(20) DEFAULT 'pending'",
    "shopify_order_number": "VARCHAR(50)",
    "shipping_name":        "VARCHAR(255)",
    "shipping_address":     "TEXT",
}

for col, typedef in cols.items():
    if col_exists("holds", col):
        print(f"  ✓ holds.{col} already exists")
    else:
        cur.execute(f"ALTER TABLE holds ADD COLUMN {col} {typedef}")
        print(f"  + holds.{col} added")

conn.commit()
cur.close()
conn.close()
print("\nDone — kiosk champion migration complete.")
