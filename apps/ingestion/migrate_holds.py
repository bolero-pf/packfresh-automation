"""
Migration 005: Holds system + raw card additions

Adds:
  - holds table (customer hold requests from kiosk)
  - hold_items table (individual cards on a hold)
  - PENDING_RETURN to raw_cards state machine
  - proposed_price to raw_cards
  - proposed_price to inventory_product_cache (sealed)

Safe to re-run.
"""

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()

def col_exists(table, col):
    cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (table, col))
    return bool(cur.fetchone())

def table_exists(table):
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table,))
    return bool(cur.fetchone())

def index_exists(idx):
    cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (idx,))
    return bool(cur.fetchone())

print("Running migration 005: holds system + raw card additions...")

# ── 1. holds table ────────────────────────────────────────────────────────────
print("\n[1/4] holds table")
if not table_exists("holds"):
    cur.execute("""
        CREATE TABLE holds (
            id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            customer_name   VARCHAR(255) NOT NULL,
            customer_phone  VARCHAR(50),
            status          VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
            -- PENDING: submitted, not yet pulled
            -- PULLING: staff is retrieving cards
            -- READY:   cards pulled, awaiting customer decision
            -- ACCEPTED: customer took some/all cards → listings created
            -- RETURNED: all cards returned to inventory
            -- EXPIRED:  timed out after 2h in READY state
            notes           TEXT,
            item_count      INTEGER NOT NULL DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ready_at        TIMESTAMP,   -- when status → READY
            expires_at      TIMESTAMP,   -- ready_at + 2 hours
            resolved_at     TIMESTAMP
        )
    """)
    print("  ✓ Created holds")
    for idx, col in [("idx_holds_status", "status"), ("idx_holds_created", "created_at DESC")]:
        cur.execute(f"CREATE INDEX {idx} ON holds({col})")
    print("  ✓ Created indexes on holds")
else:
    print("  · holds already exists")

# ── 2. hold_items table ───────────────────────────────────────────────────────
print("\n[2/4] hold_items table")
if not table_exists("hold_items"):
    cur.execute("""
        CREATE TABLE hold_items (
            id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            hold_id         UUID NOT NULL REFERENCES holds(id) ON DELETE CASCADE,
            raw_card_id     UUID NOT NULL REFERENCES raw_cards(id),
            barcode         VARCHAR(100) NOT NULL,

            -- Per-item status within the hold
            status          VARCHAR(30) NOT NULL DEFAULT 'REQUESTED',
            -- REQUESTED: on the pick list
            -- PULLED:    scanned out of storage by staff
            -- ACCEPTED:  customer kept it → Shopify listing created
            -- REJECTED:  customer didn't want it → PENDING_RETURN
            -- RETURNED:  back in storage

            shopify_product_id  BIGINT,    -- set when listing created
            shopify_variant_id  BIGINT,

            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pulled_at       TIMESTAMP,
            resolved_at     TIMESTAMP
        )
    """)
    print("  ✓ Created hold_items")
    cur.execute("CREATE INDEX idx_hold_items_hold ON hold_items(hold_id)")
    cur.execute("CREATE INDEX idx_hold_items_card ON hold_items(raw_card_id)")
    cur.execute("CREATE INDEX idx_hold_items_barcode ON hold_items(barcode)")
    print("  ✓ Created indexes on hold_items")
else:
    print("  · hold_items already exists")

# ── 3. raw_cards additions ────────────────────────────────────────────────────
print("\n[3/4] raw_cards additions")

new_cols = [
    ("proposed_price",  "DECIMAL(10,2)"),
    ("current_hold_id", "UUID"),   # which hold has this card right now
]
for col, defn in new_cols:
    if not col_exists("raw_cards", col):
        cur.execute(f"ALTER TABLE raw_cards ADD COLUMN {col} {defn}")
        print(f"  ✓ Added {col} to raw_cards")
    else:
        print(f"  · {col} already exists")

# PENDING_RETURN is a valid state — update the view comment, nothing to migrate
# raw_cards.state valid values: PURCHASED, STORED, PULLED, PENDING_RETURN, PENDING_SALE, REMOVED
print("  · State machine: PENDING_RETURN is now a valid state (no schema change needed)")

# ── 4. inventory_product_cache addition (sealed proposed price) ───────────────
print("\n[4/4] inventory_product_cache: proposed_price")
if not col_exists("inventory_product_cache", "proposed_price"):
    cur.execute("ALTER TABLE inventory_product_cache ADD COLUMN proposed_price DECIMAL(10,2)")
    print("  ✓ Added proposed_price to inventory_product_cache")
else:
    print("  · proposed_price already exists on inventory_product_cache")

conn.commit()
cur.close()
conn.close()
print("\n✅ Migration 005 complete.")
