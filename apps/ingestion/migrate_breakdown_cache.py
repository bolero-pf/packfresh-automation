"""
Migration: Add sealed_breakdown_cache table.

Stores what sealed products break into (components, quantities, prices, notes).
Used by:
  - ingest.pack-fresh.com: Pre-fill breakdown modal + show parent vs breakdown value
  - offers.pack-fresh.com: Show breakdown value in item list + store check
  - Both: "Count as broken down" option in store check

Run once against the shared database.
"""

import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Running breakdown cache migration...")

# ── sealed_breakdown_cache ─────────────────────────────────────────
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_name = 'sealed_breakdown_cache'
""")
if not cur.fetchone():
    cur.execute("""
        CREATE TABLE sealed_breakdown_cache (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

            -- The sealed product being broken down
            tcgplayer_id BIGINT NOT NULL UNIQUE,
            product_name VARCHAR(500) NOT NULL,

            -- Aggregate totals (denormalized for quick display)
            total_component_market DECIMAL(10, 2) NOT NULL DEFAULT 0,
            component_count INTEGER NOT NULL DEFAULT 0,

            -- Optional metadata
            promo_notes TEXT,

            -- Audit
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated_by VARCHAR(100),

            -- How many times this cache entry was used to pre-fill a breakdown
            use_count INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX idx_sbc_tcgplayer ON sealed_breakdown_cache(tcgplayer_id)")
    print("  ✓ Created sealed_breakdown_cache table")
else:
    print("  ✓ sealed_breakdown_cache already exists")

# ── sealed_breakdown_components ────────────────────────────────────
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_name = 'sealed_breakdown_components'
""")
if not cur.fetchone():
    cur.execute("""
        CREATE TABLE sealed_breakdown_components (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

            breakdown_id UUID NOT NULL
                REFERENCES sealed_breakdown_cache(id) ON DELETE CASCADE,

            -- The component product
            tcgplayer_id BIGINT,
            product_name VARCHAR(500) NOT NULL,
            set_name VARCHAR(255),

            -- How many of this component per parent unit
            quantity_per_parent INTEGER NOT NULL DEFAULT 1,

            -- Last known market price (updated when cache is saved)
            market_price DECIMAL(10, 2) NOT NULL DEFAULT 0,

            -- Optional notes for this component (e.g. "Alt Art included in some boxes")
            notes TEXT,

            display_order INTEGER NOT NULL DEFAULT 0,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX idx_sbc_comp_breakdown ON sealed_breakdown_components(breakdown_id)")
    cur.execute("CREATE INDEX idx_sbc_comp_tcgplayer ON sealed_breakdown_components(tcgplayer_id)")
    print("  ✓ Created sealed_breakdown_components table")
else:
    print("  ✓ sealed_breakdown_components already exists")

conn.commit()
cur.close()
conn.close()
print("Done!")
