"""
Migration: Sealed breakdown cache with multi-variant support.

Adds sealed_breakdown_variants table between the cache and components.
Safely migrates any existing single-variant data.

Schema after migration:
  sealed_breakdown_cache        one row per tcgplayer_id
    sealed_breakdown_variants   one or more named configs per product
      sealed_breakdown_components  individual components per variant

Run once against the shared database (safe to re-run).
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("Running breakdown cache migration (multi-variant)...")

def table_exists(name):
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s", (name,))
    return bool(cur.fetchone())

def column_exists(table, col):
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (table, col))
    return bool(cur.fetchone())

# ── sealed_breakdown_cache (unchanged core columns, add new denorm cols) ──
if not table_exists("sealed_breakdown_cache"):
    cur.execute("""
        CREATE TABLE sealed_breakdown_cache (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            tcgplayer_id BIGINT NOT NULL UNIQUE,
            product_name VARCHAR(500) NOT NULL,
            -- denorm totals from best variant
            best_variant_market DECIMAL(10,2) NOT NULL DEFAULT 0,
            variant_count INTEGER NOT NULL DEFAULT 0,
            -- legacy columns kept for compat
            total_component_market DECIMAL(10,2) NOT NULL DEFAULT 0,
            component_count INTEGER NOT NULL DEFAULT 0,
            promo_notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated_by VARCHAR(100),
            use_count INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX idx_sbc_tcgplayer ON sealed_breakdown_cache(tcgplayer_id)")
    print("  + Created sealed_breakdown_cache")
else:
    # Add new columns if upgrading
    for col, defn in [
        ("best_variant_market", "DECIMAL(10,2) NOT NULL DEFAULT 0"),
        ("variant_count", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        if not column_exists("sealed_breakdown_cache", col):
            cur.execute(f"ALTER TABLE sealed_breakdown_cache ADD COLUMN {col} {defn}")
            print(f"  + Added column {col}")
    print("  = sealed_breakdown_cache exists")

# ── sealed_breakdown_variants ─────────────────────────────────────────────
if not table_exists("sealed_breakdown_variants"):
    cur.execute("""
        CREATE TABLE sealed_breakdown_variants (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            breakdown_id UUID NOT NULL
                REFERENCES sealed_breakdown_cache(id) ON DELETE CASCADE,
            variant_name VARCHAR(200) NOT NULL DEFAULT 'Standard',
            notes TEXT,
            -- denorm totals for this variant
            total_component_market DECIMAL(10,2) NOT NULL DEFAULT 0,
            component_count INTEGER NOT NULL DEFAULT 0,
            display_order INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX idx_sbv_breakdown ON sealed_breakdown_variants(breakdown_id)")
    print("  + Created sealed_breakdown_variants")
else:
    print("  = sealed_breakdown_variants exists")

# ── sealed_breakdown_components (add variant_id FK, keep breakdown_id for compat) ─
if not table_exists("sealed_breakdown_components"):
    cur.execute("""
        CREATE TABLE sealed_breakdown_components (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            variant_id UUID NOT NULL
                REFERENCES sealed_breakdown_variants(id) ON DELETE CASCADE,
            -- legacy: keep breakdown_id col for migration detection
            breakdown_id UUID,
            tcgplayer_id BIGINT,
            product_name VARCHAR(500) NOT NULL,
            set_name VARCHAR(255),
            quantity_per_parent INTEGER NOT NULL DEFAULT 1,
            market_price DECIMAL(10,2) NOT NULL DEFAULT 0,
            notes TEXT,
            display_order INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("CREATE INDEX idx_sbco_variant ON sealed_breakdown_components(variant_id)")
    cur.execute("CREATE INDEX idx_sbco_tcgplayer ON sealed_breakdown_components(tcgplayer_id)")
    print("  + Created sealed_breakdown_components (new schema)")
else:
    # Check if old schema (breakdown_id FK, no variant_id)
    has_variant_id = column_exists("sealed_breakdown_components", "variant_id")
    if not has_variant_id:
        print("  > Migrating components to variant structure...")
        # Add variant_id column (nullable until we migrate)
        cur.execute("ALTER TABLE sealed_breakdown_components ADD COLUMN variant_id UUID REFERENCES sealed_breakdown_variants(id) ON DELETE CASCADE")

        # For each cache entry, create a default variant and link components
        cur.execute("SELECT id, tcgplayer_id, product_name, total_component_market, component_count, promo_notes FROM sealed_breakdown_cache")
        caches = cur.fetchall()
        migrated = 0
        for c in caches:
            # Check if already has a variant
            cur.execute("SELECT id FROM sealed_breakdown_variants WHERE breakdown_id=%s LIMIT 1", (str(c["id"]),))
            if cur.fetchone():
                continue
            # Get components
            cur.execute("SELECT * FROM sealed_breakdown_components WHERE breakdown_id=%s AND variant_id IS NULL ORDER BY display_order", (str(c["id"]),))
            comps = cur.fetchall()
            if not comps:
                # Still create an empty default variant so the product appears
                cur.execute("""
                    INSERT INTO sealed_breakdown_variants (breakdown_id, variant_name, notes, total_component_market, component_count)
                    VALUES (%s, 'Standard', %s, %s, %s) RETURNING id
                """, (str(c["id"]), c.get("promo_notes"), c["total_component_market"], c["component_count"]))
            else:
                total = sum(float(x["market_price"] or 0) * int(x.get("quantity_per_parent") or 1) for x in comps)
                cur.execute("""
                    INSERT INTO sealed_breakdown_variants (breakdown_id, variant_name, notes, total_component_market, component_count)
                    VALUES (%s, 'Standard', %s, %s, %s) RETURNING id
                """, (str(c["id"]), c.get("promo_notes"), total, len(comps)))
            vid = str(cur.fetchone()["id"])
            # Link components to this variant
            for comp in comps:
                cur.execute("UPDATE sealed_breakdown_components SET variant_id=%s WHERE id=%s", (vid, str(comp["id"])))
            migrated += 1

        conn.commit()  # commit migration before making variant_id NOT NULL would fail
        print(f"  + Migrated {migrated} cache entries to variant structure")
    else:
        print("  = sealed_breakdown_components exists (new schema)")

# ── Refresh denorm totals ─────────────────────────────────────────────────
cur.execute("""
    UPDATE sealed_breakdown_cache sbc SET
        variant_count = (SELECT COUNT(*) FROM sealed_breakdown_variants WHERE breakdown_id=sbc.id),
        best_variant_market = COALESCE(
            (SELECT MAX(total_component_market) FROM sealed_breakdown_variants WHERE breakdown_id=sbc.id), 0
        )
""")
print("  + Refreshed denorm totals")

conn.commit()
cur.close()
conn.close()
print("Done!")
