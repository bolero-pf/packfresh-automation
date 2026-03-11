"""
migrate.py

Run once to bootstrap the inventory tables in Postgres.
Also optionally migrates data from the old SQLite inventory.db if provided.

Usage:
    # Bootstrap tables only:
    DATABASE_URL=postgresql://... python migrate.py

    # Migrate from old SQLite db:
    DATABASE_URL=postgresql://... python migrate.py --from-sqlite /path/to/inventory.db
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

import db


def run_migration():
    logger.info("Creating inventory tables…")

    db.execute("""
        CREATE TABLE IF NOT EXISTS inventory_product_cache (
            shopify_product_id  BIGINT NOT NULL,
            shopify_variant_id  BIGINT NOT NULL,
            title               VARCHAR(500),
            handle              VARCHAR(500),
            status              VARCHAR(50),
            tags                TEXT,
            shopify_price       NUMERIC(10,2),
            shopify_qty         INTEGER,
            inventory_item_id   BIGINT,
            tcgplayer_id        BIGINT,
            is_damaged          BOOLEAN DEFAULT FALSE,
            last_synced         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (shopify_product_id, shopify_variant_id)
        )
    """)
    logger.info("  ✓ inventory_product_cache")

    db.execute("""
        CREATE TABLE IF NOT EXISTS inventory_overrides (
            shopify_variant_id  BIGINT PRIMARY KEY,
            physical_count      INTEGER,
            notes               TEXT,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logger.info("  ✓ inventory_overrides")

    db.execute("""
        CREATE TABLE IF NOT EXISTS inventory_cache_meta (
            id                      INTEGER PRIMARY KEY DEFAULT 1,
            last_refreshed_at       TIMESTAMP NOT NULL DEFAULT '1970-01-01',
            last_refreshed_reason   VARCHAR(200),
            last_order_number       INTEGER,
            last_product_updated_at TIMESTAMP
        )
    """)
    logger.info("  ✓ inventory_cache_meta")

    db.execute("""
        CREATE TABLE IF NOT EXISTS breakdown_ignore (
            tcgplayer_id  BIGINT PRIMARY KEY,
            product_name  VARCHAR(500),
            reason        VARCHAR(500),
            ignored_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logger.info("  ✓ breakdown_ignore")

    db.execute("""
        CREATE TABLE IF NOT EXISTS breakdown_base_components (
            tcgplayer_id  BIGINT PRIMARY KEY,
            product_name  VARCHAR(500),
            marked_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logger.info("  ✓ breakdown_base_components")

    # Indexes
    db.execute("CREATE INDEX IF NOT EXISTS idx_inv_cache_title ON inventory_product_cache(title)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_inv_cache_status ON inventory_product_cache(status)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_inv_cache_tcg ON inventory_product_cache(tcgplayer_id)")
    logger.info("  ✓ indexes")


def migrate_from_sqlite(sqlite_path: str):
    """
    Read the old inventory table from SQLite and populate inventory_overrides
    with physical_count (was 'total amount (4/1)') and notes.

    Only migrates rows that have a variant_id (i.e. Shopify-backed rows).
    Local-only rows without a variant_id are skipped — they don't exist in
    the new Postgres cache yet.
    """
    import sqlite3

    if not os.path.exists(sqlite_path):
        logger.error(f"SQLite file not found: {sqlite_path}")
        return

    logger.info(f"Migrating data from {sqlite_path}…")
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM inventory")
        rows = cur.fetchall()
    except Exception as e:
        logger.error(f"Could not read 'inventory' table from SQLite: {e}")
        conn.close()
        return

    migrated = skipped = 0
    for row in rows:
        d = dict(row)
        variant_id = d.get("variant_id")
        if not variant_id:
            skipped += 1
            continue

        physical_count = d.get("total amount (4/1)")
        notes          = d.get("notes")

        if physical_count is None and not notes:
            skipped += 1
            continue

        try:
            db.execute("""
                INSERT INTO inventory_overrides (shopify_variant_id, physical_count, notes)
                VALUES (%s, %s, %s)
                ON CONFLICT (shopify_variant_id) DO UPDATE SET
                    physical_count = EXCLUDED.physical_count,
                    notes          = EXCLUDED.notes
            """, (int(variant_id), int(physical_count) if physical_count is not None else None, notes))
            migrated += 1
        except Exception as e:
            logger.warning(f"  Could not migrate variant_id={variant_id}: {e}")
            skipped += 1

    conn.close()
    logger.info(f"Migration complete: {migrated} rows migrated, {skipped} skipped")


if __name__ == "__main__":
    db.init_pool()
    run_migration()

    if "--from-sqlite" in sys.argv:
        idx = sys.argv.index("--from-sqlite")
        sqlite_path = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        if sqlite_path:
            migrate_from_sqlite(sqlite_path)
        else:
            logger.error("--from-sqlite requires a path argument")
            sys.exit(1)

    logger.info("Done.")
