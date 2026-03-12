"""
migrate_shopify_links.py

Adds shopify_product_id to intake_items and product_mappings,
enabling Shopify store link persistence even when TCGPlayer ID is absent.

Run once:
    python migrate_shopify_links.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import db

def run():
    print("Adding shopify_product_id to intake_items...")
    db.execute("""
        ALTER TABLE intake_items
        ADD COLUMN IF NOT EXISTS shopify_product_id BIGINT,
        ADD COLUMN IF NOT EXISTS shopify_product_name VARCHAR(500)
    """)

    print("Adding shopify_product_id to product_mappings...")
    db.execute("""
        ALTER TABLE product_mappings
        ADD COLUMN IF NOT EXISTS shopify_product_id BIGINT,
        ADD COLUMN IF NOT EXISTS shopify_product_name VARCHAR(500)
    """)

    # Allow product_mappings to record name→shopify even with no TCGPlayer ID.
    # The existing UNIQUE(collectr_name, product_type) constraint stays — we just
    # relax the NOT NULL on tcgplayer_id for rows that are shopify-only links.
    print("Relaxing NOT NULL on product_mappings.tcgplayer_id for shopify-only rows...")
    db.execute("""
        ALTER TABLE product_mappings
        ALTER COLUMN tcgplayer_id DROP NOT NULL
    """)

    print("Done.")

if __name__ == "__main__":
    run()
