"""
migrate_shopify_variant_links.py

Captures the *specific* Shopify variant an intake item is linked to (not just
the product). Needed because distro products not in Scrydex (e.g. Dragon Shield
sleeve colors) have many same-title variants — the store-link picker now shows
a variant label and persists which variant the operator chose.

Adds:
  - intake_items.shopify_variant_id
  - product_mappings.shopify_variant_id  (so re-imports auto-link the variant)
  - inventory_product_cache.variant_label (also added by CacheManager._migrate_columns;
    added here too for deploy-order safety so /api/store/search never 500s)

Run once:
    python migrate_shopify_variant_links.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
import db

def run():
    print("Adding shopify_variant_id to intake_items...")
    db.execute("ALTER TABLE intake_items ADD COLUMN IF NOT EXISTS shopify_variant_id BIGINT")

    print("Adding shopify_variant_id to product_mappings...")
    db.execute("ALTER TABLE product_mappings ADD COLUMN IF NOT EXISTS shopify_variant_id BIGINT")

    print("Adding variant_label to inventory_product_cache...")
    db.execute("ALTER TABLE inventory_product_cache ADD COLUMN IF NOT EXISTS variant_label VARCHAR(255)")

    print("Done.")

if __name__ == "__main__":
    run()
