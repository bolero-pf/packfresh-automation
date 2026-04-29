-- Track the active Shopify draft listing on raw_cards directly.
-- Was only stored on hold_items, so cards listed via the Sell tab (no hold)
-- had no way to undo — couldn't find the product to delete without an
-- extra Shopify search by SKU. With these columns, both flows undo the
-- same way: read the IDs, DELETE the product, clear the IDs.

ALTER TABLE raw_cards
    ADD COLUMN IF NOT EXISTS shopify_product_id BIGINT,
    ADD COLUMN IF NOT EXISTS shopify_variant_id BIGINT;

-- Backfill from hold_items so currently-PENDING_SALE cards from finished
-- holds are immediately undoable in the new Sell-tab Active Listings panel.
UPDATE raw_cards rc
SET shopify_product_id = hi.shopify_product_id,
    shopify_variant_id = hi.shopify_variant_id
FROM hold_items hi
WHERE hi.raw_card_id = rc.id
  AND rc.state = 'PENDING_SALE'
  AND rc.shopify_product_id IS NULL
  AND hi.shopify_product_id IS NOT NULL;
