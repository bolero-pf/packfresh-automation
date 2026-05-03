-- Permanent allow-list of (domain, key) tuples that the nightly price
-- updaters should NEVER auto-modify. Created so a runaway $10 -> $13k
-- price suggestion (data corruption / catalog mismatch) can be muted in
-- one click without disabling the entire updater.
--
-- Domain conventions:
--   raw    -> block_key = scrydex_id when present, else 'tcg:<tcgplayer_id>'
--   slab   -> block_key = variant_gid (each slab is a unique listing)
--   sealed -> block_key = variant_id (the Shopify numeric variant id)

CREATE TABLE IF NOT EXISTS price_auto_block (
    id          SERIAL PRIMARY KEY,
    domain      TEXT NOT NULL,
    block_key   TEXT NOT NULL,
    -- Snapshot of identity when blocked, so /dashboard/price-blocks can
    -- show what we blocked even after the underlying card is gone.
    label       TEXT,
    reason      TEXT,
    blocked_by  TEXT,
    blocked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(domain, block_key)
);

CREATE INDEX IF NOT EXISTS idx_price_auto_block_domain ON price_auto_block(domain);
