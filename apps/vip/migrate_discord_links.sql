-- Discord <-> Shopify customer link table
-- Run once against your Railway PostgreSQL database.

CREATE TABLE IF NOT EXISTS discord_links (
    shopify_customer_gid TEXT PRIMARY KEY,
    discord_user_id      TEXT NOT NULL,
    discord_username     TEXT,
    linked_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Index for reverse lookups (Discord user -> Shopify customer)
CREATE UNIQUE INDEX IF NOT EXISTS idx_discord_links_discord_user
    ON discord_links (discord_user_id);
