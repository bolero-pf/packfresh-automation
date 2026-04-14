-- ── Scrydex <-> TCGPlayer ID mapping table ──────────────────
-- Populated automatically by ScrydexClient when it sees both IDs,
-- and by the nightly set-based price updater.

CREATE TABLE IF NOT EXISTS scrydex_tcg_map (
    scrydex_id    TEXT PRIMARY KEY,
    tcgplayer_id  INTEGER NOT NULL,
    product_type  TEXT DEFAULT 'card',  -- 'card' or 'sealed'
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_scrydex_tcg_map_tcg
    ON scrydex_tcg_map(tcgplayer_id);

-- For the nightly sealed price comparison log
CREATE TABLE IF NOT EXISTS sealed_price_comparison (
    id            SERIAL PRIMARY KEY,
    variant_id    TEXT NOT NULL,
    product_name  TEXT,
    selenium_price NUMERIC(10,2),
    scrydex_price  NUMERIC(10,2),
    delta_pct      NUMERIC(6,2),
    recorded_at    DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE INDEX IF NOT EXISTS idx_sealed_price_comp_date
    ON sealed_price_comparison(recorded_at);
