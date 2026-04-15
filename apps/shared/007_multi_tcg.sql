-- Multi-TCG support: add game column to all Scrydex tables.
-- Existing data defaults to 'pokemon' — zero breakage.

-- 1. scrydex_price_cache
ALTER TABLE scrydex_price_cache ADD COLUMN IF NOT EXISTS game TEXT NOT NULL DEFAULT 'pokemon';

-- Drop old unique constraint (name varies by DB), recreate with game
ALTER TABLE scrydex_price_cache DROP CONSTRAINT IF EXISTS scrydex_price_cache_scrydex_id_variant_condition_price_ty_key;
ALTER TABLE scrydex_price_cache DROP CONSTRAINT IF EXISTS scrydex_price_cache_game_unique;
ALTER TABLE scrydex_price_cache ADD CONSTRAINT scrydex_price_cache_game_unique
    UNIQUE(game, scrydex_id, variant, condition, price_type, grade_company_key, grade_value_key);

CREATE INDEX IF NOT EXISTS idx_scrydex_cache_game ON scrydex_price_cache(game);

-- 2. scrydex_sync_log
ALTER TABLE scrydex_sync_log ADD COLUMN IF NOT EXISTS game TEXT NOT NULL DEFAULT 'pokemon';

-- Change PK from (expansion_id) to (game, expansion_id)
ALTER TABLE scrydex_sync_log DROP CONSTRAINT IF EXISTS scrydex_sync_log_pkey;
ALTER TABLE scrydex_sync_log ADD PRIMARY KEY (game, expansion_id);

-- 3. scrydex_tcg_map
ALTER TABLE scrydex_tcg_map ADD COLUMN IF NOT EXISTS game TEXT NOT NULL DEFAULT 'pokemon';
