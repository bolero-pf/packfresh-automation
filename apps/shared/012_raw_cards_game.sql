-- Add a `game` column to raw_cards so kiosk can filter by IP
-- (pokemon, onepiece, magic, lorcana, riftbound) without joining
-- scrydex_price_cache on every browse query.
--
-- Backfill: copy from scrydex_price_cache via tcgplayer_id.
-- Anything still NULL (manual entries, off-catalog cards) defaults
-- to 'pokemon' since that's the historical implicit default.

ALTER TABLE raw_cards ADD COLUMN IF NOT EXISTS game TEXT;

-- One-time backfill from cache. DISTINCT ON keeps a single row per
-- tcgplayer_id (a tcg_id can show up in multiple cache rows).
UPDATE raw_cards rc
SET game = sc.game
FROM (
    SELECT DISTINCT ON (tcgplayer_id) tcgplayer_id, game
    FROM scrydex_price_cache
    WHERE tcgplayer_id IS NOT NULL
) sc
WHERE rc.tcgplayer_id = sc.tcgplayer_id AND rc.game IS NULL;

UPDATE raw_cards SET game = 'pokemon' WHERE game IS NULL;

-- Index for the kiosk filter
CREATE INDEX IF NOT EXISTS idx_raw_cards_game ON raw_cards(game);
