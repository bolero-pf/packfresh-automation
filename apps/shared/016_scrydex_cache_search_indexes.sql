-- ── Trigram + btree indexes for scrydex_price_cache substring search ──
-- The /api/ingest/search-cards endpoint (relink autocomplete) does
-- `ILIKE '%term%'` across product_name / product_name_en / expansion_name /
-- expansion_name_en. Without a trigram index that's a sequential scan over
-- every row in the cache (one row per card × variant × condition × grade
-- across every synced game). These indexes drop that lookup from ~2.6s to
-- <100ms on a fat cache.
--
-- Each GIN index is filtered (WHERE price_type='raw' AND condition='NM')
-- to match the same predicate the search endpoint uses, keeping the index
-- to ~1/5 of the table rows. If you add search paths that query other
-- conditions or price_types, add matching partial indexes rather than
-- removing this filter.
--
-- Build via `python ingestion/migrate_search_indexes.py` — it uses
-- CREATE INDEX CONCURRENTLY so it's safe to run while staff are working.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_scrydex_cache_product_name_trgm
    ON scrydex_price_cache USING gin (product_name gin_trgm_ops)
    WHERE price_type = 'raw' AND condition = 'NM';

CREATE INDEX IF NOT EXISTS idx_scrydex_cache_product_name_en_trgm
    ON scrydex_price_cache USING gin (product_name_en gin_trgm_ops)
    WHERE price_type = 'raw' AND condition = 'NM';

CREATE INDEX IF NOT EXISTS idx_scrydex_cache_expansion_name_trgm
    ON scrydex_price_cache USING gin (expansion_name gin_trgm_ops)
    WHERE price_type = 'raw' AND condition = 'NM';

CREATE INDEX IF NOT EXISTS idx_scrydex_cache_expansion_name_en_trgm
    ON scrydex_price_cache USING gin (expansion_name_en gin_trgm_ops)
    WHERE price_type = 'raw' AND condition = 'NM';

CREATE INDEX IF NOT EXISTS idx_scrydex_cache_card_number
    ON scrydex_price_cache (card_number)
    WHERE price_type = 'raw' AND condition = 'NM';
