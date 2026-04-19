-- Per-variant TCGPlayer ID support.
-- Each Scrydex card can have multiple variants (normal, altArt, foil, etc.)
-- and each variant has its OWN TCGPlayer product_id in marketplaces[].
-- The old PK on (scrydex_id) only allowed one tcg_id per card; we lost the
-- altArt's id (e.g. OP14-041 had both 668333 normal and 668335 altArt — only
-- 668333 was kept).

ALTER TABLE scrydex_tcg_map DROP CONSTRAINT IF EXISTS scrydex_tcg_map_pkey;
ALTER TABLE scrydex_tcg_map ADD PRIMARY KEY (scrydex_id, tcgplayer_id);

-- The non-unique idx_scrydex_tcg_map_tcg from migration 007 stays as-is —
-- multiple rows now legitimately share a tcgplayer_id (cross-set promos AND
-- now multi-variant entries).
