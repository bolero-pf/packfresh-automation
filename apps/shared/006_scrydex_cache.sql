-- ── Scrydex local price cache ─────────────────────────────────
-- Mirrors the full Scrydex catalog locally. Updated nightly via set pulls.
-- Every runtime price lookup becomes a DB read — zero API calls.

-- One row per card × variant × condition (denormalized for fast lookups)
CREATE TABLE IF NOT EXISTS scrydex_price_cache (
    id                  SERIAL PRIMARY KEY,
    scrydex_id          TEXT NOT NULL,           -- e.g. "sv3pt5-199"
    tcgplayer_id        INTEGER,                 -- from marketplaces (cards only)
    expansion_id        TEXT NOT NULL,            -- e.g. "sv3pt5"
    expansion_name      TEXT,                     -- e.g. "151"
    product_type        TEXT NOT NULL DEFAULT 'card',  -- 'card' or 'sealed'
    product_name        TEXT,
    card_number         TEXT,                     -- e.g. "199" (cards only)
    rarity              TEXT,
    variant             TEXT NOT NULL DEFAULT 'normal', -- holofoil, reverseHolofoil, normal, etc.
    condition           TEXT NOT NULL DEFAULT 'NM',     -- NM, LP, MP, HP, DM, U
    price_type          TEXT NOT NULL DEFAULT 'raw',    -- 'raw' or 'graded'
    grade_company       TEXT,                     -- PSA, CGC, BGS, SGC (graded only)
    grade_value         TEXT,                     -- 10, 9.5, 9, etc. (graded only)
    market_price        NUMERIC(10,2),
    low_price           NUMERIC(10,2),
    mid_price           NUMERIC(10,2),            -- graded only
    high_price          NUMERIC(10,2),            -- graded only
    trend_1d_pct        NUMERIC(6,2),
    trend_7d_pct        NUMERIC(6,2),
    trend_30d_pct       NUMERIC(6,2),
    image_small         TEXT,
    image_medium        TEXT,
    image_large         TEXT,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(scrydex_id, variant, condition, price_type, COALESCE(grade_company, ''), COALESCE(grade_value, ''))
);

-- Fast lookups by TCGPlayer ID (the primary access pattern during migration)
CREATE INDEX IF NOT EXISTS idx_scrydex_cache_tcg
    ON scrydex_price_cache(tcgplayer_id);

-- Fast lookups by expansion (for nightly bulk upsert)
CREATE INDEX IF NOT EXISTS idx_scrydex_cache_expansion
    ON scrydex_price_cache(expansion_id);

-- Fast lookups by scrydex_id + variant (for direct Scrydex ID access)
CREATE INDEX IF NOT EXISTS idx_scrydex_cache_sid_variant
    ON scrydex_price_cache(scrydex_id, variant);

-- Track which expansions have been pulled and when
CREATE TABLE IF NOT EXISTS scrydex_sync_log (
    expansion_id    TEXT PRIMARY KEY,
    expansion_name  TEXT,
    card_count      INTEGER,
    last_synced     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    credits_used    INTEGER DEFAULT 0,
    active          BOOLEAN DEFAULT TRUE  -- false = skip in nightly runs
);
