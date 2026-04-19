-- Capture every Scrydex field. Two new tables (one per card, one per expansion)
-- + a JSONB raw column on each so we never silently lose newly-added fields.
-- High-value fields are promoted to dedicated columns for indexable querying.

CREATE TABLE IF NOT EXISTS scrydex_card_meta (
    game                       TEXT NOT NULL,
    scrydex_id                 TEXT NOT NULL,
    -- Universal display
    printed_number             TEXT,                -- on-card "OP14-041" / "4/102"
    rarity_code                TEXT,                -- "L", "★H"
    artist                     TEXT,
    flavor_text                TEXT,
    rules                      JSONB,               -- list of strings
    subtypes                   JSONB,               -- list of strings
    -- Pokemon-only
    hp                         INTEGER,
    supertype                  TEXT,                -- Pokémon / Trainer / Energy
    types                      JSONB,
    national_pokedex_numbers   JSONB,
    evolves_from               JSONB,
    attacks                    JSONB,
    abilities                  JSONB,
    weaknesses                 JSONB,
    resistances                JSONB,
    retreat_cost               JSONB,
    converted_retreat_cost     INTEGER,
    legalities                 JSONB,
    -- One Piece-only
    card_type                  TEXT,                -- Leader / Character / Event / Stage
    attribute                  TEXT,                -- Special / Slash / Strike
    colors                     JSONB,
    life                       INTEGER,
    power                      INTEGER,
    printings                  JSONB,               -- list of expansion ids it appears in
    tags                       JSONB,
    -- Catch-all so newly-added Scrydex fields are never lost
    raw                        JSONB NOT NULL,
    fetched_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game, scrydex_id)
);

CREATE INDEX IF NOT EXISTS idx_scrydex_card_meta_printed_number
    ON scrydex_card_meta(printed_number);
CREATE INDEX IF NOT EXISTS idx_scrydex_card_meta_artist
    ON scrydex_card_meta(artist);


CREATE TABLE IF NOT EXISTS scrydex_expansion_meta (
    game            TEXT NOT NULL,
    expansion_id    TEXT NOT NULL,
    code            TEXT,                -- short set code (BS for base1, OP14 for OP14)
    name            TEXT,
    type            TEXT,                -- Booster Pack, ETB, etc.
    total           INTEGER,
    printed_total   INTEGER,             -- on-card "/102"
    release_date    DATE,
    series          TEXT,                -- Pokemon: Base, Scarlet & Violet
    language        TEXT,
    language_code   TEXT,
    logo            TEXT,
    symbol          TEXT,
    sort_order      INTEGER,
    is_online_only  BOOLEAN,
    raw             JSONB NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game, expansion_id)
);

CREATE INDEX IF NOT EXISTS idx_scrydex_expansion_meta_release_date
    ON scrydex_expansion_meta(release_date);


-- Denormalized so search doesn't need to JOIN with scrydex_card_meta.
-- The on-card printed number ("OP14-041") differs from card_number ("41")
-- and customers search by what's printed, not what's stored.
ALTER TABLE scrydex_price_cache ADD COLUMN IF NOT EXISTS printed_number TEXT;
CREATE INDEX IF NOT EXISTS idx_scrydex_cache_printed_number
    ON scrydex_price_cache(printed_number);
