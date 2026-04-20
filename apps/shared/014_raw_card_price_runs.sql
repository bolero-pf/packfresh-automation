-- Audit trail for raw_card_updater. One row per raw card scanned per run.
-- Mirrors slab_price_runs but operates on raw_cards (kiosk inventory) and
-- only updates raw_cards.current_price — no Shopify mutations (Shopify
-- products for raw cards are created on-demand at Champion checkout).

CREATE TABLE IF NOT EXISTS raw_card_price_runs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    -- raw_cards identity
    raw_card_id     UUID,
    barcode         TEXT,
    tcgplayer_id    BIGINT,
    scrydex_id      TEXT,
    card_name       TEXT,
    set_name        TEXT,
    card_number     TEXT,
    condition       TEXT,
    variant         TEXT,
    cost_basis      NUMERIC(10,2),
    -- Pricing
    old_price       NUMERIC(10,2),
    new_price       NUMERIC(10,2),    -- if auto-applied this run
    suggested_price NUMERIC(10,2),    -- charm-rounded suggestion
    cache_market    NUMERIC(10,2),    -- raw market price from scrydex_price_cache
    cache_low       NUMERIC(10,2),
    delta_pct       NUMERIC(7,2),     -- (old - cache_market) / cache_market * 100
    -- Decision
    action          TEXT NOT NULL,    -- auto_applied | flag_overpriced | flag_underpriced | ok | skip | error
    reason          TEXT,
    -- Per-row review state for the flagged rows
    apply_status    TEXT DEFAULT 'pending',  -- pending | applied | dismissed
    applied_at      TIMESTAMPTZ,
    applied_price   NUMERIC(10,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_runs_run_id     ON raw_card_price_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_raw_runs_action     ON raw_card_price_runs(action);
CREATE INDEX IF NOT EXISTS idx_raw_runs_apply_st   ON raw_card_price_runs(apply_status);
CREATE INDEX IF NOT EXISTS idx_raw_runs_started    ON raw_card_price_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_runs_card       ON raw_card_price_runs(raw_card_id);
