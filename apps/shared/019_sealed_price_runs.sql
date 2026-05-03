-- Audit trail for the sealed price updater (dailyrunner.py).
-- Replaces the legacy CSV files (price_updates_pushed.csv,
-- price_updates_needs_review.csv, price_updates_untouched.csv,
-- price_updates_missing_listing.csv) which were lost on every Railway
-- redeploy. One row per Shopify variant scanned per nightly run.

CREATE TABLE IF NOT EXISTS sealed_price_runs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    -- Shopify identification
    product_gid     TEXT,
    variant_id      TEXT,
    sku             TEXT,
    title           TEXT,
    handle          TEXT,
    tcgplayer_id    TEXT,
    qty             INTEGER,
    -- Pricing snapshot
    old_price       NUMERIC(10,2),     -- Shopify price before this run
    tcg_price       NUMERIC(10,2),     -- raw TCGplayer featured price
    suggested_price NUMERIC(10,2),     -- charm-rounded competitive undercut
    new_price       NUMERIC(10,2),     -- if auto-updated this run
    delta_pct       NUMERIC(7,2),
    -- Decision
    action          TEXT NOT NULL,     -- updated | review | missing | untouched | skip | error
    reason          TEXT,
    -- Per-row review state
    apply_status    TEXT DEFAULT 'pending',  -- pending | applied | dismissed
    applied_at      TIMESTAMPTZ,
    applied_price   NUMERIC(10,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sealed_runs_run_id   ON sealed_price_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_sealed_runs_action   ON sealed_price_runs(action);
CREATE INDEX IF NOT EXISTS idx_sealed_runs_apply_st ON sealed_price_runs(apply_status);
CREATE INDEX IF NOT EXISTS idx_sealed_runs_started  ON sealed_price_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sealed_runs_variant  ON sealed_price_runs(variant_id);
