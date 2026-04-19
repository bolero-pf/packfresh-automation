-- Audit trail for slab_updater. Every slab evaluated in a run gets a row,
-- whether we adjusted the price, flagged it for review, skipped it, or hit
-- an error. Lets the prices.pack-fresh.com dashboard show what changed and
-- surface listings that are missing required metadata.

CREATE TABLE IF NOT EXISTS slab_price_runs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID NOT NULL,                  -- one UUID per slab_updater invocation
    started_at      TIMESTAMPTZ NOT NULL,           -- when the run as a whole started
    -- Product identification (Shopify-side)
    product_gid     TEXT,
    variant_gid     TEXT,
    sku             TEXT,
    title           TEXT,
    qty             INTEGER,
    cost_basis      NUMERIC(10,2),
    -- Card identification (catalog-side)
    tcgplayer_id    INTEGER,
    company         TEXT,                           -- PSA / BGS / CGC / SGC / ACE / TAG
    grade           TEXT,                           -- "10", "9.5", etc.
    -- Pricing snapshot
    old_price       NUMERIC(10,2),                  -- Shopify price before this run
    new_price       NUMERIC(10,2),                  -- Shopify price after (NULL if unchanged)
    suggested_price NUMERIC(10,2),                  -- What we'd set if we were applying (for flagged)
    median          NUMERIC(10,2),                  -- live comp median used as the target
    low_comp        NUMERIC(10,2),
    high_comp       NUMERIC(10,2),
    comps_count     INTEGER,
    delta_pct       NUMERIC(7,2),                   -- (old - median) / median * 100
    trend_7d        NUMERIC(6,2),
    -- Decision
    action          TEXT NOT NULL,                  -- adjusted | flag_overpriced | flag_underpriced | ok | skip | error
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_slab_runs_run_id     ON slab_price_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_slab_runs_action     ON slab_price_runs(action);
CREATE INDEX IF NOT EXISTS idx_slab_runs_started    ON slab_price_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_slab_runs_variant    ON slab_price_runs(variant_gid);
