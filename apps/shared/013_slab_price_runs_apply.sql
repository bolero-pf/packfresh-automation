-- Per-row apply state for slab_price_runs.
-- Slabs are now flag-only on the cron. Sean reviews flagged rows in the
-- dashboard and clicks Apply on the ones to push to Shopify. We need to
-- track which rows have been applied (or dismissed) so they don't keep
-- showing as pending across page reloads / re-runs.

ALTER TABLE slab_price_runs
    ADD COLUMN IF NOT EXISTS apply_status   TEXT DEFAULT 'pending',  -- pending / applied / dismissed / stale
    ADD COLUMN IF NOT EXISTS applied_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS applied_price  NUMERIC(10,2);

CREATE INDEX IF NOT EXISTS idx_slab_runs_apply_status
    ON slab_price_runs(apply_status);
