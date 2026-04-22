-- ── graded_comps_cache: persistent cache for live eBay comps ─────────
-- One row per (scrydex_id, variant, days) tuple. Payload is the exact
-- shape returned by shared/graded_pricing.get_all_graded_comps:
--   {"PSA": {"10": {"price":..., "confidence":..., "count":..., "method":...}, ...}, ...}
--
-- Read path checks fetched_at > NOW() - INTERVAL '24 hours' as TTL.
-- Expired rows are overwritten on next fetch (no background cleanup needed).
--
-- Problem it solves: the ingest routing page enriches every unique card
-- in a session with live eBay comps (1+ Scrydex calls each, several
-- paginated pages for popular cards). A 439-card session generated
-- ~1500 API calls per page load, and the in-memory `_enrich_cache` was
-- lost on every Railway redeploy — so any deploy meant minutes of
-- re-fetching to restore the routing UI.
--
-- The table is also auto-created via `CREATE TABLE IF NOT EXISTS` inside
-- shared/graded_pricing.py (_ensure_graded_cache_table) so it appears on
-- first use without a separate migration step. This file is the schema
-- source of truth.

CREATE TABLE IF NOT EXISTS graded_comps_cache (
    scrydex_id   TEXT NOT NULL,
    variant      TEXT NOT NULL DEFAULT '',
    days         INTEGER NOT NULL DEFAULT 90,
    comps_data   JSONB NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (scrydex_id, variant, days)
);

CREATE INDEX IF NOT EXISTS idx_graded_comps_fetched_at
    ON graded_comps_cache(fetched_at);
