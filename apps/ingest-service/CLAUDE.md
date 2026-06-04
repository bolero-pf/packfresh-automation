# Intake Service (ingest-service/)
> This is the **intake** service despite the directory name. See root CLAUDE.md.

## Key Files
- **app.py** — Flask routes and API endpoints
- **intake.py** — Business logic for sessions (create, list, update offers, add items)
- **schema.sql** — DB schema
- **templates/intake_dashboard.html** — Main dashboard UI (single-page app with tabs)

## Re-link cache (product_mappings) — keying invariant
The Collectr→link cache must be keyed on the **Collectr-parsed identity** (what
the parser emits and a re-import reproduces), NEVER the Scrydex display values
the operator picks at link time. `map_item` keys `save_mapping` on the pre-update
`intake_items` row for raw cards for exactly this reason — keying on Scrydex
`setNameEn` ("SWSH09: Brilliant Stars" vs Collectr's "Brilliant Stars") was the
bug that stranded every relinked raw card on re-import.
- `get_cached_link()` is the lookup (returns tcgplayer_id + scrydex_id). Raw is
  two-tier: Tier 1 exact (name+set+number+variance); Tier 2 set-insensitive on
  name+number+variance, resolving only when all candidates point at one card —
  same-number reprints abstain so the operator picks. Sealed = name+type only.
- `get_cached_mapping()` is a back-compat shim returning just tcgplayer_id.
- `product_mappings.scrydex_id` lets JP / Scrydex-only links round-trip.
- `migrate_mapping_scrydex_heal.py` backfills the cache from intake_items history
  (additive/idempotent). Residual re-link misses are Collectr name-suffix drift
  ("(Full Art)", "(Secret)", "ex" vs "EX") — a future name-normalization job.

## Session Status Flow
```
in_progress → offered → accepted → received → partially_ingested → ingested → finalized
                      → rejected
in_progress → cancelled
```

## Dashboard Tabs (intake_dashboard.html)
- **New Intake** — Unified tab: manual entry (sealed + cards in one session), Collectr CSV, Generic CSV, Paste HTML. Creates `session_type: 'mixed'`. Type toggle switches between sealed search and card search (with grading support).
- **Active Sessions** — Shows statuses: in_progress, offered, accepted, partially_ingested
- **Completed** — Shows statuses: received, ingested, finalized
- **Cancelled** — Shows cancelled sessions

## Breakdown Integration
- app.py registers shared/breakdown_routes.py blueprint for breakdown API
- Breakdown recipes managed via shared/breakdown_logic.py, not inline SQL

## Key Patterns
- Session status filtering is driven by the frontend dropdown and default load in `intake_dashboard.html`
- Status badge colors are defined in a `statusColors` JS object in the dashboard template
- `intake.py` has guard clauses that block modifications (offer %, adding items) based on session status
- `list_sessions()` accepts comma-separated status strings for flexible filtering

## Per-session bulk pricing tiers
- `intake_sessions.bulk_tiers JSONB` holds up to 3 ascending `{max, pct}` brackets that override the session % for raw cards (default `[{"max":2,"pct":25}]` preserves the legacy "<$2 → 25%" rule).
- `calc_offer_price(..., bulk_tiers=...)` walks the list ascending by `max` and uses the first matching pct; above the top tier the session pct applies. `_session_bulk_tiers(session)` normalizes/defaults; pass it to every raw-card calc.
- `_recalc_session_item_prices(session_id, base_pct)` is the one Python-side recalc used by both `update_session_percentages` and `accept_offer`. Don't reintroduce inline `CASE WHEN market_price < 2.0` SQL — it can't express variable tiers.
- JS mirror: `_computeOfferBreakdown` (intake_dashboard.js) reads tiers off `window._sessionMeta.bulk_tiers`; the New Intake form + session-detail editor both round-trip through `/api/intake/session/<id>/bulk-tiers`.

## Pricing: ALWAYS Scrydex-first, PPT fallback
PPT graded data is unreliable (often 3× off from market). Scrydex has holes (Japanese,
Scrydex-only cards) so PPT stays as a fallback — **never** as the primary source. All
scalar API returns are USD (JPY rows auto-converted via `SCRYDEX_JPY_USD_RATE`).

- **Raw per-condition:** `pricing.get_raw_condition_price(tcgplayer_id=..., condition=..., variant=...) → Decimal | None`.
  Cache-first, PPT fallback baked in.
- **Graded per-grade:** `get_live_graded_comps(tcg_id, company, grade, db, ...)` from
  `shared/graded_pricing.py` first. On miss, `pricing.get_graded_price(tcgplayer_id=..., company=..., grade=...)`.
- **Card view (variants + graded + images):** `pricing.get_card_view(tcgplayer_id=...)`
  for condition-picker endpoints.
- **Sort/entry note:** `add_single_raw_item` explodes qty>1 into N qty=1 rows with
  staggered `created_at` so a manually-entered stack keeps its physical order through
  intake → ingest → routing. `get_session_items` sorts by `is_mapped ASC, created_at ASC`
  (unmapped floats up, then entry order). **Do not** re-add alphabetical/price sort to
  the backend default — those are frontend opt-ins.
- **Self-check:** grep any pricing diff for `ppt_client.get_card_by_tcgplayer_id` —
  if it's not preceded by a `PriceCache` / `get_live_graded_comps` call in the same
  function, the change is wrong. Rewrite before committing.
