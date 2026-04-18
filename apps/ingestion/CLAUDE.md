# Ingest Service (ingestion/)
> This is the **ingest/data ingestion** service (ingest.pack-fresh.com) despite the directory name suggesting otherwise. See root CLAUDE.md.

## Key Files
- **app.py** — Flask routes: session list, session detail, item actions (damage, relink, qty, delete, break down), verify endpoints, stage transitions, push to Shopify. Registers shared/breakdown_routes.py blueprint.
- **ingest.py** — Business logic: session queries, item manipulation, verification (verify_item_here, verify_item_missing, complete_verification, complete_breakdown), breakdown execution (`break_down_item`, `split_then_break_down`), `get_breakdown_summary_for_items()` with JIT price refresh
- **templates/ingest_dashboard.html** — Single-page app with queue tabs (Pending/Completed) and 3-stage session detail (Verify → Breakdown → Push)
- DB via shared/db.py (no local db.py)

## Session Flow (4-Stage)
```
received → verified → breakdown_complete → [partially_ingested →] ingested
```
- Sessions arrive from Deals (intake) with status `received`
- **Stage 1 — Verify**: Staff confirms each item is here/missing/damaged. Persisted via `verified_at` column on intake_items. Partial qty supported (split into good + missing portions).
- **Stage 2 — Breakdown**: Staff decides what to break down. Uses shared breakdown modal. Damaged items can be broken down to recover margin.
- **Stage 3 — Push**: Push to Shopify. Dry run preview, partial push supported.
- Stages are navigable — can go back to any completed stage to make changes
- "Force Mark Ingested" closes without pushing

## Queue Structure
- **Pending tab**: received/verified/breakdown_complete/partially_ingested sessions as clickable cards
- **Completed tab**: ingested sessions as compact table rows with date filter (7/14/30/90 days)

## Grouping & Sorting
All 3 stages support:
- **Group by**: None, Product Type (from store tags or name parsing), Set
- **Sort by**: Default, Alphabetical, Price High→Low, Price Low→High

## Breakdown Integration
- Recipe CRUD + batch summaries via shared/breakdown_logic.py; API routes via shared/breakdown_routes.py blueprint
- `get_breakdown_summary_for_items()` returns market + store breakdown values with deep values (store-based)
- JIT refreshes stale component market prices from PPT API (>4 hour TTL)
- `break_down_item()` creates children with `item_status = 'good'` and `parent_item_id` (execution logic stays in ingest.py)
- Children CAN be broken down again if they have recipes (nested breakdown supported)
- Parent gets `item_status = 'broken_down'` (blocks re-breakdown of same item)
- **Breakdown modal shows store price as primary, market as secondary/fallback**

## Key Patterns
- PPT client available as `ppt` global (initialized from `PPT_API_KEY` env var)
- Uses `shared/ppt_client.py`, `shared/breakdown_helpers.py`
- Breakdown recipes shared with intake and inventory via same DB tables
- All state persisted to DB — no client-side session state (replaces old _approvedItems JS Set)

## Pricing: ALWAYS Scrydex-first, PPT fallback
PPT is unreliable (graded data often 3× off). Scrydex has holes (Japanese, Scrydex-only),
so PPT stays as a fallback — **never** as the primary source.

- **Raw per-condition price:** `PriceCache.get_card_by_tcgplayer_id(tcg_id)` →
  `ScrydexClient.extract_condition_price(card_data, condition, variant=...)`. On
  miss, fall back to `ppt.get_card_by_tcgplayer_id()` + `PriceProvider.extract_condition_price()`.
- **Graded per-grade price:** `get_live_graded_comps(tcg_id, company, grade, db, ...)` from
  `shared/graded_pricing.py` (live eBay comps → cache inside). On miss, `PriceProvider.get_graded_price(card_data, company, grade)` from PPT.
- Existing examples to copy from: `update_item_grade` (graded), `update_item_condition`
  (raw) in `ingest.py`.
- **Self-check:** grep any pricing diff for `ppt_client.get_card_by_tcgplayer_id` —
  if it's not preceded by a `PriceCache` / `get_live_graded_comps` call in the same
  function, the change is wrong. Rewrite before committing.
