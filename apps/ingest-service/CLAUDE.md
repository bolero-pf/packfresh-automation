# Intake Service (ingest-service/)
> This is the **intake** service despite the directory name. See root CLAUDE.md.

## Key Files
- **app.py** — Flask routes and API endpoints. `/api/intake/sessions` is the main session listing endpoint (~line 589)
- **intake.py** — Business logic for sessions (create, list, update offers, add items)
- **ingestion_app.py** — Ingestion-side logic (push items, partial ingestion)
- **schema.sql** — DB schema
- **templates/intake_dashboard.html** — Main dashboard UI (single-page app with tabs)

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

## Pricing: ALWAYS Scrydex-first, PPT fallback
PPT graded data is unreliable (often 3× off from market). Scrydex has holes (Japanese,
Scrydex-only cards) so PPT stays as a fallback — **never** as the primary source.

- **Raw per-condition:** `PriceCache.get_card_by_tcgplayer_id(tcg_id)` →
  `ScrydexClient.extract_condition_price(card_data, condition, variant=...)`. PPT fallback
  only on cache miss.
- **Graded per-grade:** `get_live_graded_comps(tcg_id, company, grade, db, ...)` from
  `shared/graded_pricing.py`. PPT fallback via `PriceProvider.get_graded_price()` only on miss.
- **Sort/entry note:** `add_single_raw_item` explodes qty>1 into N qty=1 rows with
  staggered `created_at` so a manually-entered stack keeps its physical order through
  intake → ingest → routing. `get_session_items` sorts by `is_mapped ASC, created_at ASC`
  (unmapped floats up, then entry order). **Do not** re-add alphabetical/price sort to
  the backend default — those are frontend opt-ins.
- **Self-check:** grep any pricing diff for `ppt_client.get_card_by_tcgplayer_id` —
  if it's not preceded by a `PriceCache` / `get_live_graded_comps` call in the same
  function, the change is wrong. Rewrite before committing.
