# Ingest Service (ingestion/)
> This is the **ingest/data ingestion** service (ingest.pack-fresh.com) despite the directory name suggesting otherwise. See root CLAUDE.md.

## Key Files
- **app.py** — Flask routes: session list, session detail, item actions (damage, relink, qty, delete, break down), push to Shopify. Registers shared/breakdown_routes.py blueprint.
- **ingest.py** — Business logic: session queries, item manipulation, breakdown execution (`break_down_item`, `split_then_break_down`), `get_breakdown_summary_for_items()` with JIT price refresh
- **templates/ingest_dashboard.html** — Single-page app with two tabs: "Ready to Ingest" (pending) and "Completed" (ingested)
- DB via shared/db.py (no local db.py)

## Session Flow
```
received → partially_ingested → ingested
```
- Sessions arrive from intake with status `received`
- Staff reviews items, approves them (checkbox), breaks down sealed products
- "Push Live" sends approved items to Shopify → `partially_ingested` if some remain, `ingested` when all done
- "Force Mark Ingested" closes without pushing

## Queue Structure
- **Pending tab**: received + partially_ingested sessions as big clickable cards
- **Completed tab**: ingested sessions as compact table rows with date filter (7/14/30/90 days)

## Breakdown Integration
- Recipe CRUD + batch summaries via shared/breakdown_logic.py; API routes via shared/breakdown_routes.py blueprint
- `get_breakdown_summary_for_items()` returns market + store breakdown values with deep values (store-based)
- JIT refreshes stale component market prices from PPT API (>4 hour TTL)
- `break_down_item()` creates children with `item_status = 'good'` and `parent_item_id` (execution logic stays in ingest.py)
- Children CAN be broken down again if they have recipes (nested breakdown supported)
- Parent gets `item_status = 'broken_down'` (blocks re-breakdown of same item)

## Key Patterns
- PPT client available as `ppt` global (initialized from `PPT_API_KEY` env var)
- Uses `shared/ppt_client.py`, `shared/breakdown_helpers.py`
- Breakdown recipes shared with intake and inventory via same DB tables
