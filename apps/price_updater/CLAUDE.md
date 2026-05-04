# Price Updater (price_updater/)
> Nightly price sync hub for sealed, raw, and graded slabs (prices.pack-fresh.com)

## Updaters
| Script | Domain | Source of truth | Decision policy |
|---|---|---|---|
| `dailyrunner.py` | sealed (Shopify) | TCGplayer featured-price scrape | always raise; review drops; bulk-approve in dashboard |
| `slab_updater.py` | graded slabs (Shopify) | Scrydex live eBay comps | always raise; flag drops |
| `raw_card_updater.py` | raw cards (`raw_cards.current_price`) | scrydex_price_cache | always raise + small drift; flag drops |

All three follow the same shape now: load `price_auto_block` (skip blocked
items), classify each row, write **every** row to a per-domain `*_price_runs`
table for the dashboard. Auto-applied changes write the new price + set
`apply_status='applied'` inline.

## Key Files
- **review_dashboard.py** — Flask app + all dashboard routes
- **dailyrunner.py** — sealed scrape + DB writer (writes `sealed_price_runs`)
- **slab_updater.py** — graded slabs (writes `slab_price_runs`)
- **raw_card_updater.py** — raw cards (writes `raw_card_price_runs`)
- **slab_backfill.py** — TCG-ID metafield backfill matcher for slabs
- **heal_raw_card_bindings.py** — auto-heal mis-bound raw_cards before manual rebind
- **Dockerfile** — Includes Chromium for the TCGplayer scraper

## Dashboard surface
- `/dashboard/sealed-runs` (+ `/<run_id>`) — per-run apply / dismiss / **bulk-approve all <X% drops** / block
- `/dashboard/slab-runs` (+ `/<run_id>`) — per-row apply / dismiss / block / TCG backfill helper
- `/dashboard/raw-runs` (+ `/<run_id>`) — group-by-print apply / dismiss / block
- `/dashboard/raw-rebind` — manual scrydex rebind for raw_cards the updater can't price.
  Search modal uses shared `pricing.search_cards()` (shared/price_provider.py) — same
  cache→live orchestration as intake's `/api/search/cards` and ingestion's. Returns
  whole-card rows; chip per variant (NM price); click chip → bind every copy of that
  identity to (scrydex_id, variant). To add fields/tokens to the search, edit
  `shared/price_cache.py:search_cards` — don't fork it here.
- `/dashboard/big-movers` — auto-applied rows with |Δ%| ≥ threshold across all 3 latest runs (+ Block per row)
- `/dashboard/price-blocks` — list every active block with Remove
- `/dashboard/runlog` + `/stream-log` — live tail of dailyrunner output

## Block list (`price_auto_block`)
Every updater checks at start of run; matching rows become `action='skip'`,
`reason='auto-block'`. Permanent until removed via `/dashboard/price-blocks`.
- raw → `block_key = scrydex_id` (else `tcg:<tcgplayer_id>`)
- slab → `block_key = variant_gid`
- sealed → `block_key = variant_id` (Shopify numeric, as string)
Helpers in `shared/price_auto_block.py`.

## Auth
- JWT cookie via `register_auth_hooks()` (owner role required)
- Public prefixes: /static, /pf-static, /reddit-feed.csv
- JWT-skipped prefixes: /price_update, /run-raw-updater, /run-scrydex-sync, /run-slab-updater (cron POSTs use X-Flow-Secret)
- Reddit feed has its own basic auth (REDDIT_USER_NAME/REDDIT_USER_PASS)

## Theme
- Shared dark theme: pf_theme.css + pf_ui.js
- Templates: sealed_runs.html + sealed_run_detail.html + slab_runs.html + slab_run_detail.html + raw_runs.html + raw_run_detail.html + raw_rebind.html + big_movers.html + price_blocks.html + runlog.html

## Schema migrations (apply manually to DATABASE_URL)
- `019_sealed_price_runs.sql` — sealed run audit trail
- `020_price_auto_block.sql` — shared block list

## Legacy Subdirectories (pending removal)
- **vip/** — Migrated to apps/vip/ — blueprints no longer registered
- **screening/** — Migrated to apps/screening/ — blueprints no longer registered
- **integrations/klaviyo.py** — Migrated to shared/klaviyo.py
- These directories can be deleted
