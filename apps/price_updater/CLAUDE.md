# Price Updater (price_updater/)
> Nightly sealed SKU price sync (prices.pack-fresh.com)

## Key Files
- **review_dashboard.py** — Main Flask app: price review dashboard, CSV management, nightly run trigger
- **dailyrunner.py** — The actual nightly price sync script (launched via /price_update endpoint)
- **Dockerfile** — Includes Chromium for headless scraping
- **inventory/routes.py** — Inventory sub-routes (still active)

## Auth
- JWT cookie via `register_auth_hooks()` from shared/auth.py — owner role required
- Public prefixes: /static, /pf-static, /reddit-feed.csv (reddit feed has its own basic auth)
- JWT-skipped prefixes: /price_update (server-triggered POST)
- No more HTTP Basic Auth fallback for dashboard routes (removed legacy @requires_auth)
- Reddit feed still uses its own basic auth (REDDIT_USER_NAME/REDDIT_USER_PASS)

## Theme
- Uses shared dark theme: pf_theme.css + pf_ui.js (as of 2026-03-27)
- Templates: review.html, runlog.html, index.html, merge_preview.html

## Legacy Subdirectories (pending removal)
- **vip/** — Migrated to apps/vip/ (vip.pack-fresh.com) — blueprints no longer registered
- **screening/** — Migrated to apps/screening/ (screening.pack-fresh.com) — blueprints no longer registered
- **integrations/klaviyo.py** — Migrated to shared/klaviyo.py
- These directories can be deleted
