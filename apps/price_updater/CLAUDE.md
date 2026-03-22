# Price Updater (price_updater/)
> Nightly sealed SKU price sync (prices.pack-fresh.com)
> NOTE: VIP and Screening have been extracted to standalone services (apps/vip/, apps/screening/).
> The old vip/ and screening/ subdirectories still exist here but are no longer registered as blueprints.
> They will be deleted once the standalone services are fully verified.

## Key Files
- **review_dashboard.py** — Main Flask app: price review dashboard, CSV management, nightly run trigger
- **dailyrunner.py** — The actual nightly price sync script (launched via /price_update endpoint)
- **Dockerfile** — Includes Chromium for headless scraping
- **inventory/routes.py** — Inventory sub-routes (still active)

## Auth
- JWT cookie from admin portal (shared/auth.py) — primary auth
- HTTP Basic Auth as fallback (DASHBOARD_USER/DASHBOARD_PASS)
- /price_update endpoint now lives on this service directly (was under /vip/ before)

## Legacy Subdirectories (pending removal)
- **vip/** — Migrated to apps/vip/ (vip.pack-fresh.com)
- **screening/** — Migrated to apps/screening/ (screening.pack-fresh.com)
- **integrations/klaviyo.py** — Migrated to shared/klaviyo.py
- These are still present but blueprints are still registered for backward compatibility
- Once standalone services are verified, these directories will be deleted
