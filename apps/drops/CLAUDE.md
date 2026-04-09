# Drop Planner (drops/)
> Drop scheduling + management (drops.pack-fresh.com)

## Key Files
- **app.py** — Flask app: UI dashboard + all API endpoints
- **service.py** — Shopify operations: product search, price, tags, channels, metafields
- **migrate_drops.py** — Extend drop_events table with planner columns
- DB via shared/db.py (no local db.py)

## Drop Types
- **weekly**: set price, add `unavailable-{month}-{day}` + `drop` + `weekly deals` + `limit-X` tags, remove from non-Online channels
- **vip**: everything above + add `vip-drop` tag (permanent) + set `custom.vip_price_cents` metafield

## Tag Lifecycle
- **Setup adds**: unavailable-*, drop, weekly deals, limit-X (+ vip-drop for VIP)
- **Release removes**: unavailable-* and drop ONLY
- **Persistent**: weekly deals, limit-X, vip-drop (never auto-removed)

## Flow
1. Search product → set date, price, type, limit
2. System: sets Shopify price, adds tags, removes from non-Online channels, records in drop_events
3. At 11 AM: /release removes unavailable + drop tags, publishes to all channels
4. Analytics service excludes drop dates from velocity calculations

## Analytics Integration
- `drop_events` table checked by analytics during velocity computation
- `NOT EXISTS (SELECT 1 FROM drop_events WHERE variant_id AND date)` excludes drop-day sales
- Backfill past drops to clean historical velocity data

## Auth
- JWT cookie (owner only) for UI
- /release accepts Flow secret for daily trigger
