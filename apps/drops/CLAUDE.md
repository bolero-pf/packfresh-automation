# Drop Planner (drops/)
> Drop scheduling + management (drops.pack-fresh.com)

## Key Files
- **app.py** — Flask app: UI dashboard + all API endpoints
- **service.py** — Shopify operations: product search, price, tags, channels, metafields
- **migrate_drops.py** — Extend drop_events table with planner columns
- **db.py** — Database connection pool

## Drop Types
- **weekly**: set price, add `unavailable-{month}-{day}` + `drop` tags, remove from channels
- **vip**: everything above + add `vip-drop` tag (permanent) + set `custom.vip_price_cents` metafield

## Flow
1. Search for product → select it
2. Set drop date, price, type (weekly/vip), qty offered
3. System: sets Shopify price, adds tags, removes from non-Online channels, records in drop_events
4. At 11 AM on drop date: `/release` removes unavailable + drop tags, publishes to all channels
5. `vip-drop` tag is NEVER auto-removed (controls VIP early access in Liquid theme)

## Endpoints
- `POST /api/drops` — create a new drop
- `POST /release` — release today's scheduled drops (called by Flow at 11 AM CST)
- `GET /api/drops` — list drops (filter by status)
- `POST /api/drops/backfill` — record a past drop for analytics exclusion
- `GET /api/candidates` — high-inventory items sorted by qty (deal finder)
- `GET /api/search` — search Shopify products by name
- `DELETE /api/drops/<id>` — remove a drop record

## Analytics Integration
- `drop_events` table is checked by analytics service during velocity computation
- Sales on drop dates are excluded from velocity calculations
- Backfill past drops to clean historical analytics data

## VIP Price Metafield
- `custom.vip_price_cents` — integer, price in cents (e.g., 4599 = $45.99)
- Displayed in Shopify Liquid: yellow for VIP members, locked with upsell for non-VIP
- VIP3: 24h early access, VIP2: 1h early access, VIP1: same as drop time
