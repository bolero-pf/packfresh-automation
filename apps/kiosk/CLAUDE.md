# Kiosk — Raw Card Browser (kiosk/)
> Customer-facing in-store card browsing + hold request system (kiosk.pack-fresh.com)

## Key Files
- **app.py** — Flask routes: `/api/browse` (aggregated cards with filters), `/api/sets`, `/api/eras`, `/api/card` (detail), `/api/hold` (create hold)
- **templates/index.html** — Single-page mobile-first app (grid browse, detail overlay, cart/checkout overlay)
- **db.py** — Database connection pool

## Features
- Browse cards aggregated by (card_name, set_name, tcgplayer_id) with per-condition qty
- **Filters**: condition pills (NM/LP/MP/HP/DMG toggle), set dropdown, era dropdown, price min/max
- **Sort**: Name A-Z, Price Low-High, Price High-Low, Newest
- Collapsible filter drawer (mobile-first: pills + sort always visible, rest in drawer)
- Card detail with condition rows and qty adjuster
- Cart (max 20 cards) with localStorage persistence
- Hold submission → staff pulls cards via card_manager

## Database Tables (read-only)
- `raw_cards` — individual physical cards (state, condition, price, bin, hold)

## Database Tables (write)
- `holds` — hold requests (customer_name, phone, status, expires_at)
- `hold_items` — individual items in a hold (raw_card_id, barcode, status)

## Era Mapping
Eras derived from set name keywords: Scarlet & Violet, Sword & Shield, Sun & Moon, XY, Classic.
Defined in `ERA_KEYWORDS` dict in app.py. `/api/eras` returns available eras with set counts.

## Key Patterns
- Themed confirm dialog (not browser confirm) for destructive actions
- Cards shown only if `state = 'STORED'` and `current_hold_id IS NULL`
- 24 cards per page, server-side pagination
- Hold expiry: 2 hours after READY status
