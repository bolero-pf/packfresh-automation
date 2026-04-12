# Kiosk — Raw Card Browser (kiosk/)
> Customer-facing card browsing + hold/checkout system (kiosk.pack-fresh.com)

## Two Cohorts
1. **Guests** (in-store) — browse + hold request → staff pulls → pay at register
2. **Champions** (VIP3, remote) — browse + checkout via Shopify Storefront API → native Shopify checkout

## Key Files
- **app.py** — Flask routes: browse API, guest holds, Champion identify/checkout, order webhook, cleanup cron
- **templates/index.html** — Single-page mobile-first app (grid browse, detail overlay, cart with guest/Champion modes)
- **db.py** — Database connection pool (from shared/)

## Routes
- `/api/browse` — aggregated card search with filters
- `/api/sets`, `/api/eras` — filter metadata
- `/api/card` — card detail (individual copies)
- `/api/hold` — guest hold request (name + phone)
- `/api/champion/identify` — email → Shopify customer lookup → VIP3 check
- `/api/checkout` — Champion checkout: create hold + Shopify products + Storefront API cart → checkout URL
- `/api/webhooks/order-paid` — Shopify orders/create webhook → mark hold as paid
- `/api/cleanup/abandoned` — expire unpaid Champion holds (cron, every 10 min)

## Champion Checkout Flow
1. Champion enters email → `/api/champion/identify` verifies VIP3 tag via Shopify GraphQL
2. Champion clicks "Checkout on Shopify" → `/api/checkout`:
   - Creates hold (locks cards via `current_hold_id`)
   - Creates real ACTIVE Shopify products (published ONLY to "Kiosk" headless channel, invisible on Online Store)
   - Creates Storefront API cart with variant GIDs → returns `checkoutUrl`
   - Applies `CHAMPION_RAW_FREESHIP` discount code if cart total >= threshold
3. Customer redirected to Shopify native checkout → logs in → pays
4. Shopify `orders/create` webhook → `/api/webhooks/order-paid` marks hold completed
5. Cleanup cron deletes unpurchased products + releases holds after 30 min

## Database Tables (read-only)
- `raw_cards` — individual physical cards (state, condition, price, bin, hold)

## Database Tables (write)
- `holds` — hold requests (customer_name, status, cohort, customer_email, checkout_url, checkout_status)
- `hold_items` — items in a hold (raw_card_id, barcode, status, shopify_product_id, shopify_variant_id)

## Environment Variables
- `SHOPIFY_STORE`, `SHOPIFY_TOKEN` — Admin API (product creation, customer lookup)
- `SHOPIFY_STOREFRONT_TOKEN` — Storefront API (cart creation)
- `KIOSK_PUBLICATION_ID` — GID of the Kiosk headless channel publication
- `SHOPIFY_WEBHOOK_SECRET` — HMAC verification for order webhooks
- `KIOSK_FREE_SHIP_THRESHOLD` — cart total for free shipping (default $200)
- `KIOSK_FREE_SHIP_CODE` — Shopify discount code for free shipping
- `CLEANUP_SECRET` — bearer token for cleanup cron endpoint

## Key Patterns
- Delegated click handlers for condition rows + cart remove (data attributes, no inline onclick with user data)
- `esc()` escapes `& < > " '` to prevent XSS in HTML attribute contexts
- Cards shown only if `state = 'STORED'` and `current_hold_id IS NULL`
- 24 cards per page, server-side pagination
- Guest holds: 2 hours to READY, Champion holds: 30 minutes to pay
- Champion state persisted in localStorage (`pf_champion`)
