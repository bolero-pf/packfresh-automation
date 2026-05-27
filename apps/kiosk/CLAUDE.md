# Kiosk — Customer Browser (kiosk/)
> Customer-facing browse + hold system (kiosk.pack-fresh.com). Three catalogs:
> Raw cards (always), plus Sealed and Slabs in in-store mode.

## Two Cohorts
1. **Guests** (in-store iPad) — browse Raw / Sealed / Slabs → hold request → staff pulls → pay at register/POS
2. **VIPs** (VIP1 / VIP2 / VIP3, remote) — browse Raw only → checkout via Shopify Storefront API → native Shopify checkout. Internally still called the "champion" cohort (DB `cohort='champion'`, header `X-Champion-Email`); the customer-facing label is tier-aware (`Champion` for VIP3, `VIP1`/`VIP2` otherwise). Tier check lives in `VIP_TIERS` / `_resolve_vip_tier` in `app.py`.

## Mode detection (instore vs champion)
The cohort is determined per-request in `_resolve_kiosk_mode` (sets `g.kiosk_mode`):
- **`instore`** — request carries the `pf_kiosk_device` HttpOnly cookie that points to a non-revoked row in `kiosk_devices`. Set once via `/activate?token=…` during iPad setup. Legacy `?key=KIOSK_ACCESS_KEY` URL param is still accepted as a fallback.
- **`champion`** — request carries `X-Champion-Email` header set by the frontend after VIP verification (any of VIP1/VIP2/VIP3).
- **`None`** — anonymous; locked out except for the gate UI and `/api/champion/identify`.

`_allowed_kinds(mode)` returns `['raw','sealed','slab']` for instore and `['raw']` for champion. Sealed/slab endpoints 403 in champion mode (defense in depth).

## Activation flow
1. Manager hits `POST /api/admin/mint-activation` (JWT-gated via shared/auth) and gets a one-time `activation_url`.
2. Staff opens the URL in Kiosk Pro on the iPad once → `/activate` consumes the token, mints a device_id, and sets `pf_kiosk_device` (HttpOnly, Secure, SameSite=Strict, 10y).
3. Subsequent visits to `/` flow with no token; the cookie alone proves in-store identity.
4. Devices can be revoked from `POST /api/admin/devices/<id>/revoke`.

## Key Files
- **app.py** — Flask routes: browse API, guest holds, Champion identify/checkout, order webhook, cleanup cron
- **templates/index.html** — Single-page mobile-first app (grid browse, detail overlay, cart with guest/Champion modes)
- **db.py** — Database connection pool (from shared/)

## Routes
### Customer (kiosk UI)
- `/` — single-page app
- `/activate?token=…` — one-time device activation (sets cookie)
- `/api/mode` — returns `{mode, show_kinds}` for the frontend bootstrap
- `/api/browse`, `/api/sets`, `/api/eras`, `/api/games`, `/api/filter-meta` — Raw catalog (existing)
- `/api/card` — Raw card detail (individual copies)
- `/api/products?kind=sealed|slab` — Sealed/Slab catalog from `inventory_product_cache` (in-store only)
- `/api/hold` — mixed Raw / Sealed / Slab hold creation (one customer, one cart)
- `/api/champion/identify` — email → Shopify customer lookup → VIP tier check (VIP1/VIP2/VIP3); returns `tier` so the frontend can render a tier-aware label
- `/api/checkout` — Champion checkout (Raw only): hold + temp Shopify products + Storefront cart

### Staff
- `/staff/pulls` — scan UI (REQUESTED → PULLED → RETURNED/SOLD/UNRESOLVED). JWT-gated.
- `/api/staff/scan-out` — barcode/SKU → mark PULLED
- `/api/staff/scan-return` — sealed/slab only → mark RETURNED
- `/api/staff/pulls` — JSON for the staff page

### Admin
- `/api/admin/mint-activation` — manager+; returns activation URL
- `/api/admin/devices` — list devices
- `/api/admin/devices/<id>/revoke` — revoke a device cookie

### Webhooks / cron
- `/api/webhooks/order-paid` — matches Champion holds via `variant_id` AND in-store sealed/slab pulls via `sku`
- `/api/cleanup/abandoned` — Champion expiry (existing); background loop also handles in-store unclaimed REQUESTED + PULLED→UNRESOLVED reconciliation

## Champion Checkout Flow
1. VIP enters email → `/api/champion/identify` verifies any of VIP1/VIP2/VIP3 tag via Shopify GraphQL
2. VIP clicks "Checkout on Shopify" → `/api/checkout` (re-verifies any VIP tier):
   - Creates hold (locks cards via `current_hold_id`)
   - Creates real ACTIVE Shopify products (published ONLY to "Kiosk" headless channel, invisible on Online Store)
   - Creates Storefront API cart with variant GIDs → returns `checkoutUrl`
   - Applies `CHAMPION_RAW_FREESHIP` discount code if cart total >= threshold
3. Customer redirected to Shopify native checkout → logs in → pays
4. Shopify `orders/create` webhook → `/api/webhooks/order-paid` marks hold completed
5. Cleanup cron deletes unpurchased products + releases holds after 30 min

## Database Tables (read-only)
- `raw_cards` — individual physical cards (state, condition, price, bin, hold)
- `inventory_product_cache` — Shopify product mirror (read-only here; refreshed by inventory service via `shared/cache_manager.py`). Sealed/Slab catalog reads from this; tag CSV match `LOWER(',' || tags || ',') LIKE '%,sealed,%'` (or `slab`). `sku` column populated by the cache refresh and used as the scan key.

## Database Tables (write)
- `holds` — hold requests (customer_name, status, cohort, customer_email, checkout_url, checkout_status)
- `hold_items` — items in a hold. Extended for sealed/slab:
  - `item_kind` — `'raw'` (default) | `'sealed'` | `'slab'`
  - `raw_card_id` — nullable (NULL for sealed/slab)
  - `sku`, `title`, `image_url`, `unit_price` — populated for sealed/slab from `inventory_product_cache`
  - `returned_at`, `returned_by` — set by `/api/staff/scan-return`
  - `shopify_order_id` — set by the order-paid webhook for SOLD reconciliation
  - status flow: `REQUESTED → PULLED → SOLD | RETURNED | UNRESOLVED | EXPIRED_UNCLAIMED`
- `kiosk_devices` — one row per activated iPad (device_id, label, activated_at, last_seen_at, revoked_at)
- `kiosk_activation_tokens` — one-time tokens (24h TTL); single-use via `used_at`/`used_by_device_id`

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
- Cards shown only if `state IN ('STORED','DISPLAY')` and `current_hold_id IS NULL` (DISPLAY = front glass + binders, distinguished by `bin_id` / `storage_bins.location_type`)
- 24 cards per page, server-side pagination
- Guest holds: 2 hours to READY, Champion holds: 30 minutes to pay
- Champion state persisted in localStorage (`pf_champion`); device cookie `pf_kiosk_device` is HttpOnly and lives 10y
- Cart key: raw items keyed on (name, set, condition, variant_key, tcg_id/scrydex_id); sealed/slab keyed on (kind, shopify_variant_id)
- Idle reset (in-store mode only): 90s no-touch → "Still there?" warning → 10s → reset cart, search, champion identity. Device cookie preserved. Champions get no idle watchdog — they're remote, not on a shared iPad.
- In-store available_qty for sealed/slab: `shopify_qty − count(active hold_items REQUESTED+PULLED for that variant)` — prevents double-allocation across simultaneous customers
