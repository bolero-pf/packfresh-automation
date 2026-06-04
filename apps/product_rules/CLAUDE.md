# Product Rules (product_rules/)
> Tag-driven product rule engine (rules.pack-fresh.com)

## Purpose
Replaces Timesact (pre-orders) and Avada Order Limits (qty caps). Operators tag products in Shopify Admin; this service materializes those tags into product/customer metafields that the Phase 2 theme app extension and Phase 3 checkout-validation Function read.

## Key Files
- **app.py** — Flask app: webhooks, dashboard, override CRUD, release endpoint
- **service.py** — Tag parsing, metafield sync, purchase log update, HMAC verification
- **migrate_product_rules.py** — Creates `preorder_overrides` + `product_rule_state` tables

## Tag Conventions (regex-parsed)
- `limit-N` — max N per order (matches existing Avada convention)
- `limit-N-per-day` / `limit-N-per-week` / `limit-N-per-month` — windowed
- `limit-N-all-time` — lifetime cap per customer
- `preorder-YYYY-MM-DD` — pre-order until that date

Scope is always per-customer-ID. Plus checkout always carries a customer; guests skip.

## Metafields Written
- Product `custom.qty_limit_rule` (json) — `{max_qty, window_unit, window_count, scope}`
- Product `custom.preorder_rule`  (json) — `{street_date, display_name, button_text, pdp_message, cart_message}`
- Customer `custom.purchase_log`  (json) — `{shopify_product_id: ["YYYY-MM-DD", ...]}`, trimmed to last 365 days

## Webhooks
- `POST /webhooks/products/create`
- `POST /webhooks/products/update`
- `POST /webhooks/orders/create`

All three verify Shopify's `X-Shopify-Hmac-Sha256` against `SHOPIFY_WEBHOOK_SECRET`. Register in Shopify Admin → Settings → Notifications, point at `https://rules.pack-fresh.com/webhooks/...`, format JSON.

## Pre-Order + Screening Integration
When `preorder-YYYY-MM-DD` is seen, the webhook also ensures the bare `pre-order` tag is present (idempotent) so `/screening/`'s no-combine + skip-signature logic keeps working. Both tags are stripped at release time.

## Release Cron
`POST /release` — clears expired `preorder-*` tags from every tagged product, also strips `pre-order`. Triggered daily by Shopify Flow (`X-Flow-Secret: $VIP_FLOW_SECRET`) or via the dashboard's "Release Now" button (owner only).

## DB
- `preorder_overrides` (tag PK, display_name, button_text, pdp_message, cart_message)
- `product_rule_state` (shopify_product_id PK, rule_tags TEXT[], last_synced_at) — webhook-maintained mirror; drives dashboard counts and release lookups without re-listing the Shopify catalog

## Env
- `DATABASE_URL`, `SHOPIFY_TOKEN`, `SHOPIFY_STORE`, `ADMIN_JWT_SECRET` (shared)
- `SHOPIFY_WEBHOOK_SECRET` — new, set in Shopify Admin webhook config and Railway
- `VIP_FLOW_SECRET` — shared with screening/vip, used for `/release` Flow trigger

## What's NOT here yet
- Theme app extension (Phase 2) — reads the product metafields, swaps button, injects messaging
- Shopify Function (Phase 3) — reads product + customer metafields at checkout, hard-rejects over-limit
- Cutover from Timesact + Avada (Phase 4) — done once Phase 2 + 3 verified
