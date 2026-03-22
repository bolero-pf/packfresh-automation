# VIP Service (vip/)
> VIP tier management system (vip.pack-fresh.com)

## Key Files
- **app.py** — Flask app entry point
- **service.py** — VIP logic (~950 lines): rolling spend, tier calc, Shopify metafields, Klaviyo sync
- **routes.py** — Webhook endpoints + admin API (~16K)
- **update_tags.py** — CLI tool for bulk retagging

## VIP Tiers
| Tier | Min 90-day Spend |
|------|-----------------|
| VIP0 | $0 |
| VIP1 | $500 |
| VIP2 | $1,250 |
| VIP3 | $2,500 |

## Webhook Endpoints
- POST /vip/order_paid — Recalculate tier on payment
- POST /vip/refund_created — Recalculate tier on refund

## Admin Endpoints
- POST /vip/backfill — Batch tier backfill
- POST /vip/sweep_vips — Bulk tier update
- POST /vip/retag_only — Tag normalization
- GET /vip/state — Customer state query

## Dependencies
- `shared/shopify_graphql.py` — Shopify Admin GraphQL client (with local debug/dry-run wrapper)
- `shared/klaviyo.py` — Klaviyo profile upsert for tier transitions
- `shared/webhook_verify.py` — X-Flow-Secret validation

## Key Patterns
- Rolling 90-day spend calculated from Shopify order history
- Tier stored in customer metafields (custom.loyalty_vip_tier)
- Lock window prevents downgrades during grace period
- Shopify tags set for discount eligibility (VIP1, VIP2, VIP3)
- VIP_DRY_RUN mode skips all Shopify mutations
