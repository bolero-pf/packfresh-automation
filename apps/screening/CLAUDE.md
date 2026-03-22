# Screening Service (screening/)
> Order fraud detection + verification system (screening.pack-fresh.com)

## Key Files
- **app.py** — Flask app entry point
- **service.py** — All screening logic (940 lines): abuse detection, verification, combine, signature, fraud
- **routes.py** — 5 webhook endpoints from Shopify Flows

## Webhook Endpoints
| Endpoint | Trigger | Checks |
|----------|---------|--------|
| POST /screening/order_created | First-time orders | FIRSTTIME5 abuse |
| POST /screening/order_combine | Every order | Verification, spend spike, combine, signature |
| POST /screening/fraud_risk | Medium/high fraud | Hold or auto-cancel |
| POST /screening/order_cancelled | Cancelled w/ FIRSTTIME5 tag | Abuse confirmation |
| POST /screening/order_fulfilled | Fulfilled w/ hold tag | Tag/hold cleanup |

## Dependencies
- `shared/shopify_graphql.py` — Shopify Admin GraphQL client
- `shared/klaviyo.py` — Klaviyo profile upsert
- `shared/webhook_verify.py` — X-Flow-Secret validation

## Key Patterns
- Stateless — no database, all state in Shopify (tags, notes, holds, metafields)
- All Shopify mutations via GraphQL (not REST)
- Klaviyo properties trigger email flows
- Pre-orders skip combine + signature checks
- Multi-violation priority: $1000 verification > $700 > medium fraud (single email)
