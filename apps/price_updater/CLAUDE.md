# Price Updater (price_updater/)
> Includes sub-services: vip/ and screening/ (prices.pack-fresh.com)

## Key Files
- **app.py** — Main price sync service (nightly sealed SKU price updates)
- **screening/service.py** — Order screening logic (fraud, verification, combine, signature)
- **screening/routes.py** — Webhook endpoints for Shopify order events
- **vip/service.py** — VIP segment management, `shopify_gql()` helper

## Screening Sub-service (screening/)
Processes 5 Shopify webhooks:

| Webhook | Checks |
|---------|--------|
| order_created (first-time) | FIRSTTIME5 discount abuse |
| order_combine (every order) | Cumulative verification, Spend spike, Combine shipping, Signature |
| fraud_risk (medium/high) | Shopify fraud risk → hold or auto-cancel |
| order_cancelled | Clear verification flags, log abuse |
| order_fulfilled | Remove screening tags, release holds |

### Screening Thresholds
- **Tier 1 verification**: $700+ cumulative, no delivered orders → photo ID
- **Tier 2 verification**: $1000+ cumulative → photo ID + selfie
- **Spend spike**: $1000+ order, max previous < 20% of current
- **Signature**: $500+ individual or combined shipment
- **High fraud**: Auto-cancel + refund
- **Medium fraud**: Hold for verification

### Pre-order Handling
Pre-orders (tagged `pre-order`/`preorder`/`pre_order`) skip: combine shipping, signature check.
NOT skipped: verification, spend spike, fraud.

### Multi-violation Priority
Only one Klaviyo verification email fires. Priority: $1000 verification > $700 verification > medium fraud.
Tags and holds still apply for all violations.

### Order Notes (concise format)
- "Signature Required"
- "Waiting on ID Verification ($Amount)"
- "Combine Order (#OrderNumbers)"
- "Medium Fraud Verification"
- "High Fraud Risk — Auto-cancelled"

## VIP Sub-service (vip/)
- All Shopify GraphQL goes through `shopify_gql()` in vip/service.py
- Klaviyo integration in integrations/klaviyo.py

## Key Patterns
- Screening uses Klaviyo profile properties to trigger email flows
- Tags on orders and customers track screening state
- Fulfillment holds prevent shipping until review complete
