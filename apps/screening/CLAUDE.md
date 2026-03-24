# Screening Service (screening/)
> Order fraud detection + review console (screening.pack-fresh.com)

## Key Files
- **app.py** — Flask app: console UI (verification queue, combine shipping), webhook routes via blueprint
- **service.py** — All screening logic (940 lines): abuse detection, verification, combine, signature, fraud
- **routes.py** — 5 webhook endpoints from Shopify Flows

## Console UI (screening.pack-fresh.com/)
Two tabs:

### Verification Queue
- Orders with hold-for-review tag needing identity/fraud verification
- Shows: order #, customer, email, amount, check type, order note, items
- **Verify & Release**: clears tags, releases fulfillment holds, clears Klaviyo flags
- **Cancel & Refund**: full refund + restock + notify customer + cleanup tags

### Combine Shipping
- Orders grouped by customer that ship together
- Combined packing list across all orders in group
- Links to Shopify admin for each order (buy labels)
- **Release All**: releases holds on all grouped orders

## Webhook Endpoints
| Endpoint | Trigger | Checks |
|----------|---------|--------|
| POST /screening/order_created | First-time orders | FIRSTTIME5 abuse |
| POST /screening/order_combine | Every order | Verification, spend spike, combine, signature |
| POST /screening/fraud_risk | Medium/high fraud | Hold or auto-cancel |
| POST /screening/order_cancelled | Cancelled w/ FIRSTTIME5 tag | Abuse confirmation |
| POST /screening/order_fulfilled | Fulfilled w/ hold tag | Tag/hold cleanup |

## Auth
- JWT cookie (owner + manager) for console UI
- Webhook endpoints use X-Flow-Secret header
