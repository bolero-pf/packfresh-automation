# Screening Service (screening/)
> Order fraud detection + review console (screening.pack-fresh.com)

## Key Files
- **app.py** — Flask app: console UI (verification queue, combine shipping, customer notes), API endpoints, DB init
- **service.py** — All screening logic: abuse detection, verification, combine, signature, fraud, customer notes
- **routes.py** — 5 webhook endpoints from Shopify Flows

## Console UI (screening.pack-fresh.com/)
Three tabs:

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

### Customer Notes
- Per-customer notes/rules that auto-apply to incoming orders
- **Note type**: appends text to Shopify order note (e.g., address warnings)
- **Hold type**: holds every order from customer + appends note (shows as "Customer Hold" in verification queue)
- Stored in `customer_notes` DB table, matched by email
- Applied in `screen_every_order()` before other checks

## Database
- Uses shared/db.py (PostgreSQL via DATABASE_URL)
- Table: `customer_notes` (auto-created on startup)

## Webhook Endpoints
| Endpoint | Trigger | Checks |
|----------|---------|--------|
| POST /screening/order_created | First-time orders | FIRSTTIME5 abuse |
| POST /screening/order_combine | Every order | Customer notes, verification, spend spike, combine, signature |
| POST /screening/fraud_risk | Medium/high fraud | Hold or auto-cancel |
| POST /screening/order_cancelled | Cancelled w/ FIRSTTIME5 tag | Abuse confirmation |
| POST /screening/order_fulfilled | Fulfilled w/ hold tag | Tag/hold cleanup |

## Auth
- JWT cookie (owner + manager) for console UI
- Webhook endpoints use X-Flow-Secret header
