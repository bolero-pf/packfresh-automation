# VIP Service (vip/)
> VIP tier management + console (vip.pack-fresh.com)

## Key Files
- **app.py** — Flask app: console UI (customer list, detail view, tier management), webhook routes via blueprint
- **service.py** — VIP logic (~950 lines): rolling spend, tier calc, Shopify metafields, Klaviyo sync
- **routes.py** — Webhook endpoints + admin API (~16K)
- **update_tags.py** — CLI tool for bulk retagging

## Console UI (vip.pack-fresh.com/)
- **Customer list**: filterable by tier, searchable, sortable (spend, gap to next, lock expiry, orders)
- **Customer detail**: click row → full profile, order history, VIP status with progress bar
- **Set Tier**: modal to manually assign any tier + lock duration (for partners/advertisers)
- **Recalculate**: replays order history, respects active locks, proposes correct tier/lock
  - If lock active + tier matches spend → no change
  - If lock active + spend exceeds → promote with fresh lock
  - If lock expired → recalculate from current spend
- **Pagination**: cursor-based, accumulates across pages, sorts full loaded list

## VIP Tiers
| Tier | Name | Min 90-day Spend |
|------|------|-----------------|
| VIP0 | (none) | $0 |
| VIP1 | Adventurer | $500 |
| VIP2 | Guardian | $1,250 |
| VIP3 | Champion | $2,500 |

## Lock Window
- Protects tier for 90 days from qualifying purchase
- Prevents downgrades during lock period even if spend drops
- Recalculate respects active locks — only changes on promotion or expiry

## Auth
- JWT cookie (owner + manager) for console UI
- Webhook endpoints (/vip/*) use X-Flow-Secret header (Shopify Flows)

## Dependencies
- shared/shopify_graphql.py (with local debug/dry-run wrapper)
- shared/klaviyo.py for tier transition emails
- shared/webhook_verify.py for Flow signature validation
