# Shipping Discount: Champion Free Shipping

Shopify Function (type: `shipping_discounts`) that applies 100% off every
delivery option for VIP3 customers when the cart qualifies.

## Rules (either qualifies)

- **(A) Same-cart combine** — cart has BOTH card line items (products
  tagged `kiosk-raw`) AND non-card line items, and the cart total is at
  least `combine_min_total`.
- **(B) Card threshold** — the card line subtotal alone is at least
  `card_threshold`.

A non-VIP3 customer never triggers the discount.

## Configuration

Tunable without redeploying via a JSON metafield on the discount itself:

| Namespace | Key | Type | Example |
|---|---|---|---|
| `$app:champion-freeship` | `config` | `json` | `{"card_threshold": 200, "combine_min_total": 100}` |

If the metafield is missing or invalid, defaults are `card_threshold=200`
and `combine_min_total=100`.

## Deployment

This function ships as part of a Shopify app via the Shopify CLI:

```bash
# From the Shopify app directory (not this monorepo)
shopify app generate extension --type shipping_discounts
# Copy run.js + input.graphql into the generated extension dir
shopify app deploy
```

After deploy, create the discount and set its config metafield:

1. Shopify Admin → Discounts → Create discount → **Automatic shipping
   discount** → choose the **Champion Free Shipping** function.
2. Open the new discount → set the metafield
   `$app:champion-freeship.config` to the JSON above. Tune later from the
   same page.

## Files

- `run.js` — function body (VIP3 check + threshold logic)
- `input.graphql` — cart + delivery + discount-config the function reads
