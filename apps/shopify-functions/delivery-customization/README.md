# Delivery Customization: Kiosk Raw Card Shipping

Shopify Function that hides free shipping options when the cart contains
products tagged `kiosk-raw` and the cart subtotal is under $50.

## What it does

- Cart has `kiosk-raw` items AND total < $50 → free shipping hidden, customer pays shipping
- Cart has `kiosk-raw` items AND total >= $50 → free shipping allowed (Champion benefit)
- Cart has NO `kiosk-raw` items → no changes (normal Champion free shipping applies)

## Deployment

This function must be deployed as part of a Shopify app via the Shopify CLI.

```bash
# Install Shopify CLI if not already
npm install -g @shopify/cli

# From the repo root, init the app and add this extension
shopify app init
shopify app generate extension --type delivery_customization

# Copy run.js and input.graphql into the generated extension directory
# Then deploy
shopify app deploy
```

After deployment, activate the customization in:
Shopify Admin → Settings → Shipping and delivery → Delivery customizations

## Files

- `run.js` — The function logic (hide free shipping for kiosk-raw under $50)
- `input.graphql` — Declares what cart data the function receives
