// Shipping Discount Function: Champion Free Shipping
//
// Applies 100% off shipping when a VIP3 customer's cart qualifies under
// either rule below. Configuration lives on the discount itself as a JSON
// metafield so thresholds can change without redeploying.
//
// Rules (either qualifies):
//   (A) Same-cart combine: cart has BOTH card and non-card items, and the
//       total is at least combine_min_total.
//   (B) Card threshold: card line subtotal alone is at least card_threshold.
//
// Config metafield (namespace: $app:champion-freeship, key: config):
//   { "card_threshold": 200, "combine_min_total": 100 }
// Missing or unparsable config falls back to those defaults.
//
// "Card" = product tagged kiosk-raw (matches the existing delivery
// customization). "VIP3" = customer tag set by the vip service.

const NO_DISCOUNT = { discounts: [] };
const DEFAULTS = { card_threshold: 200, combine_min_total: 100 };

/**
 * @param {RunInput} input
 * @returns {FunctionRunResult}
 */
export function run(input) {
  const customer = input.cart.buyerIdentity && input.cart.buyerIdentity.customer;
  const isVip3 = customer
    && Array.isArray(customer.hasTags)
    && customer.hasTags.some((t) => t.tag === "VIP3" && t.hasTag);
  if (!isVip3) return NO_DISCOUNT;

  const cfg = readConfig(input.discountNode && input.discountNode.metafield);

  let cardSubtotal = 0;
  let nonCardSubtotal = 0;
  for (const line of input.cart.lines) {
    const amount = parseFloat(line.cost.subtotalAmount.amount);
    if (!Number.isFinite(amount)) continue;
    const isCard = line.merchandise.__typename === "ProductVariant"
      && line.merchandise.product
      && line.merchandise.product.hasAnyTag === true;
    if (isCard) cardSubtotal += amount;
    else nonCardSubtotal += amount;
  }
  const cartTotal = cardSubtotal + nonCardSubtotal;

  const hitsCardThreshold = cardSubtotal >= cfg.card_threshold;
  const hitsCombine = cardSubtotal > 0
    && nonCardSubtotal > 0
    && cartTotal >= cfg.combine_min_total;
  if (!hitsCardThreshold && !hitsCombine) return NO_DISCOUNT;

  const targets = [];
  for (const group of input.deliveryGroups) {
    for (const opt of group.deliveryOptions) {
      targets.push({ deliveryOption: { handle: opt.handle } });
    }
  }
  if (!targets.length) return NO_DISCOUNT;

  return {
    discounts: [{
      message: hitsCardThreshold
        ? "Champion free shipping"
        : "Champion combined-cart free shipping",
      targets,
      value: { percentage: { value: "100" } },
    }],
  };
}

function readConfig(metafield) {
  if (!metafield || typeof metafield.value !== "string") return { ...DEFAULTS };
  try {
    const parsed = JSON.parse(metafield.value);
    return {
      card_threshold: typeof parsed.card_threshold === "number"
        ? parsed.card_threshold
        : DEFAULTS.card_threshold,
      combine_min_total: typeof parsed.combine_min_total === "number"
        ? parsed.combine_min_total
        : DEFAULTS.combine_min_total,
    };
  } catch (_) {
    return { ...DEFAULTS };
  }
}
