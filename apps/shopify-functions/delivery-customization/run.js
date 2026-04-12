// Delivery Customization Function
// Hides free shipping when cart contains kiosk-raw products and total < threshold
//
// Logic:
//   - If cart has ANY product tagged "kiosk-raw" AND cart total < $50 → hide $0 shipping rates
//   - Otherwise → no changes (Champion free shipping applies normally)

/**
 * @param {RunInput} input
 * @returns {FunctionRunResult}
 */
export function run(input) {
  const THRESHOLD = 50.0;

  // Check if any cart line has a kiosk-raw tagged product
  const hasKioskRaw = input.cart.lines.some(
    (line) => line.merchandise.__typename === "ProductVariant"
      && line.merchandise.product.hasAnyTag
  );

  if (!hasKioskRaw) {
    return { operations: [] };
  }

  const cartTotal = parseFloat(input.cart.cost.subtotalAmount.amount);

  if (cartTotal >= THRESHOLD) {
    return { operations: [] };
  }

  // Hide all free ($0) shipping options
  const operations = [];
  for (const group of input.deliveryGroups) {
    for (const option of group.deliveryOptions) {
      if (parseFloat(option.cost.amount) === 0) {
        operations.push({
          hide: {
            deliveryOptionHandle: option.handle,
          },
        });
      }
    }
  }

  return { operations };
}
