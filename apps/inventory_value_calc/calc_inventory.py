import os, sys, requests, csv
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

SHOP = os.environ.get("SHOPIFY_STORE")              # e.g., pack-fresh.myshopify.com
TOKEN = os.environ.get("SHOPIFY_TOKEN")     # Admin API access token

if not SHOP or not TOKEN:
    sys.exit("Set SHOPIFY_SHOP and SHOPIFY_ACCESS_TOKEN environment variables.")

URL = f"https://{SHOP}/admin/api/2024-04/graphql.json"
HDRS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

QUERY = """
query ProductsForValuation($cursor: String) {
  products(
    first: 100
    after: $cursor
    query: "status:active OR status:draft"
  ) {
    edges {
      cursor
      node {
        id
        title
        vendor
        handle
        totalVariants
        variants(first: 100) {
          edges {
            node {
              id
              title
              sku
              price
              inventoryQuantity
            }
          }
        }
      }
    }
    pageInfo { hasNextPage }
  }
}
"""

def gfetch(cursor=None):
    r = requests.post(URL, headers=HDRS, json={"query": QUERY, "variables": {"cursor": cursor}})
    r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]["products"]

def to_decimal(x):
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

grand_total = Decimal("0")
products_checked = 0
variants_counted = 0

variant_rows = []   # for CSV (variant-level)
product_totals = defaultdict(Decimal)  # product_title -> value

cursor = None
while True:
    data = gfetch(cursor)
    for pedge in data["edges"]:
        p = pedge["node"]
        products_checked += 1
        has_multi_variants = (p.get("totalVariants") or 0) > 1

        for vedge in p["variants"]["edges"]:
            v = vedge["node"]

            # Skip ghost "Default Title" if product now has multiple variants
            if has_multi_variants and (v["title"] or "").strip().lower() == "default title":
                continue

            qty = v.get("inventoryQuantity") or 0
            if qty <= 0:
                continue

            price = to_decimal(v.get("price"))
            if not price or price <= 0:
                continue

            value = price * qty
            grand_total += value
            variants_counted += 1

            variant_rows.append({
                "product_title": p["title"],
                "vendor": p.get("vendor") or "",
                "handle": p.get("handle") or "",
                "variant_title": v["title"] or "",
                "sku": v.get("sku") or "",
                "price": f"{price:.2f}",
                "quantity": qty,
                "value": f"{value:.2f}",
            })
            product_totals[p["title"]] += value

    if data["pageInfo"]["hasNextPage"]:
        cursor = data["edges"][-1]["cursor"]
    else:
        break

# ---- Write CSVs ----
with open("inventory_value_variants.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=[
        "product_title","vendor","handle","variant_title","sku","price","quantity","value"
    ])
    w.writeheader()
    for row in sorted(variant_rows, key=lambda r: Decimal(r["value"]), reverse=True):
        w.writerow(row)

with open("inventory_value_products.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["product_title","total_value"])
    for pt, val in sorted(product_totals.items(), key=lambda kv: kv[1], reverse=True):
        w.writerow([pt, f"{val:.2f}"])

# ---- Pretty print summary ----
def fmt_money(d: Decimal) -> str:
    return "${:,.2f}".format(d)

print(f"\nProducts scanned: {products_checked}")
print(f"Variants counted: {variants_counted}")
print(f"Total listed value (price × available): {fmt_money(grand_total)}\n")

# Top 25 variants
print("Top 25 variants by value:")
for row in sorted(variant_rows, key=lambda r: Decimal(r["value"]), reverse=True)[:25]:
    print(f"  {row['product_title']} — {row['variant_title']}  "
          f"x{row['quantity']} @ ${row['price']}  =  ${Decimal(row['value']):,.2f}")

# Top 25 products
print("\nTop 25 products by total value:")
for pt, val in list(sorted(product_totals.items(), key=lambda kv: kv[1], reverse=True))[:25]:
    print(f"  {pt}: {fmt_money(val)}")

print("\nWrote CSVs: inventory_value_variants.csv, inventory_value_products.csv")