import requests
import time
import re

API_VERSION = "2023-10"
SHOPIFY_TOKEN = "***REMOVED******REMOVED***"
SHOPIFY_STORE = "***REMOVED***.myshopify.com"

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_TOKEN
}

HOT_SETS = {
    "prismatic evolutions",
    "surging sparks",
    "151",
    "paldean fates",
    "crown zenith",
    "celebrations"
}

COLLECTOR_KEYWORDS = [
    "ultra", "premium", "vmax", "shiny treasure", "upc"
]

KEYWORD_TAGS = {
    "blister": "blister",
    "elite trainer": "etb",
    "etb": "etb",
    "tin": "tin",
    # "bundle": "bundle"  # Disabled due to false positive with "booster bundle"
}

# All tags ever added by logic (used to subtract stale ones)
LEGACY_MANAGED_TAGS = {
    "hot set", "high value", "collector",
    "etb", "blister", "tin", "bundle",
    "ad_eligible"
}

MANAGED_TAGS = {
    "hot set", "high value", "collector",
    "etb", "blister", "tin",
    "ad_eligible"
}

def get_all_products_graphql():
    products = []
    cursor = None
    endpoint = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"

    while True:
        query = """
        query ($cursor: String) {
          products(first: 250, after: $cursor) {
            edges {
              node {
                id
                title
                tags
                variants(first: 1) {
                  edges {
                    node {
                      price
                      inventoryQuantity
                    }
                  }
                }
              }
              cursor
            }
            pageInfo {
              hasNextPage
            }
          }
        }
        """
        variables = {"cursor": cursor} if cursor else {}
        resp = requests.post(endpoint, headers=HEADERS, json={"query": query, "variables": variables})
        if resp.status_code != 200:
            print(f"❌ GraphQL error: {resp.status_code} - {resp.text}")
            break

        data = resp.json()
        edges = data.get("data", {}).get("products", {}).get("edges", [])
        for edge in edges:
            node = edge["node"]
            variant = node["variants"]["edges"][0]["node"] if node["variants"]["edges"] else {}
            price = float(variant.get("price", 0.0))
            inventory = int(variant.get("inventoryQuantity", 0))
            products.append({
                "id": node["id"],
                "title": node["title"],
                "tags": node["tags"],
                "price": price,
                "inventory_quantity": inventory
            })

        if data["data"]["products"]["pageInfo"]["hasNextPage"]:
            cursor = edges[-1]["cursor"]
        else:
            break
    return products

def normalize_tags(tag_input):
    return set(t.strip().lower() for t in tag_input if t.strip()) if isinstance(tag_input, list) else set()

def update_tags(product_id_gid, updated_tags):
    product_id = re.sub(r"gid://shopify/Product/", "", product_id_gid)
    endpoint = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/products/{product_id}.json"
    payload = {
        "product": {
            "id": int(product_id),
            "tags": ", ".join(sorted(updated_tags))
        }
    }
    resp = requests.put(endpoint, headers=HEADERS, json=payload)
    if resp.status_code == 200:
        print(f"✅ Updated {product_id}")
    else:
        print(f"❌ Failed to update {product_id}: {resp.status_code} - {resp.text}")

def main():
    products = get_all_products_graphql()

    for product in products:
        title = product["title"].lower()
        tags = normalize_tags(product.get("tags", []))
        price = product.get("price", 0.0)
        new_managed_tags = set()

        # === EXISTING TAGGING LOGIC ===

        # hot set — based on presence of hot set tags
        if any(hot in tags for hot in HOT_SETS):
            new_managed_tags.add("hot set")

        if price > 100:
            new_managed_tags.add("high value")

        # collector — price or keyword match with strict boundaries
        if price > 150 or any(re.search(rf"\b{re.escape(kw)}\b", title) for kw in COLLECTOR_KEYWORDS):
            new_managed_tags.add("collector")

        # keyword-derived structural tags
        for kw, tag in KEYWORD_TAGS.items():
            if re.search(rf"\b{re.escape(kw)}\b", title):
                new_managed_tags.add(tag)

        # === AD ELIGIBILITY LOGIC ===

        manual_force = "force_ads" in tags
        manual_exclude = "exclude_ads" in tags
        inventory_ok = product.get("inventory_quantity", 0) > 1
        price_ok = price >= 20.0

        # Force always wins
        if manual_force:
            new_managed_tags.add("ad_eligible")
        elif manual_exclude:
            new_managed_tags.discard("ad_eligible")  # do not set or maintain
        else:
            if inventory_ok and price_ok:
                new_managed_tags.add("ad_eligible")
            else:
                new_managed_tags.discard("ad_eligible")  # remove if previously set

        # CLEANUP: Remove stale managed tags (but keep manual ones)
        cleaned_tags = (tags - LEGACY_MANAGED_TAGS - {"ad_eligible"}) | new_managed_tags

        if cleaned_tags != tags:
            update_tags(product["id"], cleaned_tags)
            time.sleep(0.25)

if __name__ == "__main__":
    main()
