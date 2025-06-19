import requests
import time
import re

API_VERSION = "2023-10"
SHOPIFY_TOKEN = "***REMOVED******REMOVED***"
SHOPIFY_STORE = "***REMOVED***.myshopify.com"
RC_BEARER = "***REMOVED***.eyJ1c2VybmFtZSI6IlBhY2tGcmVzaFNTWiIsInN1YiI6MjI5NzM2LCJ1c2VySWQiOjIyOTczNiwicm9sZXMiOlsiU1RPUkVfT1dORVIiXSwicGVybWlzc2lvbnMiOlsiQ1JFQVRFX1NUT1JFIl0sImF1dGgwSWQiOm51bGwsImlhdCI6MTc0MDM3MDg5OCwiZXhwIjoxNzcxOTA2ODk4fQ.8vKRvkS1eP59Xren3tY0e_cto9mrsYpNIBidY_OUTK8"
import pandas as pd

HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_TOKEN
}

def get_all_shopify_gtins():
    results = []
    cursor = None
    endpoint = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"

    while True:
        query = """
        query ($cursor: String) {
          products(first: 100, after: $cursor) {
            edges {
              node {
                title
                variants(first: 25) {
                  edges {
                    node {
                      id
                      barcode
                      inventoryItem {
                        id
                      }
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
            product = edge["node"]
            for variant_edge in product["variants"]["edges"]:
                variant = variant_edge["node"]
                inv_id_gid = variant["inventoryItem"]["id"] if variant["inventoryItem"] else None
                inv_id = int(inv_id_gid.split("/")[-1]) if inv_id_gid else None
                results.append({
                    "title": product["title"],
                    "variant_gid": variant["id"],
                    "gtin": variant["barcode"],
                    "inventory_item_id": inv_id,
                })

        if data["data"]["products"]["pageInfo"]["hasNextPage"]:
            cursor = edges[-1]["cursor"]
        else:
            break

    return results


def update_rc_gtin(rc_bearer_token, inventory_item_id, gtin):
    url = "https://api.rarecandy.com/graphql"
    headers = {
        "Authorization": f"Bearer {rc_bearer_token}",
        "Content-Type": "application/json"
    }

    mutation = """
    mutation UpdateItemGtin($inventoryItemId: Int!, $input: InventoryItemInput!) { 
      updateInventoryItem(inventoryItemId: $inventoryItemId, input: $input) {
        id
        gtin
        __typename
      }
    }
    """

    variables = {
        "inventoryItemId": inventory_item_id,
        "input": {
            "gtin": gtin
        }
    }

    response = requests.post(url, headers=headers, json={"query": mutation, "variables": variables})
    if response.status_code != 200:
        print(f"❌ HTTP error from RC for {inventory_item_id}: {response.status_code} — {response.text}")
        return False

    data = response.json()
    if "errors" in data:
        print(f"❌ RC mutation error for {inventory_item_id}: {data['errors']}")
        return False

    print(f"✅ RC GTIN updated: {inventory_item_id} → {gtin}")
    return True

def sync_gtins_to_rc(gtin_records, df_csv, rc_bearer_token):
    success_count = 0
    skip_missing_title = 0
    skip_missing_gtin = 0
    skip_missing_match = 0
    missing_gtin_titles = []
    missing_rcid_titles = []

    for record in gtin_records:
        title = record["title"].strip().lower()
        gtin = record["gtin"]
        if not title:
            skip_missing_title += 1
            continue
        if not gtin:
            skip_missing_gtin += 1
            missing_gtin_titles.append(title)
            continue

        matches = df_csv[df_csv["__key__"] == title]
        if matches.empty:
            skip_missing_match += 1
            continue

        rc_id_series = matches["inventory_item_id"].dropna()
        if rc_id_series.empty:
            missing_rcid_titles.append(title)
            continue

        rc_id = int(rc_id_series.iloc[0])
        updated = update_rc_gtin(rc_bearer_token, rc_id, gtin)
        if updated:
            success_count += 1
        time.sleep(0.25)

    print("\n========== GTIN SYNC SUMMARY ==========")
    print(f"✅ Successfully updated: {success_count}")
    print(f"🚫 Skipped — missing GTIN: {skip_missing_gtin}")
    print(f"🚫 Skipped — unmatched title: {skip_missing_match}")
    print(f"🚫 Skipped — blank title: {skip_missing_title}")
    print("=======================================\n")

    if missing_gtin_titles:
        print("📭 Titles missing GTIN in Shopify:")
        for title in missing_gtin_titles:
            print(f" - {title}")

    if missing_rcid_titles:
        print("\n🛑 Titles missing Rare Candy inventory ID:")
        for title in missing_rcid_titles:
            print(f" - {title}")

df = pd.read_csv(".venv/Scripts/InventoryFinal.csv")
gtin_records = get_all_shopify_gtins()
sync_gtins_to_rc(gtin_records, df, RC_BEARER)
