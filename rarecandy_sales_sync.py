
# rarecandy_sales_sync.py

import requests
import pandas as pd
from datetime import datetime
from meta_tracker import get_last_sync, update_last_sync
from normalize_name import normalize_name  # assumes you extracted this as a helper

RARE_CANDY_GRAPHQL_URL = "https://api.rarecandy.com/graphql"

SALES_QUERY = """
query storeOrdersV2($storeOrdersV2Input2: SearchInput, $includeRarecandyOnlyInfo: Boolean!) {
  me {
    store {
      storeOrdersV2(input: $storeOrdersV2Input2) {
        data {
          status
          placedAt
          orderLines {
            quantity
            product {
              name
            }
          }
        }
      }
    }
  }
}
"""

def fetch_rarecandy_sales(bearer_token, since_iso):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json"
    }
    page = 1
    sales = []

    while True:
        variables = {
            "storeOrdersV2Input2": {
                "query": "",
                "page": page,
                "pageSize": 100,
                "filters": [
                    {"id": "sort", "values": ["placedAtTimestamp:desc"]}
                ]
            },
            "includeRarecandyOnlyInfo": False
        }

        resp = requests.post(RARE_CANDY_GRAPHQL_URL, headers=headers, json={"query": SALES_QUERY, "variables": variables})
        resp.raise_for_status()
        data = resp.json()

        orders = data["data"]["me"]["store"]["storeOrdersV2"]["data"]
        if not orders:
            break

        for order in orders:
            if order["status"] in ["CANCELED", "REFUNDED"]:
                continue
            if order["placedAt"] < since_iso:
                return sales  # All future pages will be older

            for line in order["orderLines"]:
                product = line["product"]
                if not product:
                    continue
                sales.append({
                    "name": product["name"],
                    "quantity": line["quantity"],
                    "placedAt": order["placedAt"]
                })

        page += 1

    return sales

def apply_rarecandy_sales_to_inventory(inventory_df, bearer_token):
    last_sync = get_last_sync("rarecandy")
    sales = fetch_rarecandy_sales(bearer_token, last_sync)
    print(f"✅ Pulled {len(sales)} RC sales since {last_sync}")

    # Build key → index map
    inventory_df["__key__"] = inventory_df["name"].apply(normalize_name)
    name_to_index = {normalize_name(name): idx for idx, name in enumerate(inventory_df["name"])}

    for sale in sales:
        key = normalize_name(sale["name"])
        if key in name_to_index:
            idx = name_to_index[key]
            if pd.notna(inventory_df.at[idx, "total amount (4/1)"]):
                inventory_df.at[idx, "total amount (4/1)"] -= sale["quantity"]

    if sales:
        latest = max(s["placedAt"] for s in sales)
        update_last_sync("rarecandy", latest)

    return inventory_df
