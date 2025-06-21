import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()


SHOPIFY_ADMIN_TOKEN = os.environ.get("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
API_VERSION = "2023-10"
GRAPHQL_ENDPOINT = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"
CSV_PATH = ".venv/Scripts/shopifyupdate.csv"

import time

def update_variant_price(variant_id, new_price, retries=3):
    url = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/variants/{variant_id}.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN
    }
    payload = {
        "variant": {
            "id": variant_id,
            "price": str(new_price)
        }
    }

    for attempt in range(retries):
        response = requests.put(url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"✅ Updated variant {variant_id} to ${new_price}")
            break
        elif response.status_code == 429:
            print(f"⏳ Rate limit hit — waiting before retrying variant {variant_id}...")
            time.sleep(1.5)  # wait a bit longer if throttled
        else:
            print(f"❌ Failed to update variant {variant_id}: {response.status_code} - {response.text}")
            break

    time.sleep(0.5)  # Respect 2 req/sec baseline

def main():
    df = pd.read_csv(CSV_PATH)

    updates = df[df["final_price_to_upload"].notnull()]

    print(f"📦 Uploading {len(updates)} price updates...\n")

    for _, row in updates.iterrows():
        variant_id = int(row["variant_id"])
        new_price = float(row["final_price_to_upload"])
        update_variant_price(variant_id, new_price)

    print("\n✅ All eligible prices updated.")

if __name__ == "__main__":
    main()
