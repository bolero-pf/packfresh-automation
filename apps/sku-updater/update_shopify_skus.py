import requests
import os
from dotenv import load_dotenv

load_dotenv()
STORE_URL = os.environ.get('SHOPIFY_STORE')
ACCESS_TOKEN = os.environ.get('SHOPIFY_TOKEN')

SHOPIFY_URL = "https://"+STORE_URL+"/admin/api/2023-10/graphql.json"
print(SHOPIFY_URL)

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

def fetch_all_products_with_gtin(cursor=None):
    query = '''
    query ($cursor: String) {
      products(first: 100, after: $cursor) {
        edges {
          node {
            id
            title
            variants(first: 10) {
              edges {
                node {
                  id
                  title
                  sku
                  barcode
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
    '''
    variables = {"cursor": cursor}
    response = requests.post(SHOPIFY_URL, headers=HEADERS, json={"query": query, "variables": variables})

    try:
        response.raise_for_status()
    except Exception as err:
        print("Response text:", response.status_code, response.text)

    return response.json()

def update_sku(variant_id, new_sku):
    mutation = '''
    mutation UpdateVariant($input: ProductVariantInput!) {
      productVariantUpdate(input: $input) {
        productVariant {
          id
          sku
        }
        userErrors {
          field
          message
        }
      }
    }
    '''
    variables = {
        "input": {
            "id": variant_id,
            "sku": new_sku
        }
    }
    response = requests.post(SHOPIFY_URL, headers=HEADERS, json={"query": mutation, "variables": variables})
    response.raise_for_status()
    return response.json()

def sync_sku_to_gtin():
    missing_gtins = []
    cursor = None
    while True:
        data = fetch_all_products_with_gtin(cursor)
        products = data["data"]["products"]["edges"]
        print("Fetched all products")
        for product in products:
            for variant_edge in product["node"]["variants"]["edges"]:
                variant = variant_edge["node"]
                barcode = variant["barcode"]
                if barcode:
                    update_response = update_sku(variant["id"], barcode)
                    if update_response["data"]["productVariantUpdate"]["userErrors"]:
                        print("⚠️ Error:", update_response["data"]["productVariantUpdate"]["userErrors"])
                else:
                    missing_gtins.append((product["node"]["title"], variant["title"]))
        if not data["data"]["products"]["pageInfo"]["hasNextPage"]:
            break
        cursor = products[-1]["cursor"]

    print("✅ Done updating SKUs to GTINs.")
    if missing_gtins:
        print("\n🚨 Missing GTINs:")
        for product_title, variant_title in missing_gtins:
            print(f"- {product_title} / {variant_title}")

sync_sku_to_gtin()