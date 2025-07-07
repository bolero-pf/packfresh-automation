import requests
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

SHOP_NAME = os.getenv("SHOPIFY_STORE")
API_VERSION = "2024-04"
ACCESS_TOKEN = os.getenv("SHOPIFY_TOKEN")
API_URL = f"https://{SHOP_NAME}/admin/api/{API_VERSION}/graphql.json"

HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

TAG_PREFIX = "unavailable-"
TODAY = datetime.today().strftime('%B').lower() + '-' + str(datetime.today().day)  # e.g. "july-8"

def fetch_products(after_cursor=None):
    query = '''
    query ($cursor: String) {
      products(first: 100, after: $cursor) {
        pageInfo {
          hasNextPage
        }
        edges {
          cursor
          node {
            id
            title
            tags
          }
        }
      }
    }
    '''
    variables = { "cursor": after_cursor }
    response = requests.post(API_URL, headers=HEADERS, json={"query": query, "variables": variables})
    response.raise_for_status()
    return response.json()["data"]["products"]

def update_product_tags(product_id, tags):
    mutation = '''
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
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
            "id": product_id,
            "tags": tags
        }
    }
    response = requests.post(API_URL, headers=HEADERS, json={"query": mutation, "variables": variables})
    response.raise_for_status()
    data = response.json()
    if data.get("errors") or data["data"]["productUpdate"]["userErrors"]:
        print("Error updating", product_id, data)
    return data

def run():
    cursor = None
    while True:
        data = fetch_products(cursor)
        for edge in data["edges"]:
            product = edge["node"]
            tags = product["tags"]
            new_tags = []
            changed = False

            for tag in tags:
                if tag.lower().startswith(TAG_PREFIX):
                    date_str = tag[len(TAG_PREFIX):].lower()
                    if date_str == TODAY:
                        changed = True
                        continue  # remove this tag
                new_tags.append(tag)

            if changed:
                print(f"Updating {product['title']} ({product['id']})")
                update_product_tags(product["id"], new_tags)

        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["edges"][-1]["cursor"]

if __name__ == "__main__":
    run()
