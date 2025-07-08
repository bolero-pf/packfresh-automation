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
def get_shop_publication_id():
    query = '''
    query {
      publications(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    '''
    response = requests.post(API_URL, headers=HEADERS, json={"query": query})
    response.raise_for_status()
    data = response.json()

    if "data" not in data or "publications" not in data["data"]:
        print("❌ Unexpected response from API:")
        print(data)
        raise Exception("Failed to fetch publications.")

    for edge in data["data"]["publications"]["edges"]:
        if edge["node"]["name"] == "Shop":
            print (edge["node"]["id"])
            return edge["node"]["id"]

    raise Exception("Shop publication ID not found.")
def publish_product(product_id, publication_id):
    mutation = '''
    mutation PublishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors {
          field
          message
        }
      }
    }
    '''
    variables = {
        "id": product_id,
        "input": [{
            "publicationId": publication_id
        }]
    }

    resp = requests.post(API_URL, headers=HEADERS, json={"query": mutation, "variables": variables})
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print("❌ Top-level GraphQL error:", data["errors"])
        return

    errors = data["data"]["publishablePublish"]["userErrors"]
    if errors:
        print("❌ Error publishing", product_id, errors)
    else:
        print("✅ Published", product_id)






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
                publish_product(product["id"], get_shop_publication_id())

        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["edges"][-1]["cursor"]

if __name__ == "__main__":
    run()
