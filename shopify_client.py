import requests
import os

SHOPIFY_TOKEN = "***REMOVED******REMOVED***"
SHOPIFY_STORE = "***REMOVED***.myshopify.com"
METAFIELD_NAMESPACE = "tcg"
METAFIELD_KEY = "tcgplayer_id"

def get_shopify_products():
    query = """
    {
      products(first: 250) {
        edges {
          node {
            title
            variants(first: 1) {
              edges {
                node {
                  id
                  price
                  metafield(namespace: "%s", key: "%s") {
                    value
                  }
                }
              }
            }
          }
        }
      }
    }
    """ % (TCGPLAYER_METAFIELD_NAMESPACE, TCGPLAYER_METAFIELD_KEY)

    url = f"https://{SHOPIFY_STORE}/admin/api/2023-07/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json"
    }

    response = requests.post(url, json={"query": query}, headers=headers)
    response.raise_for_status()
    data = response.json()

    products = []
    for edge in data["data"]["products"]["edges"]:
        node = edge["node"]
        variant = node["variants"]["edges"][0]["node"]
        price = float(variant["price"])
        tcg_id = None
        if variant.get("metafield") and variant["metafield"].get("value"):
            tcg_id = variant["metafield"]["value"]

        products.append({
            "title": node["title"],
            "variant_id": variant["id"],
            "price": price,
            "tcgplayer_id": tcg_id
        })

    return products

def update_variant_price(variant_id, new_price):
    mutation = """
    mutation {
      productVariantUpdate(input: {
        id: "%s",
        price: "%0.2f"
      }) {
        productVariant {
          id
          price
        }
        userErrors {
          field
          message
        }
      }
    }
    """ % (variant_id, new_price)

    url = f"https://{SHOPIFY_STORE}/admin/api/2023-07/graphql.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json"
    }

    response = requests.post(url, json={"query": mutation}, headers=headers)
    response.raise_for_status()
    return response.json()