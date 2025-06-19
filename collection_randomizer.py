import requests
import random
import json

SHOPIFY_DOMAIN = '***REMOVED***.myshopify.com'
ACCESS_TOKEN = '***REMOVED******REMOVED***'
BASE_URL = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04"
THEME_ID = 147343704284


HEADERS = {
    'X-Shopify-Access-Token': ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

# Global: how many items to show in each homepage collection
NUM_PRODUCTS = 12

def graphql_query(query, variables=None):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def get_collections():
    custom_url = f"{BASE_URL}/custom_collections.json?limit=250"
    smart_url = f"{BASE_URL}/smart_collections.json?limit=250"

    custom_res = requests.get(custom_url, headers=HEADERS)
    smart_res = requests.get(smart_url, headers=HEADERS)

    custom_res.raise_for_status()
    smart_res.raise_for_status()

    return custom_res.json()["custom_collections"], smart_res.json()["smart_collections"]

def get_collects_for_collection(collection_id):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/collects.json?collection_id={collection_id}&limit=250"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["collects"]

def delete_collect(collect_id):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/collects/{collect_id}.json"
    response = requests.delete(url, headers=HEADERS)
    response.raise_for_status()

def get_products_in_collection(collection_id):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/collections/{collection_id}/products.json?limit=250"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()["products"]

def get_products_in_collection_graphql(collection_gid, limit=100):
    query = """
    query ($collectionId: ID!, $first: Int!) {
      collection(id: $collectionId) {
        products(first: $first) {
          edges {
            node {
              id
              title
              variants(first: 1) {
                edges {
                  node {
                    availableForSale
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    variables = {
        "collectionId": f"gid://shopify/Collection/{collection_gid}",
        "first": limit
    }
    data = graphql_query(query, variables)
    try:
        edges = data["data"]["collection"]["products"]["edges"]
        result = []
        for edge in edges:
            node = edge["node"]
            try:
                available = node["variants"]["edges"][0]["node"]["availableForSale"]
            except:
                available = False
            if available:
                result.append(node)
        return result
    except Exception as e:
        print(f"⚠️ Failed parsing GraphQL response for collection {collection_gid}: {e}")
        print("Raw response:", json.dumps(data, indent=2))
        return []

def create_collect(product_id, collection_id):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/collects.json"
    data = {
        "collect": {
            "product_id": product_id,
            "collection_id": collection_id
        }
    }
    response = requests.post(url, json=data, headers=HEADERS)
    response.raise_for_status()

def sync_homepage_collections():
    custom_collections, smart_collections = get_collections()

    for homepage in custom_collections:
        handle = homepage["handle"]
        if not handle.endswith("-homepage"):
            continue

        base_handle = handle.replace('-homepage', '')
        parent = next((c for c in smart_collections if c['handle'] == base_handle), None)
        if not parent:
            print(f"❌ No parent found for {homepage['handle']}")
            continue

        homepage_id = homepage['id']
        parent_id = parent['id']

        # 1. Remove all current collects from homepage collection
        collects = get_collects_for_collection(homepage_id)
        for collect in collects:
            delete_collect(collect['id'])

        # 2. Get parent products and shuffle
        parent_products = get_products_in_collection(parent_id)
        product_ids = [p['id'] for p in parent_products]
        random.shuffle(product_ids)
        selected = product_ids[:NUM_PRODUCTS]

        # 3. Add selected products to homepage collection
        for pid in selected:
            create_collect(pid, homepage_id)

        print(f"✅ Synced {homepage['title']} with {len(selected)} products")

def get_index_data(theme_id):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/themes/{theme_id}/assets.json"
    params = {"asset[key]": "templates/index.json"}
    res = requests.get(url, headers=HEADERS, params=params)
    res.raise_for_status()

    data = res.json()['asset']['value']
    index_data = json.loads(data)
    return index_data

def update_index_data(index_data, theme_id):
    payload = {
        "asset": {
            "key": "templates/index.json",
            "value": json.dumps(index_data, indent=2)
        }
    }
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/themes/{theme_id}/assets.json"
    res = requests.put(url, headers=HEADERS, json=payload)
    res.raise_for_status()
    print("✅ index.json updated.")

def curated_random_carousel_blocks(index_json_string):
    index_data = json.loads(index_json_string)

    section_id = next(
        (k for k, v in index_data['sections'].items() if v['type'] == 'random-carousel-collections'),
        None
    )
    if not section_id:
        raise ValueError("⚠️ Could not find a 'random-carousel-collections' section in index.json.")

    block_map = index_data['sections'][section_id]['blocks']

    # Define categories by block ID
    product_type_blocks = {
        "collection_block_dJVnUz",  # ETBs
        "collection_block_PHE4Xi",  # Tins
        "collection_block_6GMPLP",  # Booster Boxes
        "collection_block_AfdRQH",  # Booster Packs
        "collection_block_RtKEAG",  # Collection Boxes
    }
    ip_blocks = {
        "collection_block_jyNWYw",  # MTG
        "collection_block_mA4p6n",  # Lorcana
        "collection_block_6zfnAf",  # One Piece
        "collection_block_EexjnG",  # All Pokémon
    }
    curated_blocks = {
        "collection_block_cCwXh4",  # Hot Pokémon Sets
        "collection_block_HPihgx",  # Premium Sealed
        "collection_block_nzUciF",  # Modern Sealed
    }

    # Sample according to strategy
    selected = []
    selected += random.sample(list(product_type_blocks & block_map.keys()), 2)
    selected += random.sample(list(ip_blocks & block_map.keys()), 1)
    selected += random.sample(list(curated_blocks & block_map.keys()), 1)

    random.shuffle(selected)
    index_data['sections'][section_id]['block_order'] = selected

    return json.dumps(index_data, indent=2)

def update_featured_picks(collection_id, product_ids):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    gid_collection = f"gid://shopify/Collection/{collection_id}"
    gid_products = product_ids

    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          key
          value
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    variables = {
        "metafields": [
            {
                "ownerId": gid_collection,
                "namespace": "custom",
                "key": "featured_products",
                "type": "list.product_reference",
                "value": json.dumps(gid_products)
            }
        ]
    }

    response = requests.post(url, headers=headers, json={"query": mutation, "variables": variables})
    response.raise_for_status()

    data = response.json()
    errors = data.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if errors:
        print(f"❌ GraphQL metafield error: {errors}")
    else:
        print(f"✨ Updated featured picks for collection {collection_id}")

def sync_featured_picks_for_all_collections(smart_collections):
    for collection in smart_collections:
        collection_id = collection["id"]
        title = collection["title"]

        try:
            products = get_products_in_collection_graphql(collection_id)
        except Exception as e:
            print(f"⚠️ Skipping {title} due to product fetch error: {e}")
            continue
        # Filter in-stock products
        in_stock = []
        for p in products:
            try:
                available = p["variants"]["edges"][0]["node"]["availableForSale"]
            except Exception as e:
                available = False

            print(f"   - {p['title']} → {available}")

            if available:
                in_stock.append(p)
        if not in_stock:
            print(f"🚫 No in-stock items for {title}")
            continue

        # Pick up to 6 featured items
        random.shuffle(in_stock)
        featured = in_stock[:6]
        featured_ids = [p["id"] for p in featured]

        try:
            update_featured_picks(collection_id, featured_ids)
        except Exception as e:
            print(f"❌ Failed to update featured picks for {title}: {e}")
    print(f"✨ Updated featured picks for collection {collection_id}")

def get_theme_id():
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/themes.json"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    themes = response.json()['themes']

    for theme in themes:
        print(f"{'[✔️ LIVE]' if theme['role'] == 'main' else '       '} {theme['name']} — ID: {theme['id']}")


#index_data = get_index_data(THEME_ID)
#shuffled_data = curated_random_carousel_blocks(json.dumps(index_data))
#update_index_data(json.loads(shuffled_data), THEME_ID)
#sync_homepage_collections()
_, smart_collections = get_collections()
sync_featured_picks_for_all_collections(smart_collections)