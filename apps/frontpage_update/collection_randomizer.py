import requests
import random
import json
from dotenv import load_dotenv
import os

load_dotenv()

SHOPIFY_DOMAIN = os.environ.get("SHOPIFY_STORE")
ACCESS_TOKEN = os.environ.get("SHOPIFY_TOKEN")
BASE_URL = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04"
THEME_ID = 159537758428


HEADERS = {
    'X-Shopify-Access-Token': ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

# Global: how many items to show in each homepage collection
NUM_PRODUCTS = 12
import os, time, random, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = (5, 45)  # connect, read

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=6, connect=6, read=6,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET", "POST", "PUT", "DELETE"},
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

from requests.exceptions import ConnectionError, Timeout

def _respect_rate_limit(resp):
    # Shopify sends X-Request-Id, X-Shopify-Shop-Api-Call-Limit: "N/80"
    lim = resp.headers.get("X-Shopify-Shop-Api-Call-Limit")
    if lim:
        used, burst = map(int, lim.split("/"))
        if used >= burst - 4:
            time.sleep(0.5 + random.random())  # brief cool-off near the ceiling

def _req_json(method, url, **kwargs):
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    for attempt in range(1, 4):
        try:
            resp = SESSION.request(method, url, **kwargs)
            if resp.status_code == 429:
                delay = float(resp.headers.get("Retry-After", "1"))
                time.sleep(delay + random.random())
                continue
            _respect_rate_limit(resp)
            resp.raise_for_status()
            try:
                return resp, (resp.json() if resp.content else {})
            except ValueError:
                return resp, {}
        except (ConnectionError, Timeout) as e:
            if attempt == 3:
                raise
            time.sleep((2 ** attempt) + random.random())

def _req_ok(method, url, **kwargs) -> bool:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    for attempt in range(1, 4):
        try:
            resp = SESSION.request(method, url, **kwargs)
            if resp.status_code in (200, 204):
                _respect_rate_limit(resp)
                return True
            if resp.status_code == 429:
                delay = float(resp.headers.get("Retry-After", "1"))
                time.sleep(delay + random.random())
                continue
            _respect_rate_limit(resp)
            resp.raise_for_status()
            return True
        except (ConnectionError, Timeout):
            if attempt == 3:
                return False
            time.sleep((2 ** attempt) + random.random())

def graphql_query(query, variables=None):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/graphql.json"
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    _, data = _req_json("POST", url, json=payload)
    return data

def get_collections():
    custom_url = f"{BASE_URL}/custom_collections.json?limit=250"
    smart_url  = f"{BASE_URL}/smart_collections.json?limit=250"
    _, custom = _req_json("GET", custom_url)
    _, smart  = _req_json("GET", smart_url)
    return custom.get("custom_collections", []), smart.get("smart_collections", [])

def get_collects_for_collection(collection_id: int, limit: int = 250) -> list[dict]:
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/collects.json"
    params = {"collection_id": collection_id, "limit": limit}
    collects = []
    while True:
        resp, data = _req_json("GET", url, params=params)
        batch = data.get("collects", [])
        collects.extend(batch)
        # Shopify REST pagination via Link header
        link = resp.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        # Parse next page URL (simple split; you may already have a helper)
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>").strip()
                break
        if not next_url:
            break
        url, params = next_url, None  # next_url already has query
    return collects

def delete_collect(collect_id) -> bool:
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/collects/{collect_id}.json"
    return _req_ok("DELETE", url)


def get_products_in_collection(collection_id: int, limit: int = 250) -> list[dict]:
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/products.json"
    params = {"collection_id": collection_id, "limit": limit, "fields": "id,title,product_type,tags"}
    products = []
    while True:
        resp, data = _req_json("GET", url, params=params)
        batch = data.get("products", [])
        products.extend(batch)
        link = resp.headers.get("Link", "")
        if 'rel="next"' not in link:
            break
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>").strip()
                break
        if not next_url:
            break
        url, params = next_url, None
    return products

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
    resp, data = _req_json("POST", url, json=data)
    # Expected: {"collect": {...}}
    return data.get("collect", {})

def sync_homepage_collections():
    custom_collections, smart_collections = get_collections()
    for homepage in custom_collections:
        handle = homepage["handle"]
        if not handle.endswith("-homepage"):
            continue

        parent = next((c for c in smart_collections if c['handle'] == handle.replace('-homepage','')), None)
        if not parent:
            print(f"❌ No parent found for {homepage['handle']}")
            continue

        homepage_id = homepage['id']
        parent_id   = parent['id']

        # Clear (keep going on failures)
        for c in get_collects_for_collection(homepage_id):
            ok = delete_collect(c['id'])
            if not ok:
                print(f"⚠️ delete_collect failed for collect_id={c['id']} (pid={c.get('product_id')})")

        # Fill
        parent_products = get_products_in_collection(parent_id)
        product_ids = [p['id'] for p in parent_products]
        random.shuffle(product_ids)
        for pid in product_ids[:NUM_PRODUCTS]:
            try:
                create_collect(pid, homepage_id)
                time.sleep(0.12)  # gentle pacing
            except Exception as e:
                print(f"⚠️ create_collect failed for pid={pid}: {e}")

        print(f"✅ Synced {homepage['title']} with {min(NUM_PRODUCTS, len(product_ids))} products")



def get_index_data(theme_id):
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/themes/{theme_id}/assets.json"
    params = {"asset[key]": "templates/index.json"}
    _, data = _req_json("GET", url, params=params)
    raw = data["asset"]["value"]
    return json.loads(raw)

def update_index_data(index_data, theme_id):
    payload = {"asset": {"key": "templates/index.json",
                         "value": json.dumps(index_data, indent=2)}}
    url = f"https://{SHOPIFY_DOMAIN}/admin/api/2024-04/themes/{theme_id}/assets.json"
    _, _ = _req_json("PUT", url, json=payload)  # <-- PUT, not POST
    print("✅ index.json updated.", flush=True)

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
        "collection_block_R4WJRB",  # Booster Boxes
        "collection_block_7mejda",  # Booster Packs
        "collection_block_GEemDK",  # Collection Boxes
        "collection_block_HDN8mG",  # Sleeved Booster Packs
        "collection_block_zP3pHm",  # Blister Packs

    }

    ip_blocks = {
        "collection_block_CVm8CT",  # MTG
        "collection_block_aM6Yrr",  # Lorcana
        "collection_block_CdCAWt",  # One Piece
        "collection_block_EexjnG",  # All Pokémon
    }

    curated_blocks = {
        "collection_block_WfYULP",  # Hot Pokémon Sets
        "collection_block_HPihgx",  # Premium Sealed
        "collection_block_j6Dtjp",  # Modern Sealed
        "collection_block_EVY4XF",  # Newly Added
        "collection_block_pbWfhy",  # Graded Cards
        "collection_block_yegXtE",  # Clearance
        "collection_block_zLMMbE",  # International Version
        "collection_block_mkzLL3",  # Damaged Goods
        "collection_block_rdJWyC",  # Sealed Cases
    }

    fandom_blocks = {
        "collection_block_pcfXCh",  # Charizard Fans
        "collection_block_JMKRJ7",  # Pikachu Fans
        "collection_block_eDYNx4",  # Eevee Fans
    }

    era_blocks = {
        "collection_block_MUHGzi",  # Mega Evolution
        "collection_block_fpHhpC",  # Scarlet & Violet
        "collection_block_W6eamh",  # Sword & Shield
        "collection_block_nNUrFC",  # Sun & Moon
        "collection_block_QdnMGm",  # X&Y
        "collection_block_JeGbi3",  # Vintage
    }

    seasonal_blocks = {
        "collection_block_tXiMKf",  # Vault Worthy
        "collection_block_qRRdLA",  # New Trainers
        "collection_block_7xX4Ge",  # Just For Fun
    }

    def _pick(keys, block_map, k):
        pool = list(keys & block_map.keys())
        k = min(k, len(pool))
        return random.sample(pool, k) if k > 0 else []

    # Sample according to strategy
    selected = []
    selected += _pick(product_type_blocks, block_map, 2)
    # selected += _pick(ip_blocks, block_map, 1)  # still optional
    selected += _pick(curated_blocks, block_map, 1)
    selected += _pick(seasonal_blocks, block_map, 1)
    selected += _pick(era_blocks, block_map, 1)
    selected += _pick(fandom_blocks, block_map, 1)

    random.shuffle(selected)
    existing_order = index_data['sections'][section_id]['block_order']
    remaining = [b for b in existing_order if b not in selected]
    index_data['sections'][section_id]['block_order'] = selected + remaining

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


index_data = get_index_data(THEME_ID)
shuffled_data = curated_random_carousel_blocks(json.dumps(index_data))
update_index_data(json.loads(shuffled_data), THEME_ID)
sync_homepage_collections()
_, smart_collections = get_collections()
sync_featured_picks_for_all_collections(smart_collections)