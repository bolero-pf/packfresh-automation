"""
Drop planner Shopify operations.

Handles: product search, price updates, tag management,
sales channel publishing/unpublishing, metafield setting.
"""

import os
import logging
import time
from datetime import datetime

from shopify_graphql import shopify_gql, gid_numeric

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# GraphQL Queries & Mutations
# ═══════════════════════════════════════════════════════════════════════════════

SEARCH_PRODUCTS_Q = """
query SearchProducts($query: String!, $first: Int!) {
  products(first: $first, query: $query) {
    edges {
      node {
        id
        title
        status
        tags
        totalInventory
        variants(first: 10) {
          edges {
            node {
              id
              price
              compareAtPrice
              inventoryQuantity
              sku
            }
          }
        }
        featuredMedia {
          preview { image { url } }
        }
      }
    }
  }
}
"""

TAGS_ADD = """
mutation TagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node { ... on Product { id tags } }
    userErrors { field message }
  }
}
"""

TAGS_REMOVE = """
mutation TagsRemove($id: ID!, $tags: [String!]!) {
  tagsRemove(id: $id, tags: $tags) {
    node { ... on Product { id tags } }
    userErrors { field message }
  }
}
"""

VARIANT_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price }
    userErrors { field message }
  }
}
"""

PUBLICATIONS_Q = """
query { publications(first: 50) { nodes { id name } } }
"""

PUBLISH = """
mutation PublishablePublish($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

UNPUBLISH = """
mutation PublishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
  publishableUnpublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

METAFIELD_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""

PRODUCT_TAGS_Q = """
query ProductTags($id: ID!) {
  product(id: $id) {
    id
    tags
  }
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Operations
# ═══════════════════════════════════════════════════════════════════════════════

def search_products(query: str, limit: int = 20) -> list:
    data = shopify_gql(SEARCH_PRODUCTS_Q, {"query": query, "first": limit})
    results = []
    for edge in data.get("data", {}).get("products", {}).get("edges", []):
        node = edge["node"]
        variants = []
        for ve in node.get("variants", {}).get("edges", []):
            v = ve["node"]
            variants.append({
                "id": v["id"],
                "numeric_id": int(gid_numeric(v["id"])),
                "price": float(v["price"]),
                "compare_at_price": float(v["compareAtPrice"]) if v.get("compareAtPrice") else None,
                "qty": v.get("inventoryQuantity", 0),
                "sku": v.get("sku"),
            })
        img = None
        if node.get("featuredMedia", {}) and node["featuredMedia"].get("preview", {}).get("image"):
            img = node["featuredMedia"]["preview"]["image"]["url"]
        results.append({
            "id": node["id"],
            "numeric_id": int(gid_numeric(node["id"])),
            "title": node["title"],
            "status": node["status"],
            "tags": node.get("tags", []),
            "total_inventory": node.get("totalInventory", 0),
            "variants": variants,
            "image_url": img,
        })
    return results


def set_drop_price(product_gid: str, variant_gid: str, price: float):
    data = shopify_gql(VARIANT_UPDATE, {
        "productId": product_gid,
        "variants": [{"id": variant_gid, "price": str(round(price, 2))}],
    })
    errs = data.get("data", {}).get("productVariantsBulkUpdate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"Price update failed: {errs}")


def add_tags(product_gid: str, tags: list[str]):
    data = shopify_gql(TAGS_ADD, {"id": product_gid, "tags": tags})
    errs = data.get("data", {}).get("tagsAdd", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"Tag add failed: {errs}")


def remove_tags(product_gid: str, tags: list[str]):
    data = shopify_gql(TAGS_REMOVE, {"id": product_gid, "tags": tags})
    errs = data.get("data", {}).get("tagsRemove", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"Tag remove failed: {errs}")


def get_product_tags(product_gid: str) -> list[str]:
    data = shopify_gql(PRODUCT_TAGS_Q, {"id": product_gid})
    return data.get("data", {}).get("product", {}).get("tags", [])


def set_vip_price_cents(product_gid: str, price_cents: int):
    """Set the custom.vip_price_cents metafield for VIP/MSRP drops."""
    data = shopify_gql(METAFIELD_SET, {"metafields": [{
        "ownerId": product_gid,
        "namespace": "custom",
        "key": "vip_price_cents",
        "value": str(price_cents),
        "type": "number_integer",
    }]})
    errs = data.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"Metafield set failed: {errs}")


_pub_cache = None

def _get_publications():
    global _pub_cache
    if _pub_cache is None:
        data = shopify_gql(PUBLICATIONS_Q)
        _pub_cache = data.get("data", {}).get("publications", {}).get("nodes", [])
    return _pub_cache


def unpublish_from_non_online(product_gid: str):
    """Remove product from all sales channels EXCEPT Online Store."""
    pubs = _get_publications()
    to_unpublish = [p for p in pubs if "online store" not in (p.get("name") or "").lower()]
    if to_unpublish:
        shopify_gql(UNPUBLISH, {
            "id": product_gid,
            "input": [{"publicationId": p["id"]} for p in to_unpublish],
        })


def publish_to_all(product_gid: str):
    """Publish product to all sales channels."""
    pubs = _get_publications()
    if pubs:
        shopify_gql(PUBLISH, {
            "id": product_gid,
            "input": [{"publicationId": p["id"]} for p in pubs],
        })


def setup_drop(product_gid: str, variant_gid: str, drop_date: str,
               drop_price: float, drop_type: str = "weekly",
               vip_price_cents: int = None):
    """
    Full drop setup:
    1. Set drop price
    2. Add unavailable + drop tags (+ vip-drop for VIP type)
    3. Remove from non-Online Store channels
    4. Set VIP metafield if VIP drop
    """
    # Parse date for tag
    from datetime import datetime as _dt
    dt = _dt.strptime(drop_date, "%Y-%m-%d")
    month_name = dt.strftime("%B").lower()
    day = dt.day
    unavail_tag = f"unavailable-{month_name}-{day}"

    # 1. Set price
    set_drop_price(product_gid, variant_gid, drop_price)

    # 2. Tags
    tags = [unavail_tag, "drop"]
    if drop_type == "vip":
        tags.append("vip-drop")
    add_tags(product_gid, tags)

    # 3. Remove from non-Online channels
    unpublish_from_non_online(product_gid)

    # 4. VIP metafield
    if drop_type == "vip" and vip_price_cents:
        set_vip_price_cents(product_gid, vip_price_cents)

    return {"unavail_tag": unavail_tag, "tags_added": tags}


def release_drop(product_gid: str):
    """
    Release a drop:
    1. Get current tags
    2. Remove unavailable-* and drop tags (keep vip-drop)
    3. Publish to all channels
    """
    tags = get_product_tags(product_gid)
    to_remove = [t for t in tags if t.lower().startswith("unavailable-") or t.lower() == "drop"]
    if to_remove:
        remove_tags(product_gid, to_remove)
    publish_to_all(product_gid)
    return {"tags_removed": to_remove}
