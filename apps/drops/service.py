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

FOUNDERS_PICKS_Q = """
query FoundersPicks($query: String!, $first: Int!) {
  products(first: $first, query: $query) {
    edges {
      node {
        id
        title
        tags
        totalInventory
        metafields(first: 10, namespace: "custom") {
          edges {
            node { key value }
          }
        }
        featuredMedia {
          preview { image { url } }
        }
        variants(first: 1) {
          edges {
            node { id price }
          }
        }
      }
    }
  }
}
"""

METAFIELDS_DELETE = """
mutation MetafieldsDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields { ownerId namespace key }
    userErrors { field message }
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
               vip_price_cents: int = None, limit_qty: int = None):
    """
    Full drop setup:
    1. Set drop price
    2. Add unavailable + drop + weekly-deals tags (+ vip-drop for VIP, + limit-X)
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
    tags = [unavail_tag, "drop", "weekly deals"]
    if drop_type == "vip":
        tags.append("vip-drop")
    if limit_qty:
        tags.append(f"limit-{limit_qty}")
    add_tags(product_gid, tags)

    # 3. Remove from non-Online channels
    unpublish_from_non_online(product_gid)

    # 4. VIP metafield
    if drop_type == "vip" and vip_price_cents:
        set_vip_price_cents(product_gid, vip_price_cents)

    return {"unavail_tag": unavail_tag, "tags_added": tags}


FOUNDERS = ["sean", "stuart", "kayla", "hayley"]


def get_founders_picks() -> list:
    """Fetch all products currently tagged as a founder's pick."""
    query_str = " OR ".join(f"tag:{f}" for f in FOUNDERS)
    data = shopify_gql(FOUNDERS_PICKS_Q, {"query": query_str, "first": 100})
    results = []
    for edge in data.get("data", {}).get("products", {}).get("edges", []):
        node = edge["node"]
        # Only include if product actually has a founder tag
        tags_lower = [t.lower() for t in node.get("tags", [])]
        founder = None
        for f in FOUNDERS:
            if f in tags_lower:
                founder = f
                break
        if not founder:
            continue

        # Extract metafields
        mf_founder = None
        mf_note = None
        for me in node.get("metafields", {}).get("edges", []):
            mn = me["node"]
            if mn["key"] == "founder":
                mf_founder = mn["value"]
            elif mn["key"] == "founder_note":
                mf_note = mn["value"]

        img = None
        if node.get("featuredMedia", {}) and node["featuredMedia"].get("preview", {}).get("image"):
            img = node["featuredMedia"]["preview"]["image"]["url"]

        v = {}
        for ve in node.get("variants", {}).get("edges", []):
            v = ve["node"]
            break

        results.append({
            "product_gid": node["id"],
            "numeric_id": int(gid_numeric(node["id"])),
            "title": node["title"],
            "tags": node.get("tags", []),
            "total_inventory": node.get("totalInventory", 0),
            "founder": mf_founder or founder,
            "founder_note": mf_note or "",
            "image_url": img,
            "price": float(v.get("price", 0)) if v else 0,
        })
    return results


def set_founder_pick(product_gid: str, founder: str, note: str):
    """Make a product a founder's pick: add tag + set metafields."""
    founder = founder.lower().strip()
    if founder not in FOUNDERS:
        raise ValueError(f"Invalid founder: {founder}. Must be one of {FOUNDERS}")

    # 1. Add founder name tag
    add_tags(product_gid, [founder])

    # 2. Set metafields
    shopify_gql(METAFIELD_SET, {"metafields": [
        {
            "ownerId": product_gid,
            "namespace": "custom",
            "key": "founder",
            "value": founder,
            "type": "single_line_text_field",
        },
        {
            "ownerId": product_gid,
            "namespace": "custom",
            "key": "founder_note",
            "value": note,
            "type": "multi_line_text_field",
        },
    ]})
    return {"ok": True, "founder": founder}


def remove_founder_pick(product_gid: str):
    """Remove founder's pick status: remove founder tags + delete metafields."""
    # 1. Get current tags and remove any founder tags
    tags = get_product_tags(product_gid)
    founder_tags = [t for t in tags if t.lower() in FOUNDERS]
    if founder_tags:
        remove_tags(product_gid, founder_tags)

    # 2. Delete founder metafields
    data = shopify_gql(METAFIELDS_DELETE, {"metafields": [
        {"ownerId": product_gid, "namespace": "custom", "key": "founder"},
        {"ownerId": product_gid, "namespace": "custom", "key": "founder_note"},
    ]})
    errs = data.get("data", {}).get("metafieldsDelete", {}).get("userErrors", [])
    if errs:
        logger.warning(f"Metafield delete warnings for {product_gid}: {errs}")

    return {"ok": True, "tags_removed": founder_tags}


def release_drop(product_gid: str):
    """
    Release a drop:
    1. Get current tags
    2. Remove unavailable-* and drop tags (keep vip-drop)
    3. Publish to all channels
    """
    tags = get_product_tags(product_gid)
    to_remove = [t for t in tags if (
        t.lower().startswith("unavailable-")
        or t.lower() == "drop"
    )]
    if to_remove:
        remove_tags(product_gid, to_remove)
    publish_to_all(product_gid)
    return {"tags_removed": to_remove}
