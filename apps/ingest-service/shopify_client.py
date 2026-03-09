"""
Shopify GraphQL client for the ingest service.
Queries store products by TCGPlayer ID metafield to check inventory and pricing.

Env vars:
    SHOPIFY_TOKEN  — Admin API access token
    SHOPIFY_STORE  — Store domain (e.g., my-store.myshopify.com)
"""

import os
import time
import logging
import requests

logger = logging.getLogger(__name__)


class ShopifyClient:
    """Lightweight Shopify Admin GraphQL client."""

    def __init__(self, token: str, store: str, api_version: str = "2025-10"):
        self.token = token
        self.store = store
        self.endpoint = f"https://{store}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        }

    # ─── Low-level GraphQL ──────────────────────────────────────────

    def _gql(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query and return the data payload."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        resp = requests.post(self.endpoint, headers=self.headers, json=payload, timeout=30)
        logger.info(f"Shopify GraphQL: status={resp.status_code}")
        resp.raise_for_status()
        body = resp.json()
        if "errors" in body:
            logger.error(f"Shopify GraphQL errors: {body['errors']}")
            raise ShopifyError(f"GraphQL errors: {body['errors']}")
        return body.get("data", {})

    # ─── Fetch all products with TCG metafields ─────────────────────

    def iter_products_pages(self, batch_size: int = 100):
        """
        Generator that yields (page_products, has_more) tuples one page at a time.
        Lets callers stream/yield heartbeats between pages without buffering everything.
        """
        query = """
        query getProducts($first: Int!, $cursor: String) {
          products(first: $first, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title handle status tags
                variants(first: 10) {
                  edges { node { id price sku inventoryQuantity inventoryItem { id } } }
                }
                metafields(namespace: "tcg", first: 5) {
                  edges { node { key value } }
                }
              }
            }
          }
        }
        """
        cursor = None
        has_next = True
        while has_next:
            data = self._gql(query, {"first": batch_size, "cursor": cursor})
            edges = data["products"]["edges"]
            page_products = []
            for edge in edges:
                node = edge["node"]
                tcg_id = None
                for mf in (node.get("metafields") or {}).get("edges", []):
                    if mf["node"]["key"] == "tcgplayer_id":
                        val = mf["node"]["value"]
                        if isinstance(val, str) and val.startswith("["):
                            val = val.strip("[]").replace('"', "").replace("'", "")
                        try:
                            tcg_id = int(val) if val else None
                        except (ValueError, TypeError):
                            tcg_id = None
                        break
                tags = node.get("tags", [])
                is_damaged = ("damaged" in [t.lower() for t in tags]) or "[DAMAGED]" in node.get("title", "").upper()
                for var_edge in node["variants"]["edges"]:
                    variant = var_edge["node"]
                    inv_item_id = None
                    if variant.get("inventoryItem"):
                        inv_item_id = variant["inventoryItem"]["id"].split("/")[-1]
                    page_products.append({
                        "product_gid": node["id"],
                        "shopify_product_id": int(node["id"].split("/")[-1]),
                        "title": node["title"],
                        "handle": node["handle"],
                        "status": node.get("status", "ACTIVE"),
                        "variant_id": int(variant["id"].split("/")[-1]),
                        "shopify_price": float(variant["price"]),
                        "shopify_qty": variant["inventoryQuantity"],
                        "sku": variant.get("sku"),
                        "inventory_item_id": inv_item_id,
                        "tcgplayer_id": tcg_id,
                        "is_damaged": is_damaged,
                    })
            has_next = data["products"]["pageInfo"]["hasNextPage"]
            cursor = data["products"]["pageInfo"].get("endCursor") if has_next else None
            yield page_products, has_next
            if has_next:
                time.sleep(0.3)

    def get_cache_staleness_signals(self) -> dict:
        """
        Cheap single-request check for cache staleness signals.
        Returns latest order number and latest product updated_at.
        One REST call each — fast enough to run on every cache read.
        """
        import requests as _req
        base = f"https://{self.store}/admin/api/2025-10"
        headers = {"X-Shopify-Access-Token": self.token}

        signals = {}

        # Latest order number
        try:
            r = _req.get(f"{base}/orders.json",
                         params={"limit": 1, "status": "any", "fields": "order_number"},
                         headers=headers, timeout=10)
            r.raise_for_status()
            orders = r.json().get("orders", [])
            if orders:
                signals["latest_order_number"] = orders[0]["order_number"]
        except Exception as e:
            logger.warning(f"Order number fetch failed: {e}")

        # Latest product updated_at
        try:
            r = _req.get(f"{base}/products.json",
                         params={"limit": 1, "order": "updated_at desc", "fields": "updated_at"},
                         headers=headers, timeout=10)
            r.raise_for_status()
            products = r.json().get("products", [])
            if products:
                signals["latest_product_updated_at"] = products[0]["updated_at"]
        except Exception as e:
            logger.warning(f"Product updated_at fetch failed: {e}")

        return signals

    def get_all_products(self, batch_size: int = 100) -> list[dict]:
        """
        Paginate through all store products, returning a flat list with:
            product_gid, title, handle, variant_id, shopify_price,
            shopify_qty, sku, tcgplayer_id, inventory_item_id
        Mirrors the price_updater's get_shopify_products() structure.
        """
        query = """
        query getProducts($first: Int!, $cursor: String) {
          products(first: $first, after: $cursor) {
            pageInfo { hasNextPage }
            edges {
              cursor
              node {
                id
                title
                handle
                status
                tags
                variants(first: 10) {
                  edges {
                    node {
                      id
                      price
                      sku
                      inventoryQuantity
                      inventoryItem { id }
                    }
                  }
                }
                metafields(namespace: "tcg", first: 5) {
                  edges {
                    node { key value }
                  }
                }
              }
            }
          }
        }
        """
        products = []
        cursor = None
        has_next = True

        while has_next:
            data = self._gql(query, {"first": batch_size, "cursor": cursor})
            edges = data["products"]["edges"]

            for edge in edges:
                node = edge["node"]
                # Extract tcgplayer_id from metafields
                tcg_id = None
                mf_edges = (node.get("metafields") or {}).get("edges", [])
                if not products and not mf_edges:
                    # Log first product's raw metafields for debugging
                    logger.warning(f"First product '{node.get('title')}' has no metafield edges. Raw metafields: {node.get('metafields')}")
                for mf in mf_edges:
                    if mf["node"]["key"] == "tcgplayer_id":
                        val = mf["node"]["value"]
                        # Handle JSON array format: ["12345"] or plain "12345"
                        if isinstance(val, str) and val.startswith("["):
                            val = val.strip("[]").replace('"', '').replace("'", '')
                        try:
                            tcg_id = int(val) if val else None
                        except (ValueError, TypeError):
                            logger.warning(f"Could not parse tcgplayer_id '{val}' for {node.get('title')}")
                            tcg_id = None
                        break

                for var_edge in node["variants"]["edges"]:
                    variant = var_edge["node"]
                    inv_item_id = None
                    if variant.get("inventoryItem"):
                        inv_item_id = variant["inventoryItem"]["id"].split("/")[-1]

                    tags = node.get("tags", [])
                    is_damaged = ("damaged" in [t.lower() for t in tags]) or "[DAMAGED]" in node.get("title", "").upper()

                    products.append({
                        "product_gid": node["id"],
                        "shopify_product_id": int(node["id"].split("/")[-1]),
                        "title": node["title"],
                        "handle": node["handle"],
                        "status": node.get("status", "ACTIVE"),
                        "variant_id": int(variant["id"].split("/")[-1]),
                        "shopify_price": float(variant["price"]),
                        "shopify_qty": variant["inventoryQuantity"],
                        "sku": variant.get("sku"),
                        "inventory_item_id": inv_item_id,
                        "tcgplayer_id": tcg_id,
                        "is_damaged": is_damaged,
                    })

            has_next = data["products"]["pageInfo"]["hasNextPage"]
            if has_next:
                cursor = edges[-1]["cursor"]
                time.sleep(0.3)  # respect rate limits

        logger.info(f"Fetched {len(products)} variants from Shopify")
        return products


class ShopifyError(Exception):
    pass
