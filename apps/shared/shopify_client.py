"""
shared/shopify_client.py

Shopify GraphQL + REST client shared across intake, ingestion, and inventory.

Single source of truth — do not copy this into individual apps.
Each app's Dockerfile adds /app/shared to PYTHONPATH.

Env vars (read by callers, not here):
    SHOPIFY_TOKEN  — Admin API access token
    SHOPIFY_STORE  — Store domain (e.g. my-store.myshopify.com)
"""

import os
import time
import logging
import requests

logger = logging.getLogger(__name__)


class ShopifyClient:
    """Lightweight Shopify Admin GraphQL + REST client."""

    def __init__(self, token: str, store: str, api_version: str = "2025-10"):
        self.token = token
        self.store = store
        self.api_version = api_version
        self.endpoint = f"https://{store}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        }

    # ─── Low-level GraphQL ──────────────────────────────────────────────────

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

    # ─── Low-level REST ─────────────────────────────────────────────────────

    def _rest_headers(self) -> dict:
        return {
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json",
        }

    def _rest(self, method: str, path: str, **kwargs) -> dict:
        url = f"https://{self.store}/admin/api/{self.api_version}{path}"
        resp = requests.request(method, url, headers=self._rest_headers(), timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()

    # ─── Staleness signals ──────────────────────────────────────────────────

    def get_cache_staleness_signals(self) -> dict:
        """
        Cheap two-request check: latest order number + latest product updated_at.
        Used by CacheManager to decide if a background refresh is needed.
        """
        base = f"https://{self.store}/admin/api/{self.api_version}"
        headers = {"X-Shopify-Access-Token": self.token}
        signals = {}

        try:
            r = requests.get(f"{base}/orders.json",
                             params={"limit": 1, "status": "any", "fields": "order_number"},
                             headers=headers, timeout=10)
            r.raise_for_status()
            orders = r.json().get("orders", [])
            if orders:
                signals["latest_order_number"] = orders[0]["order_number"]
        except Exception as e:
            logger.warning(f"Order number fetch failed: {e}")

        try:
            r = requests.get(f"{base}/products.json",
                             params={"limit": 1, "order": "updated_at desc", "fields": "updated_at"},
                             headers=headers, timeout=10)
            r.raise_for_status()
            products = r.json().get("products", [])
            if products:
                signals["latest_product_updated_at"] = products[0]["updated_at"]
        except Exception as e:
            logger.warning(f"Product updated_at fetch failed: {e}")

        return signals

    # ─── Product pagination ─────────────────────────────────────────────────

    def iter_products_pages(self, batch_size: int = 100):
        """
        Generator that yields (page_products, has_more) tuples one page at a time.

        Each product dict includes:
            product_gid, shopify_product_id, title, handle, status,
            variant_id, shopify_price, shopify_qty, sku,
            inventory_item_id, tcgplayer_id, is_damaged, tags_csv
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

                # Extract TCGPlayer ID from metafields
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
                tags_csv = ", ".join(tags) if isinstance(tags, list) else (tags or "")
                is_damaged = (
                    "damaged" in [t.lower() for t in (tags if isinstance(tags, list) else [])]
                    or "[DAMAGED]" in node.get("title", "").upper()
                )

                for var_edge in node["variants"]["edges"]:
                    variant = var_edge["node"]
                    inv_item_id = None
                    if variant.get("inventoryItem"):
                        inv_item_id = variant["inventoryItem"]["id"].split("/")[-1]
                    page_products.append({
                        "product_gid":        node["id"],
                        "shopify_product_id": int(node["id"].split("/")[-1]),
                        "title":              node["title"],
                        "handle":             node["handle"],
                        "status":             node.get("status", "ACTIVE"),
                        "variant_id":         int(variant["id"].split("/")[-1]),
                        "shopify_price":      float(variant["price"]),
                        "shopify_qty":        variant["inventoryQuantity"],
                        "sku":                variant.get("sku"),
                        "inventory_item_id":  inv_item_id,
                        "tcgplayer_id":       tcg_id,
                        "is_damaged":         is_damaged,
                        "tags_csv":           tags_csv,
                    })

            has_next = data["products"]["pageInfo"]["hasNextPage"]
            cursor = data["products"]["pageInfo"].get("endCursor") if has_next else None
            yield page_products, has_next
            if has_next:
                time.sleep(0.3)

    def get_all_products(self, batch_size: int = 100) -> list[dict]:
        """Convenience wrapper — returns flat list of all product dicts."""
        products = []
        for page, _ in self.iter_products_pages(batch_size=batch_size):
            products.extend(page)
        logger.info(f"Fetched {len(products)} variants from Shopify")
        return products

    # ─── Inventory REST helpers ─────────────────────────────────────────────

    def update_variant_price(self, variant_id: int, price: float) -> None:
        """Update a single variant's price."""
        self._rest("PUT", f"/variants/{variant_id}.json", json={
            "variant": {"id": variant_id, "price": f"{price:.2f}"}
        })

    def set_inventory_level(self, inventory_item_id: int, location_id: int, qty: int) -> None:
        """Set available quantity for an inventory item at a location."""
        self._rest("POST", "/inventory_levels/set.json", json={
            "location_id":        location_id,
            "inventory_item_id":  inventory_item_id,
            "available":          qty,
        })

    def create_draft_product_stub(self, name: str) -> dict:
        """
        Create a minimal DRAFT Shopify product with no enrichment.
        Used by inventory for slabs / accessories without a TCGPlayer ID.
        Returns: { product_id, variant_id, inventory_item_id }
        """
        result = self._rest("POST", "/products.json", json={
            "product": {
                "title":    name,
                "status":   "draft",
                "variants": [{"inventory_management": "shopify"}],
            }
        })
        product = result["product"]
        variant = product["variants"][0]
        return {
            "product_id":        product["id"],
            "variant_id":        variant["id"],
            "inventory_item_id": variant.get("inventory_item_id"),
        }


class ShopifyError(Exception):
    pass
