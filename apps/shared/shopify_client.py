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


def _extract_committed(variant: dict) -> int:
    """Pull committed quantity from inventoryItem.inventoryLevels quantities."""
    try:
        levels = (variant.get("inventoryItem") or {}).get("inventoryLevels", {}).get("edges", [])
        if not levels:
            return 0
        for q in levels[0]["node"].get("quantities", []):
            if q.get("name") == "committed":
                return int(q.get("quantity", 0))
    except Exception:
        pass
    return 0


class ShopifyError(Exception):
    """Raised when Shopify API returns errors."""
    pass


class ShopifyClient:
    """Lightweight Shopify Admin GraphQL + REST client."""

    def __init__(self, token: str, store: str, api_version: str = "2025-10"):
        self.token = token
        self.store = store
        self.api_version = api_version
        self._location_id: str | None = None
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
          products(first: $first, after: $cursor, query: "status:active OR status:draft") {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                id title handle status tags
                variants(first: 10) {
                  edges { node { id price sku inventoryQuantity
                    inventoryItem { id
                      unitCost { amount }
                      inventoryLevels(first: 1) {
                        edges { node { quantities(names: ["committed"]) { name quantity } } }
                      }
                    }
                  } }
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
                    unit_cost = None
                    if variant.get("inventoryItem"):
                        inv_item_id = variant["inventoryItem"]["id"].split("/")[-1]
                        cost_data = variant["inventoryItem"].get("unitCost")
                        if cost_data and cost_data.get("amount"):
                            try:
                                unit_cost = float(cost_data["amount"])
                            except (ValueError, TypeError):
                                pass
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
                        "committed":          _extract_committed(variant),
                        "tcgplayer_id":       tcg_id,
                        "is_damaged":         is_damaged,
                        "tags_csv":           tags_csv,
                        "unit_cost":          unit_cost,
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


    # ─── Location ───────────────────────────────────────────────────────────────

    def get_location_id(self) -> str:
        """Get the primary location ID for inventory operations (cached)."""
        if self._location_id:
            return self._location_id
        data = self._gql("{ locations(first: 1) { edges { node { id } } } }")
        edges = data.get("locations", {}).get("edges", [])
        if not edges:
            raise ShopifyError("No locations found in store")
        self._location_id = edges[0]["node"]["id"]
        return self._location_id

    # ─── Inventory item lookups ──────────────────────────────────────────────────

    def get_inventory_item_id(self, variant_id: int) -> str | None:
        """Look up the inventory item GID for a variant."""
        variant_gid = f"gid://shopify/ProductVariant/{variant_id}"
        data = self._gql("""
            query($id: ID!) {
                productVariant(id: $id) { inventoryItem { id } }
            }
        """, {"id": variant_gid})
        inv = data.get("productVariant", {}).get("inventoryItem", {})
        return inv.get("id", "").split("/")[-1] or None

    def get_inventory_item_cost_and_qty(self, inventory_item_id: str) -> tuple[float | None, int]:
        """Fetch current unitCost and total on_hand qty for an inventory item.
        Uses on_hand (not available) so committed orders don't skew COGS weighted average."""
        inv_gid = f"gid://shopify/InventoryItem/{inventory_item_id}"
        data = self._gql("""
            query($id: ID!) {
                inventoryItem(id: $id) {
                    unitCost { amount }
                    inventoryLevels(first: 10) {
                        edges { node { quantities(names: ["on_hand"]) { name quantity } } }
                    }
                }
            }
        """, {"id": inv_gid})
        item = data.get("inventoryItem") or {}
        cost_data = item.get("unitCost")
        unit_cost = float(cost_data["amount"]) if cost_data and cost_data.get("amount") else None
        levels = item.get("inventoryLevels", {}).get("edges", [])
        current_qty = 0
        for edge in levels:
            for q in edge.get("node", {}).get("quantities", []):
                if q.get("name") == "on_hand":
                    current_qty += q.get("quantity", 0)
        return unit_cost, current_qty

    def set_unit_cost(self, inventory_item_id: str, unit_cost: float) -> dict:
        """Set the unit cost (COGS) on an inventory item."""
        inv_gid = f"gid://shopify/InventoryItem/{inventory_item_id}"
        mutation = """
        mutation inventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
            inventoryItemUpdate(id: $id, input: $input) {
                inventoryItem { id unitCost { amount } }
                userErrors { field message }
            }
        }
        """
        data = self._gql(mutation, {"id": inv_gid, "input": {"cost": str(round(unit_cost, 2))}})
        errors = data.get("inventoryItemUpdate", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Set unit cost failed: {errors}")
        logger.info(f"Set unit cost for {inventory_item_id} to {unit_cost:.2f}")
        return data

    def adjust_inventory(self, inventory_item_id: str, qty_delta: int, reason: str = "correction") -> dict:
        """Adjust inventory quantity by a delta amount."""
        location_id = self.get_location_id()
        mutation = """
        mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
          inventoryAdjustQuantities(input: $input) {
            inventoryAdjustmentGroup { reason }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": {
                "reason": reason,
                "name": "available",
                "changes": [{
                    "delta": qty_delta,
                    "inventoryItemId": f"gid://shopify/InventoryItem/{inventory_item_id}",
                    "locationId": location_id,
                }]
            }
        }
        data = self._gql(mutation, variables)
        errors = data.get("inventoryAdjustQuantities", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Inventory adjust failed: {errors}")
        logger.info(f"Adjusted inventory {inventory_item_id} by {qty_delta}")
        return data

    def set_inventory_quantity(self, inventory_item_id: str, quantity: int) -> dict:
        """Set inventory to an exact quantity."""
        location_id = self.get_location_id()
        mutation = """
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
          inventorySetQuantities(input: $input) {
            inventoryAdjustmentGroup { reason }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": {
                "reason": "correction",
                "name": "available",
                "ignoreCompareQuantity": True,
                "quantities": [{
                    "inventoryItemId": f"gid://shopify/InventoryItem/{inventory_item_id}",
                    "locationId": location_id,
                    "quantity": quantity,
                }]
            }
        }
        data = self._gql(mutation, variables)
        errors = data.get("inventorySetQuantities", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Set inventory failed: {errors}")
        logger.info(f"Set inventory {inventory_item_id} to {quantity}")
        return data

    # ─── Product management ──────────────────────────────────────────────────────

    def duplicate_product_as_damaged(self, product_gid: str, new_title: str) -> dict:
        """Duplicate a product for damaged sales."""
        mutation = """
        mutation productDuplicate($productId: ID!, $newTitle: String!) {
          productDuplicate(productId: $productId, newTitle: $newTitle, includeImages: true) {
            newProduct {
              id title handle
              variants(first: 5) {
                edges { node { id price inventoryQuantity inventoryItem { id } } }
              }
            }
            userErrors { field message }
          }
        }
        """
        data = self._gql(mutation, {"productId": product_gid, "newTitle": new_title})
        errors = data.get("productDuplicate", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Product duplicate failed: {errors}")
        new_product = data["productDuplicate"]["newProduct"]
        logger.info(f"Duplicated product as '{new_title}' -> {new_product['id']}")
        return new_product

    def add_tags(self, product_gid: str, tags: list[str]) -> dict:
        """Add tags to a product."""
        mutation = """
        mutation tagsAdd($id: ID!, $tags: [String!]!) {
          tagsAdd(id: $id, tags: $tags) { userErrors { field message } }
        }
        """
        data = self._gql(mutation, {"id": product_gid, "tags": tags})
        errors = data.get("tagsAdd", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Tags add failed: {errors}")
        return data

    def update_variant_price_gql(self, product_gid: str, variant_gid: str, new_price: float) -> dict:
        """Update a variant price via GraphQL bulk mutation (used by ingestion)."""
        mutation = """
        mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id price }
            userErrors { field message }
          }
        }
        """
        data = self._gql(mutation, {
            "productId": product_gid,
            "variants": [{"id": variant_gid, "price": str(new_price)}]
        })
        errors = data.get("productVariantsBulkUpdate", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Price update failed: {errors}")
        return data

    def create_product(self, title: str, price: float, sku: str = None,
                       tags: list[str] = None, tcgplayer_id: int = None,
                       quantity: int = 0) -> dict:
        """Create a new product with a single variant using productSet (2025+ API)."""
        mutation = """
        mutation productSet($input: ProductSetInput!) {
          productSet(input: $input) {
            product {
              id title handle
              variants(first: 1) {
                edges { node { id price inventoryQuantity inventoryItem { id } } }
              }
            }
            userErrors { field message }
          }
        }
        """
        product_input = {
            "title": title,
            "productOptions": [{"name": "Title", "position": 1, "values": [{"name": "Default Title"}]}],
            "variants": [{"optionValues": [{"optionName": "Title", "name": "Default Title"}], "price": str(price)}],
        }
        if tags:
            product_input["tags"] = tags
        if sku:
            product_input["variants"][0]["sku"] = sku

        data = self._gql(mutation, {"input": product_input})
        errors = data.get("productSet", {}).get("userErrors", [])
        if errors:
            raise ShopifyError(f"Product create failed: {errors}")

        product = data["productSet"]["product"]

        if tcgplayer_id:
            self._set_metafield(product["id"], "tcg", "tcgplayer_id", str(tcgplayer_id))

        if quantity > 0:
            var_node = product["variants"]["edges"][0]["node"]
            inv_id = var_node.get("inventoryItem", {}).get("id", "").split("/")[-1]
            if inv_id:
                self.set_inventory_quantity(inv_id, quantity)

        logger.info(f"Created product '{title}' -> {product['id']}")
        return product

    def _set_metafield(self, owner_id: str, namespace: str, key: str, value: str):
        """Set a metafield on a resource."""
        mutation = """
        mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { id }
            userErrors { field message }
          }
        }
        """
        self._gql(mutation, {"metafields": [{
            "ownerId": owner_id,
            "namespace": namespace,
            "key": key,
            "value": value,
            "type": "single_line_text_field",
        }]})

    # ─── Inventory REST helpers (used by inventory app) ──────────────────────────

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


