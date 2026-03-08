"""
Shopify GraphQL client for the ingest service.
Handles inventory adjustments, product duplication (damaged), and new listings.

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
    """Shopify Admin GraphQL client with mutation support."""

    def __init__(self, token: str, store: str, api_version: str = "2025-10"):
        self.token = token
        self.store = store
        self.endpoint = f"https://{store}/admin/api/{api_version}/graphql.json"
        self.headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": token,
        }
        self._location_id = None

    # ─── Low-level GraphQL ──────────────────────────────────────────

    def _gql(self, query: str, variables: dict = None) -> dict:
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

    # ─── Location ────────────────────────────────────────────────────

    def get_location_id(self) -> str:
        """Get the primary location ID for inventory operations."""
        if self._location_id:
            return self._location_id
        data = self._gql("{ locations(first: 1) { edges { node { id } } } }")
        edges = data.get("locations", {}).get("edges", [])
        if not edges:
            raise ShopifyError("No locations found in store")
        self._location_id = edges[0]["node"]["id"]
        return self._location_id

    # ─── Inventory ───────────────────────────────────────────────────

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
        """Fetch current unitCost and total available qty for an inventory item.
        Returns (unit_cost_or_None, current_qty).
        """
        inv_gid = f"gid://shopify/InventoryItem/{inventory_item_id}"
        data = self._gql("""
            query($id: ID!) {
                inventoryItem(id: $id) {
                    unitCost { amount }
                    inventoryLevels(first: 10) {
                        edges { node { quantities(names: ["available"]) { name quantity } } }
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
                if q.get("name") == "available":
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
        data = self._gql(mutation, {
            "id": inv_gid,
            "input": {"cost": str(round(unit_cost, 2))}
        })
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

    # ─── Product Duplication (for damaged variants) ──────────────────

    def duplicate_product_as_damaged(self, product_gid: str, new_title: str) -> dict:
        """
        Duplicate a product for damaged sales.
        Returns the new product data with variant info.
        """
        mutation = """
        mutation productDuplicate($productId: ID!, $newTitle: String!) {
          productDuplicate(productId: $productId, newTitle: $newTitle, includeImages: true) {
            newProduct {
              id
              title
              handle
              variants(first: 5) {
                edges {
                  node {
                    id
                    price
                    inventoryQuantity
                    inventoryItem { id }
                  }
                }
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

    def update_variant_price(self, product_gid: str, variant_gid: str, new_price: float) -> dict:
        """Update a variant's price."""
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

    # ─── Product Creation ────────────────────────────────────────────

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
                edges {
                  node { id price inventoryQuantity inventoryItem { id } }
                }
              }
            }
            userErrors { field message }
          }
        }
        """
        variant_input = {"price": str(price)}
        if sku:
            variant_input["sku"] = sku

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

        # Set tcgplayer_id metafield if provided
        if tcgplayer_id:
            self._set_metafield(product["id"], "tcg", "tcgplayer_id", str(tcgplayer_id))

        # Set inventory if requested
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


class ShopifyError(Exception):
    pass
