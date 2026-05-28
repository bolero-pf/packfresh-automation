"""
shared/shopify_client.py

Shopify GraphQL + REST client shared across intake, ingestion, inventory, events.

Single source of truth — do not copy this into individual apps.
Each app's Dockerfile adds /app/shared to PYTHONPATH.

Env vars (read by callers, not here):
    SHOPIFY_TOKEN  — Admin API access token
    SHOPIFY_STORE  — Store domain (e.g. my-store.myshopify.com)
"""

import os
import time
import json
import logging
import requests

logger = logging.getLogger(__name__)


# ─── Metaobject helpers (module-level utilities) ─────────────────────────────

def metaobject_fields_to_dict(node: dict) -> dict:
    """Flatten a Shopify metaobject node's `fields` list into {key: value}.
    Includes id/handle/displayName/type as top-level keys.

    Resolves File and Metaobject references when the query selected them. For
    File refs, value becomes {id, url, alt, width, height}. For Metaobject refs,
    value becomes {id, handle, type} (nested fields can be re-fetched if needed).

    Booleans ("true"/"false") are converted to Python bool.
    """
    out = {
        "id": node.get("id"),
        "handle": node.get("handle"),
        "displayName": node.get("displayName"),
        "type": node.get("type"),
    }
    for f in node.get("fields") or []:
        k = f.get("key")
        v = f.get("value")
        ref = f.get("reference")
        if ref:
            if "image" in ref:
                img = ref.get("image") or {}
                out[k] = {
                    "id": ref.get("id"),
                    "url": img.get("url"),
                    "alt": img.get("altText"),
                    "width": img.get("width"),
                    "height": img.get("height"),
                }
                continue
            if "handle" in ref and "type" in ref:
                out[k] = {
                    "id": ref.get("id"),
                    "handle": ref.get("handle"),
                    "type": ref.get("type"),
                }
                continue
        if v in ("true", "false"):
            out[k] = v == "true"
        else:
            out[k] = v
    return out


def build_metaobject_field_inputs(fields: dict) -> list[dict]:
    """Convert {key: value} into Shopify MetaobjectFieldInput list.

    Handles:
      - rich text / money values as dict/list → JSON-encoded string
      - booleans → 'true'/'false'
      - None values are skipped (avoids clobbering existing field with null)
    """
    out = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, bool):
            out.append({"key": k, "value": "true" if v else "false"})
        elif isinstance(v, (dict, list)):
            out.append({"key": k, "value": json.dumps(v)})
        else:
            out.append({"key": k, "value": str(v)})
    return out


def plain_text_to_rich_text(text: str) -> dict:
    """Convert plain text (blank-line paragraph breaks) into Shopify rich text JSON."""
    if not text:
        return {"type": "root", "children": []}
    paragraphs = [p.strip() for p in text.replace("\r\n", "\n").split("\n\n") if p.strip()]
    children = []
    for p in paragraphs:
        flat = " ".join(line.strip() for line in p.split("\n") if line.strip())
        children.append({
            "type": "paragraph",
            "children": [{"type": "text", "value": flat}],
        })
    return {"type": "root", "children": children}


def rich_text_to_plain(rich_value) -> str:
    """Best-effort extract plain text from a Shopify rich text JSON value."""
    if not rich_value:
        return ""
    try:
        tree = json.loads(rich_value) if isinstance(rich_value, str) else rich_value
    except Exception:
        return rich_value if isinstance(rich_value, str) else ""
    paragraphs = []

    def _walk(node):
        if not isinstance(node, dict):
            return ""
        t = node.get("type")
        if t == "text":
            return node.get("value", "")
        children = node.get("children") or []
        return "".join(_walk(c) for c in children)

    for child in (tree.get("children") or []):
        paragraphs.append(_walk(child))
    return "\n\n".join(p for p in paragraphs if p)


# ─── Factory for env-configured client (used by services) ────────────────────

def shopify_client_from_env() -> "ShopifyClient":
    """Build a ShopifyClient using SHOPIFY_TOKEN + SHOPIFY_STORE env vars.
    Raises RuntimeError if either is unset."""
    token = os.getenv("SHOPIFY_TOKEN")
    store = os.getenv("SHOPIFY_STORE")
    if not token or not store:
        raise RuntimeError("SHOPIFY_TOKEN and SHOPIFY_STORE env vars are required")
    return ShopifyClient(token, store)


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
                featuredImage { url }
                variants(first: 10) {
                  edges { node { id price sku barcode inventoryQuantity
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
                image_url = ((node.get("featuredImage") or {}).get("url")) or None
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
                        "barcode":            variant.get("barcode"),
                        "inventory_item_id":  inv_item_id,
                        "committed":          _extract_committed(variant),
                        "tcgplayer_id":       tcg_id,
                        "is_damaged":         is_damaged,
                        "tags_csv":           tags_csv,
                        "unit_cost":          unit_cost,
                        "image_url":          image_url,
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

    # ─── Metaobjects ─────────────────────────────────────────────────────────────

    # GraphQL fragment that resolves both File (MediaImage) and Metaobject references
    # in a single fields {} block. Harmless on fields that aren't references.
    _METAOBJECT_FIELDS_FRAGMENT = """
      id
      handle
      displayName
      type
      fields {
        key
        value
        reference {
          ... on MediaImage {
            id
            image { url altText width height }
          }
          ... on Metaobject {
            id
            handle
            type
          }
        }
      }
    """

    def metaobjects_list(self, type_str: str, first: int = 100) -> list[dict]:
        """List all metaobjects of a given type. Returns list of flattened dicts
        (see metaobject_fields_to_dict). Pages up to `first` (max 250)."""
        query = f"""
        query listMO($first: Int!) {{
          metaobjects(type: "{type_str}", first: $first) {{
            edges {{ node {{ {self._METAOBJECT_FIELDS_FRAGMENT} }} }}
          }}
        }}
        """
        data = self._gql(query, {"first": first})
        edges = (data.get("metaobjects") or {}).get("edges") or []
        return [metaobject_fields_to_dict(e["node"]) for e in edges]

    def metaobject_get(self, gid: str) -> dict | None:
        """Fetch a single metaobject by GID."""
        query = f"""
        query getMO($id: ID!) {{
          metaobject(id: $id) {{ {self._METAOBJECT_FIELDS_FRAGMENT} }}
        }}
        """
        data = self._gql(query, {"id": gid})
        node = data.get("metaobject")
        return metaobject_fields_to_dict(node) if node else None

    def metaobject_create(self, type_str: str, fields: dict) -> dict:
        """Create a metaobject. `fields` keys map to the metaobject's field handles."""
        mutation = """
        mutation createMO($metaobject: MetaobjectCreateInput!) {
          metaobjectCreate(metaobject: $metaobject) {
            metaobject { id handle displayName }
            userErrors { field message code }
          }
        }
        """
        data = self._gql(mutation, {"metaobject": {
            "type": type_str,
            "fields": build_metaobject_field_inputs(fields),
        }})
        result = data.get("metaobjectCreate") or {}
        errs = result.get("userErrors") or []
        if errs:
            raise ShopifyError(f"metaobjectCreate({type_str}) errors: {errs}")
        return result.get("metaobject") or {}

    def metaobject_update(self, gid: str, fields: dict) -> dict:
        """Update a metaobject's fields. Unspecified fields are left unchanged."""
        mutation = """
        mutation updateMO($id: ID!, $metaobject: MetaobjectUpdateInput!) {
          metaobjectUpdate(id: $id, metaobject: $metaobject) {
            metaobject { id handle displayName }
            userErrors { field message code }
          }
        }
        """
        data = self._gql(mutation, {
            "id": gid,
            "metaobject": {"fields": build_metaobject_field_inputs(fields)},
        })
        result = data.get("metaobjectUpdate") or {}
        errs = result.get("userErrors") or []
        if errs:
            raise ShopifyError(f"metaobjectUpdate errors: {errs}")
        return result.get("metaobject") or {}

    def metaobject_delete(self, gid: str) -> None:
        """Delete a metaobject by GID."""
        mutation = """
        mutation deleteMO($id: ID!) {
          metaobjectDelete(id: $id) {
            deletedId
            userErrors { field message code }
          }
        }
        """
        data = self._gql(mutation, {"id": gid})
        errs = (data.get("metaobjectDelete") or {}).get("userErrors") or []
        if errs:
            raise ShopifyError(f"metaobjectDelete errors: {errs}")

    # ─── File upload (Shopify Files API) ─────────────────────────────────────────

    def upload_image_to_files(self, file_bytes: bytes, filename: str, mime_type: str) -> str:
        """Upload an image to Shopify Files. Returns the MediaImage GID.

        Three steps: stagedUploadsCreate → POST bytes to staged target → fileCreate."""
        # Step 1: staged upload target
        staged_mutation = """
        mutation staged($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets { url resourceUrl parameters { name value } }
            userErrors { field message }
          }
        }
        """
        data = self._gql(staged_mutation, {"input": [{
            "filename": filename,
            "mimeType": mime_type,
            "httpMethod": "POST",
            "resource": "IMAGE",
        }]})
        staged = data.get("stagedUploadsCreate") or {}
        errs = staged.get("userErrors") or []
        if errs:
            raise ShopifyError(f"stagedUploadsCreate errors: {errs}")
        targets = staged.get("stagedTargets") or []
        if not targets:
            raise ShopifyError("stagedUploadsCreate returned no targets")
        target = targets[0]
        upload_url = target["url"]
        resource_url = target["resourceUrl"]
        params = {p["name"]: p["value"] for p in target.get("parameters", [])}

        # Step 2: POST bytes (S3-style multipart)
        files = {"file": (filename, file_bytes, mime_type)}
        r = requests.post(upload_url, data=params, files=files, timeout=60)
        if not r.ok:
            raise ShopifyError(f"staged upload failed: {r.status_code} {r.text[:200]}")

        # Step 3: register file as MediaImage
        create_mutation = """
        mutation registerFile($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files { ... on MediaImage { id image { url } } }
            userErrors { field message code }
          }
        }
        """
        data = self._gql(create_mutation, {"files": [{
            "alt": filename,
            "contentType": "IMAGE",
            "originalSource": resource_url,
        }]})
        created = data.get("fileCreate") or {}
        errs = created.get("userErrors") or []
        if errs:
            raise ShopifyError(f"fileCreate errors: {errs}")
        out_files = created.get("files") or []
        if not out_files:
            raise ShopifyError("fileCreate returned no files")
        return out_files[0]["id"]


