"""
generic_product_create.py — Create a Shopify draft product from a non-TCG bulk-add payload.

Distinct from shared/product_enrichment.py (which is Pokemon TCG specific). No era,
no tcgplayer_id metafield, no template_suffix, no remove.bg — uploads original photos as-is.

Vendor is hard-coded to "Common Lands" (Sean's store sells everything in-house).

Entry point:
    create_draft_product(payload, image_paths, qty=0) -> dict
"""

import os
import io
import re
import time
import logging
import unicodedata
import requests
from PIL import Image

logger = logging.getLogger(__name__)

SHOPIFY_VERSION = "2025-10"
VENDOR = "Common Lands"
TEMPLATE_SUFFIX = "cro-alt"

# Search terms used to look up Shopify taxonomy categories at runtime.
# None = don't bother (too generic to map well).
PRODUCT_TYPE_TAXONOMY_SEARCH = {
    "Board Game": "Board Games",
    "Card Game (Non-TCG)": "Card Games",
    "Puzzle": "Jigsaw Puzzles",
    "Toy / Plush": "Stuffed Animals",
    "Accessory": None,
    "Collectible": None,
    "Misc": None,
}

_TAXONOMY_CACHE: dict[str, str | None] = {}


def _slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s)
    return re.sub(r"-{2,}", "-", s).strip("-").lower()


def _shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": os.environ["SHOPIFY_TOKEN"],
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _rest(method: str, path: str, **kwargs) -> dict:
    store = os.environ["SHOPIFY_STORE"]
    url = f"https://{store}/admin/api/{SHOPIFY_VERSION}{path}"
    resp = requests.request(method, url, headers=_shopify_headers(), timeout=30, **kwargs)
    resp.raise_for_status()
    return resp.json() if resp.text else {}


def _gql(query: str, variables: dict = None) -> dict:
    store = os.environ["SHOPIFY_STORE"]
    url = f"https://{store}/admin/api/{SHOPIFY_VERSION}/graphql.json"
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(url, headers=_shopify_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL errors: {body['errors']}")
    return body.get("data", {})


def _normalize_image_for_shopify(path: str, max_long_edge: int = 2048) -> tuple[bytes, str]:
    """Read source image, downscale if huge, return (bytes, ext)."""
    im = Image.open(path)
    if im.mode in ("RGBA", "LA", "P"):
        im = im.convert("RGB")
    w, h = im.size
    if max(w, h) > max_long_edge:
        scale = max_long_edge / max(w, h)
        im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=90, optimize=True)
    return buf.getvalue(), "jpg"


def _upload_image_to_shopify(slug: str, idx: int, jpg_bytes: bytes, alt: str) -> str:
    """Stage upload + fileCreate, poll until URL ready. Returns hosted URL."""
    filename = f"{slug}-{idx}.jpg"

    staged = _gql("""
        mutation StagedUploads($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets { url resourceUrl parameters { name value } }
            userErrors { field message }
          }
        }
    """, {"input": [{
        "resource": "IMAGE",
        "filename": filename,
        "mimeType": "image/jpeg",
        "httpMethod": "POST",
    }]})
    errs = staged.get("stagedUploadsCreate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"stagedUploadsCreate: {errs}")
    target = staged["stagedUploadsCreate"]["stagedTargets"][0]

    form = {p["name"]: p["value"] for p in target["parameters"]}
    s3 = requests.post(
        target["url"], data=form,
        files={"file": (filename, jpg_bytes, "image/jpeg")},
        timeout=60,
    )
    s3.raise_for_status()

    fc = _gql("""
        mutation FileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files { __typename ... on MediaImage { id image { url } } }
            userErrors { field message }
          }
        }
    """, {"files": [{
        "originalSource": target["resourceUrl"],
        "contentType": "IMAGE",
        "alt": alt,
        "filename": filename,
    }]})
    errs = fc.get("fileCreate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"fileCreate: {errs}")
    files = fc["fileCreate"]["files"]
    if not files:
        raise RuntimeError("fileCreate returned no files")
    fid = files[0]["id"]

    for _ in range(30):
        node = _gql("""
            query($id: ID!) {
              node(id: $id) { __typename ... on MediaImage { image { url } } }
            }
        """, {"id": fid})
        n = node.get("node", {})
        if n.get("__typename") == "MediaImage":
            url = (n.get("image") or {}).get("url")
            if url:
                return url
        time.sleep(0.5)
    raise TimeoutError(f"Image URL not ready for file {fid}")


def _attach_image(product_gid: str, image_url: str, alt: str, variant_ids: list[int] = None) -> None:
    """Attach a hosted image URL to product. Optionally link to specific variant_ids."""
    product_id = product_gid.split("/")[-1]
    payload = {"image": {"src": image_url, "alt": alt}}
    if variant_ids:
        payload["image"]["variant_ids"] = variant_ids
    _rest("POST", f"/products/{product_id}/images.json", json=payload)


def create_draft_product(
    payload: dict,
    image_paths_by_filename: dict[str, str],
    qty: int = 0,
) -> dict:
    """
    Create a Shopify draft product from a reviewed bulk-add payload.

    payload (after operator review) includes:
        title, product_type, publisher, body_html, tags (list), weight_oz_estimate,
        msrp_usd, variant_option_name, variants[] (each with filename, option_value, sku, barcode)

    image_paths_by_filename maps each variant's filename to its local file path.

    Returns: {"product_id", "product_gid", "title", "admin_url", "variants_created", "errors"}
    """
    title = payload["title"]
    body_html = payload.get("body_html", "")
    product_type = payload.get("product_type", "Misc")
    tags = payload.get("tags", [])
    weight_oz = float(payload.get("weight_oz_estimate") or 8.0)
    msrp = payload.get("msrp_usd")
    price_str = f"{float(msrp):.2f}" if msrp is not None else "0.00"
    option_name = payload.get("variant_option_name") or "Title"
    variants = payload.get("variants") or []
    if not variants:
        raise RuntimeError("create_draft_product: no variants in payload")

    is_single = (
        len(variants) == 1
        and (option_name.lower() == "title" or variants[0].get("option_value", "").lower() in ("default title", ""))
    )

    rest_variants = []
    for v in variants:
        rv = {
            "price": price_str,
            "sku": v.get("sku") or "",
            "inventory_management": "shopify",
            "requires_shipping": True,
            "weight": weight_oz,
            "weight_unit": "oz",
        }
        if v.get("barcode"):
            rv["barcode"] = v["barcode"]
        if not is_single:
            rv["option1"] = v.get("option_value") or "Default Title"
        rest_variants.append(rv)

    product_payload = {
        "product": {
            "title": title,
            "status": "draft",
            "product_type": product_type,
            "vendor": VENDOR,
            "template_suffix": TEMPLATE_SUFFIX,
            "body_html": body_html,
            "tags": ", ".join(tags),
            "variants": rest_variants,
        }
    }
    if not is_single:
        product_payload["product"]["options"] = [{"name": option_name}]

    result = _rest("POST", "/products.json", json=product_payload)
    product = result["product"]
    product_id = product["id"]
    product_gid = product["admin_graphql_api_id"]
    created_variants = product.get("variants", [])

    summary = {
        "product_id": product_id,
        "product_gid": product_gid,
        "title": title,
        "variants_created": len(created_variants),
        "errors": [],
    }

    slug = _slugify(title)
    fn_to_variant_id: dict[str, int] = {}
    for v_payload, v_created in zip(variants, created_variants):
        fn_to_variant_id[v_payload["filename"]] = v_created["id"]

    for idx, v in enumerate(variants):
        fn = v["filename"]
        path = image_paths_by_filename.get(fn)
        if not path:
            summary["errors"].append(f"image missing for {fn}")
            continue
        try:
            jpg, _ = _normalize_image_for_shopify(path)
            url = _upload_image_to_shopify(slug, idx, jpg, alt=f"{title} {v.get('option_value', '')}")
            variant_id = fn_to_variant_id.get(fn)
            _attach_image(product_gid, url, alt=title,
                          variant_ids=[variant_id] if (variant_id and not is_single) else None)
        except Exception as e:
            logger.warning("Image upload failed for %s: %s", fn, e)
            summary["errors"].append(f"image {fn}: {e}")

    try:
        _publish_to_all_channels(product_gid)
    except Exception as e:
        summary["errors"].append(f"publish: {e}")

    try:
        cat_gid = _find_taxonomy_gid(product_type)
        if cat_gid:
            _set_product_category(product_gid, cat_gid)
    except Exception as e:
        summary["errors"].append(f"category: {e}")

    try:
        _set_agentic_metafields(product_gid, payload)
    except Exception as e:
        summary["errors"].append(f"agentic: {e}")

    if qty > 0 and created_variants:
        try:
            _set_inventory(created_variants[0]["id"], qty)
        except Exception as e:
            summary["errors"].append(f"inventory: {e}")

    store_handle = os.environ.get("SHOPIFY_STORE_HANDLE", "")
    summary["admin_url"] = (
        f"https://admin.shopify.com/store/{store_handle}/products/{product_id}"
        if store_handle else f"https://admin.shopify.com/products/{product_id}"
    )
    logger.info("Created draft product %s (%s) with %d variants",
                product_id, title, len(created_variants))
    return summary


def _publish_to_all_channels(product_gid: str) -> None:
    data = _gql("query { publications(first: 50) { nodes { id name } } }")
    pubs = data.get("publications", {}).get("nodes", [])
    for pub in pubs:
        try:
            _gql("""
                mutation Publish($id: ID!, $pub: ID!) {
                  publishablePublish(id: $id, input: { publicationId: $pub }) {
                    userErrors { field message }
                  }
                }
            """, {"id": product_gid, "pub": pub["id"]})
        except Exception as e:
            logger.warning("Publish to %s failed: %s", pub.get("name"), e)


def _find_taxonomy_gid(product_type: str) -> str | None:
    """Look up best-match Shopify taxonomy category for a product_type. Cached."""
    if product_type in _TAXONOMY_CACHE:
        return _TAXONOMY_CACHE[product_type]
    search = PRODUCT_TYPE_TAXONOMY_SEARCH.get(product_type)
    if not search:
        _TAXONOMY_CACHE[product_type] = None
        return None
    try:
        data = _gql("""
            query TaxonomyLookup($s: String!) {
              taxonomy {
                categories(search: $s, first: 5) {
                  nodes { id name fullName isLeaf }
                }
              }
            }
        """, {"s": search})
        nodes = (data.get("taxonomy") or {}).get("categories", {}).get("nodes", []) or []
        leaf = next((n for n in nodes if n.get("isLeaf")), None)
        gid = (leaf or (nodes[0] if nodes else {})).get("id")
        _TAXONOMY_CACHE[product_type] = gid
        if gid:
            logger.info("Taxonomy %r → %s (%s)", product_type, gid,
                        (leaf or nodes[0]).get("fullName"))
        return gid
    except Exception as e:
        logger.warning("Taxonomy lookup failed for %r: %s", product_type, e)
        _TAXONOMY_CACHE[product_type] = None
        return None


def _set_product_category(product_gid: str, category_gid: str) -> None:
    result = _gql("""
        mutation SetCategory($id: ID!, $cat: ID!) {
          productUpdate(product: { id: $id, category: $cat }) {
            product { id category { fullName } }
            userErrors { field message }
          }
        }
    """, {"id": product_gid, "cat": category_gid})
    errs = result.get("productUpdate", {}).get("userErrors", [])
    if errs:
        logger.warning("set_product_category errors: %s", errs)


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html or "")
    return re.sub(r"\s+", " ", text).strip()


def _set_agentic_metafields(product_gid: str, payload: dict) -> None:
    """Mirror the TCG Add Item flow's agentic.* metafields for AI shopping agents."""
    title = payload["title"]
    product_type = payload.get("product_type", "")
    body_text = _strip_html(payload.get("body_html", ""))
    sentences = re.split(r"(?<=[.!?])\s+", body_text)
    agentic_desc = " ".join(sentences[:2])[:300] or f"{product_type} sold by {VENDOR}."

    metafields = [
        {"ownerId": product_gid, "namespace": "agentic", "key": "title",
         "value": title, "type": "single_line_text_field"},
        {"ownerId": product_gid, "namespace": "agentic", "key": "description",
         "value": agentic_desc, "type": "multi_line_text_field"},
        {"ownerId": product_gid, "namespace": "agentic", "key": "category",
         "value": product_type, "type": "single_line_text_field"},
    ]
    result = _gql("""
        mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { id }
            userErrors { field message }
          }
        }
    """, {"metafields": metafields})
    errs = result.get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")


def _set_inventory(variant_id: int, quantity: int) -> None:
    variant_gid = f"gid://shopify/ProductVariant/{variant_id}"
    data = _gql("""
        query($id: ID!) { productVariant(id: $id) { inventoryItem { id } } }
    """, {"id": variant_gid})
    inv_gid = data.get("productVariant", {}).get("inventoryItem", {}).get("id", "")
    inv_item_id = inv_gid.split("/")[-1]
    if not inv_item_id:
        return
    loc = _gql("{ locations(first: 1) { edges { node { id } } } }")
    location_id = loc["locations"]["edges"][0]["node"]["id"]
    _gql("""
        mutation($input: InventorySetQuantitiesInput!) {
          inventorySetQuantities(input: $input) {
            userErrors { field message }
          }
        }
    """, {"input": {
        "reason": "correction",
        "name": "available",
        "ignoreCompareQuantity": True,
        "quantities": [{
            "inventoryItemId": f"gid://shopify/InventoryItem/{inv_item_id}",
            "locationId": location_id,
            "quantity": quantity,
        }],
    }})
