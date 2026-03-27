"""
ai_enrichment.py — AI-powered product enrichment via Claude API.

Generates agentic storefront metadata (title, description, category),
product descriptions (body_html), and GTINs for Pokemon TCG products.

Entry points:
    generate_ai_fields(product_title, set_name, product_tags, price) -> dict
    push_ai_fields(product_gid, fields, set_body_html=False) -> dict

Env vars required:
    ANTHROPIC_API_KEY
    SHOPIFY_TOKEN, SHOPIFY_STORE  (for push)
"""

import os
import json
import logging

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")


SYSTEM_PROMPT = """You are a product data specialist for Pack Fresh (packfreshcards.com), a Pokemon TCG sealed product retailer.

Your job is to generate structured product metadata for AI shopping agents and product pages.

Rules:
- Be FACTUAL. Only list contents you are certain about based on the product name and set.
- Never guess pack counts, bonus items, or promo cards. If you don't know exact contents, describe what you do know.
- For GTIN/UPC: Only provide if you are highly confident this is the correct barcode for this exact product. Pokemon sealed products have known UPCs. If there's any doubt, return null.
- Agentic fields should be concise and optimized for LLM parsing — not marketing copy.
- The agentic_category should be specific within Pokemon TCG (e.g., "Pokemon TCG Booster Box", "Pokemon TCG Elite Trainer Box", "Pokemon TCG Booster Bundle").
- For description_html: Start with an H2 hook that is NOT just the product title. Write a brief sentence about the product. Then include an "Includes:" section listing known contents as a bullet list. Use clean HTML."""


OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "agentic_title": {
            "type": "string",
            "description": "Concise, LLM-parseable product title. Example: 'Pokemon SV Prismatic Evolutions Elite Trainer Box'",
        },
        "agentic_description": {
            "type": "string",
            "description": "1-2 sentence factual description for AI shopping agents. What this product is, what set, what it contains.",
        },
        "agentic_category": {
            "type": "string",
            "description": "Specific product category. Examples: 'Pokemon TCG Booster Box', 'Pokemon TCG Elite Trainer Box', 'Pokemon TCG Booster Bundle', 'Pokemon TCG Collection Box', 'Pokemon TCG Tin', 'Pokemon TCG Blister Pack'",
        },
        "gtin": {
            "type": ["string", "null"],
            "description": "UPC/EAN barcode if confidently known for this exact product. null if unsure.",
        },
        "description_html": {
            "type": "string",
            "description": "HTML product description with: H2 hook (not repeating title), brief blurb, 'Includes:' bullet list of known contents.",
        },
    },
    "required": ["agentic_title", "agentic_description", "agentic_category", "gtin", "description_html"],
    "additionalProperties": False,
}


def generate_ai_fields(
    product_title: str,
    set_name: str = "",
    product_tags: list[str] | str = "",
    price: float | None = None,
) -> dict:
    """
    Generate AI-enriched fields for a product using Claude.

    Returns dict with keys:
        agentic_title, agentic_description, agentic_category, gtin, description_html
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Build the prompt with all available context
    tags_str = ", ".join(product_tags) if isinstance(product_tags, list) else product_tags
    parts = [f"Product Title: {product_title}"]
    if set_name:
        parts.append(f"Set: {set_name}")
    if tags_str:
        parts.append(f"Tags: {tags_str}")
    if price:
        parts.append(f"Price: ${price:.2f}")

    user_prompt = "\n".join(parts)
    user_prompt += "\n\nGenerate the agentic metadata, GTIN, and product description for this product."

    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": OUTPUT_SCHEMA,
            }
        },
    )

    text = next((b.text for b in response.content if b.type == "text"), "{}")
    result = json.loads(text)

    logger.info(
        f"AI enrichment generated for '{product_title}': "
        f"agentic_title='{result.get('agentic_title', '')[:50]}', "
        f"gtin={'set' if result.get('gtin') else 'none'}"
    )

    return result


def push_ai_fields(product_gid: str, fields: dict, set_body_html: bool = False) -> dict:
    """
    Write AI-generated fields to Shopify.

    Sets agentic metafields via metafieldsSet.
    Optionally sets GTIN on variant barcode.
    Optionally sets description_html as body_html.

    Returns summary dict.
    """
    from shopify_graphql import shopify_gql, shopify_metafields_set

    summary = {"product_gid": product_gid, "metafields_set": False, "gtin_set": False, "body_html_set": False, "errors": []}

    # 1) Agentic metafields
    metafields = []
    if fields.get("agentic_title"):
        metafields.append({
            "ownerId": product_gid,
            "namespace": "agentic",
            "key": "title",
            "value": fields["agentic_title"],
            "type": "single_line_text_field",
        })
    if fields.get("agentic_description"):
        metafields.append({
            "ownerId": product_gid,
            "namespace": "agentic",
            "key": "description",
            "value": fields["agentic_description"],
            "type": "multi_line_text_field",
        })
    if fields.get("agentic_category"):
        metafields.append({
            "ownerId": product_gid,
            "namespace": "agentic",
            "key": "category",
            "value": fields["agentic_category"],
            "type": "single_line_text_field",
        })

    if metafields:
        try:
            shopify_metafields_set(metafields)
            summary["metafields_set"] = True
        except Exception as e:
            summary["errors"].append(f"metafields: {e}")

    # 2) GTIN on variant barcode
    gtin = fields.get("gtin")
    if gtin:
        try:
            product_id = product_gid.split("/")[-1]
            # Get first variant ID
            data = shopify_gql("""
                query($id: ID!) {
                    product(id: $id) {
                        variants(first: 1) { edges { node { id } } }
                    }
                }
            """, {"id": product_gid})
            variant_gid = data.get("data", {}).get("product", {}).get("variants", {}).get("edges", [{}])[0].get("node", {}).get("id")
            if variant_gid:
                variant_id = variant_gid.split("/")[-1]
                _rest = _get_rest_client()
                _rest("PUT", f"/variants/{variant_id}.json", json={"variant": {"id": int(variant_id), "barcode": gtin}})
                summary["gtin_set"] = True
        except Exception as e:
            summary["errors"].append(f"gtin: {e}")

    # 3) Body HTML (only when explicitly requested for new products)
    if set_body_html and fields.get("description_html"):
        try:
            product_id = product_gid.split("/")[-1]
            _rest = _get_rest_client()
            _rest("PUT", f"/products/{product_id}.json", json={"product": {"id": int(product_id), "body_html": fields["description_html"]}})
            summary["body_html_set"] = True
        except Exception as e:
            summary["errors"].append(f"body_html: {e}")

    return summary


def _get_rest_client():
    """Get the Shopify REST API client function."""
    token = os.environ.get("SHOPIFY_TOKEN")
    store = os.environ.get("SHOPIFY_STORE")
    version = "2025-10"

    import requests as req

    def _rest(method, path, **kwargs):
        url = f"https://{store}/admin/api/{version}{path}"
        headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
        resp = req.request(method, url, headers=headers, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    return _rest
