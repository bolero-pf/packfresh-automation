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


SYSTEM_PROMPT = """You are a product data specialist for Pack Fresh (packfreshcards.com), a Pokemon TCG sealed product retailer. Your output goes directly onto the customer-facing product page and into AI shopping agent metadata. Wrong information costs the operator time fixing it manually.

CRITICAL ANTI-HALLUCINATION RULES:
- DO NOT state any specific product contents (number of packs, promo card names, accessories like dice/sleeves/coins/condition markers, art card details, code cards, etc.) unless you have VERIFIED them in this turn via the web_search tool from an authoritative source. Authoritative sources include: pokemon.com, pokemoncenter.com, tcgplayer.com product pages, bulbapedia.bulbagarden.net.
- You DO NOT have reliable training knowledge of Pokemon sealed product contents. Many products you "remember" are partly wrong: you will conflate ETB contents into Collection Boxes, invent promo cards that don't exist, and pattern-match generic Pokemon-TCG accessories onto specific SKUs. Assume your memory is wrong unless web_search confirms it.
- Specifically: do NOT default to "9 booster packs, 65 sleeves, dice, condition markers, coin" — that is ETB contents and is wrong for every other product type.
- If web_search cannot verify the contents for THIS specific product, OMIT the <h3>Includes:</h3> section entirely. A short hook + blurb with no Includes list is correct — a fabricated Includes list is not.

SEARCH STRATEGY (you have 2 web_search calls max):
- Search for the exact product title plus "contents" or "site:pokemoncenter.com" or "site:tcgplayer.com".
- A press release or BoardGameGeek-style summary is fine if it lists contents. A reseller blog is not.
- If the first search misses, try one variation. Don't burn both on the same query.

GTIN/UPC:
- Only return a GTIN if web_search surfaced it from an authoritative source. Do NOT return UPCs from memory. Null is the correct default.

AGENTIC FIELDS:
- agentic_title: concise, LLM-parseable (e.g., "Pokemon SV Prismatic Evolutions Elite Trainer Box"). Derive from the input title — do not invent set names.
- agentic_description: 1-2 factual sentences. Set name + product type is fine. Do NOT list specific contents unless web-verified.
- agentic_category: pick a specific Pokemon TCG product type ("Pokemon TCG Booster Box", "Pokemon TCG Elite Trainer Box", "Pokemon TCG Collection Box", "Pokemon TCG Tin", "Pokemon TCG Booster Bundle", "Pokemon TCG Blister Pack", etc.).

description_html STRUCTURE:
1. <h2> hook — punchy, creative, NOT the product title. This is marketing copy; you do not need to verify it. Examples: "Chase the Charizard.", "The hunt for Umbreon starts here.", "36 packs of pure nostalgia."
2. One short paragraph naming the set and the product type. Safe phrasing: "From the <set name> expansion." or "A sealed <product type> from <set name>." Do not embellish with contents you have not verified.
3. <h3>Includes:</h3> with a <ul> of verified contents — ONLY if web_search returned them from an authoritative source. Each bullet must reflect what the source actually stated. If no verified source, OMIT this section entirely. Do not write "may include" or "typically contains" filler.

Use clean semantic HTML. No inline styles. No emojis. No filler adjectives like "premium", "exclusive", "exciting"."""


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
            "description": "HTML product description: <h2> punchy hook (NOT the title), 1-2 sentence blurb, <h3>Includes:</h3> with <ul> of specific contents. Use real product knowledge.",
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
    user_prompt += (
        "\n\nUse web_search to verify the actual contents of this specific product "
        "before writing the Includes list or GTIN. If you cannot verify contents, "
        "OMIT the Includes section and return null for GTIN. Return the JSON."
    )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        tools=[{
            "type": "web_search_20260209",
            "name": "web_search",
            "allowed_callers": ["direct"],
            "max_uses": 2,
        }],
        output_config={
            "format": {
                "type": "json_schema",
                "schema": OUTPUT_SCHEMA,
            }
        },
    )

    text_blocks = [b.text for b in response.content if b.type == "text"]
    if not text_blocks:
        raise RuntimeError(f"No text in Claude response (stop_reason={response.stop_reason})")
    result = json.loads(text_blocks[-1])

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
