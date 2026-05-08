"""
bulk_vision_enrichment.py — Vision + web-search Claude pass for non-TCG bulk product creation.

Per product group (one or more product photos sharing a name), generates:
    title, product_type, publisher, body_html, tags, weight_oz_estimate,
    msrp_usd + msrp_source_url, variant_option_name, per-variant SKU/barcode/option_value,
    notes (operator review flags).

Uses claude-sonnet-4-6 with web_search_20260209 tool for MSRP lookup
and structured JSON output via output_config.format.

Env vars required:
    ANTHROPIC_API_KEY
"""

import os
import io
import json
import base64
import logging
from PIL import Image

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

PRODUCT_TYPES = [
    "Board Game",
    "Card Game (Non-TCG)",
    "Puzzle",
    "Toy / Plush",
    "Accessory",
    "Collectible",
    "Misc",
]

SYSTEM_PROMPT = f"""You are a product data specialist for Common Lands, a hobby and collectibles retailer expanding into board games, non-TCG card games, and tabletop accessories.

Your job is to look at product photos and produce a clean Shopify product draft. The operator will review every field before saving — so be honest about what you can see and what's uncertain. Surface uncertainty in the `notes` field rather than guessing.

INPUTS:
- One or more photos of the same product. If there are multiple photos with different filenames, treat them as variants of the SAME product (e.g. color or size variants), unless the photos clearly show different products.
- A name hint extracted from the filename. Use this as a starting point, but trust the photos when they conflict.

PRODUCT TYPE — pick exactly one from this list:
{", ".join(PRODUCT_TYPES)}

VARIANTS:
- If there is one image, treat it as a single-variant product. Set `variant_option_name` to "Title" and produce one variant entry with `option_value` "Default Title".
- If there are multiple images, infer the variant axis from the filename suffixes (e.g. "_Blue", "_Red" → variant_option_name "Color"; "_Small", "_Medium", "_Large" → "Size"). Choose the most natural single-word axis name. If the suffixes don't share an obvious axis, use "Variant".
- Generate a SKU per variant in the form CL-<short-product-slug>-<short-option-slug>, uppercase, dashes only.
- If you can clearly read a UPC/EAN barcode in any photo, return it on that variant. Otherwise null.

MSRP — be diligent, this is the price the store will list at:
- Search the manufacturer/publisher's official site FIRST (e.g. ravensburger.com, asmodee.com, mattel.com). Manufacturer MSRP is the source of truth — Common Lands sells at MSRP.
- If the manufacturer site doesn't list a price, search 2-3 major retailers (Target, Barnes & Noble, Walmart, Amazon's own listing — NOT third-party sellers). Take the most common price.
- Ignore eBay, Etsy, marketplace third-party prices, and anything that looks discounted/closeout. Those reflect demand, not MSRP.
- Run multiple searches if needed: try the exact product title, try "<title> MSRP", try "<title> site:<manufacturer>.com".
- If you find conflicting prices, prefer the manufacturer's number and note the discrepancy in `notes`.
- Return msrp_usd as a number with no currency symbol; msrp_source_url should be the page that showed the price you used.
- Only return null/null if multiple searches turn up nothing — in that case explain in `notes`.

UPC / Barcode:
- First, look at every photo for a visible UPC barcode (often on the back or bottom of the box). Read the digits below the bars. 12-13 digits.
- If no UPC is visible in the photos, search upcitemdb.com for the product (e.g. `site:upcitemdb.com "<exact product title>"`). Their listing pages show the UPC at the top.
- Also try barcodelookup.com or the manufacturer's product page (sometimes lists UPC in spec table).
- Validate: 12 digits = UPC-A, 13 digits = EAN-13. Reject anything else.
- If you find variant-specific UPCs (one per color/size), assign them per variant. If only one UPC for the whole product line, leave variant barcodes null and note that in `notes`.
- Better to return null than a wrong UPC. The barcode goes on the Shopify variant and customers may scan it in store.

BODY HTML:
- A short <h2> hook, a 1-2 sentence pitch, and an <h3>About:</h3> section with a <ul> of relevant facts (player count, age range, play time, components — only what you can confirm from the box or a credible source).
- Clean semantic HTML, no inline styles, no marketing fluff.

TAGS:
- 3-8 short lowercase tags (game type, mechanics, theme, audience). Don't pad.

WEIGHT:
- Best estimate in ounces for shipping. Standard board game box ~32oz, large box ~64oz, small card game ~6oz. Be conservative; operator can adjust.

NOTES field:
- Anything you're unsure about: ambiguous photos, unreadable text, conflicting filename hints, multiple products that look like they might not belong together, etc. Empty string if nothing to flag.
"""

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {
            "type": "string",
            "description": "Clean product title, no SKU/variant suffixes.",
        },
        "product_type": {"type": "string", "enum": PRODUCT_TYPES},
        "publisher": {
            "type": "string",
            "description": "Publisher/maker as it appears on the product. Empty string if not visible.",
        },
        "body_html": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "weight_oz_estimate": {"type": "number"},
        "msrp_usd": {"type": ["number", "null"]},
        "msrp_source_url": {"type": ["string", "null"]},
        "variant_option_name": {"type": "string"},
        "variants": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "option_value": {"type": "string"},
                    "sku": {"type": "string"},
                    "barcode": {"type": ["string", "null"]},
                },
                "required": ["filename", "option_value", "sku", "barcode"],
                "additionalProperties": False,
            },
        },
        "notes": {"type": "string"},
    },
    "required": [
        "title", "product_type", "publisher", "body_html", "tags",
        "weight_oz_estimate", "msrp_usd", "msrp_source_url",
        "variant_option_name", "variants", "notes",
    ],
    "additionalProperties": False,
}


def _resize_for_vision(image_path: str, max_long_edge: int = 1024) -> tuple[bytes, str]:
    """Open image, resize so long edge <= max_long_edge, return (bytes, media_type)."""
    im = Image.open(image_path)
    media_type = "image/jpeg"
    if im.mode in ("RGBA", "P", "LA"):
        im = im.convert("RGB")

    w, h = im.size
    long_edge = max(w, h)
    if long_edge > max_long_edge:
        scale = max_long_edge / long_edge
        im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)

    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=85, optimize=True)
    return buf.getvalue(), media_type


def analyze_product_group(name_hint: str, variants: list[dict]) -> dict:
    """
    Run Claude vision + web-search analysis on a product group.

    Args:
        name_hint: The base name parsed from filenames (e.g. "Monster Prism Tube").
        variants: List of {"filename": str, "image_path": str, "option_hint": str}.
                  option_hint is the underscore-suffix from the filename ("" for base image).

    Returns: dict matching OUTPUT_SCHEMA.
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    content_blocks = []
    for v in variants:
        img_bytes, media_type = _resize_for_vision(v["image_path"])
        b64 = base64.standard_b64encode(img_bytes).decode("ascii")
        content_blocks.append({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": b64},
        })

    variant_lines = "\n".join(
        f"- filename: {v['filename']}  | option_hint: {v['option_hint'] or '(base)'}"
        for v in variants
    )
    prompt_text = (
        f"Name hint from filenames: {name_hint}\n\n"
        f"Variants in this group:\n{variant_lines}\n\n"
        "Look at the images, search the web for MSRP if useful, and return the JSON product draft."
    )
    content_blocks.append({"type": "text", "text": prompt_text})

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content_blocks}],
        tools=[{"type": "web_search_20260209", "name": "web_search"}],
        output_config={
            "format": {"type": "json_schema", "schema": OUTPUT_SCHEMA},
            "effort": "medium",
        },
    )

    text_blocks = [b.text for b in response.content if b.type == "text"]
    if not text_blocks:
        raise RuntimeError(f"No text in Claude response (stop_reason={response.stop_reason})")
    result = json.loads(text_blocks[-1])

    logger.info(
        "Bulk vision enrichment for %r: type=%s variants=%d msrp=%s",
        name_hint, result.get("product_type"),
        len(result.get("variants", [])), result.get("msrp_usd"),
    )
    return result
