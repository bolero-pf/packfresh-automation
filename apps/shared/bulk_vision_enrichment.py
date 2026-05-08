"""
bulk_vision_enrichment.py — Vision + bounded web-search Claude pass for non-TCG bulk product creation.

Per product group (one or more product photos sharing a name), generates:
    title, product_type, publisher, body_html, tags, weight_oz_estimate,
    msrp_usd + msrp_source_url, variant_option_name, per-variant
    SKU/barcode/option_value, notes.

Uses claude-haiku-4-5 with structured JSON output via output_config.format
and web_search with max_uses=1 (one search max per product, keeps token
cost bounded so Analyze All doesn't saturate the 50K TPM window).

The prompt heavily emphasizes using TRAINING-DATA KNOWLEDGE first — Haiku
knows mainstream board games (player count, age range, play time, designer,
mechanics, publisher, year released). Web search is for MSRP and obscure
products only.

Env vars required:
    ANTHROPIC_API_KEY
"""

import os
import io
import json
import time
import base64
import logging
from PIL import Image

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

_CANONICAL_TAGS_CACHE: list[str] = []
_CANONICAL_TAGS_AT: float = 0.0
_CANONICAL_TAGS_TTL = 3600  # 1 hour

PRODUCT_TYPES = [
    "Board Game",
    "Card Game (Non-TCG)",
    "Puzzle",
    "Toy / Plush",
    "Accessory",
    "Collectible",
    "Misc",
]

SYSTEM_PROMPT_TMPL = f"""You are a product data specialist for Common Lands, a hobby and collectibles retailer carrying board games, non-TCG card games, and tabletop accessories.

USE YOUR KNOWLEDGE — but DON'T HALLUCINATE FUNCTION. Two opposing failure modes to avoid:

(a) Sandbagging things you actually know. For mainstream board games — Horrified, Disney Villainous, Catan, Ticket to Ride, Pandemic, Wingspan, Azul, Splendor, Sushi Go, Dixit, Carcassonne — you already know publisher, player count, age range, play time, mechanics, theme, designer. Fill those in. Don't write "play time not visible on box, operator should confirm" when you literally know Horrified is 60 minutes. Box photos rarely show every spec — that's what your knowledge is for.

(b) Hallucinating function from category context. For ACCESSORIES (tubes, cases, sleeves, dice towers, mats, deckboxes), READ THE PRODUCT LABEL to determine what it stores or does. Do NOT assume "card storage" just because we're a hobby store. Examples: a "Monster Prism Tube" could store playmats, posters, cards, dice — the label tells you which. A "Premium Case" could be for slabs, cards, miniatures, or comics. If the label says "PLAYMAT TUBE" or "FOR PLAYMATS", say playmats. If you can't read the function clearly off the box, say so in notes — don't guess based on the store's category.

ONLY say "not visible / not certain" in the notes field for things you genuinely don't know.

INPUTS:
- One or more photos of the same product. If there are multiple photos with different filenames, treat them as variants of the SAME product (e.g. color or size variants), unless the photos clearly show different products.
- A name hint extracted from the filename. Use this as a starting point.

PRODUCT TYPE — pick exactly one from this list:
{", ".join(PRODUCT_TYPES)}

VARIANTS:
- If there is one image, treat it as a single-variant product. Set `variant_option_name` to "Title" and produce one variant entry with `option_value` "Default Title".
- If there are multiple images, infer the variant axis from the filename suffixes (e.g. "_Blue", "_Red" → variant_option_name "Color"; "_Small", "_Medium", "_Large" → "Size"). Choose the most natural single-word axis name. If the suffixes don't share an obvious axis, use "Variant".
- Generate a SKU per variant in the form CL-<short-product-slug>-<short-option-slug>, uppercase, dashes only.
- If you can clearly read a UPC/EAN barcode in any photo, return it on that variant. Otherwise null.

MSRP — you have ONE web_search call to spend per product:
- Use it ONLY if you don't already know the price from training. For mainstream titles you may already have a confident answer.
- When searching, target a live retailer page: "<title> site:target.com" or "<title> site:ravensburger.us". Don't waste the search on press releases or BoardGameGeek.
- Preserve the exact cents shown ($29.99 not $30). $34.99 not $35.
- If you can't get a confident live-page price in one search, return null and explain in `notes`. Operator will fill from manufacturer site.
- msrp_source_url should be the live shopping page if you have one, otherwise null.

UPC / Barcode:
- Look at every photo for a visible UPC barcode (often on the back or bottom of the box). Read the digits below the bars. 12 digits = UPC-A, 13 digits = EAN-13.
- If no UPC is visible in the photos, return null. Don't waste the search budget on barcode lookups.

BODY HTML — write a real listing, not a stub:
- An <h2> hook (NOT the product title). Punchy, draws a player in. e.g. "Survive the night before the monsters reach town." or "The villains finally get their happy ending."
- A 2-3 sentence pitch describing what playing this game feels like, what makes it interesting, what player or audience would love it. Use your training knowledge of the actual gameplay.
- An <h3>About:</h3> section with a <ul> of concrete facts. For mainstream titles fill ALL of these from your knowledge:
  * Player count (e.g., "1-5 players")
  * Age range (e.g., "Ages 10+")
  * Play time (e.g., "60 minutes")
  * Designer (e.g., "Designed by Prospero Hall")
  * Publisher
  * Year released (if you know it)
  * Mechanic / category (e.g., "Cooperative survival")
- Clean semantic HTML, no inline styles, no fluff.

TAGS — Common Lands has a fixed canonical tag set that drives storefront collections. Inventing new tags BREAKS the store.
- The allowed tag list is provided below. You may ONLY return tags that are spelled exactly as they appear on this list (case-insensitive).
- Pick zero or more that genuinely apply to THIS product. Returning an empty array is correct and expected when nothing fits — do not pad.
- Do NOT invent tags like "maze-game" or "labyrinth" or "family-game". If a perfect tag doesn't exist on the list, omit it.
- Allowed tags:
{{allowed_tags_block}}

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


def _get_canonical_tags() -> list[str]:
    """Fetch all tags currently used by products in the store. Cached 1h.

    Returns a sorted list. Empty list on failure (caller should handle —
    means we'll skip tags entirely rather than invent garbage).
    """
    global _CANONICAL_TAGS_CACHE, _CANONICAL_TAGS_AT
    now = time.time()
    if _CANONICAL_TAGS_CACHE and (now - _CANONICAL_TAGS_AT) < _CANONICAL_TAGS_TTL:
        return _CANONICAL_TAGS_CACHE
    try:
        from shopify_graphql import shopify_gql
        all_tags: set[str] = set()
        cursor = None
        for _ in range(40):  # safety bound: 40 * 250 = 10K tags
            after = f', after: "{cursor}"' if cursor else ""
            data = shopify_gql(f"""
                query {{
                  productTags(first: 250{after}) {{
                    edges {{ node cursor }}
                    pageInfo {{ hasNextPage }}
                  }}
                }}
            """)
            edges = (data.get("data", {}).get("productTags", {}) or {}).get("edges", []) or []
            if not edges:
                break
            for e in edges:
                t = (e.get("node") or "").strip()
                if t:
                    all_tags.add(t)
            page_info = (data.get("data", {}).get("productTags", {}) or {}).get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = edges[-1].get("cursor")
        _CANONICAL_TAGS_CACHE = sorted(all_tags)
        _CANONICAL_TAGS_AT = now
        logger.info("Loaded %d canonical tags from Shopify", len(_CANONICAL_TAGS_CACHE))
        return _CANONICAL_TAGS_CACHE
    except Exception as e:
        logger.warning("Canonical tag fetch failed (%s); falling back to empty list", e)
        return _CANONICAL_TAGS_CACHE or []


def _resize_for_vision(image_path: str, max_long_edge: int = 768) -> tuple[bytes, str]:
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
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, max_retries=10)

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
        "Look at the images, use your training knowledge of this product to fill in "
        "specs and description, and use your one web_search only if you need to "
        "verify the current MSRP. Return the JSON product draft."
    )
    content_blocks.append({"type": "text", "text": prompt_text})

    canonical_tags = _get_canonical_tags()
    if canonical_tags:
        allowed_block = "\n".join(f"  - {t}" for t in canonical_tags)
    else:
        allowed_block = "  (no canonical tags loaded — return an empty array)"
    system_prompt = SYSTEM_PROMPT_TMPL.replace("{{allowed_tags_block}}", allowed_block)

    response = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=3000,
        system=system_prompt,
        messages=[{"role": "user", "content": content_blocks}],
        tools=[{
            "type": "web_search_20260209",
            "name": "web_search",
            "allowed_callers": ["direct"],
            "max_uses": 1,
        }],
        output_config={
            "format": {"type": "json_schema", "schema": OUTPUT_SCHEMA},
        },
    )

    text_blocks = [b.text for b in response.content if b.type == "text"]
    if not text_blocks:
        raise RuntimeError(f"No text in Claude response (stop_reason={response.stop_reason})")
    result = json.loads(text_blocks[-1])

    result["title"] = _clean_title(result.get("title", ""), name_hint)
    result["tags"] = _scrub_tags(result.get("tags") or [], canonical_tags)

    logger.info(
        "Bulk vision enrichment for %r: type=%s variants=%d msrp=%s tags=%d",
        name_hint, result.get("product_type"),
        len(result.get("variants", [])), result.get("msrp_usd"),
        len(result.get("tags", [])),
    )
    return result


# Tags that duplicate product_type or describe a publisher's other product lines —
# strip these regardless of what Claude returns, since they break storefront collections.
_BANNED_TAG_TOKENS = {
    "board game", "board games", "boardgame", "boardgames",
    "card game", "card games", "cardgame", "cardgames",
    "puzzle", "puzzles", "jigsaw", "jigsaw puzzle", "jigsaw puzzles",
    "toy", "toys", "plush", "stuffed animal", "stuffed animals",
    "accessory", "accessories", "collectible", "collectibles",
    "ravensburger", "asmodee", "mattel", "hasbro", "wizkids", "z-man games",
    "common lands", "common-lands",
}


def _scrub_tags(tags: list, canonical: list[str] | None = None) -> list:
    """Filter to canonical tags only (case-insensitive). Banned tokens always
    stripped, even if they ended up in canonical somehow. Preserves the
    canonical casing/spelling on output so Shopify de-dupes properly."""
    canon_lower = {t.lower(): t for t in (canonical or [])}
    seen = set()
    out = []
    for t in tags:
        tl = (t or "").strip().lower()
        if not tl or tl in _BANNED_TAG_TOKENS or tl in seen:
            continue
        if canonical:
            if tl not in canon_lower:
                continue
            tag_out = canon_lower[tl]
        else:
            tag_out = tl
        seen.add(tl)
        out.append(tag_out)
    return out


def _clean_title(title: str, fallback: str) -> str:
    """If Claude returns junk (empty, whitespace, just punctuation, < 3 chars),
    fall back to the filename-derived name_hint. Strips leading/trailing
    whitespace + dangling punctuation."""
    import re
    t = (title or "").strip().strip(",.;:!?-_ ").strip()
    letters = re.sub(r"[^A-Za-z0-9]", "", t)
    if len(letters) < 3:
        logger.warning("Bulk vision returned junk title %r; falling back to %r", title, fallback)
        return fallback.strip()
    return t
