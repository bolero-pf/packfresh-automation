"""
product_enrichment.py

Standalone utility for creating and enriching Shopify product listings
from PPT (PokemonPriceTracker) data.

Handles:
- Image download → bg removal → matte → Shopify upload
- Tag inference (type, set, era, featured pokemon)
- Weight inference by product type
- Metafield setting (era, TCGPlayer ID)
- Publishing to all channels
- Category assignment (Gaming Cards in Collectible Trading Cards)
- Template suffix, vendor, product type

Entry points:
    enrich_product_gid(product_gid, ppt_item, offer_price=None)  — enrich existing product
    create_draft_listing(ppt_item, offer_price=None)              — create new draft + enrich

Env vars required:
    SHOPIFY_TOKEN, SHOPIFY_STORE
"""

import io
import os
import re
import logging
import requests
import time
import unicodedata
from PIL import Image

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

MATTE_W, MATTE_H = 2000, 2000
CONTENT_W, CONTENT_H = 1600, 1600   # sealed product content area (200px matte each side)
TAXONOMY_CATEGORY_GID = "gid://shopify/TaxonomyCategory/ae-2-2-3-2"  # Gaming Cards > Collectible Trading Cards
SHOPIFY_VERSION = "2025-10"

# ─── Era / Set index ──────────────────────────────────────────────────────────

# Map set name keywords → era tag. Checked case-insensitively against set name.
# Order matters — more specific entries first to avoid false matches.
ERA_SETS = {
    "mega": [
        "mega evolution", "phantasmal flames", "ascended heroes", "perfect order",
    ],
    "sv": [
        "scarlet & violet", "scarlet and violet", "paldea evolved", "obsidian flames",
        "151", "paradox rift", "paldean fates", "temporal forces", "stellar crown",
        "shrouded fable", "surging sparks", "twilight masquerade", "prismatic evolution",
        "journey together", "destined rivals", "black bolt", "white flare",
    ],
    "swsh": [
        "sword & shield", "sword and shield", "rebel clash", "darkness ablaze",
        "champion's path", "vivid voltage", "shining fates", "battle styles",
        "chilling reign", "evolving skies", "celebrations", "fusion strike",
        "brilliant stars", "astral radiance", "pokemon go", "lost origin",
        "silver tempest", "crown zenith",
    ],
    "sm": [
        "sun & moon", "sun and moon", "guardians rising", "burning shadows",
        "shining legends", "crimson invasion", "ultra prism", "forbidden light",
        "celestial storm", "dragon majesty", "lost thunder", "team up",
        "detective pikachu", "unbroken bonds", "unified minds", "hidden fates",
        "cosmic eclipse",
        # NOTE: Japanese/Korean exclusive sets (Eevee Heroes, Shiny Star V, etc.)
        # are intentionally omitted — they will fall through to NEEDS for manual review
    ],
    "xy": [
        "xy", "flashfire", "furious fists", "phantom forces", "primal clash",
        "double crisis", "roaring skies", "ancient origins", "breakthrough",
        "breakpoint", "generations", "fates collide", "steam siege", "evolutions",
    ],
    "vintage": [
        "base set", "jungle", "fossil", "team rocket", "gym heroes", "gym challenge",
        "neo genesis", "neo discovery", "neo revelation", "neo destiny",
        "legendary collection", "expedition", "aquapolis", "skyridge",
        "ruby & sapphire", "ruby and sapphire", "sandstorm", "dragon", "team magma",
        "team aqua", "hidden legends", "firered & leafgreen", "team rocket returns",
        "deoxys", "emerald", "unseen forces", "delta species", "legend maker",
        "holon phantoms", "crystal guardians", "dragon frontiers", "power keepers",
        "diamond & pearl", "diamond and pearl", "mysterious treasures", "secret wonders",
        "great encounters", "majestic dawn", "legends awakened", "stormfront",
        "platinum", "rising rivals", "supreme victors", "arceus",
        "heartgold soulsilver", "unleashed", "undaunted", "triumphant",
        "call of legends", "black & white", "black and white", "emerging powers",
        "noble victories", "next destinies", "dark explorers", "dragons exalted",
        "boundaries crossed", "plasma storm", "plasma freeze", "plasma blast",
        "legendary treasures",
    ],
}

# Reverse map: set name → era (built once at import time)
SET_TO_ERA: dict[str, str] = {}
for _era, _sets in ERA_SETS.items():
    for _s in _sets:
        SET_TO_ERA[_s.lower()] = _era

MCAP_SET = "miscellaneous cards & products"
SKIP_SET_TAGS = {MCAP_SET, "miscellaneous", ""}

# Base era names that are valid set names but also umbrella terms.
# When scanning a product name for a set tag, these should only match
# if no more specific set name is found first.
# e.g. "Scarlet & Violet 151 Mini Tin" → tag should be "scarlet & violet 151", not "scarlet & violet"
BASE_ERA_NAMES = {
    "scarlet & violet", "scarlet and violet",
    "sword & shield", "sword and shield",
    "sun & moon", "sun and moon",
    "xy", "x&y",
    "mega evolution",
}

# ─── Type / taxonomy tag inference ───────────────────────────────────────────

# Each entry: (regex_pattern, list_of_tags_to_add)
# Patterns matched against product name (case-insensitive). Order matters.
TYPE_RULES = [
    # Must come before generic "box" rules
    (r"ultra[\s-]?premium collection|super premium collection", ["collection box", "ultra premium collection"]),
    (r"elite trainer box|etb", ["etb"]),
    (r"pokemon center elite trainer box|pc ?etb", ["etb", "pcetb"]),
    (r"booster box", ["booster box"]),
    (r"build\s*&?\s*battle", ["buildbattle", "booster pack"]),
    (r"booster bundle", ["booster pack"]),
    (r"sleeved booster", ["sleeved", "booster pack"]),
    # blister before generic booster pack
    (r"blister", ["booster pack", "blister"]),
    (r"booster pack", ["booster pack"]),
    # display cases (mini tin display, tin display) — heavier than a single tin
    (r"\bdisplay\b", ["display"]),
    # tin / chest — before collection rules so "Radiant Collection Tin" → tin
    (r"\btin\b|\bchest\b", ["tin"]),
    # collection variants
    (r"premium collection|special collection|collection box", ["collection box"]),
    # plain "collection" without box — still collection box
    (r"\bcollection\b(?!\s+box)", ["collection box"]),
    # box alone (not preceded by booster, and not already matched)
    (r"(?<!booster\s)\bbox\b", ["collection box"]),
]

# Weight by primary tag (oz)
WEIGHT_MAP = {
    "sleeved": 3,
    "blister": 5,
    "booster pack": 2,
    "booster box": 32,
    "etb": 40,
    "tin": 16,
    "display": 72,
    "collection box": 32,
    "ultra premium collection": 64,
    "buildbattle": 16,
}

# Featured Pokémon patterns
FEATURED_POKEMON = {
    "pikachu": ["pikachu"],
    "charizard": ["charizard"],
    "eevee": ["eevee", "sylveon", "umbreon", "leafeon", "vaporeon",
               "glaceon", "espeon", "flareon", "jolteon"],
}

# ─── Tag inference ────────────────────────────────────────────────────────────

def infer_tags(product_name: str, set_name: str) -> list[str]:
    """Return sorted, deduplicated list of tags for a sealed product."""
    tags = {"pokemon", "sealed"}
    name_lower = product_name.lower()
    set_lower = (set_name or "").lower()

    # Type tags
    type_tags_found = []
    for pattern, add_tags in TYPE_RULES:
        if re.search(pattern, name_lower, re.IGNORECASE):
            for t in add_tags:
                tags.add(t)
                type_tags_found.append(t)
            break  # first match wins for type

    # Set name as tag — skip generic catch-all categories
    if set_name and set_name.lower() not in SKIP_SET_TAGS:
        if set_lower in BASE_ERA_NAMES:
            # Set name is an umbrella (e.g. "Scarlet & Violet") — look for a more specific
            # set name in the product title first (e.g. "scarlet & violet 151")
            detected_set = _detect_set_from_name(name_lower)
            tags.add(detected_set if detected_set else set_lower)
        else:
            tags.add(set_lower)
    elif set_lower in SKIP_SET_TAGS:
        # For MCAP items, try to find the real set from the product name
        detected_set = _detect_set_from_name(name_lower)
        if detected_set:
            tags.add(detected_set)

    # Era goes into metafield only — not a tag

    # Featured Pokémon
    combined = f"{name_lower} {set_lower}"
    for tag, keywords in FEATURED_POKEMON.items():
        if any(kw in combined for kw in keywords):
            tags.add(tag)

    return sorted(tags)


def _detect_era(text: str) -> str | None:
    """Return era tag by matching text against known set names. Most specific first."""
    # Try exact set name matches first
    for set_key, era in SET_TO_ERA.items():
        # Use word-boundary-aware matching; "evolutions" needs to not match "mega evolution"
        if _safe_set_match(set_key, text):
            return era
    return None


def _safe_set_match(set_key: str, text: str) -> bool:
    """
    Match a set name key against text, with special handling for ambiguous names.
    'evolutions' only matches SWSH era — but must not match 'mega evolution' (different key).
    'xy' must not fire on 'xy' that's part of another era's set name.
    """
    # Escape and use word boundaries
    pattern = r'\b' + re.escape(set_key) + r'\b'
    return bool(re.search(pattern, text, re.IGNORECASE))


def _detect_set_from_name(name_lower: str) -> str | None:
    """
    Scan a product name for a known set name and return it (lowercase) if found.

    Prefers more specific set names over base era umbrella names.
    e.g. "Scarlet & Violet 151 Mini Tin" → "scarlet & violet 151" not "scarlet & violet"

    Strategy: collect all matches, then pick the longest (most specific) one,
    excluding base era names unless they are the only match.
    """
    matches = [key for key in SET_TO_ERA if _safe_set_match(key, name_lower)]
    if not matches:
        return None

    # Prefer non-base matches; fall back to base if nothing else found
    specific = [m for m in matches if m not in BASE_ERA_NAMES]
    candidates = specific if specific else matches

    # Return the longest match (most specific set name wins)
    return max(candidates, key=len)


def _detect_era_from_card_mechanic(name: str) -> str | None:
    """
    Infer era from card mechanic keywords present in the product name.
    These are high-confidence signals that don't require a set name.

    Rules (applied to original-case name to preserve V vs v distinction):
      - "VMAX", "VSTAR", "V-UNION", or standalone " V " / " V)" etc → swsh
        (but not if it's part of "EX" or "GX" context — check swsh markers first)
      - lowercase " ex" (not followed by capital letter) → sv
      - "GX" → sm
      - "EX" (all-caps, not part of a longer word) → xy  (could also be vintage EX era,
        but we group those as xy since vintage EX sets are in the xy list)
      - "Mega" alone without "ex"/"EX" context → unknown, return None
    """
    # VMAX / VSTAR / V-UNION → swsh (check before V to avoid substring issues)
    if re.search(r'\bVMAX\b|\bVSTAR\b|\bV-UNION\b', name):
        return "swsh"

    # Standalone V (uppercase, word boundary, not part of VMAX etc) → swsh
    # Match " V " or " V)" or "(V)" but not "EV" or "UV" etc
    if re.search(r'(?<![A-Z])\bV\b(?![A-Z])', name):
        return "swsh"

    # lowercase "ex" as suffix (e.g. "Charizard ex", "Miraidon ex") → sv
    # Must be preceded by space/letter, not be "EX", not followed by capital
    if re.search(r'\s[a-z]+\s+ex\b', name, re.IGNORECASE) or re.search(r'\w ex\b(?![A-Z])', name):
        # Double-check it's actually lowercase ex, not EX
        if re.search(r'\b(?<!\bE)ex\b', name) and not re.search(r'\bEX\b', name):
            return "sv"

    # GX → sm
    if re.search(r'\bGX\b', name):
        return "sm"

    # EX (all caps) → xy era
    if re.search(r'\bEX\b', name):
        return "xy"

    # Mega without other markers → could be XY or Mega era, can't be confident
    # Don't guess — return None

    return None


def infer_weight_oz(product_name: str) -> float:
    """Return estimated product weight in oz based on product name."""
    name_lower = product_name.lower()
    for pattern, add_tags in TYPE_RULES:
        if re.search(pattern, name_lower, re.IGNORECASE):
            for t in add_tags:
                if t in WEIGHT_MAP:
                    return WEIGHT_MAP[t]
    return 8.0  # safe default


def infer_era(product_name: str, set_name: str) -> str | None:
    """
    Return era string or None if it cannot be confidently determined.

    Priority:
    1. Known set name match (most reliable)
    2. Product name contains a known set keyword
    3. Card mechanic keyword in product name (V, VMAX, ex, GX, EX)
    4. None — caller should treat as unknown and flag for manual review
    """
    set_lower = (set_name or "").lower()
    name_lower = product_name.lower()
    is_mcap = set_lower in SKIP_SET_TAGS

    # 1. Set name lookup (skip for MCAP since it's a catch-all)
    if not is_mcap:
        era = _detect_era(set_lower)
        if era:
            return era

    # 2. Product name contains a known set keyword
    era = _detect_era(name_lower)
    if era:
        return era

    # 3. Card mechanic signal in product name
    era = _detect_era_from_card_mechanic(product_name)  # pass original case for V vs v
    if era:
        return era

    # 4. Unknown — don't guess
    return None

# ─── Image processing ─────────────────────────────────────────────────────────

def _download_image(url: str) -> Image.Image:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return Image.open(io.BytesIO(r.content)).convert("RGBA")


def _remove_background_rembg(im: Image.Image) -> Image.Image:
    """
    Remove background using rembg (U2Net ML model) — local inference, no API key needed.
    Model (~170MB) is downloaded on first call and cached in ~/.u2net/.
    Pre-warmed at startup so this never stalls mid-request.
    """
    try:
        from rembg import remove as rembg_remove
    except ImportError:
        raise RuntimeError("rembg not installed — add 'rembg' to requirements.txt")

    buf_in = io.BytesIO()
    im.save(buf_in, format="PNG")
    result_bytes = rembg_remove(buf_in.getvalue())
    return Image.open(io.BytesIO(result_bytes)).convert("RGBA")


def _remove_background_removebg(im: Image.Image) -> Image.Image:
    """
    Remove background via remove.bg API (high quality, 50 free credits/month).
    Raises RuntimeError if API key not set or request fails.
    """
    api_key = os.environ.get("REMOVE_BG_API_KEY", "")
    if not api_key:
        raise RuntimeError("REMOVE_BG_API_KEY not set")

    buf = io.BytesIO()
    im.save(buf, format="PNG")
    resp = requests.post(
        "https://api.remove.bg/v1.0/removebg",
        files={"image_file": ("image.png", buf.getvalue(), "image/png")},
        data={"size": "auto"},
        headers={"X-Api-Key": api_key},
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"remove.bg error {resp.status_code}: {resp.text[:200]}")
    return Image.open(io.BytesIO(resp.content)).convert("RGBA")


def _prewarm_rembg() -> None:
    """
    Pre-warm the rembg model at startup so the first listing creation isn't slow.
    Runs in a background thread — doesn't block Flask startup.
    """
    try:
        from rembg import remove as rembg_remove
        dummy = Image.new("RGB", (64, 64), (255, 255, 255))
        buf = io.BytesIO()
        dummy.save(buf, format="PNG")
        rembg_remove(buf.getvalue())
        logger.info("rembg model pre-warmed successfully")
    except Exception as e:
        logger.warning(f"rembg pre-warm failed (will retry on first use): {e}")


def _matte_product(src_im: Image.Image) -> Image.Image:
    """
    Scale source image to fit within CONTENT_W x CONTENT_H (preserving aspect ratio),
    then center it on a transparent 2000x2000 canvas.
    Exactly mirrors the Canva matte workflow.
    """
    src_w, src_h = src_im.size
    scale = min(CONTENT_W / src_w, CONTENT_H / src_h)
    new_w = max(1, int(round(src_w * scale)))
    new_h = max(1, int(round(src_h * scale)))
    resized = src_im.resize((new_w, new_h), Image.LANCZOS)

    canvas = Image.new("RGBA", (MATTE_W, MATTE_H), (0, 0, 0, 0))
    off_x = (MATTE_W - new_w) // 2
    off_y = (MATTE_H - new_h) // 2
    canvas.alpha_composite(resized, (off_x, off_y))
    return canvas


def _png_bytes(im: Image.Image) -> bytes:
    buf = io.BytesIO()
    im.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def process_product_image(image_url: str, product_name: str) -> bytes:
    """
    Download image from URL, remove background, apply matte.
    Returns PNG bytes ready for Shopify upload.

    Strategy: try remove.bg first (best quality), fall back to rembg (local ML).
    rembg is pre-warmed at startup so fallback is always fast.
    """
    logger.info(f"Downloading image: {image_url}")
    src = _download_image(image_url)

    # Try remove.bg first
    api_key = os.environ.get("REMOVE_BG_API_KEY", "")
    if api_key:
        try:
            logger.info("Removing background via remove.bg")
            no_bg = _remove_background_removebg(src)
            logger.info("remove.bg succeeded")
        except RuntimeError as e:
            logger.warning(f"remove.bg failed ({e}), falling back to rembg")
            no_bg = _remove_background_rembg(src)
    else:
        logger.info("REMOVE_BG_API_KEY not set — using rembg (local ML)")
        no_bg = _remove_background_rembg(src)

    logger.info("Applying matte")
    matted = _matte_product(no_bg)
    return _png_bytes(matted)


# ─── Shopify helpers ──────────────────────────────────────────────────────────

def _shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": os.environ["SHOPIFY_TOKEN"],
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


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


def _rest(method: str, path: str, **kwargs) -> dict:
    store = os.environ["SHOPIFY_STORE"]
    url = f"https://{store}/admin/api/{SHOPIFY_VERSION}{path}"
    resp = requests.request(method, url, headers=_shopify_headers(), timeout=30, **kwargs)
    resp.raise_for_status()
    return resp.json()


def upload_product_image(product_name: str, png_bytes: bytes) -> str:
    """
    Stage PNG to Shopify S3, create a MediaImage, poll until URL is ready.
    Returns the hosted image URL.
    """
    store = os.environ["SHOPIFY_STORE"]
    filename = _slugify(product_name) + ".png"

    # 1) Stage upload
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
        "mimeType": "image/png",
        "httpMethod": "POST",
    }]})
    errs = staged.get("stagedUploadsCreate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"stagedUploadsCreate error: {errs}")
    target = staged["stagedUploadsCreate"]["stagedTargets"][0]

    # 2) POST to S3
    form = {p["name"]: p["value"] for p in target["parameters"]}
    s3 = requests.post(target["url"], data=form,
                       files={"file": (filename, png_bytes, "image/png")}, timeout=60)
    s3.raise_for_status()

    # 3) fileCreate → get MediaImage id
    fc = _gql("""
        mutation FileCreate($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files {
              __typename
              ... on MediaImage { id image { url } }
            }
            userErrors { field message }
          }
        }
    """, {"files": [{
        "originalSource": target["resourceUrl"],
        "contentType": "IMAGE",
        "alt": product_name,
        "filename": filename,
    }]})
    errs = fc.get("fileCreate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"fileCreate error: {errs}")

    files = fc["fileCreate"]["files"]
    if not files:
        raise RuntimeError("fileCreate returned no files")
    fid = files[0]["id"]

    # 4) Poll until URL is ready
    for _ in range(20):
        node = _gql("""
            query FileNode($id: ID!) {
              node(id: $id) {
                __typename
                ... on MediaImage { image { url } }
              }
            }
        """, {"id": fid})
        n = node.get("node", {})
        if n.get("__typename") == "MediaImage":
            url = (n.get("image") or {}).get("url")
            if url:
                return url
        time.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for image URL for file {fid}")


def set_product_image(product_gid: str, image_url: str, alt: str = "") -> None:
    """Attach a hosted image URL to a Shopify product."""
    product_id = product_gid.split("/")[-1]
    _rest("POST", f"/products/{product_id}/images.json", json={
        "image": {"src": image_url, "alt": alt}
    })


def publish_to_all_channels(product_gid: str) -> None:
    """Publish product to every available sales channel."""
    data = _gql("query { publications(first: 50) { nodes { id name } } }")
    pubs = data.get("publications", {}).get("nodes", [])
    logger.info(f"Publishing to {len(pubs)} channels: {[p['name'] for p in pubs]}")

    if not pubs:
        logger.warning("No publications found")
        return

    # Use publishablePublish per channel — most reliable across API versions
    published = []
    failed = []
    for pub in pubs:
        try:
            r = _gql("""
                mutation Publish($id: ID!, $pub: ID!) {
                  publishablePublish(id: $id, input: { publicationId: $pub }) {
                    publishable { ... on Product { id } }
                    userErrors { field message }
                  }
                }
            """, {"id": product_gid, "pub": pub["id"]})
            errs = r.get("publishablePublish", {}).get("userErrors", [])
            if errs:
                msg = "; ".join(e.get("message","") for e in errs)
                if "already published" not in msg.lower():
                    failed.append(f"{pub['name']}: {msg}")
            else:
                published.append(pub["name"])
        except Exception as ex:
            failed.append(f"{pub['name']}: {ex}")

    logger.info(f"Published to: {published}")
    if failed:
        logger.warning(f"Publish failures: {failed}")


def set_product_category(product_gid: str) -> None:
    """
    Set the Shopify taxonomy category using productUpdate with the category GID.
    Requires API version 2024-07+. The category field takes the GID string directly.
    """
    result = _gql("""
        mutation SetCategory($id: ID!, $categoryId: ID!) {
          productUpdate(input: {
            id: $id
            category: $categoryId
          }) {
            product {
              id
              category { fullName }
            }
            userErrors { field message }
          }
        }
    """, {"id": product_gid, "categoryId": TAXONOMY_CATEGORY_GID})
    errs = result.get("productUpdate", {}).get("userErrors", [])
    if errs:
        logger.warning(f"set_product_category errors: {errs}")
    else:
        cat = (result.get("productUpdate", {})
                     .get("product", {})
                     .get("category", {})
                     .get("fullName", ""))
        logger.info(f"Category set to: {cat or TAXONOMY_CATEGORY_GID}")


def set_product_metafields(product_gid: str, tcgplayer_id: str, era: str | None) -> None:
    """Set era and TCGPlayer ID metafields on a product."""
    metafields = []
    import json as _json
    if tcgplayer_id:
        metafields.append({
            "ownerId": product_gid,
            "namespace": "tcg",
            "key": "tcgplayer_id",
            # Store definition shows type = List of single_line_text_field
            "value": _json.dumps([str(tcgplayer_id)]),
            "type": "list.single_line_text_field",
        })
    if era:
        metafields.append({
            "ownerId": product_gid,
            "namespace": "custom",
            "key": "era",
            "value": era,
            "type": "single_line_text_field",
        })
    if not metafields:
        return

    result = _gql("""
        mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { key value }
            userErrors { field message }
          }
        }
    """, {"metafields": metafields})
    errs = result.get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        logger.warning(f"set_product_metafields errors: {errs}")


def set_variant_weight(product_gid: str, weight_oz: float) -> None:
    """Set weight on the first (default) variant of a product."""
    product_id = product_gid.split("/")[-1]
    # Fetch first variant id
    data = _gql("""
        query($id: ID!) {
          product(id: $id) {
            variants(first: 1) { edges { node { id } } }
          }
        }
    """, {"id": product_gid})
    edges = data.get("product", {}).get("variants", {}).get("edges", [])
    if not edges:
        return
    variant_gid = edges[0]["node"]["id"]
    variant_id = variant_gid.split("/")[-1]

    _rest("PUT", f"/variants/{variant_id}.json", json={
        "variant": {
            "id": int(variant_id),
            "weight": weight_oz,
            "weight_unit": "oz",
        }
    })


def set_variant_cost(product_gid: str, unit_cost: float) -> None:
    """Set COGS (unit cost) on the inventory item of the first variant."""
    data = _gql("""
        query($id: ID!) {
          product(id: $id) {
            variants(first: 1) {
              edges { node { inventoryItem { id } } }
            }
          }
        }
    """, {"id": product_gid})
    edges = data.get("product", {}).get("variants", {}).get("edges", [])
    if not edges:
        return
    inv_gid = edges[0]["node"].get("inventoryItem", {}).get("id", "")
    inv_id = inv_gid.split("/")[-1]
    if not inv_id:
        return

    result = _gql("""
        mutation inventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
          inventoryItemUpdate(id: $id, input: $input) {
            inventoryItem { id unitCost { amount } }
            userErrors { field message }
          }
        }
    """, {
        "id": inv_gid,
        "input": {"cost": str(round(unit_cost, 2))},
    })
    errs = result.get("inventoryItemUpdate", {}).get("userErrors", [])
    if errs:
        logger.warning(f"set_variant_cost errors: {errs}")


# ─── Core enrichment ──────────────────────────────────────────────────────────

def enrich_product(product_gid: str, ppt_item: dict, offer_price: float | None = None) -> dict:
    """
    Enrich an existing Shopify product with all inferred data from a PPT item.
    Safe to call on newly created or pre-existing products.

    ppt_item fields used:
        name, setName, tcgPlayerId, imageCdnUrl800 (or imageCdnUrl)

    Returns a summary dict of what was set.
    """
    product_name = ppt_item.get("name", "")
    set_name = ppt_item.get("setName", "")
    tcgplayer_id = str(ppt_item.get("tcgPlayerId", ""))
    image_url = (ppt_item.get("imageCdnUrl800")
                 or ppt_item.get("imageCdnUrl")
                 or ppt_item.get("imageCdnUrl400", ""))

    tags = infer_tags(product_name, set_name)
    era = infer_era(product_name, set_name)
    weight_oz = infer_weight_oz(product_name)
    unit_cost = (offer_price / max(1, ppt_item.get("quantity", 1))
                 if offer_price else None)

    # Build "NEEDS" note for anything we couldn't determine
    needs = []
    if era is None:
        needs.append("ERA (set metafield — could not be determined from name or set)")
    # Could add more signals here in future (e.g. GTIN, description)

    needs_html = ""
    if needs:
        items_html = "".join(f"<li>{n}</li>" for n in needs)
        needs_html = f"<p><strong>⚠ NEEDS MANUAL REVIEW:</strong></p><ul>{items_html}</ul>"

    summary = {
        "product_gid": product_gid,
        "tags": tags,
        "era": era,
        "weight_oz": weight_oz,
        "unit_cost": unit_cost,
        "needs": needs,
        "image_processed": False,
        "errors": [],
    }

    # 1) Tags + core fields via productUpdate (include NEEDS note in body_html if present)
    product_id = product_gid.split("/")[-1]
    try:
        update_payload = {
            "id": int(product_id),
            "tags": ", ".join(tags),
            "product_type": "Pokemon",
            "vendor": "Pack Fresh",
            "template_suffix": "cro-alt",
        }
        if needs_html:
            update_payload["body_html"] = needs_html
        _rest("PUT", f"/products/{product_id}.json", json={"product": update_payload})
    except Exception as e:
        summary["errors"].append(f"product update: {e}")

    # 2) Publish to all channels (product stays DRAFT status)
    try:
        publish_to_all_channels(product_gid)
    except Exception as e:
        summary["errors"].append(f"publish channels: {e}")

    # 3) Taxonomy category
    try:
        set_product_category(product_gid)
    except Exception as e:
        summary["errors"].append(f"category: {e}")

    # 4) Metafields (era + TCGPlayer ID)
    try:
        set_product_metafields(product_gid, tcgplayer_id, era)
    except Exception as e:
        summary["errors"].append(f"metafields: {e}")

    # 5) Variant weight
    try:
        set_variant_weight(product_gid, weight_oz)
    except Exception as e:
        summary["errors"].append(f"weight: {e}")

    # 6) COGS
    if unit_cost is not None:
        try:
            set_variant_cost(product_gid, unit_cost)
        except Exception as e:
            summary["errors"].append(f"cost: {e}")

    # 7) Image (most expensive — do last)
    if image_url:
        try:
            png = process_product_image(image_url, product_name)
            hosted_url = upload_product_image(product_name, png)
            set_product_image(product_gid, hosted_url, alt=product_name)
            summary["image_processed"] = True
            summary["image_url"] = hosted_url
        except Exception as e:
            summary["errors"].append(f"image: {e}")

    return summary


def create_draft_listing(ppt_item: dict, price: float, offer_price: float | None = None) -> dict:
    """
    Create a new DRAFT Shopify product from a PPT item, then enrich it.
    Returns enrichment summary with added 'product_gid' and 'product_id'.
    """
    product_name = ppt_item.get("name", "Unknown Product")
    set_name = ppt_item.get("setName", "")
    tcgplayer_id = str(ppt_item.get("tcgPlayerId", ""))

    payload = {
        "product": {
            "title": product_name,
            "status": "draft",
            "product_type": "Pokemon",
            "vendor": "Pack Fresh",
            "template_suffix": "cro-alt",
            "variants": [{
                "price": str(round(price, 2)),
                "inventory_management": "shopify",
                "requires_shipping": True,
            }],
        }
    }

    result = _rest("POST", "/products.json", json=payload)
    product = result["product"]
    product_id = product["id"]
    product_gid = product["admin_graphql_api_id"]

    logger.info(f"Created draft product {product_id} — {product_name}")

    summary = enrich_product(product_gid, ppt_item, offer_price=offer_price)
    summary["product_id"] = product_id
    summary["product_gid"] = product_gid
    summary["title"] = product_name
    return summary


# ─── Utilities ────────────────────────────────────────────────────────────────

def _slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s)
    return re.sub(r"-{2,}", "-", s).strip("-").lower()
