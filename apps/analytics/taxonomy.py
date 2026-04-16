"""
Product taxonomy classifier.

Classifies every SKU in inventory_product_cache into dimensional
categories (IP, form_factor, set, era, product_type) and writes
to the product_taxonomy table.

Reuses TYPE_RULES and ERA_SETS from shared/product_enrichment.py
for form factor and era detection (Pokemon). Non-Pokemon IPs are
detected via title keywords.

Rows with manual_override=TRUE are never overwritten.
"""

import re
import logging
from datetime import datetime, timezone

import db

logger = logging.getLogger(__name__)

# ── Classification rules (mirrored from product_enrichment.py) ───────────────
# These are duplicated here to avoid importing product_enrichment.py which
# pulls in PIL/Pillow and other heavy deps not needed for classification.
# When new sets release, update ERA_SETS here AND in product_enrichment.py.

TYPE_RULES = [
    (r"ultra[\s-]?premium collection|super premium collection", ["collection box", "ultra premium collection"]),
    (r"elite trainer box|etb", ["etb"]),
    (r"pokemon center elite trainer box|pc ?etb", ["etb", "pcetb"]),
    (r"booster box", ["booster box"]),
    (r"build\s*&?\s*battle", ["buildbattle", "booster pack"]),
    (r"booster bundle", ["booster pack"]),
    (r"sleeved booster", ["sleeved", "booster pack"]),
    (r"blister", ["booster pack", "blister"]),
    (r"booster pack", ["booster pack"]),
    (r"\bdisplay\b", ["display"]),
    (r"\btin\b|\bchest\b", ["tin"]),
    (r"tech\s+sticker", ["booster pack", "blister"]),
    (r"premium collection|special collection|collection box", ["collection box"]),
    (r"\bcollection\b(?!\s+box)", ["collection box"]),
    (r"(?<!booster\s)\bbox\b", ["collection box"]),
]

ERA_SETS = {
    "mega": [
        "mega evolution", "phantasmal flames", "ascended heroes", "perfect order",
    ],
    "sv": [
        "scarlet & violet", "scarlet and violet", "paldea evolved", "obsidian flames",
        "151", "paradox rift", "paldean fates", "temporal forces", "stellar crown",
        "shrouded fable", "surging sparks", "twilight masquerade", "prismatic evolutions",
        "prismatic evolution", "journey together", "destined rivals", "black bolt", "white flare",
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

# Reverse map: set name → era
SET_TO_ERA: dict[str, str] = {}
for _era, _sets in ERA_SETS.items():
    for _s in _sets:
        SET_TO_ERA[_s.lower()] = _era

# ── IP detection keywords ────────────────────────────────────────────────────

# Checked against title + tags (case-insensitive). Order matters — first match wins.
IP_RULES = [
    ("pokemon", [
        r"\bpokemon\b", r"\bpok[ée]mon\b", r"\bpikachu\b", r"\bcharizard\b",
        r"\beevee\b", r"\bmewtwo\b", r"\bbooster pack\b.*\b(sv|swsh|sm|xy)\b",
    ]),
    ("mtg", [
        r"\bmagic\b", r"\bmagic:\s*the\s*gathering\b", r"\bmtg\b",
        r"\bcommander\b", r"\bplaneswalker\b", r"\bset\s*booster\b",
        r"\bdraft\s*booster\b", r"\bcollector\s*booster\b",
    ]),
    ("yugioh", [
        r"\byu-?gi-?oh\b", r"\byugioh\b",
    ]),
    ("onepiece", [
        r"\bone\s*piece\b",
    ]),
    ("lorcana", [
        r"\blorcana\b",
    ]),
]

# ── Form factor mapping ─────────────────────────────────────────────────────

# TYPE_RULES returns tag lists like ["etb"], ["booster box"], etc.
# Map the first recognized tag to our canonical form_factor enum.
TAG_TO_FORM_FACTOR = {
    "etb": "etb",
    "pcetb": "etb",
    "booster box": "booster_box",
    "booster pack": "booster_pack",
    "sleeved": "booster_pack",
    "blister": "blister",
    "tin": "tin",
    "collection box": "collection_box",
    "ultra premium collection": "upc",
    "buildbattle": "build_battle",
    "display": "display",
}


def _detect_ip(title: str, tags: str) -> str:
    """Detect IP/game from title and tags. Returns lowercase IP name."""
    text = f"{title} {tags}".lower()
    for ip_name, patterns in IP_RULES:
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return ip_name
    return "other"


def _detect_form_factor(title: str) -> str:
    """Detect form factor from title using TYPE_RULES from product_enrichment."""
    title_lower = title.lower()

    # Check if this is a single card (no sealed product keywords)
    # Single cards typically don't match any TYPE_RULES
    for pattern, add_tags in TYPE_RULES:
        if re.search(pattern, title_lower, re.IGNORECASE):
            # Map first recognized tag to our canonical form_factor
            for tag in add_tags:
                if tag in TAG_TO_FORM_FACTOR:
                    return TAG_TO_FORM_FACTOR[tag]
            # Matched a rule but no canonical mapping — use first tag
            return add_tags[0].replace(" ", "_") if add_tags else "other"

    # No sealed product rule matched — likely a single card or slab
    if re.search(r"\bpsa\b|\bbgs\b|\bcgc\b|\bslab\b|\bgraded\b", title_lower):
        return "slab"

    return "single_card"


def _detect_product_type(form_factor: str) -> str:
    """Derive product_type from form_factor."""
    if form_factor in ("single_card", "slab"):
        return "card"
    return "sealed"


def _build_scrydex_map() -> dict:
    """Pre-fetch all scrydex expansion data keyed by tcgplayer_id."""
    rows = db.query("""
        SELECT DISTINCT ON (tcgplayer_id)
            tcgplayer_id, expansion_id, expansion_name
        FROM scrydex_price_cache
        WHERE tcgplayer_id IS NOT NULL
    """)
    return {r["tcgplayer_id"]: r for r in rows}


def _detect_expansion(tcgplayer_id: int, title: str, scrydex_map: dict) -> tuple:
    """
    Detect expansion_id and set_name.
    1. Try scrydex_map lookup by tcgplayer_id (pre-fetched)
    2. Fallback: parse title against ERA_SETS
    Returns (expansion_id, set_name, era) — any may be None.
    """
    expansion_id = None
    set_name = None
    era = None

    # Try scrydex lookup from pre-fetched map
    if tcgplayer_id and tcgplayer_id in scrydex_map:
        row = scrydex_map[tcgplayer_id]
        expansion_id = row["expansion_id"]
        set_name = row["expansion_name"]

    # Derive era from set_name
    if set_name:
        era = SET_TO_ERA.get(set_name.lower())

    # Fallback: parse title for set name
    if not set_name:
        title_lower = title.lower()
        for era_name, set_list in ERA_SETS.items():
            for s in set_list:
                if s.lower() in title_lower:
                    set_name = s
                    era = era_name
                    break
            if set_name:
                break

    return expansion_id, set_name, era


def classify_taxonomy():
    """
    Classify all SKUs in inventory_product_cache into product_taxonomy.
    Skips rows with manual_override=TRUE.
    Returns count of classified SKUs.
    """
    logger.info("Classifying product taxonomy...")

    # Get all inventory variants
    rows = db.query("""
        SELECT shopify_variant_id, shopify_product_id, tcgplayer_id,
               title, tags, is_damaged
        FROM inventory_product_cache
        WHERE shopify_variant_id IS NOT NULL
    """)

    if not rows:
        logger.info("No inventory rows to classify")
        return {"classified": 0}

    # Pre-fetch scrydex expansion data (one query instead of per-SKU)
    scrydex_map = _build_scrydex_map()
    logger.info(f"Loaded {len(scrydex_map)} scrydex expansion mappings")

    # Get existing manual overrides to skip
    overrides = db.query(
        "SELECT shopify_variant_id FROM product_taxonomy WHERE manual_override = TRUE"
    )
    override_set = {r["shopify_variant_id"] for r in overrides}

    now = datetime.now(timezone.utc)
    classified = 0

    for row in rows:
        vid = row["shopify_variant_id"]
        if vid in override_set:
            continue

        title = row["title"] or ""
        tags = row["tags"] or ""
        tcg_id = row["tcgplayer_id"]

        ip = _detect_ip(title, tags)
        form_factor = _detect_form_factor(title)
        product_type = _detect_product_type(form_factor)
        expansion_id, set_name, era = _detect_expansion(tcg_id, title, scrydex_map)

        db.execute("""
            INSERT INTO product_taxonomy (
                shopify_variant_id, shopify_product_id, tcgplayer_id, title,
                ip, product_type, form_factor, expansion_id, set_name, era,
                classified_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (shopify_variant_id) DO UPDATE SET
                shopify_product_id = EXCLUDED.shopify_product_id,
                tcgplayer_id = EXCLUDED.tcgplayer_id,
                title = EXCLUDED.title,
                ip = EXCLUDED.ip,
                product_type = EXCLUDED.product_type,
                form_factor = EXCLUDED.form_factor,
                expansion_id = EXCLUDED.expansion_id,
                set_name = EXCLUDED.set_name,
                era = EXCLUDED.era,
                classified_at = EXCLUDED.classified_at,
                updated_at = EXCLUDED.updated_at
            WHERE product_taxonomy.manual_override = FALSE
        """, (
            vid, row["shopify_product_id"], tcg_id, title,
            ip, product_type, form_factor, expansion_id, set_name, era,
            now, now,
        ))
        classified += 1

    logger.info(f"Classified {classified} SKUs ({len(override_set)} manual overrides skipped)")
    return {"classified": classified, "overrides_skipped": len(override_set)}
