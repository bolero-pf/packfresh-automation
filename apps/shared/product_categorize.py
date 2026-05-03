"""Canonical product categorizer for sealed + raw intake items.

The single source of truth is the Shopify product tag set. The bulk
editor in /inventory/ keeps tags consistent across the catalog, so
classification just reads them. When tags are missing (cache hasn't
been synced yet, or item never matched a Shopify product), we fall
back to product-name regex.

Sean's exclusion rules — a "plain" booster pack is the tag set
{booster pack} minus {sleeved, blister, buildbattle, booster bundle}.
The ordering of checks below encodes that: more-specific categories
match first so a Sleeved Booster Pack never falls through to Booster
Pack.

Returns a stable string label suitable for grouping and display.
"""

from typing import Iterable

# Display labels in priority order (more-specific → more-general).
# Each entry: (label, tag_predicates, name_predicates)
#   tag_predicates: any tag in this set ⇒ match
#   name_predicates: any substring (lowercased) ⇒ match
#
# Raw/Graded are handled before this list since they key off product_type.
SEALED_RULES = [
    ("ETB",                  {"etb"},                            ("elite trainer box",)),
    ("Build & Battle",       {"buildbattle"},                    ("build & battle", "build and battle", "build battle box")),
    ("Blister",              {"blister"},                        ("blister",)),
    ("Sleeved Booster Pack", {"sleeved"},                        ("sleeved booster",)),
    ("Booster Bundle",       {"booster bundle"},                 ("booster bundle",)),
    ("Booster Box",          {"booster box"},                    ("booster box",)),
    ("Booster Pack",         {"booster pack"},                   ("booster pack",)),
    ("Premium Collection",   {"premium collection"},             ("premium collection", "ultra premium")),
    # Collection Box is the bulk of our 'collection' SKUs — V Box, VMAX
    # Box, promo collection boxes, etc. Match it before generic
    # 'collection' so the catchall doesn't swallow it.
    ("Collection Box",       {"collection box"},                 ("collection box",)),
    ("Collection",           {"collection"},                     ("collection",)),
    ("Tin",                  {"tin"},                            (" tin", "tin ")),
    ("Trainer Kit",          {"trainer kit"},                    ("trainer kit",)),
    ("Theme Deck",           {"theme deck", "deck"},             ("theme deck",)),
]


def _normalize_tags(tags_csv: str) -> set[str]:
    """Split a comma-separated tag string into a lowercased set."""
    if not tags_csv:
        return set()
    return {t.strip().lower() for t in tags_csv.split(",") if t.strip()}


def classify_item(item: dict, tags_csv: str = "") -> str:
    """Return the canonical category label for an intake item.

    Args:
        item:       dict with at least 'product_type'. Optional:
                    'is_graded', 'product_name'.
        tags_csv:   comma-separated Shopify tags from
                    inventory_product_cache.tags. May be empty.

    Returns:
        One of: "Graded", "Raw", "ETB", "Booster Box", "Booster Bundle",
        "Booster Pack", "Sleeved Booster Pack", "Blister",
        "Build & Battle", "Premium Collection", "Collection", "Tin",
        "Trainer Kit", "Theme Deck", "Other".
    """
    if (item.get("product_type") or "").lower() == "raw":
        return "Graded" if item.get("is_graded") else "Raw"

    tags = _normalize_tags(tags_csv)
    name = (item.get("product_name") or "").lower()
    name_padded = f" {name} "  # so " tin" matches the word, not "Latin"

    for label, tag_set, name_subs in SEALED_RULES:
        if tags & tag_set:
            return label
        if not tags and any(s in name_padded for s in name_subs):
            # Name fallback only when tags are absent — once a product is
            # tagged, the tag set is authoritative. Tagging "blister" but
            # leaving "booster pack" out is intentional and the classifier
            # should respect that.
            return label

    return "Other"


# Display order for charts/badges so all consumers render the same
# left-to-right. Roughly value-density first.
DISPLAY_ORDER = [
    "Booster Box",
    "ETB",
    "Build & Battle",
    "Booster Bundle",
    "Premium Collection",
    "Collection Box",
    "Collection",
    "Blister",
    "Sleeved Booster Pack",
    "Booster Pack",
    "Tin",
    "Trainer Kit",
    "Theme Deck",
    "Graded",
    "Raw",
    "Other",
]


def sort_categories(categories: Iterable[str]) -> list[str]:
    """Return categories ordered by DISPLAY_ORDER, with unknowns last."""
    cats = list(categories)
    known = [c for c in DISPLAY_ORDER if c in cats]
    extra = [c for c in cats if c not in DISPLAY_ORDER]
    return known + sorted(extra)
