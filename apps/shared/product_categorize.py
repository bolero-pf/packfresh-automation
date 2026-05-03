"""Canonical product categorizer for sealed + raw intake items.

Source of truth: the Shopify product tag set. Sean keeps tags consistent
via the bulk editor in /inventory/. When tags are missing (cache cold,
or item never matched a Shopify product), we fall back to product-name
substring matching on the same vocabulary.

Pack Fresh tag taxonomy — these are NOT disjoint. A PCETB carries both
'pcetb' and 'etb'. A blister carries both 'blister' and 'booster pack'.
The 'booster pack' tag is an umbrella applied to anything that is, or
contains, a booster pack — Sean uses it as a 'lower-priced items' label
for a PMAX advertising campaign, so its semantics are 'cheap product'
rather than 'literally just a booster pack.'

A plain 'Booster Pack' is therefore by absentia: tagged 'booster pack'
but NOT (blister, sleeved, buildbattle, booster bundle, booster box,
collection box, etb, pcetb). Rule ordering below encodes that — more-
specific categories match first, so the Booster Pack rule only ever
sees rows that escaped every more-specific bucket.

Returns a stable string label suitable for grouping and display.
"""

from typing import Iterable

# (label, tag_predicates, name_predicates)
#   tag_predicates: any tag in this set ⇒ match
#   name_predicates: any substring (lowercased) ⇒ match (only used when
#                    tags are absent — once a product is tagged the tag
#                    set is authoritative)
#
# Raw / Graded are handled before this list (they key off product_type).
SEALED_RULES = [
    # PCETB before ETB so 'pcetb' wins when both tags are present.
    ("PCETB",                {"pcetb"},                          ("premium collection etb", " pcetb ")),
    # Both-side word-boundary 'etb' — short enough to false-positive on
    # 'EtbBox' or similar typos if matched as a bare substring.
    ("ETB",                  {"etb"},                            ("elite trainer box", " etb ")),
    # Case (booster-box case) — sits above Booster Box because a Case
    # row may also carry 'booster box'. 19 SKUs in production today.
    ("Case",                 {"case"},                           ("booster box case",)),
    # Build & Battle and Booster Bundle don't have confirmed canonical
    # tags yet — Sean will add 'booster bundle' via the bulk editor
    # when ready. Until then they match by name when tags are absent
    # and otherwise fall through to Booster Pack (which is fine given
    # the umbrella semantics).
    ("Build & Battle",       {"buildbattle"},                    ("build & battle", "build and battle", "build battle box")),
    ("Booster Bundle",       {"booster bundle"},                 ("booster bundle",)),
    ("Blister",              {"blister"},                        ("blister",)),
    ("Sleeved Booster Pack", {"sleeved"},                        ("sleeved booster",)),
    ("Booster Box",          {"booster box"},                    ("booster box",)),
    ("Collection Box",       {"collection box"},                 ("collection box",)),
    ("Tin",                  {"tin"},                            (" tin", "tin ")),
    # By-absentia bucket — only catches rows that escaped every more-
    # specific rule above. Sean uses 'booster pack' as an umbrella tag
    # (applied to blister/sleeved/buildbattle/etc.) for a lower-priced-
    # items PMAX advertising campaign, so this bucket represents
    # 'a bare booster pack with no other form factor context.'
    ("Booster Pack",         {"booster pack"},                   ("booster pack",)),
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
    "Case",
    "Booster Box",
    "PCETB",
    "ETB",
    "Build & Battle",
    "Booster Bundle",
    "Collection Box",
    "Blister",
    "Sleeved Booster Pack",
    "Booster Pack",
    "Tin",
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
