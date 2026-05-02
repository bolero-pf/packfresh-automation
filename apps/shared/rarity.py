"""Rarity normalization + tier-order display helpers.

Used by the kiosk to display rarity facets in collector-tier order rather than
count-DESC, and by the ingest path to fold word-order duplicates ("Rare Holo"
vs "Holo Rare", "Rare Shiny" vs "Shiny Rare") to one canonical form before
they hit raw_cards.
"""

# Canonical map: lowered+stripped variant → canonical form.
# Pokemon-specific. Other games' rarities are clean enough not to need this.
_POKEMON_CANONICAL = {
    "rare holo": "Holo Rare",
    "holo rare": "Holo Rare",
    "rare shiny": "Shiny Rare",
    "shiny rare": "Shiny Rare",
}

# Display order for Pokemon rarity chips on the kiosk filter sheet. Loosely
# tracks collector progression bulk → rare → ultra → secret. Anything not
# in this list (new rarities, edge cases, non-English) sorts to the end in
# alphabetical order.
POKEMON_TIER_ORDER = [
    "Common",
    "Uncommon",
    "Rare",
    "Holo Rare",
    "Shiny Holo Rare",
    "Radiant Rare",
    "Double Rare",
    "Ultra Rare",
    "Shiny Rare",
    "Shiny Ultra Rare",
    "Hyper Rare",
    "Secret Rare",
    "Illustration Rare",
    "Special Illustration Rare",
    "Promo",
]

# Compact display labels for chips. Filtering still keys off the full name
# (sent as `value`); only the on-chip text changes. Common community
# abbreviations so collectors recognize them at a glance and the chip
# doesn't have to wrap to two lines.
POKEMON_RARITY_DISPLAY = {
    "Special Illustration Rare": "SIR",
    "Illustration Rare":         "IR",
}


def pokemon_chip_label(rarity):
    """Return the on-chip display string for a Pokemon rarity (abbreviated
    when there's a known short form, otherwise the original)."""
    if not rarity:
        return rarity
    return POKEMON_RARITY_DISPLAY.get(rarity, rarity)


def canonicalize_rarity(rarity, game=None):
    """Return the canonical form of a rarity string for storage.

    Folds known word-order duplicates ("Rare Holo" → "Holo Rare", etc.) so
    the kiosk facet doesn't show two chips for the same thing. The folded
    forms are Pokemon terminology — Magic / Lorcana / OP rarities don't
    share these word patterns — so the rule is safe to apply unconditionally
    when game isn't known. Returns the input unchanged when no mapping
    applies (unknown / non-English rarities pass through untouched).
    """
    if not rarity:
        return rarity
    key = rarity.strip().lower()
    return _POKEMON_CANONICAL.get(key, rarity)


def pokemon_tier_index(rarity):
    """Sort key for Pokemon rarity chips. Unknown rarities sort last."""
    if not rarity:
        return (1, "")
    try:
        return (0, POKEMON_TIER_ORDER.index(rarity))
    except ValueError:
        return (1, rarity.lower())
