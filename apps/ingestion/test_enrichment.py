"""
Quick smoke test for product_enrichment inference logic.
Run with: python test_enrichment.py
Does NOT call Shopify or remove.bg.
"""
import sys
sys.path.insert(0, ".")
from product_enrichment import infer_tags, infer_era, infer_weight_oz

CASES = [
    # (product_name, set_name, expected_type_tag, expected_era)
    ("Obsidian Flames Booster Box",             "Obsidian Flames",       "booster box",    "sv"),
    ("Obsidian Flames Booster Bundle",          "Obsidian Flames",       "booster pack",   "sv"),
    ("Scarlet & Violet Elite Trainer Box",      "Scarlet & Violet",      "etb",            "sv"),
    ("Evolving Skies Elite Trainer Box",        "Evolving Skies",        "etb",            "swsh"),
    ("Evolving Skies Booster Pack",             "Evolving Skies",        "booster pack",   "swsh"),
    ("Crimson Invasion 3-Pack Blister",         "Crimson Invasion",      "blister",        "sm"),
    ("Sun & Moon Sleeved Booster Pack",         "Sun & Moon",            "sleeved",        "sm"),
    ("Generations Radiant Collection Tin",      "Generations",           "tin",            "xy"),
    ("Destined Rivals Booster Box",             "Destined Rivals",       "booster box",    "sv"),
    ("Paradox Rift Build & Battle Box",         "Paradox Rift",          "buildbattle",    "sv"),
    ("Pikachu V Box",                           "Sword & Shield",        "collection box", "swsh"),
    ("Charizard Premium Collection",            "Brilliant Stars",       "collection box", "swsh"),
    ("Eevee Heroes Special Collection",         "Eevee Heroes",          "collection box", "vintage"),
    ("Ultra-Premium Collection Charizard",      "Scarlet & Violet",      "ultra premium collection", "sv"),
    ("Phantasmal Flames Booster Box",           "Phantasmal Flames",     "booster box",    "mega"),
    ("Base Set Booster Pack",                   "Base Set",              "booster pack",   "vintage"),
    ("XY Evolutions Booster Pack",              "XY Evolutions",         "booster pack",   "xy"),
    ("Crown Zenith Premium Figure Collection",  "Crown Zenith",          "collection box", "swsh"),
    ("Hidden Fates Tin",                        "Hidden Fates",          "tin",            "sm"),
    ("Brilliant Stars Booster Pack",            "Brilliant Stars",       "booster pack",   "swsh"),
]

PASS = FAIL = 0
for name, set_name, expected_type, expected_era in CASES:
    tags = infer_tags(name, set_name)
    era = infer_era(name, set_name)
    weight = infer_weight_oz(name)

    type_ok = expected_type in tags
    era_ok = era == expected_era

    status = "✅" if (type_ok and era_ok) else "❌"
    if type_ok and era_ok:
        PASS += 1
    else:
        FAIL += 1

    if not (type_ok and era_ok):
        print(f"{status} {name!r}")
        if not type_ok:
            print(f"   type: expected '{expected_type}' in {tags}")
        if not era_ok:
            print(f"   era:  expected '{expected_era}', got '{era}'")
    else:
        print(f"{status} {name!r}  [{expected_type}, era={era}, {weight}oz]")

print(f"\n{PASS}/{PASS+FAIL} passed")
