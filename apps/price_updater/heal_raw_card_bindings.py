"""
heal_raw_card_bindings — re-bind in-stock raw_cards to scrydex when the
nightly updater can't price them.

Targets the rows that the new scrydex-first / tcg-fallback lookup misses
at the card's condition. For each candidate, searches scrydex_price_cache
by (card_name, set_name, card_number, raw). When the search yields exactly
one (scrydex_id, variant) tuple, the row is healed in place — scrydex_id
and variant are written, leaving tcgplayer_id and everything else alone.

Ambiguous (multiple variants — e.g. Bulbasaur ME #133 has both holofoil
and expansionStamp) and no-match candidates are written to
data/heal_raw_review.csv for human resolution.

Default: dry-run. Pass --apply to write changes.

Usage:
    python heal_raw_card_bindings.py            # dry-run
    python heal_raw_card_bindings.py --apply    # write
"""
import argparse
import csv
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "shared"))

from dotenv import load_dotenv
for env_path in [HERE / ".env",
                 HERE.parent / "admin" / ".env",
                 HERE.parent / "inventory" / ".env",
                 HERE.parent / "analytics" / ".env"]:
    if env_path.exists():
        load_dotenv(env_path)
        break

import db as shared_db
shared_db.init_pool()


# Candidates: in-stock raw_cards whose current (scrydex_id | tcgplayer_id)
# binding produces no cache hit at their condition. Mirrors the variant-
# fold rule used by raw_card_updater._lookup_cache_price.
_CANDIDATES_SQL = """
SELECT i.id, i.tcgplayer_id, i.scrydex_id, i.card_name, i.set_name,
       i.card_number, i.condition, i.variant
FROM raw_cards i
WHERE i.state IN ('STORED', 'DISPLAY')
  AND i.current_hold_id IS NULL
  AND i.is_graded = FALSE
  AND NOT EXISTS (
    SELECT 1 FROM scrydex_price_cache c
    WHERE c.product_type = 'card' AND c.price_type = 'raw'
      AND c.market_price IS NOT NULL
      AND UPPER(c.condition) = UPPER(i.condition)
      AND CASE WHEN c.variant IS NULL
                 OR regexp_replace(LOWER(c.variant), '[^a-z0-9]', '', 'g') IN ('normal','holofoil')
               THEN ''
               ELSE regexp_replace(LOWER(c.variant), '[^a-z0-9]', '', 'g')
          END
        = CASE WHEN i.variant IS NULL
                 OR regexp_replace(LOWER(i.variant), '[^a-z0-9]', '', 'g') IN ('normal','holofoil')
               THEN ''
               ELSE regexp_replace(LOWER(i.variant), '[^a-z0-9]', '', 'g')
          END
      AND ((i.scrydex_id IS NOT NULL AND c.scrydex_id = i.scrydex_id)
           OR (i.scrydex_id IS NULL AND i.tcgplayer_id IS NOT NULL
               AND c.tcgplayer_id = i.tcgplayer_id))
  )
ORDER BY i.card_name, i.set_name, i.card_number
"""

_CANDIDATE_MATCHES_SQL = """
SELECT DISTINCT scrydex_id, variant,
       (ARRAY_AGG(tcgplayer_id) FILTER (WHERE tcgplayer_id IS NOT NULL))[1] AS tcgplayer_id
FROM scrydex_price_cache
WHERE product_type = 'card' AND price_type = 'raw'
  AND market_price IS NOT NULL
  AND LOWER(product_name) = LOWER(%s)
  AND LOWER(expansion_name) = LOWER(%s)
  AND card_number = %s
GROUP BY scrydex_id, variant
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="apply heals (default: dry-run)")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="print every heal, not just first 20")
    args = ap.parse_args()

    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}\n")

    candidates = shared_db.query(_CANDIDATES_SQL)
    print(f"Candidates (in-stock raw_cards that fail the price lookup): "
          f"{len(candidates)}\n")
    if not candidates:
        return

    healed = []
    ambiguous = []
    no_match = []

    for c in candidates:
        matches = shared_db.query(
            _CANDIDATE_MATCHES_SQL,
            (c["card_name"], c["set_name"], c["card_number"]),
        )
        if not matches:
            no_match.append(dict(c))
            continue

        chosen = None
        if len(matches) == 1:
            chosen = matches[0]
        elif c["tcgplayer_id"] is not None:
            # Disambiguate by raw_cards.tcgplayer_id — Scrydex publishes one
            # canonical tcg per (scrydex_id, variant), so a match here picks
            # the exact variant.
            tcg_hits = [m for m in matches
                        if m["tcgplayer_id"] == c["tcgplayer_id"]]
            if len(tcg_hits) == 1:
                chosen = tcg_hits[0]

        if chosen is not None:
            healed.append({**dict(c), "new_scrydex_id": chosen["scrydex_id"],
                           "new_variant": chosen["variant"]})
        else:
            ambiguous.append({**dict(c), "options": [dict(m) for m in matches]})

    print("=== Heal candidates (unambiguous) ===")
    for i, h in enumerate(healed):
        if args.verbose or i < 20:
            print(f"  {h['card_name']} {h['set_name']} #{h['card_number']} "
                  f"{h['condition']} variant '{h['variant']}' "
                  f"-> scrydex_id={h['new_scrydex_id']} "
                  f"variant='{h['new_variant']}'")
    if not args.verbose and len(healed) > 20:
        print(f"  ... and {len(healed) - 20} more (use -v to see all)")

    print(f"\n=== Summary ===")
    print(f"Healed (unambiguous):        {len(healed)}")
    print(f"Ambiguous (multi-variant):   {len(ambiguous)}")
    print(f"No match by name/set/number: {len(no_match)}")

    out_dir = HERE / "data"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "heal_raw_review.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["raw_card_id", "card_name", "set_name", "card_number",
                    "condition", "current_variant", "current_tcgplayer_id",
                    "current_scrydex_id", "category", "options"])
        for a in ambiguous:
            opts = "; ".join(
                f"{o['scrydex_id']}|{o['variant']}|tcg={o['tcgplayer_id']}"
                for o in a["options"]
            )
            w.writerow([a["id"], a["card_name"], a["set_name"],
                        a["card_number"], a["condition"], a["variant"],
                        a["tcgplayer_id"], a["scrydex_id"],
                        "ambiguous", opts])
        for n in no_match:
            w.writerow([n["id"], n["card_name"], n["set_name"],
                        n["card_number"], n["condition"], n["variant"],
                        n["tcgplayer_id"], n["scrydex_id"],
                        "no_match", ""])
    print(f"Review CSV: {out_path}\n")

    if args.apply and healed:
        print(f"Applying {len(healed)} heal(s)...")
        applied = 0
        for h in healed:
            try:
                shared_db.execute(
                    """UPDATE raw_cards
                       SET scrydex_id = %s, variant = %s
                       WHERE id = %s""",
                    (h["new_scrydex_id"], h["new_variant"], h["id"]),
                )
                applied += 1
            except Exception as e:
                print(f"  [error] {h['card_name']} #{h['card_number']}: {e}")
        print(f"Applied: {applied}/{len(healed)}")
    elif healed:
        print(f"(re-run with --apply to write the {len(healed)} heal(s))")


if __name__ == "__main__":
    main()
