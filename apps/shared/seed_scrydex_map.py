"""
Seed the scrydex_tcg_map table by pulling all Pokémon sets from Scrydex
and extracting TCGPlayer IDs from variants[].marketplaces[].

Usage:
    python seed_scrydex_map.py [--sets sv8,sv8pt5] [--all]

With --all, pulls every English expansion (~250 sets, ~250 credits).
With --sets, pulls only the specified expansion IDs.
Without args, pulls only sets that have active inventory.
"""

import os
import sys
import time
import argparse
import logging

# Add shared/ to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Seed scrydex_tcg_map from set pulls")
    parser.add_argument("--sets", help="Comma-separated expansion IDs (e.g. sv8,sv8pt5)")
    parser.add_argument("--all", action="store_true", help="Pull ALL English expansions")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    from scrydex_client import ScrydexClient
    import db

    api_key = os.getenv("SCRYDEX_API_KEY")
    team_id = os.getenv("SCRYDEX_TEAM_ID")
    if not api_key or not team_id:
        print("Set SCRYDEX_API_KEY and SCRYDEX_TEAM_ID env vars")
        sys.exit(1)

    # Ensure mapping table exists
    db.execute("""
        CREATE TABLE IF NOT EXISTS scrydex_tcg_map (
            scrydex_id    TEXT PRIMARY KEY,
            tcgplayer_id  INTEGER NOT NULL,
            product_type  TEXT DEFAULT 'card',
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            updated_at    TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_scrydex_tcg_map_tcg
            ON scrydex_tcg_map(tcgplayer_id)
    """)

    client = ScrydexClient(api_key, team_id, db=db)

    # Determine which sets to pull
    if args.sets:
        expansion_ids = [s.strip() for s in args.sets.split(",")]
    elif args.all:
        logger.info("Fetching all English expansions...")
        expansions = client.get_expansions(language_code="EN")
        expansion_ids = [e["id"] for e in expansions]
        logger.info(f"Found {len(expansion_ids)} expansions")
    else:
        # Pull sets that have active inventory
        logger.info("Finding sets with active inventory...")
        rows = db.query("""
            SELECT DISTINCT set_name FROM raw_cards
            WHERE state IN ('STORED', 'DISPLAY')
            AND set_name IS NOT NULL AND set_name != ''
            UNION
            SELECT DISTINCT sbc.set_name
            FROM sealed_breakdown_components sbc
            WHERE sbc.set_name IS NOT NULL AND sbc.set_name != ''
        """)
        set_names = {r["set_name"] for r in rows}
        logger.info(f"Found {len(set_names)} active set names in inventory")

        # Map set names to Scrydex expansion IDs
        logger.info("Fetching Scrydex expansion list...")
        expansions = client.get_expansions(language_code="EN")
        name_to_id = {e["name"].lower(): e["id"] for e in expansions}

        expansion_ids = []
        unmapped = []
        for sn in set_names:
            eid = name_to_id.get(sn.lower())
            if eid:
                expansion_ids.append(eid)
            else:
                unmapped.append(sn)

        if unmapped:
            logger.warning(f"Could not map {len(unmapped)} set names to Scrydex: {unmapped[:10]}")
        logger.info(f"Will pull {len(expansion_ids)} mapped expansions")

    if not expansion_ids:
        print("No expansions to pull.")
        return

    if args.dry_run:
        print(f"DRY RUN: Would pull {len(expansion_ids)} expansions:")
        for eid in expansion_ids[:20]:
            print(f"  {eid}")
        if len(expansion_ids) > 20:
            print(f"  ... and {len(expansion_ids) - 20} more")
        print(f"Estimated credits: ~{len(expansion_ids) * 3}")
        return

    # Pull each set and extract TCGPlayer IDs
    total_mapped = 0
    total_cards = 0
    total_credits = 0

    for i, eid in enumerate(expansion_ids):
        logger.info(f"[{i+1}/{len(expansion_ids)}] Pulling {eid}...")
        try:
            # Get raw cards (we need the raw response for marketplaces)
            page = 1
            set_cards = 0
            set_mapped = 0

            while True:
                params = {
                    "page": page,
                    "page_size": 100,
                    "include": "prices",
                }
                resp = client._get(
                    f"{client.base_url}/pokemon/v1/expansions/{eid}/cards",
                    params
                )
                total_credits += 1
                items = resp.get("data", [])
                if not items:
                    break

                for card in items:
                    set_cards += 1
                    scrydex_id = card.get("id")
                    if not scrydex_id:
                        continue

                    # Extract TCGPlayer ID from marketplaces
                    tcg_id = None
                    for variant in (card.get("variants") or []):
                        for mp in (variant.get("marketplaces") or []):
                            if mp.get("name") == "tcgplayer" and mp.get("product_id"):
                                try:
                                    tcg_id = int(mp["product_id"])
                                    break
                                except (ValueError, TypeError):
                                    pass
                        if tcg_id:
                            break

                    if tcg_id:
                        try:
                            db.execute("""
                                INSERT INTO scrydex_tcg_map
                                    (scrydex_id, tcgplayer_id, product_type, updated_at)
                                VALUES (%s, %s, 'card', NOW())
                                ON CONFLICT (scrydex_id)
                                DO UPDATE SET tcgplayer_id = EXCLUDED.tcgplayer_id,
                                             updated_at = NOW()
                            """, (scrydex_id, tcg_id))
                            set_mapped += 1
                        except Exception as e:
                            # Likely duplicate tcgplayer_id (different variants)
                            logger.debug(f"Mapping conflict {scrydex_id}->{tcg_id}: {e}")

                total_count = resp.get("totalCount", 0)
                if page * 100 >= total_count:
                    break
                page += 1

            total_cards += set_cards
            total_mapped += set_mapped
            logger.info(f"  {eid}: {set_cards} cards, {set_mapped} mapped to TCGPlayer IDs")

        except Exception as e:
            logger.error(f"  Failed to pull {eid}: {e}")

        # Brief pause between sets to be polite
        time.sleep(0.1)

    # Also pull sealed products for each set
    logger.info("Pulling sealed products...")
    sealed_mapped = 0
    for i, eid in enumerate(expansion_ids):
        try:
            params = {"page_size": 100, "include": "prices"}
            resp = client._get(
                f"{client.base_url}/pokemon/v1/expansions/{eid}/sealed",
                params
            )
            total_credits += 1
            for item in (resp.get("data") or []):
                scrydex_id = item.get("id")
                # Sealed products don't have marketplaces, but save ID for name-based mapping later
                if scrydex_id:
                    # Check if marketplaces exists (might be added in future)
                    tcg_id = ScrydexClient._extract_tcgplayer_id(item)
                    if tcg_id:
                        try:
                            db.execute("""
                                INSERT INTO scrydex_tcg_map
                                    (scrydex_id, tcgplayer_id, product_type, updated_at)
                                VALUES (%s, %s, 'sealed', NOW())
                                ON CONFLICT (scrydex_id)
                                DO UPDATE SET tcgplayer_id = EXCLUDED.tcgplayer_id,
                                             updated_at = NOW()
                            """, (scrydex_id, tcg_id))
                            sealed_mapped += 1
                        except Exception:
                            pass
        except Exception as e:
            logger.debug(f"Sealed pull for {eid} failed: {e}")

    print(f"\nDone!")
    print(f"  Sets pulled:       {len(expansion_ids)}")
    print(f"  Cards seen:        {total_cards}")
    print(f"  Cards mapped:      {total_mapped}")
    print(f"  Sealed mapped:     {sealed_mapped}")
    print(f"  Credits used:      ~{total_credits}")

    # Show current mapping count
    row = db.query_one("SELECT COUNT(*) as cnt FROM scrydex_tcg_map")
    print(f"  Total mappings:    {row['cnt']}")


if __name__ == "__main__":
    main()
