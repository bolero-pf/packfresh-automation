"""
Seed all TCG games into scrydex_price_cache.

Usage:
    python seed_all_games.py                    # Seed all games
    python seed_all_games.py --games mtg,pokemon  # Seed specific games
    python seed_all_games.py --dry-run          # Show what would be synced
    python seed_all_games.py --games pokemon --language JA  # JP Pokemon only

This runs the full --all sync for each game. For Pokemon, it does EN + JA.
"""
import os
import sys
import time
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Short aliases for convenience
GAME_ALIASES = {
    "mtg": "magicthegathering",
    "magic": "magicthegathering",
    "op": "onepiece",
    "pkm": "pokemon",
    "poke": "pokemon",
}

ALL_GAMES = [
    ("pokemon", "EN"),
    ("pokemon", "JA"),
    ("magicthegathering", None),
    ("onepiece", None),
    ("lorcana", None),
    ("riftbound", None),
]


def main():
    parser = argparse.ArgumentParser(description="Seed all TCG games into Scrydex cache")
    parser.add_argument("--games", help="Comma-separated games (e.g., pokemon,mtg,onepiece). Default: all")
    parser.add_argument("--language", default=None, help="Override language filter (e.g., JA)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from scrydex_client import ScrydexClient
    from scrydex_nightly import sync_expansion
    import db

    api_key = os.getenv("SCRYDEX_API_KEY")
    team_id = os.getenv("SCRYDEX_TEAM_ID")
    if not api_key or not team_id:
        print("Set SCRYDEX_API_KEY and SCRYDEX_TEAM_ID")
        sys.exit(1)

    db.init_pool()

    # Determine which games to seed
    if args.games:
        requested = [g.strip().lower() for g in args.games.split(",")]
        game_list = []
        for g in requested:
            game = GAME_ALIASES.get(g, g)
            if args.language:
                game_list.append((game, args.language))
            else:
                # For pokemon, do both EN + JA unless language specified
                if game == "pokemon":
                    game_list.append(("pokemon", "EN"))
                    game_list.append(("pokemon", "JA"))
                else:
                    game_list.append((game, None))
    else:
        game_list = ALL_GAMES

    grand_start = time.time()
    grand_totals = {"expansions": 0, "cards": 0, "sealed": 0, "prices": 0, "credits": 0}

    for game, lang in game_list:
        label = f"{game}" + (f"/{lang}" if lang else "")
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"{'='*60}")

        client = ScrydexClient(api_key, team_id, db=db, game=game)

        try:
            expansions = client.get_expansions(language_code=lang)
        except Exception as e:
            print(f"  Failed to get expansions: {e}")
            continue

        expansion_ids = [e["id"] for e in expansions]
        print(f"  Found {len(expansion_ids)} expansions")

        if args.dry_run:
            for eid in expansion_ids[:10]:
                name = next((e.get("name", "") for e in expansions if e["id"] == eid), "")
                print(f"    {eid:25s} {name}")
            if len(expansion_ids) > 10:
                print(f"    ... and {len(expansion_ids) - 10} more")
            continue

        totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0}
        failures = []
        t_start = time.time()

        for i, eid in enumerate(expansion_ids):
            try:
                stats = sync_expansion(client, eid, db)
                for k in totals:
                    totals[k] += stats.get(k, 0)
                if (i + 1) % 25 == 0 or i == len(expansion_ids) - 1:
                    elapsed = int(time.time() - t_start)
                    print(f"  [{i+1}/{len(expansion_ids)}] {totals['cards']} cards, "
                          f"{totals['sealed']} sealed, {totals['credits']} credits ({elapsed}s)")
            except Exception as e:
                logger.error(f"  {eid}: {e}")
                failures.append((eid, str(e)))
            time.sleep(0.05)

        # Retry failures
        if failures:
            print(f"  --- Retrying {len(failures)} failed expansions ---")
            still_failed = []
            for eid, original_error in failures:
                try:
                    stats = sync_expansion(client, eid, db)
                    for k in totals:
                        totals[k] += stats.get(k, 0)
                    print(f"    Retry OK: {eid} ({stats['cards']} cards, {stats['sealed']} sealed)")
                except Exception as e:
                    still_failed.append((eid, original_error, str(e)))
                time.sleep(0.1)
            if still_failed:
                print(f"  *** {len(still_failed)} expansions still failed after retry:")
                for eid, err1, err2 in still_failed:
                    print(f"    {eid}: {err1[:80]}")

        elapsed = int(time.time() - t_start)
        print(f"  Done: {len(expansion_ids)} expansions, {totals['cards']} cards, "
              f"{totals['sealed']} sealed, {totals['credits']} credits in {elapsed}s")

        grand_totals["expansions"] += len(expansion_ids)
        for k in totals:
            grand_totals[k] += totals[k]

    grand_elapsed = int(time.time() - grand_start)
    print(f"\n{'='*60}")
    print(f"  ALL DONE in {grand_elapsed}s")
    print(f"  Expansions: {grand_totals['expansions']}")
    print(f"  Cards:      {grand_totals['cards']}")
    print(f"  Sealed:     {grand_totals['sealed']}")
    print(f"  Prices:     {grand_totals['prices']}")
    print(f"  Credits:    {grand_totals['credits']}")
    print(f"{'='*60}")

    try:
        client = ScrydexClient(api_key, team_id)
        usage = client.get_usage()
        remaining = usage.get("data", {}).get("credits_remaining", "?")
        print(f"  Credits remaining: {remaining}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
