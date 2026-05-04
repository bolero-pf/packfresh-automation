"""Subprocess entrypoint for the Scrydex nightly cache sync. Mirrors the
inline run_scrydex_sync() in review_dashboard.py — kept thin so the dashboard
can launch this as its own process and tee stdout into RUN_LOG.

CLI:
    python run_scrydex_sync.py
"""
import os
import sys
import time
import logging
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scrydex_sync")


def main() -> int:
    scrydex_key = os.environ.get("SCRYDEX_API_KEY", "")
    scrydex_team = os.environ.get("SCRYDEX_TEAM_ID", "")
    if not scrydex_key or not scrydex_team:
        print("⏭ Scrydex sync skipped — SCRYDEX_API_KEY/SCRYDEX_TEAM_ID not set")
        return 0

    sys.path.insert(0, str(BASE_DIR.parent / "shared"))
    from scrydex_client import ScrydexClient
    from scrydex_nightly import sync_expansion
    import db as shared_db

    shared_db.init_pool()

    games = [g.strip() for g in os.environ.get("SCRYDEX_GAMES", "pokemon").split(",") if g.strip()]
    grand_start = time.time()
    grand_totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0}

    for game in games:
        client = ScrydexClient(scrydex_key, scrydex_team, db=shared_db, game=game)
        rows = shared_db.query(
            "SELECT expansion_id FROM scrydex_sync_log WHERE game = %s AND active = TRUE", (game,))
        expansion_ids = [r["expansion_id"] for r in rows]
        if not expansion_ids:
            print(f"⏭ {game}: no active expansions in sync_log")
            continue

        print(f"🔄 {game}: {len(expansion_ids)} active expansions")
        totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0}
        failures = []
        t_start = time.time()

        for i, eid in enumerate(expansion_ids):
            try:
                stats = sync_expansion(client, eid, shared_db)
                for k in totals:
                    totals[k] += stats.get(k, 0)
                if (i + 1) % 20 == 0:
                    print(f"  ... {i+1}/{len(expansion_ids)} done ({totals['credits']} credits)")
            except Exception as e:
                print(f"  ❌ {game}/{eid}: {e}")
                failures.append((eid, str(e)))
            time.sleep(0.05)

        if failures:
            print(f"  🔁 Retrying {len(failures)} failed expansions...")
            for eid, _orig in failures:
                try:
                    stats = sync_expansion(client, eid, shared_db)
                    for k in totals:
                        totals[k] += stats.get(k, 0)
                    print(f"    ✅ Retry OK: {game}/{eid}")
                except Exception as e:
                    print(f"    ❌ Still failed: {game}/{eid}: {e}")
                time.sleep(0.1)

        elapsed = int(time.time() - t_start)
        print(f"✅ {game} done in {elapsed}s — {totals['cards']} cards, "
              f"{totals['sealed']} sealed, {totals['credits']} credits")
        for k in grand_totals:
            grand_totals[k] += totals[k]

    grand_elapsed = int(time.time() - grand_start)
    print(f"✅ All games done in {grand_elapsed}s — {grand_totals['credits']} total credits")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.exit(rc)
