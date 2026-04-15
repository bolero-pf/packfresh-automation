"""
Scrydex nightly sync — pulls all active expansions and upserts prices into
scrydex_price_cache. After this runs, every price lookup is a local DB read.

Usage:
    python scrydex_nightly.py [--sets sv8,sv3pt5] [--all] [--dry-run]

Without args: syncs all expansions that have been pulled before (scrydex_sync_log).
With --all: syncs every English expansion.
With --sets: syncs only the specified expansion IDs.

Designed to run as a Railway cron or via APScheduler alongside the Selenium
price updater. ~500-800 credits per full run (197 sets × 2-4 pages each).
"""

import os
import sys
import time
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

UPSERT_SQL = """
    INSERT INTO scrydex_price_cache (
        game, scrydex_id, tcgplayer_id, expansion_id, expansion_name,
        product_type, product_name, card_number, rarity,
        variant, condition, price_type, grade_company, grade_value,
        market_price, low_price, mid_price, high_price,
        trend_1d_pct, trend_7d_pct, trend_30d_pct,
        image_small, image_medium, image_large, fetched_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
    )
    ON CONFLICT (game, scrydex_id, variant, condition, price_type,
                 grade_company_key, grade_value_key)
    DO UPDATE SET
        tcgplayer_id   = COALESCE(scrydex_price_cache.tcgplayer_id, EXCLUDED.tcgplayer_id),
        expansion_name = EXCLUDED.expansion_name,
        product_name   = EXCLUDED.product_name,
        market_price   = EXCLUDED.market_price,
        low_price      = EXCLUDED.low_price,
        mid_price      = EXCLUDED.mid_price,
        high_price     = EXCLUDED.high_price,
        trend_1d_pct   = EXCLUDED.trend_1d_pct,
        trend_7d_pct   = EXCLUDED.trend_7d_pct,
        trend_30d_pct  = EXCLUDED.trend_30d_pct,
        image_small    = EXCLUDED.image_small,
        image_medium   = EXCLUDED.image_medium,
        image_large    = EXCLUDED.image_large,
        fetched_at     = NOW()
"""

MAP_SQL = """
    INSERT INTO scrydex_tcg_map (scrydex_id, tcgplayer_id, product_type, game, updated_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (scrydex_id) DO UPDATE SET
        tcgplayer_id = EXCLUDED.tcgplayer_id, updated_at = NOW()
"""


def _extract_images(item: dict) -> tuple[str, str, str]:
    for img in (item.get("images") or []):
        if img.get("type") == "front":
            return img.get("small", ""), img.get("medium", ""), img.get("large", "")
    return "", "", ""


def _extract_tcg_id(card: dict) -> int | None:
    for v in (card.get("variants") or []):
        for mp in (v.get("marketplaces") or []):
            if mp.get("name") == "tcgplayer" and mp.get("product_id"):
                try:
                    return int(mp["product_id"])
                except (ValueError, TypeError):
                    pass
    return None


def _collect_price_rows(item: dict, *, game: str, expansion_id: str, expansion_name: str,
                        product_type: str, tcg_id: int | None) -> list[tuple]:
    """Extract all price rows from a card or sealed item. Returns list of param tuples."""
    scrydex_id = item.get("id")
    if not scrydex_id:
        return []

    name = item.get("name", "")
    card_number = item.get("number") or item.get("printed_number")
    rarity = item.get("rarity")
    img_s, img_m, img_l = _extract_images(item)

    rows = []
    for v in (item.get("variants") or []):
        variant_name = v.get("name", "normal")
        for p in (v.get("prices") or []):
            condition = p.get("condition", "NM")
            price_type = p.get("type", "raw")
            trends = p.get("trends") or {}
            t1 = (trends.get("days_1") or {}).get("percent_change")
            t7 = (trends.get("days_7") or {}).get("percent_change")
            t30 = (trends.get("days_30") or {}).get("percent_change")
            grade_co = p.get("company") if price_type == "graded" else None
            grade_val = str(p.get("grade", "")) if price_type == "graded" else None

            rows.append((
                game, scrydex_id, tcg_id, expansion_id, expansion_name,
                product_type, name, card_number, rarity,
                variant_name, condition, price_type, grade_co, grade_val,
                p.get("market"), p.get("low"), p.get("mid"), p.get("high"),
                t1, t7, t30, img_s, img_m, img_l,
            ))
    return rows


def sync_expansion(client, expansion_id: str, db) -> dict:
    """
    Pull all cards + sealed for one expansion and batch-upsert into scrydex_price_cache.
    Uses client.game to determine the API path and game column value.
    Returns stats dict.
    """
    from psycopg2.extras import execute_batch

    game = client.game
    stats = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0, "mapped": 0}
    expansion_name = None
    price_batch = []
    map_batch = []

    # ── Cards ──────────────────────────────────────────────
    page = 1
    while True:
        resp = client._get(
            f"{client.base_url}/{game}/v1/expansions/{expansion_id}/cards",
            {"page": page, "page_size": 100, "include": "prices"}
        )
        stats["credits"] += 1
        items = resp.get("data", [])
        if not items:
            break

        for card in items:
            stats["cards"] += 1
            if not expansion_name:
                expansion_name = (card.get("expansion") or {}).get("name", "")

            tcg_id = _extract_tcg_id(card)
            if tcg_id and card.get("id"):
                map_batch.append((card["id"], tcg_id, "card", game))
                stats["mapped"] += 1

            rows = _collect_price_rows(card, game=game, expansion_id=expansion_id,
                                       expansion_name=expansion_name or "",
                                       product_type="card", tcg_id=tcg_id)
            price_batch.extend(rows)
            stats["prices"] += len(rows)

        if len(items) < 100:
            break
        page += 1

    # ── Sealed ─────────────────────────────────────────────
    try:
        resp = client._get(
            f"{client.base_url}/{game}/v1/expansions/{expansion_id}/sealed",
            {"page_size": 100, "include": "prices"}
        )
        stats["credits"] += 1
        for item in (resp.get("data") or []):
            stats["sealed"] += 1
            if not expansion_name:
                expansion_name = (item.get("expansion") or {}).get("name", "")

            rows = _collect_price_rows(item, game=game, expansion_id=expansion_id,
                                       expansion_name=expansion_name or "",
                                       product_type="sealed", tcg_id=None)
            price_batch.extend(rows)
            stats["prices"] += len(rows)
    except Exception as e:
        # Some games don't have sealed endpoints — skip gracefully
        logger.debug(f"Sealed endpoint not available for {game}/{expansion_id}: {e}")

    # ── Batch write ────────────────────────────────────────
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            if map_batch:
                execute_batch(cur, MAP_SQL, map_batch, page_size=500)
            if price_batch:
                execute_batch(cur, UPSERT_SQL, price_batch, page_size=500)
            # Sync log
            cur.execute("""
                INSERT INTO scrydex_sync_log (game, expansion_id, expansion_name, card_count, last_synced, credits_used)
                VALUES (%s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (game, expansion_id) DO UPDATE SET
                    expansion_name = EXCLUDED.expansion_name,
                    card_count = EXCLUDED.card_count,
                    last_synced = NOW(),
                    credits_used = EXCLUDED.credits_used
            """, (game, expansion_id, expansion_name, stats["cards"], stats["credits"]))
        conn.commit()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrydex nightly price cache sync")
    parser.add_argument("--sets", help="Comma-separated expansion IDs")
    parser.add_argument("--all", action="store_true", help="Sync ALL expansions for the game")
    parser.add_argument("--game", default="pokemon",
                        help="Game to sync: pokemon, magicthegathering, lorcana, onepiece, riftbound")
    parser.add_argument("--dry-run", action="store_true", help="Show plan only")
    args = parser.parse_args()

    from scrydex_client import ScrydexClient
    import db

    api_key = os.getenv("SCRYDEX_API_KEY")
    team_id = os.getenv("SCRYDEX_TEAM_ID")
    if not api_key or not team_id:
        print("Set SCRYDEX_API_KEY and SCRYDEX_TEAM_ID")
        sys.exit(1)

    game = args.game.lower().strip()
    db.init_pool()
    client = ScrydexClient(api_key, team_id, db=db, game=game)

    # Determine which expansions to sync
    if args.sets:
        expansion_ids = [s.strip() for s in args.sets.split(",")]
    elif args.all:
        expansions = client.get_expansions()
        expansion_ids = [e["id"] for e in expansions]
    else:
        # Sync previously-pulled expansions (from scrydex_sync_log)
        rows = db.query("SELECT expansion_id FROM scrydex_sync_log WHERE game = %s AND active = TRUE", (game,))
        expansion_ids = [r["expansion_id"] for r in rows]
        if not expansion_ids:
            logger.info(f"No {game} expansions in sync_log — use --all for first run")
            return

    logger.info(f"[{game}] Will sync {len(expansion_ids)} expansions")

    if args.dry_run:
        for eid in expansion_ids[:20]:
            print(f"  {eid}")
        if len(expansion_ids) > 20:
            print(f"  ... and {len(expansion_ids) - 20} more")
        return

    totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0, "mapped": 0}
    t_start = time.time()

    for i, eid in enumerate(expansion_ids):
        logger.info(f"[{i+1}/{len(expansion_ids)}] {game}/{eid}")
        try:
            stats = sync_expansion(client, eid, db)
            for k in totals:
                totals[k] += stats.get(k, 0)
            logger.info(f"  {stats['cards']} cards, {stats['sealed']} sealed, "
                        f"{stats['prices']} prices, {stats['credits']} credits")
        except Exception as e:
            logger.error(f"  FAILED: {e}")
        time.sleep(0.05)

    elapsed = int(time.time() - t_start)
    print(f"\nDone [{game}] in {elapsed}s!")
    print(f"  Expansions:  {len(expansion_ids)}")
    print(f"  Cards:       {totals['cards']}")
    print(f"  Sealed:      {totals['sealed']}")
    print(f"  Prices:      {totals['prices']}")
    print(f"  Mappings:    {totals['mapped']}")
    print(f"  Credits:     {totals['credits']}")

    try:
        usage = client.get_usage()
        remaining = usage.get("data", {}).get("credits_remaining", "?")
        print(f"  Credits left: {remaining}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
