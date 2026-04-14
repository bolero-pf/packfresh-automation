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
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def sync_expansion(client, expansion_id: str, db) -> dict:
    """
    Pull all cards + sealed for one expansion and upsert into scrydex_price_cache.
    Also updates scrydex_tcg_map with any new TCGPlayer ID mappings.
    Returns stats dict.
    """
    stats = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0, "mapped": 0}
    expansion_name = None

    # ── Cards ──────────────────────────────────────────────
    page = 1
    while True:
        resp = client._get(
            f"{client.base_url}/pokemon/v1/expansions/{expansion_id}/cards",
            {"page": page, "page_size": 100, "include": "prices"}
        )
        stats["credits"] += 1
        items = resp.get("data", [])
        if not items:
            break

        for card in items:
            stats["cards"] += 1
            scrydex_id = card.get("id")
            if not scrydex_id:
                continue

            expansion = card.get("expansion") or {}
            if not expansion_name:
                expansion_name = expansion.get("name", "")

            card_name = card.get("name", "")
            card_number = card.get("number") or card.get("printed_number")
            rarity = card.get("rarity")

            # Images
            images = card.get("images") or []
            img_s = img_m = img_l = ""
            for img in images:
                if img.get("type") == "front":
                    img_s = img.get("small", "")
                    img_m = img.get("medium", "")
                    img_l = img.get("large", "")
                    break

            # Extract TCGPlayer ID and update mapping
            tcg_id = None
            for v in (card.get("variants") or []):
                for mp in (v.get("marketplaces") or []):
                    if mp.get("name") == "tcgplayer" and mp.get("product_id"):
                        try:
                            tcg_id = int(mp["product_id"])
                        except (ValueError, TypeError):
                            pass
                        break
                if tcg_id:
                    break

            if tcg_id:
                try:
                    db.execute("""
                        INSERT INTO scrydex_tcg_map (scrydex_id, tcgplayer_id, product_type, updated_at)
                        VALUES (%s, %s, 'card', NOW())
                        ON CONFLICT (scrydex_id) DO UPDATE SET
                            tcgplayer_id = EXCLUDED.tcgplayer_id, updated_at = NOW()
                    """, (scrydex_id, tcg_id))
                    stats["mapped"] += 1
                except Exception:
                    pass

            # Upsert prices for each variant × condition
            for v in (card.get("variants") or []):
                variant_name = v.get("name", "normal")
                for p in (v.get("prices") or []):
                    condition = p.get("condition", "NM")
                    price_type = p.get("type", "raw")
                    market = p.get("market")
                    low = p.get("low")
                    mid = p.get("mid")
                    high = p.get("high")

                    trends = p.get("trends") or {}
                    t1 = (trends.get("days_1") or {}).get("percent_change")
                    t7 = (trends.get("days_7") or {}).get("percent_change")
                    t30 = (trends.get("days_30") or {}).get("percent_change")

                    grade_co = p.get("company") if price_type == "graded" else None
                    grade_val = str(p.get("grade", "")) if price_type == "graded" else None

                    _upsert_price(db, scrydex_id=scrydex_id, tcgplayer_id=tcg_id,
                                  expansion_id=expansion_id, expansion_name=expansion_name,
                                  product_type="card", product_name=card_name,
                                  card_number=card_number, rarity=rarity,
                                  variant=variant_name, condition=condition,
                                  price_type=price_type, grade_company=grade_co,
                                  grade_value=grade_val, market_price=market,
                                  low_price=low, mid_price=mid, high_price=high,
                                  trend_1d=t1, trend_7d=t7, trend_30d=t30,
                                  img_s=img_s, img_m=img_m, img_l=img_l)
                    stats["prices"] += 1

        if len(items) < 100:
            break
        page += 1

    # ── Sealed ─────────────────────────────────────────────
    resp = client._get(
        f"{client.base_url}/pokemon/v1/expansions/{expansion_id}/sealed",
        {"page_size": 100, "include": "prices"}
    )
    stats["credits"] += 1
    for item in (resp.get("data") or []):
        stats["sealed"] += 1
        scrydex_id = item.get("id")
        if not scrydex_id:
            continue

        expansion = item.get("expansion") or {}
        if not expansion_name:
            expansion_name = expansion.get("name", "")

        product_name = item.get("name", "")
        images = item.get("images") or []
        img_s = img_m = img_l = ""
        for img in images:
            if img.get("type") == "front":
                img_s = img.get("small", "")
                img_m = img.get("medium", "")
                img_l = img.get("large", "")
                break

        for v in (item.get("variants") or []):
            variant_name = v.get("name", "normal")
            for p in (v.get("prices") or []):
                condition = p.get("condition", "U")
                market = p.get("market")
                low = p.get("low")
                trends = p.get("trends") or {}
                t1 = (trends.get("days_1") or {}).get("percent_change")
                t7 = (trends.get("days_7") or {}).get("percent_change")
                t30 = (trends.get("days_30") or {}).get("percent_change")

                _upsert_price(db, scrydex_id=scrydex_id, tcgplayer_id=None,
                              expansion_id=expansion_id, expansion_name=expansion_name,
                              product_type="sealed", product_name=product_name,
                              card_number=None, rarity=None,
                              variant=variant_name, condition=condition,
                              price_type="raw", grade_company=None,
                              grade_value=None, market_price=market,
                              low_price=low, mid_price=None, high_price=None,
                              trend_1d=t1, trend_7d=t7, trend_30d=t30,
                              img_s=img_s, img_m=img_m, img_l=img_l)
                stats["prices"] += 1

    # Update sync log
    db.execute("""
        INSERT INTO scrydex_sync_log (expansion_id, expansion_name, card_count, last_synced, credits_used)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (expansion_id) DO UPDATE SET
            expansion_name = EXCLUDED.expansion_name,
            card_count = EXCLUDED.card_count,
            last_synced = NOW(),
            credits_used = EXCLUDED.credits_used
    """, (expansion_id, expansion_name, stats["cards"], stats["credits"]))

    return stats


def _upsert_price(db, *, scrydex_id, tcgplayer_id, expansion_id, expansion_name,
                  product_type, product_name, card_number, rarity,
                  variant, condition, price_type, grade_company, grade_value,
                  market_price, low_price, mid_price, high_price,
                  trend_1d, trend_7d, trend_30d, img_s, img_m, img_l):
    """Upsert a single price row into scrydex_price_cache."""
    db.execute("""
        INSERT INTO scrydex_price_cache (
            scrydex_id, tcgplayer_id, expansion_id, expansion_name,
            product_type, product_name, card_number, rarity,
            variant, condition, price_type, grade_company, grade_value,
            market_price, low_price, mid_price, high_price,
            trend_1d_pct, trend_7d_pct, trend_30d_pct,
            image_small, image_medium, image_large, fetched_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (scrydex_id, variant, condition, price_type,
                     grade_company_key, grade_value_key)
        DO UPDATE SET
            tcgplayer_id   = COALESCE(EXCLUDED.tcgplayer_id, scrydex_price_cache.tcgplayer_id),
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
    """, (
        scrydex_id, tcgplayer_id, expansion_id, expansion_name,
        product_type, product_name, card_number, rarity,
        variant, condition, price_type, grade_company, grade_value,
        market_price, low_price, mid_price, high_price,
        trend_1d, trend_7d, trend_30d,
        img_s, img_m, img_l,
    ))


def main():
    parser = argparse.ArgumentParser(description="Scrydex nightly price cache sync")
    parser.add_argument("--sets", help="Comma-separated expansion IDs")
    parser.add_argument("--all", action="store_true", help="Sync ALL English expansions")
    parser.add_argument("--dry-run", action="store_true", help="Show plan only")
    args = parser.parse_args()

    from scrydex_client import ScrydexClient
    import db

    api_key = os.getenv("SCRYDEX_API_KEY")
    team_id = os.getenv("SCRYDEX_TEAM_ID")
    if not api_key or not team_id:
        print("Set SCRYDEX_API_KEY and SCRYDEX_TEAM_ID")
        sys.exit(1)

    db.init_pool()
    client = ScrydexClient(api_key, team_id, db=db)

    # Determine which expansions to sync
    if args.sets:
        expansion_ids = [s.strip() for s in args.sets.split(",")]
    elif args.all:
        expansions = client.get_expansions(language_code="EN")
        expansion_ids = [e["id"] for e in expansions]
    else:
        # Sync previously-pulled expansions (from scrydex_sync_log)
        rows = db.query("SELECT expansion_id FROM scrydex_sync_log WHERE active = TRUE")
        expansion_ids = [r["expansion_id"] for r in rows]
        if not expansion_ids:
            logger.info("No expansions in sync_log — use --all for first run")
            return

    logger.info(f"Will sync {len(expansion_ids)} expansions")

    if args.dry_run:
        for eid in expansion_ids[:20]:
            print(f"  {eid}")
        if len(expansion_ids) > 20:
            print(f"  ... and {len(expansion_ids) - 20} more")
        return

    totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0, "mapped": 0}
    t_start = time.time()

    for i, eid in enumerate(expansion_ids):
        logger.info(f"[{i+1}/{len(expansion_ids)}] {eid}")
        try:
            stats = sync_expansion(client, eid, db)
            for k in totals:
                totals[k] += stats[k]
            logger.info(f"  {stats['cards']} cards, {stats['sealed']} sealed, "
                        f"{stats['prices']} prices, {stats['credits']} credits")
        except Exception as e:
            logger.error(f"  FAILED: {e}")
        time.sleep(0.05)

    elapsed = int(time.time() - t_start)
    print(f"\nDone in {elapsed}s!")
    print(f"  Expansions:  {len(expansion_ids)}")
    print(f"  Cards:       {totals['cards']}")
    print(f"  Sealed:      {totals['sealed']}")
    print(f"  Prices:      {totals['prices']}")
    print(f"  Mappings:    {totals['mapped']}")
    print(f"  Credits:     {totals['credits']}")

    # Check usage
    try:
        usage = client.get_usage()
        remaining = usage.get("data", {}).get("credits_remaining", "?")
        print(f"  Credits left: {remaining}")
    except Exception:
        pass


if __name__ == "__main__":
    main()
