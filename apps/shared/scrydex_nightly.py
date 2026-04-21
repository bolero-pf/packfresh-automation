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
        product_type, product_name, card_number, printed_number, rarity,
        variant, condition, price_type, grade_company, grade_value,
        market_price, low_price, mid_price, high_price,
        trend_1d_pct, trend_7d_pct, trend_30d_pct,
        image_small, image_medium, image_large,
        product_name_en, expansion_name_en, language_code,
        currency,
        fetched_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s,
        NOW()
    )
    ON CONFLICT (game, scrydex_id, variant, condition, price_type,
                 grade_company_key, grade_value_key)
    DO UPDATE SET
        tcgplayer_id      = EXCLUDED.tcgplayer_id,
        expansion_name    = EXCLUDED.expansion_name,
        product_name      = EXCLUDED.product_name,
        printed_number    = EXCLUDED.printed_number,
        product_name_en   = EXCLUDED.product_name_en,
        expansion_name_en = EXCLUDED.expansion_name_en,
        language_code     = EXCLUDED.language_code,
        market_price      = EXCLUDED.market_price,
        low_price         = EXCLUDED.low_price,
        mid_price         = EXCLUDED.mid_price,
        high_price        = EXCLUDED.high_price,
        trend_1d_pct      = EXCLUDED.trend_1d_pct,
        trend_7d_pct      = EXCLUDED.trend_7d_pct,
        trend_30d_pct     = EXCLUDED.trend_30d_pct,
        image_small       = EXCLUDED.image_small,
        image_medium      = EXCLUDED.image_medium,
        image_large       = EXCLUDED.image_large,
        currency          = EXCLUDED.currency,
        fetched_at        = NOW()
"""

MAP_SQL = """
    INSERT INTO scrydex_tcg_map (scrydex_id, tcgplayer_id, product_type, game, updated_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (scrydex_id, tcgplayer_id) DO UPDATE SET
        product_type = EXCLUDED.product_type,
        game = EXCLUDED.game,
        updated_at = NOW()
"""

CARD_META_SQL = """
    INSERT INTO scrydex_card_meta (
        game, scrydex_id,
        printed_number, rarity_code, artist, flavor_text, rules, subtypes,
        hp, supertype, types, national_pokedex_numbers, evolves_from,
        attacks, abilities, weaknesses, resistances,
        retreat_cost, converted_retreat_cost, legalities,
        card_type, attribute, colors, life, power, printings, tags,
        raw, fetched_at
    ) VALUES (
        %s, %s,
        %s, %s, %s, %s, %s::jsonb, %s::jsonb,
        %s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
        %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
        %s::jsonb, %s, %s::jsonb,
        %s, %s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb,
        %s::jsonb, NOW()
    )
    ON CONFLICT (game, scrydex_id) DO UPDATE SET
        printed_number             = EXCLUDED.printed_number,
        rarity_code                = EXCLUDED.rarity_code,
        artist                     = EXCLUDED.artist,
        flavor_text                = EXCLUDED.flavor_text,
        rules                      = EXCLUDED.rules,
        subtypes                   = EXCLUDED.subtypes,
        hp                         = EXCLUDED.hp,
        supertype                  = EXCLUDED.supertype,
        types                      = EXCLUDED.types,
        national_pokedex_numbers   = EXCLUDED.national_pokedex_numbers,
        evolves_from               = EXCLUDED.evolves_from,
        attacks                    = EXCLUDED.attacks,
        abilities                  = EXCLUDED.abilities,
        weaknesses                 = EXCLUDED.weaknesses,
        resistances                = EXCLUDED.resistances,
        retreat_cost               = EXCLUDED.retreat_cost,
        converted_retreat_cost     = EXCLUDED.converted_retreat_cost,
        legalities                 = EXCLUDED.legalities,
        card_type                  = EXCLUDED.card_type,
        attribute                  = EXCLUDED.attribute,
        colors                     = EXCLUDED.colors,
        life                       = EXCLUDED.life,
        power                      = EXCLUDED.power,
        printings                  = EXCLUDED.printings,
        tags                       = EXCLUDED.tags,
        raw                        = EXCLUDED.raw,
        fetched_at                 = NOW()
"""

EXPANSION_META_SQL = """
    INSERT INTO scrydex_expansion_meta (
        game, expansion_id, code, name, type, total, printed_total,
        release_date, series, language, language_code,
        logo, symbol, sort_order, is_online_only, raw, fetched_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
    )
    ON CONFLICT (game, expansion_id) DO UPDATE SET
        code            = EXCLUDED.code,
        name            = EXCLUDED.name,
        type            = EXCLUDED.type,
        total           = EXCLUDED.total,
        printed_total   = EXCLUDED.printed_total,
        release_date    = EXCLUDED.release_date,
        series          = EXCLUDED.series,
        language        = EXCLUDED.language,
        language_code   = EXCLUDED.language_code,
        logo            = EXCLUDED.logo,
        symbol          = EXCLUDED.symbol,
        sort_order      = EXCLUDED.sort_order,
        is_online_only  = EXCLUDED.is_online_only,
        raw             = EXCLUDED.raw,
        fetched_at      = NOW()
"""


def _extract_images(item: dict) -> tuple[str, str, str]:
    """Extract front-type image URLs from a Scrydex item or variant."""
    for img in (item.get("images") or []):
        if img.get("type") == "front":
            return img.get("small", ""), img.get("medium", ""), img.get("large", "")
    return "", "", ""


def _extract_variant_tcg_id(variant: dict) -> int | None:
    """Per-variant TCGPlayer product_id. Each variant of a Scrydex card has its
    own marketplaces entry (OP14-041 normal=668333, OP14-041 altArt=668335)."""
    for mp in (variant.get("marketplaces") or []):
        if mp.get("name") == "tcgplayer" and mp.get("product_id"):
            try:
                return int(mp["product_id"])
            except (ValueError, TypeError):
                pass
    return None


def _extract_tcg_id(card: dict) -> int | None:
    """Card-level TCGPlayer ID — first variant that has one. Kept for callers
    that don't need per-variant resolution (sealed products, sync log)."""
    for v in (card.get("variants") or []):
        tcg = _extract_variant_tcg_id(v)
        if tcg:
            return tcg
    return None


def _to_int(v):
    """Coerce numeric-string fields (Scrydex sometimes returns '5000' for power)."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None


def _to_jsonb(v):
    """Serialize a list/dict to a JSON string, or None if empty/missing."""
    import json as _json
    if v is None:
        return None
    if isinstance(v, (list, dict)) and not v:
        return None
    return _json.dumps(v, ensure_ascii=False)


def _collect_card_meta_row(card: dict, *, game: str) -> tuple | None:
    """Build a parameter tuple for CARD_META_SQL from a raw Scrydex card.
    Promotes high-value fields to columns and stores the full raw dict in `raw`."""
    scrydex_id = card.get("id")
    if not scrydex_id:
        return None
    return (
        game,
        scrydex_id,
        card.get("printed_number"),
        card.get("rarity_code"),
        card.get("artist"),
        card.get("flavor_text"),
        _to_jsonb(card.get("rules")),
        _to_jsonb(card.get("subtypes")),
        # Pokemon
        _to_int(card.get("hp")),
        card.get("supertype"),
        _to_jsonb(card.get("types")),
        _to_jsonb(card.get("national_pokedex_numbers")),
        _to_jsonb(card.get("evolves_from")),
        _to_jsonb(card.get("attacks")),
        _to_jsonb(card.get("abilities")),
        _to_jsonb(card.get("weaknesses")),
        _to_jsonb(card.get("resistances")),
        _to_jsonb(card.get("retreat_cost")),
        _to_int(card.get("converted_retreat_cost")),
        _to_jsonb(card.get("legalities")),
        # One Piece (note: top-level `type` collides with table column name `type`,
        # so it's stored as `card_type`)
        card.get("type") if game in ("onepiece",) else None,
        card.get("attribute"),
        _to_jsonb(card.get("colors")),
        _to_int(card.get("life")),
        _to_int(card.get("power")),
        _to_jsonb(card.get("printings")),
        _to_jsonb(card.get("tags")),
        _to_jsonb(card),
    )


def _collect_expansion_meta_row(exp: dict, *, game: str) -> tuple | None:
    if not exp or not exp.get("id"):
        return None
    return (
        game,
        exp.get("id"),
        exp.get("code"),
        exp.get("name"),
        exp.get("type"),
        _to_int(exp.get("total")),
        _to_int(exp.get("printed_total")),
        exp.get("release_date"),
        exp.get("series"),
        exp.get("language"),
        exp.get("language_code"),
        exp.get("logo"),
        exp.get("symbol"),
        _to_int(exp.get("sort_order") or exp.get("expansion_sort_order")),
        bool(exp.get("is_online_only")) if exp.get("is_online_only") is not None else None,
        _to_jsonb(exp),
    )


def _collect_price_rows(item: dict, *, game: str, expansion_id: str, expansion_name: str,
                        product_type: str, tcg_id: int | None) -> list[tuple]:
    """Extract all price rows from a card or sealed item. Returns list of param tuples.

    If a variant has no price data, still emit a placeholder row so the product
    exists in cache (searchable, linkable) — prices will fill in later syncs
    when Scrydex starts tracking them.
    """
    scrydex_id = item.get("id")
    if not scrydex_id:
        return []

    name = item.get("name", "")
    card_number = item.get("number") or item.get("printed_number")
    printed_number = item.get("printed_number")  # on-card "OP14-041" / "4/102"
    rarity = item.get("rarity")
    card_img_s, card_img_m, card_img_l = _extract_images(item)

    # Language + English translation (JP sets ship translation.en.name in
    # Scrydex responses). For English sets, there's no translation block —
    # mirror the native name into *_en so English searches always have a
    # column to hit without special-casing language.
    language_code = item.get("language_code")  # 'JA', 'EN', etc.
    translation_en = (item.get("translation") or {}).get("en") or {}
    product_name_en = translation_en.get("name") or (name if language_code == "EN" else None)

    expansion_obj = item.get("expansion") or {}
    expansion_translation_en = (expansion_obj.get("translation") or {}).get("en") or {}
    expansion_name_en = (expansion_translation_en.get("name")
                         or (expansion_name if language_code == "EN" else None))

    rows = []
    variants = item.get("variants") or [{"name": "normal", "prices": []}]
    for v in variants:
        variant_name = v.get("name", "normal")
        prices = v.get("prices") or []

        # Per-variant tcg_id (OP14-041 altArt has 668335, normal has 668333) —
        # falls back to card-level if variant has no marketplace entry.
        v_tcg_id = _extract_variant_tcg_id(v) or tcg_id

        # Per-variant image (OP altArt → /OP14-041A/large; Pokemon variants
        # rarely have their own image since 1st Ed and Unlimited look the same)
        v_img_s, v_img_m, v_img_l = _extract_images(v)
        if not v_img_s:
            v_img_s, v_img_m, v_img_l = card_img_s, card_img_m, card_img_l

        if not prices:
            # No price data yet — emit placeholder row with null prices.
            # Default condition: U (unopened) for sealed, NM for cards.
            default_cond = "U" if product_type == "sealed" else "NM"
            rows.append((
                game, scrydex_id, v_tcg_id, expansion_id, expansion_name,
                product_type, name, card_number, printed_number, rarity,
                variant_name, default_cond, "raw", None, None,
                None, None, None, None, None, None, None,
                v_img_s, v_img_m, v_img_l,
                product_name_en, expansion_name_en, language_code,
                None,  # currency
            ))
            continue

        for p in prices:
            condition = p.get("condition", "NM")
            price_type = p.get("type", "raw")
            trends = p.get("trends") or {}
            t1 = (trends.get("days_1") or {}).get("percent_change")
            t7 = (trends.get("days_7") or {}).get("percent_change")
            t30 = (trends.get("days_30") or {}).get("percent_change")
            grade_co = p.get("company") if price_type == "graded" else None
            grade_val = str(p.get("grade", "")) if price_type == "graded" else None
            # Scrydex sends a per-price `currency` — JP-marketplace rows for
            # Japanese cards come through as JPY while eBay-sourced graded
            # rows are USD. Capture it so downstream queries can convert.
            currency = (p.get("currency") or "USD").upper()

            rows.append((
                game, scrydex_id, v_tcg_id, expansion_id, expansion_name,
                product_type, name, card_number, printed_number, rarity,
                variant_name, condition, price_type, grade_co, grade_val,
                p.get("market"), p.get("low"), p.get("mid"), p.get("high"),
                t1, t7, t30, v_img_s, v_img_m, v_img_l,
                product_name_en, expansion_name_en, language_code,
                currency,
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
    stats = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0, "mapped": 0,
             "card_meta": 0, "expansion_meta": 0}
    expansion_name = None
    expansion_obj_for_meta = None  # remember the first card's expansion dict for meta upsert
    price_batch = []
    map_batch = []
    card_meta_batch = []

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
            if expansion_obj_for_meta is None:
                expansion_obj_for_meta = card.get("expansion")

            tcg_id = _extract_tcg_id(card)
            # Map every variant's tcgplayer_id (PK is now (scrydex_id, tcgplayer_id))
            scrydex_id_card = card.get("id")
            if scrydex_id_card:
                seen = set()
                for v in (card.get("variants") or []):
                    v_tcg = _extract_variant_tcg_id(v)
                    if v_tcg and v_tcg not in seen:
                        seen.add(v_tcg)
                        map_batch.append((scrydex_id_card, v_tcg, "card", game))
                        stats["mapped"] += 1

            meta_row = _collect_card_meta_row(card, game=game)
            if meta_row:
                card_meta_batch.append(meta_row)
                stats["card_meta"] += 1

            rows = _collect_price_rows(card, game=game, expansion_id=expansion_id,
                                       expansion_name=expansion_name or "",
                                       product_type="card", tcg_id=tcg_id)
            price_batch.extend(rows)
            stats["prices"] += len(rows)

        if len(items) < 100:
            break
        page += 1

    # ── Sealed (paginated) ───────────────────────────────────
    try:
        sealed_page = 1
        while True:
            resp = client._get(
                f"{client.base_url}/{game}/v1/expansions/{expansion_id}/sealed",
                {"page": sealed_page, "page_size": 100, "include": "prices"}
            )
            stats["credits"] += 1
            sealed_items = resp.get("data") or []
            if not sealed_items:
                break

            for item in sealed_items:
                stats["sealed"] += 1
                if not expansion_name:
                    expansion_name = (item.get("expansion") or {}).get("name", "")
                if expansion_obj_for_meta is None:
                    expansion_obj_for_meta = item.get("expansion")

                # Map every sealed variant's tcgplayer_id (Base Set Booster Pack
                # has 3: unlimited/firstEdition/shadowless). Mirrors the card loop.
                scrydex_id_sealed = item.get("id")
                if scrydex_id_sealed:
                    seen_sealed = set()
                    for v in (item.get("variants") or []):
                        v_tcg = _extract_variant_tcg_id(v)
                        if v_tcg and v_tcg not in seen_sealed:
                            seen_sealed.add(v_tcg)
                            map_batch.append((scrydex_id_sealed, v_tcg, "sealed", game))
                            stats["mapped"] += 1

                rows = _collect_price_rows(item, game=game, expansion_id=expansion_id,
                                           expansion_name=expansion_name or "",
                                           product_type="sealed", tcg_id=None)
                price_batch.extend(rows)
                stats["prices"] += len(rows)

            if len(sealed_items) < 100:
                break
            sealed_page += 1
    except Exception as e:
        # Some games don't have sealed endpoints — skip gracefully
        logger.debug(f"Sealed endpoint not available for {game}/{expansion_id}: {e}")

    # ── Batch write ────────────────────────────────────────
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            if map_batch:
                execute_batch(cur, MAP_SQL, map_batch, page_size=500)
            if card_meta_batch:
                execute_batch(cur, CARD_META_SQL, card_meta_batch, page_size=200)
            # Expansion meta — one row per sync_expansion run
            if expansion_obj_for_meta:
                exp_row = _collect_expansion_meta_row(expansion_obj_for_meta, game=game)
                if exp_row:
                    cur.execute(EXPANSION_META_SQL, exp_row)
                    stats["expansion_meta"] += 1
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
    parser.add_argument("--language", default=None,
                        help="Language code filter for expansions (e.g., EN, JA). Omit for all languages.")
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
        expansions = client.get_expansions(language_code=args.language)
        expansion_ids = [e["id"] for e in expansions]
        logger.info(f"Found {len(expansion_ids)} expansions" + (f" (language={args.language})" if args.language else " (all languages)"))
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
    failures = []  # [(expansion_id, error_message)]
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
            failures.append((eid, str(e)))
        time.sleep(0.05)

    # ── Retry failures ─────────────────────────────────────
    if failures:
        logger.info(f"\n--- Retrying {len(failures)} failed expansions ---")
        still_failed = []
        for eid, original_error in failures:
            logger.info(f"  Retry: {game}/{eid} (was: {original_error[:80]})")
            try:
                stats = sync_expansion(client, eid, db)
                for k in totals:
                    totals[k] += stats.get(k, 0)
                logger.info(f"    OK: {stats['cards']} cards, {stats['sealed']} sealed")
            except Exception as e:
                logger.error(f"    FAILED AGAIN: {e}")
                still_failed.append((eid, original_error, str(e)))
            time.sleep(0.1)

        if still_failed:
            logger.warning(f"\n{'='*60}")
            logger.warning(f"  {len(still_failed)} expansions failed after retry:")
            for eid, err1, err2 in still_failed:
                logger.warning(f"    {game}/{eid}")
                logger.warning(f"      1st: {err1[:100]}")
                logger.warning(f"      2nd: {err2[:100]}")
            logger.warning(f"{'='*60}")

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
