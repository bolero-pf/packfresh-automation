"""
kiosk — kiosk.pack-fresh.com
Customer-facing card browse + hold request system.

Two cohorts:
  1. Guests (in-store) — browse + hold requests, staff pulls cards, pay at register
  2. Champions (VIP3, remote) — browse + checkout via Shopify Storefront API cart

Read-only on raw_cards. Writes to holds + hold_items.
Cards are aggregated by (card_name, set_name) with qty per condition.
Max 20 cards per hold.
"""

import os
import hmac
import hashlib
import base64
import logging
import time
import threading
import requests as _requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Response, render_template

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()

MAX_HOLD_ITEMS = 20
HOLD_EXPIRY_HOURS = 2
CHAMPION_HOLD_MINUTES = 30

# Shopify Admin API (for product creation + customer lookup)
SHOPIFY_STORE   = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN   = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = os.environ.get("SHOPIFY_VERSION", "2025-01")

# Webhook verification
SHOPIFY_WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")


# Cleanup endpoint auth
CLEANUP_SECRET = os.environ.get("CLEANUP_SECRET", "")

# Storefront URL for cart-merge redirect
SHOPIFY_STOREFRONT_URL = os.environ.get("SHOPIFY_STOREFRONT_URL", "https://pack-fresh.com")

# Feature flag: disable Champion checkout (browse-only mode)
KIOSK_CHECKOUT_ENABLED = os.environ.get("KIOSK_CHECKOUT_ENABLED", "false").lower() == "true"

# Access key — required to use kiosk (set once on in-store tablets via URL param ?key=...)
KIOSK_ACCESS_KEY = os.environ.get("KIOSK_ACCESS_KEY", "")

# Era mapping — list of canonical set-name PREFIXES per era. A set_name is
# classified to an era when one of its prefixes matches case-insensitively.
# Prefix matching avoids the substring trap (e.g. 'Prismatic Evolutions'
# matching the literal keyword 'Evolutions' from the XY era), since "Mega
# Evolution Black Star Promos" still starts with "Mega Evolution" → Mega
# Evolution era. Order within each era doesn't matter; eras are tried in
# the order most-recent-first so newer sets always win when prefixes nest.
ERA_PREFIXES = {
    "Mega Evolution": [
        "Mega Evolution",   # base set + Mega Evolution Black Star Promos
        "Chaos Rising", "Perfect Order", "Ascended Heroes",
        "Phantasmal Flames",
    ],
    "Scarlet & Violet": [
        "Scarlet & Violet",  # base + Scarlet & Violet Black Star Promos
        "SV: ",              # SV: Prismatic Evolutions, SV: Scarlet & Violet 151
        "Paldea Evolved", "Obsidian Flames",
        "151",               # SV-era 151 set
        "Paradox Rift", "Paldean Fates", "Temporal Forces",
        "Twilight Masquerade", "Twilight Masque",
        "Shrouded Fable", "Stellar Crown", "Surging Sparks",
        "Prismatic Evolutions", "Journey Together", "Destined Rivals",
        "White Flare", "Black Bolt",
    ],
    "Sword & Shield": [
        "Sword & Shield",   # base + SWSH Black Star Promos
        "SWSH ",            # SWSH Black Star Promos, SWSH Promos
        "Rebel Clash", "Darkness Ablaze", "Vivid Voltage",
        "Battle Styles", "Chilling Reign", "Evolving Skies", "Fusion Strike",
        "Brilliant Stars", "Astral Radiance", "Lost Origin", "Silver Tempest",
        "Crown Zenith", "Shining Fates", "Champion's Path",
        "Celebrations",
        "Pokemon GO", "Pokemon Go", "Pokémon GO", "Pokémon Go",
    ],
    "Sun & Moon": [
        "Sun & Moon",       # base + Sun & Moon Promos
        "SM ",              # SM Black Star Promos
        "Guardians Rising", "Burning Shadows", "Crimson Invasion",
        "Ultra Prism", "Forbidden Light", "Celestial Storm", "Lost Thunder",
        "Team Up", "Unbroken Bonds", "Unified Minds", "Cosmic Eclipse",
        "Detective Pikachu", "Hidden Fates", "Shining Legends", "Dragon Majesty",
    ],
    "XY": [
        "XY",
        "Flashfire", "Furious Fists", "Phantom Forces", "Primal Clash",
        "Roaring Skies", "Ancient Origins", "BREAKthrough", "BREAKpoint",
        "Fates Collide", "Steam Siege",
        "Evolutions",        # exact-prefix only; 'Prismatic Evolutions'
                             # won't match because we check startswith
        "Generations", "Double Crisis",
        "Kalos Starter",
    ],
    "Black & White": [
        "Black & White",
        "Emerging Powers", "Noble Victories", "Next Destinies",
        "Dark Explorers", "Dragons Exalted", "Dragon Vault",
        "Boundaries Crossed", "Plasma Storm", "Plasma Freeze",
        "Plasma Blast", "Legendary Treasures", "Radiant Collection",
    ],
    "Promos & Misc": [
        "Wizards Black Star Promos", "Wizards of the Coast Promos",
        "WoTC", "Nintendo",
        "Miscellaneous Cards & Products", "Miscellaneous",
        "McDonald's", "Trick or Trade", "Holiday Calendar",
        "Best of Game", "POP Series", "Futsal", "Rumble",
    ],
}


# raw_cards.game uses short codes ('magic', 'pokemon', 'onepiece') while
# scrydex_card_meta uses Scrydex's API names (so 'magicthegathering' for MTG).
# Filter joins always need both names — the raw_cards side and the meta side.
_GAME_TO_SCRYDEX_GAME = {
    "magic":     "magicthegathering",
    "pokemon":   "pokemon",
    "onepiece":  "onepiece",
    "lorcana":   "lorcana",
    "riftbound": "riftbound",
}


# Per-game advanced-filter shape. The frontend reads this from
# /api/filter-meta to render only the controls that apply.
#   colors_field   — JSONB path the filter checks against (in scrydex_card_meta)
#   colors_options — canonical option list (omitted ⇒ derive from data)
#   color_modes    — ('any',) or ('any','exactly'); MTG/OP support both
#   types_field    — JSONB path / column for the type/category filter
#   types_options  — canonical option list (omitted ⇒ derive from data)
# Rarity is always raw_cards.rarity (already on every card row).
GAME_FILTER_SCHEMA = {
    "magic": {
        "colors": {
            "label":   "Color",
            "field":   "raw->'color_identity'",  # commander-style identity
            "type":    "jsonb",
            "options": ["W", "U", "B", "R", "G", "C"],
            "labels":  {"W": "White", "U": "Blue", "B": "Black",
                        "R": "Red",   "G": "Green", "C": "Colorless"},
            "modes":   ["any", "exactly"],
        },
        "card_type": {
            "label":   "Card type",
            "field":   "raw->'types'",
            "type":    "jsonb",
            "options": ["Land", "Creature", "Artifact", "Enchantment",
                        "Instant", "Sorcery", "Planeswalker", "Battle"],
        },
    },
    "pokemon": {
        "card_type": {
            "label":   "Energy",
            "field":   "types",
            "type":    "jsonb",
            "options": ["Grass", "Fire", "Water", "Lightning", "Psychic",
                        "Fighting", "Darkness", "Metal", "Fairy",
                        "Dragon", "Colorless"],
        },
    },
    "onepiece": {
        "colors": {
            "label":   "Color",
            "field":   "colors",
            "type":    "jsonb",
            "options": ["Red", "Green", "Blue", "Purple", "Black", "Yellow"],
            "modes":   ["any", "exactly"],
        },
        "card_type": {
            "label":   "Card type",
            "field":   "card_type",
            "type":    "text",
            "options": ["Leader", "Character", "Event", "Stage"],
        },
    },
}


def _classify_era(set_name: str) -> str:
    """Map a set_name to an era using case-insensitive prefix match.
    Anything unmatched (Base Set, Jungle, Fossil, Team Rocket, Gym, Neo,
    e-Card, EX series, Diamond & Pearl, Platinum, HGSS, Call of Legends,
    JP-only sets, etc.) falls into 'Vintage'."""
    if not set_name:
        return "Vintage"
    sn = set_name.strip().lower()
    for era, prefixes in ERA_PREFIXES.items():
        for p in prefixes:
            if sn.startswith(p.lower()):
                return era
    return "Vintage"



@app.route("/")
def index():
    return render_template("index.html", access_key=KIOSK_ACCESS_KEY)


def _check_access():
    """Verify the request has a valid access key or Champion identity."""
    if not KIOSK_ACCESS_KEY:
        return True
    key = request.headers.get("X-Kiosk-Key", "") or request.args.get("key", "")
    if key == KIOSK_ACCESS_KEY:
        return True
    # Champions get access via their verified email header
    champ = request.headers.get("X-Champion-Email", "")
    if champ:
        return True
    return False


@app.before_request
def gate_api():
    """Block API access without a valid access key. Health/webhook endpoints are exempt."""
    if not KIOSK_ACCESS_KEY:
        return
    path = request.path
    if path in ("/", "/health", "/api/champion/identify") or path.startswith("/api/webhooks/") or path.startswith("/api/cleanup/"):
        return
    if path.startswith("/api/"):
        if not _check_access():
            return jsonify({"error": "Access key required"}), 403


# ═══════════════════════════════════════════════════════════════════════════════
# Filter helpers (game-aware advanced filters)
# ═══════════════════════════════════════════════════════════════════════════════

def _multi_param(name: str) -> list[str]:
    """Read a comma-separated multi-select query param."""
    raw = (request.args.get(name) or "").strip()
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _build_meta_filter_subquery(game: str,
                                colors: list[str], color_mode: str,
                                card_types: list[str],
                                rarities: list[str]) -> tuple[str, list]:
    """Build a `rc.id IN (subquery)` fragment that narrows raw_cards to those
    matching the per-game advanced filters.

    Returns ('', []) when no filters apply.

    Game name maps from raw_cards.game (short code) to scrydex_card_meta.game
    (Scrydex API name) — see _GAME_TO_SCRYDEX_GAME.

    Rarity lives on raw_cards directly so it's applied via OR-list outside
    this subquery (no need to join meta for rarity alone).
    """
    schema = GAME_FILTER_SCHEMA.get(game)
    if not schema:
        return "", []

    # Rarity goes in the outer query — it's a column on raw_cards directly
    # and doesn't require a meta join.
    needs_join = bool(colors or card_types)
    if not needs_join:
        return "", []

    sx_game = _GAME_TO_SCRYDEX_GAME.get(game, game)
    parts: list[str] = []
    p: list = []

    # Colors
    if colors and "colors" in schema:
        spec = schema["colors"]
        field = spec["field"]   # e.g. "raw->'color_identity'" or "colors"
        if color_mode == "exactly":
            # Card's full color identity is exactly the selected set (BUG = exactly B,U,G).
            # Use a normalized comparison: sort + dedupe both sides as TEXT[]
            # so order-insensitive equality works.
            arr_lit_ph = ",".join(["%s"] * len(colors))
            parts.append(
                f"(SELECT COALESCE(array_agg(DISTINCT x ORDER BY x), ARRAY[]::text[]) "
                f" FROM jsonb_array_elements_text({field}) AS x) "
                f"= (SELECT COALESCE(array_agg(DISTINCT v ORDER BY v), ARRAY[]::text[]) "
                f" FROM unnest(ARRAY[{arr_lit_ph}]) AS v)"
            )
            p.extend(colors)
        else:
            # Any: card's identity contains at least one of the selected.
            # ?| takes a TEXT[] of keys.
            parts.append(f"{field} ?| %s::text[]")
            p.append(colors)

    # Card type / Energy
    if card_types and "card_type" in schema:
        spec = schema["card_type"]
        field = spec["field"]
        if spec["type"] == "jsonb":
            parts.append(f"{field} ?| %s::text[]")
            p.append(card_types)
        else:
            ph = ",".join(["%s"] * len(card_types))
            parts.append(f"{field} IN ({ph})")
            p.extend(card_types)

    if not parts:
        return "", []

    inner_where = " AND ".join(parts)

    # Walk: raw_cards.tcgplayer_id ↘ scrydex_price_cache.scrydex_id
    # ↘ scrydex_card_meta. raw_cards.scrydex_id is reliable for Pokemon but
    # NULL for MTG/OP today — going through tcgplayer_id makes this work for
    # all three games.
    #
    # Returned as a `tcgplayer_id IN (...)` clause (no `rc.` alias) so it
    # composes with the existing /api/browse query which references
    # raw_cards unaliased. The subquery is a list of distinct tcgplayer_ids
    # — not raw_cards.id — because the same physical card row never matters
    # here, only whether the card identity (tcgplayer_id) matches the meta.
    subq = (
        "tcgplayer_id IN ("
        " SELECT pc.tcgplayer_id FROM scrydex_price_cache pc"
        " JOIN scrydex_card_meta m"
        "   ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id"
        f" WHERE pc.game = %s AND {inner_where}"
        ")"
    )
    return subq, [sx_game, *p]


# ═══════════════════════════════════════════════════════════════════════════════
# Browse API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/browse")
def browse():
    """
    Aggregated card listings.
    Groups raw_cards by (card_name, set_name, tcgplayer_id)
    Returns available qty per condition, total count, price range.

    Query params: q, set, page (24 per page), plus per-game advanced filters:
      colors=W,U,B&color_mode=exactly|any
      card_type=Sorcery,Instant       (MTG types / Pokemon energy / OP card_type)
      card_rarity=Rare,Mythic         (raw_cards.rarity multi-select)
    Filter relevance is resolved server-side via GAME_FILTER_SCHEMA — passing
    `colors` while game=pokemon is silently ignored (no schema entry).
    """
    q          = (request.args.get("q") or "").strip()
    set_name   = (request.args.get("set") or "").strip()
    conditions = [c.strip().upper() for c in (request.args.get("condition") or "").split(",") if c.strip()]
    min_price  = request.args.get("min_price", type=float)
    max_price  = request.args.get("max_price", type=float)
    era        = (request.args.get("era") or "").strip()
    game       = (request.args.get("game") or "").strip().lower()
    sort       = (request.args.get("sort") or "name_asc").strip()
    page       = max(1, int(request.args.get("page", 1)))
    offset     = (page - 1) * 24

    # Game-aware advanced filters
    colors     = _multi_param("colors")
    color_mode = (request.args.get("color_mode") or "any").strip().lower()
    if color_mode not in ("any", "exactly"):
        color_mode = "any"
    card_types = _multi_param("card_type")
    rarities   = _multi_param("card_rarity")

    filters = ["state = 'STORED'", "current_hold_id IS NULL"]
    params  = []

    if game:
        # Game filter is canonical (pokemon / onepiece / magic / lorcana /
        # riftbound). Era only applies inside Pokemon — combining game=onepiece
        # with era=Vintage would zero everything, so callers should clear era
        # when switching game.
        filters.append("game = %s")
        params.append(game)
    if q:
        filters.append("(card_name ILIKE %s OR set_name ILIKE %s OR card_number ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if set_name:
        # Exact-match the picked set so 'Mega Evolution' doesn't pull in
        # 'Mega Evolution Black Star Promos' as a substring.
        filters.append("set_name = %s")
        params.append(set_name)
    if conditions:
        valid = [c for c in conditions if c in ("NM", "LP", "MP", "HP", "DMG")]
        if valid:
            cph = ",".join(["%s"] * len(valid))
            filters.append(f"condition IN ({cph})")
            params += valid
    if min_price is not None:
        filters.append("current_price >= %s")
        params.append(min_price)
    if max_price is not None:
        filters.append("current_price <= %s")
        params.append(max_price)
    if era:
        # Resolve era to the actual list of distinct set_names that classify
        # to it. Filtering by membership avoids the loose-LIKE bugs (e.g. XY
        # filter previously matched 'Prismatic Evolutions' via the literal
        # keyword 'Evolutions'). Computed against the live DB so new sets
        # show up automatically.
        all_sets = db.query(
            "SELECT DISTINCT set_name FROM raw_cards "
            "WHERE state='STORED' AND set_name IS NOT NULL"
        )
        era_sets = [r["set_name"] for r in all_sets if _classify_era(r["set_name"]) == era]
        if era_sets:
            ph = ",".join(["%s"] * len(era_sets))
            filters.append(f"set_name IN ({ph})")
            params += era_sets
        else:
            # Era selected but no in-stock sets for it — force empty result.
            filters.append("FALSE")

    # Rarity (raw_cards.rarity, case-insensitive multi-select). Only meaningful
    # when game is set; otherwise the same label means different things across
    # games (Pokemon "Rare" vs MTG "Rare").
    if rarities and game:
        ph = ",".join(["%s"] * len(rarities))
        filters.append(f"LOWER(rarity) IN ({ph})")
        params += [r.lower() for r in rarities]

    # Game-aware advanced filters (colors / card_type) — push down via subquery
    # so it doesn't blow up rows for the count/aggregation logic that follows.
    if game:
        meta_clause, meta_params = _build_meta_filter_subquery(
            game, colors, color_mode, card_types, rarities,
        )
        if meta_clause:
            filters.append(meta_clause)
            params += meta_params

    where = " AND ".join(filters)

    # Sort mapping
    sort_map = {
        "name_asc": "card_name ASC",
        "price_asc": "min_price ASC NULLS LAST",
        "price_desc": "max_price DESC NULLS LAST",
        "newest": "MAX(created_at) DESC",
    }
    order_by = sort_map.get(sort, "card_name ASC")

    # Group key folds NULL/normal/holofoil to one bucket so single-variant
    # cards don't fragment. Distinguishing variants (1st Ed vs Unlimited,
    # reverseHolofoil, etc.) get their own tile.
    group_key = ("card_name, set_name, tcgplayer_id, "
                 "CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil') "
                 "THEN '' ELSE variant END")

    count_row = db.query_one(f"""
        SELECT COUNT(DISTINCT ({group_key})) AS total
        FROM raw_cards
        WHERE {where}
          AND current_hold_id IS NULL
    """, tuple(params))
    total = count_row["total"] if count_row else 0

    # Aggregated cards with per-condition breakdown.
    # variant_key is the *bucket* value: NULL/normal/holofoil all fold to ''
    # so single-variant cards stay in one tile. variant_raw is preserved for
    # display when a card has multiple printings in Scrydex.
    rows = db.query(f"""
        SELECT
            card_name,
            set_name,
            tcgplayer_id,
            MAX(scrydex_id) AS scrydex_id,
            MAX(variant_raw) AS variant_raw,
            variant_key,
            MAX(image_url) AS image_url,
            SUM(cond_qty) AS total_qty,
            MIN(min_price) AS min_price,
            MAX(max_price) AS max_price,
            MAX(created_at) AS created_at,
            jsonb_object_agg(condition, cond_qty) AS conditions
        FROM (
            SELECT card_name, set_name, tcgplayer_id, scrydex_id,
                   variant AS variant_raw,
                   CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil')
                        THEN ''
                        ELSE variant
                   END AS variant_key,
                   image_url,
                   condition,
                   COUNT(*) AS cond_qty,
                   MIN(current_price) AS min_price,
                   MAX(current_price) AS max_price,
                   MAX(created_at) AS created_at
            FROM raw_cards
            WHERE {where}
              AND state = 'STORED'
            GROUP BY card_name, set_name, tcgplayer_id, scrydex_id,
                     variant_raw, variant_key, image_url, condition
        ) sub
        GROUP BY card_name, set_name, tcgplayer_id, variant_key
        ORDER BY {order_by}
        LIMIT 24 OFFSET %s
    """, tuple(params) + (offset,))

    # Enrich each row with two things from scrydex_price_cache:
    #   1. Image fallback when raw_cards.image_url is missing (esp. JP cards
    #      that intaked without a TCGplayer image URL).
    #   2. n_variants: how many distinct variants exist for this card across
    #      Scrydex's catalog. Frontend uses this to decide whether to show
    #      a variant badge — single-variant cards stay uncluttered, but a
    #      multi-variant card always gets the badge so the customer knows
    #      to check 1st Ed vs Unlimited.
    cards = []
    for r in rows:
        sid = r["scrydex_id"]
        tcg = r["tcgplayer_id"]
        image_url = r["image_url"]
        n_variants = 1
        if sid or tcg:
            sx = None
            if sid:
                sx = db.query_one("""
                    SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m,
                           MAX(image_small) AS img_s,
                           COUNT(DISTINCT variant) AS n
                    FROM scrydex_price_cache
                    WHERE scrydex_id = %s
                """, (sid,))
            elif tcg:
                # Multiple scrydex products can share a tcgplayer_id (reprints,
                # cross-set promos). For image purposes any of them is fine.
                sx = db.query_one("""
                    SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m,
                           MAX(image_small) AS img_s,
                           COUNT(DISTINCT variant) AS n
                    FROM scrydex_price_cache
                    WHERE tcgplayer_id = %s
                """, (tcg,))
            if sx:
                if not image_url:
                    image_url = sx.get("img_l") or sx.get("img_m") or sx.get("img_s")
                if sx.get("n"):
                    n_variants = int(sx["n"])

        # variant_key is the bucket value used for filtering & cart matching
        # (NULL/normal/holofoil all fold to ''). variant_label is the badge
        # shown on the tile only when we actually know which printing this
        # is. Showing "(unspecified)" when raw_cards.variant is empty (most
        # MTG cards historically) tells the customer nothing, and showed up
        # as purple chrome on every card — worse than no badge.
        variant_raw = (r.get("variant_raw") or "").strip()
        variant_key = r.get("variant_key") or ""
        # Only badge when we know the printing AND it's not a default bucket.
        # intake_items.variance comes through as Title Case ("Normal","Foil",
        # "Holofoil"); the SQL CASE folds case-insensitively, so the Python
        # check has to match. Default printings get no badge.
        if variant_raw and variant_raw.lower() not in ("normal", "holofoil"):
            variant_label = variant_raw
        else:
            variant_label = None

        cards.append({
            "card_name":     r["card_name"],
            "set_name":      r["set_name"],
            "tcgplayer_id":  r["tcgplayer_id"],
            "variant_key":   variant_key,
            "variant_label": variant_label,
            "image_url":     image_url,
            "total_qty":     r["total_qty"],
            "min_price":     float(r["min_price"]) if r["min_price"] else None,
            "max_price":     float(r["max_price"]) if r["max_price"] else None,
            "conditions":    r["conditions"] or {},
        })

    return jsonify({
        "cards":  cards,
        "total":  total,
        "page":   page,
        "pages":  max(1, (total + 23) // 24),
    })


@app.route("/api/sets")
def list_sets():
    era = (request.args.get("era") or "").strip()
    game = (request.args.get("game") or "").strip().lower()
    where = ["state = 'STORED'", "set_name IS NOT NULL", "current_hold_id IS NULL"]
    params: list = []
    if game:
        where.append("game = %s")
        params.append(game)
    rows = db.query(f"""
        SELECT set_name, COUNT(*) AS qty FROM raw_cards
        WHERE {' AND '.join(where)}
        GROUP BY set_name
        ORDER BY set_name ASC
        LIMIT 500
    """, tuple(params))
    sets = [{"name": r["set_name"], "qty": r["qty"]} for r in rows]
    if era:
        sets = [s for s in sets if _classify_era(s["name"]) == era]
    # Backward-compat: also return a flat name list for older callers
    return jsonify({"sets": sets, "names": [s["name"] for s in sets]})


@app.route("/api/eras")
def list_eras():
    """Return available eras based on Pokemon sets currently in stock.
    Era classification is Pokemon-only; non-Pokemon games skip this filter."""
    where = ["state = 'STORED'", "set_name IS NOT NULL", "current_hold_id IS NULL",
             "game = 'pokemon'"]
    rows = db.query(f"""
        SELECT DISTINCT set_name FROM raw_cards
        WHERE {' AND '.join(where)}
    """)
    era_counts = {}
    for r in rows:
        era = _classify_era(r["set_name"])
        era_counts[era] = era_counts.get(era, 0) + 1
    eras = [{"name": k, "set_count": v} for k, v in sorted(era_counts.items())]
    return jsonify({"eras": eras})


@app.route("/api/games")
def list_games():
    """Return distinct games (IPs) currently in stock with available counts.
    Powers the kiosk's top-level game filter."""
    rows = db.query("""
        SELECT COALESCE(game, 'pokemon') AS game, COUNT(*) AS qty
        FROM raw_cards
        WHERE state = 'STORED' AND current_hold_id IS NULL
        GROUP BY COALESCE(game, 'pokemon')
        ORDER BY qty DESC
    """)
    label_map = {
        "pokemon":   "Pokémon",
        "onepiece":  "One Piece",
        "magic":     "Magic",
        "lorcana":   "Lorcana",
        "riftbound": "Riftbound",
        "yugioh":    "Yu-Gi-Oh!",
        "other":     "Other",
    }
    games = [{
        "code":  r["game"],
        "label": label_map.get(r["game"], r["game"].title()),
        "qty":   r["qty"],
    } for r in rows]
    return jsonify({"games": games})


@app.route("/api/filter-meta")
def filter_meta():
    """Return the per-game advanced-filter schema + which option values
    actually have STORED stock right now (for chip qty badges).

    Query: ?game=magic|pokemon|onepiece (required)

    Response:
    {
      "game": "magic",
      "color_modes": ["any", "exactly"],
      "filters": {
        "colors":    {"label": "Color", "options": [{"value":"W","label":"White","qty": 12}, ...]},
        "card_type": {"label": "Card type", "options": [{"value":"Creature","qty":24}, ...]},
        "rarity":    {"label": "Rarity", "options": [{"value":"Rare","qty":38}, ...]}
      }
    }

    qty counts are independent of OTHER active filters — they reflect the
    intersection of (game, STORED, current_hold_id IS NULL) so the chip
    badges stay stable while users toggle filters. Re-querying the schema
    on every keystroke would otherwise cause "13" → "0" → "13" flicker.
    """
    game = (request.args.get("game") or "").strip().lower()
    if not game or game not in GAME_FILTER_SCHEMA:
        return jsonify({"game": game, "filters": {}, "color_modes": []})

    schema = GAME_FILTER_SCHEMA[game]
    sx_game = _GAME_TO_SCRYDEX_GAME.get(game, game)
    out: dict = {
        "game": game,
        "color_modes": list(schema.get("colors", {}).get("modes", [])),
        "filters": {},
    }

    # Helper: count distinct STORED tcgplayer_ids whose meta has the field.
    # (We count by tcgplayer_id rather than raw_cards rows so duplicates of
    # a single card don't inflate the chip badge.)
    def _count_jsonb(field: str) -> dict[str, int]:
        rows = db.query(f"""
            SELECT elem AS k, COUNT(DISTINCT rc.tcgplayer_id) AS n
            FROM raw_cards rc
            JOIN scrydex_price_cache pc
              ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
            JOIN scrydex_card_meta m
              ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
            JOIN LATERAL jsonb_array_elements_text({field}) AS elem ON TRUE
            WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
            GROUP BY elem
        """, (sx_game, game))
        return {r["k"]: int(r["n"]) for r in rows}

    def _count_text(field: str) -> dict[str, int]:
        rows = db.query(f"""
            SELECT m.{field} AS k, COUNT(DISTINCT rc.tcgplayer_id) AS n
            FROM raw_cards rc
            JOIN scrydex_price_cache pc
              ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
            JOIN scrydex_card_meta m
              ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
            WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
              AND m.{field} IS NOT NULL
            GROUP BY m.{field}
        """, (sx_game, game))
        return {r["k"]: int(r["n"]) for r in rows}

    # Colors
    if "colors" in schema:
        spec = schema["colors"]
        counts = _count_jsonb(spec["field"]) if spec["type"] == "jsonb" else _count_text(spec["field"])
        labels = spec.get("labels") or {}
        out["filters"]["colors"] = {
            "label":   spec["label"],
            "options": [
                {"value": v, "label": labels.get(v, v), "qty": counts.get(v, 0)}
                for v in spec["options"]
            ],
        }

    # Card type
    if "card_type" in schema:
        spec = schema["card_type"]
        counts = _count_jsonb(spec["field"]) if spec["type"] == "jsonb" else _count_text(spec["field"])
        out["filters"]["card_type"] = {
            "label":   spec["label"],
            "options": [
                {"value": v, "label": v, "qty": counts.get(v, 0)}
                for v in spec["options"]
            ],
        }

    # Rarity — pulled live from raw_cards (varies per game and changes as new
    # rarities ship). Hard-coding it would break when Scrydex adds a new label.
    rarity_rows = db.query("""
        SELECT rarity, COUNT(*) AS n FROM raw_cards
        WHERE state='STORED' AND current_hold_id IS NULL AND game = %s
          AND rarity IS NOT NULL AND rarity <> ''
        GROUP BY rarity
        ORDER BY n DESC
    """, (game,))
    out["filters"]["rarity"] = {
        "label":   "Rarity",
        "options": [
            {"value": r["rarity"], "label": r["rarity"], "qty": int(r["n"])}
            for r in rarity_rows
        ],
    }

    return jsonify(out)


@app.route("/api/card")
def card_detail():
    """
    Individual copies of a specific card for the detail view.
    Returns each copy with condition, price, card_number.

    Variant is part of the key so 1st Ed and Unlimited printings of the
    same card are fetched as distinct detail views. '' or omitted treats
    the default (normal/holofoil) bucket.
    """
    card_name    = request.args.get("name", "")
    set_name     = request.args.get("set", "")
    variant      = (request.args.get("variant") or "").strip()
    tcgplayer_id = request.args.get("tcgplayer_id", type=int)
    scrydex_id   = (request.args.get("scrydex_id") or "").strip()

    # Match the grouping rule from /api/browse: default variants fold to ''.
    variant_filter = "AND CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil') THEN '' ELSE variant END = %s"

    # Two cards can share name + set + variant_fold (e.g. OP14-041 Boa Hancock
    # Leader Alt Art vs OP14-112 Boa Hancock SR Alt Art — same name, same set,
    # both fold to variant_key='altArt'). The browse view groups by tcgplayer_id
    # so they show as separate cards; the detail view must filter by it too or
    # the copies + averaged price collapse them back together.
    id_filter = ""
    extra: list = []
    if scrydex_id:
        id_filter = "AND scrydex_id = %s"
        extra.append(scrydex_id)
    elif tcgplayer_id:
        id_filter = "AND tcgplayer_id = %s"
        extra.append(tcgplayer_id)

    copies = db.query(f"""
        SELECT id, barcode, card_name, set_name, card_number,
               condition, current_price, image_url, variant,
               tcgplayer_id, scrydex_id, rarity,
               COALESCE(game, 'pokemon') AS game
        FROM raw_cards
        WHERE card_name = %s AND set_name = %s
          AND state = 'STORED' AND current_hold_id IS NULL
          {variant_filter}
          {id_filter}
        ORDER BY
            CASE condition
                WHEN 'NM'  THEN 1 WHEN 'LP'  THEN 2 WHEN 'MP' THEN 3
                WHEN 'HP'  THEN 4 WHEN 'DMG' THEN 5 ELSE 9
            END,
            current_price DESC
    """, (card_name, set_name, variant, *extra))

    # Image fallback to Scrydex cache when raw_cards.image_url is missing
    # (e.g. JP cards entered before TCGplayer images were available, or
    # Scrydex-only cards that never had a PPT image to begin with).
    out = []
    for c in copies:
        d = dict(c)
        if not d.get("image_url") and (d.get("scrydex_id") or d.get("tcgplayer_id")):
            sx = None
            if d.get("scrydex_id"):
                sx = db.query_one("""
                    SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m,
                           MAX(image_small) AS img_s
                    FROM scrydex_price_cache WHERE scrydex_id = %s
                """, (d["scrydex_id"],))
            else:
                sx = db.query_one("""
                    SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m,
                           MAX(image_small) AS img_s
                    FROM scrydex_price_cache WHERE tcgplayer_id = %s
                """, (d["tcgplayer_id"],))
            if sx:
                d["image_url"] = sx.get("img_l") or sx.get("img_m") or sx.get("img_s")
        out.append(d)

    # ── Card metadata from scrydex_card_meta (static, not nightly prices) ──
    meta = None
    # Resolve scrydex_id: prefer from copies, fall back to price_cache lookup
    _sx_id = scrydex_id
    if not _sx_id and out:
        _sx_id = out[0].get("scrydex_id") or ""
    if not _sx_id and tcgplayer_id:
        row = db.query_one(
            "SELECT scrydex_id FROM scrydex_price_cache WHERE tcgplayer_id = %s LIMIT 1",
            (tcgplayer_id,),
        )
        if row:
            _sx_id = row["scrydex_id"]
    if _sx_id:
        meta_row = db.query_one("""
            SELECT hp, supertype, types, attacks, abilities, weaknesses,
                   resistances, retreat_cost, converted_retreat_cost,
                   artist, flavor_text, rules, subtypes,
                   card_type, attribute, colors, life, power, printed_number,
                   raw
            FROM scrydex_card_meta WHERE scrydex_id = %s
        """, (_sx_id,))
        if meta_row:
            m = dict(meta_row)
            raw_data = m.pop("raw", None) or {}

            # ── Double-faced cards (MTG MDFCs, transform, etc.) ──
            # Promoted columns are empty because Scrydex nests data under faces[].
            # Fall back to faces[0] (front face) for display fields.
            faces = raw_data.get("faces") or []
            if faces and not m.get("types") and not m.get("rules"):
                front = faces[0]
                if not m.get("types"):
                    m["types"] = front.get("types")
                if not m.get("subtypes"):
                    m["subtypes"] = front.get("subtypes")
                if not m.get("rules"):
                    m["rules"] = front.get("rules")
                if not m.get("flavor_text"):
                    m["flavor_text"] = front.get("flavor_text")
                if not m.get("supertype"):
                    st = front.get("supertypes") or []
                    m["supertype"] = " ".join(st) if st else None
                m["mana_cost"] = front.get("mana_cost")
                # Include back face info so frontend can show both
                if len(faces) > 1:
                    back = faces[1]
                    m["back_face"] = {
                        "name": back.get("name"),
                        "types": back.get("types"),
                        "subtypes": back.get("subtypes"),
                        "rules": back.get("rules"),
                        "flavor_text": back.get("flavor_text"),
                        "mana_cost": back.get("mana_cost"),
                    }

            # Extract back-face image from raw Scrydex images array
            images = raw_data.get("images") or []
            for img in images:
                if img.get("type") == "back":
                    m["image_back"] = img.get("large") or img.get("medium") or img.get("small")
                    break

            # Top-level artist fallback (sometimes only at raw level for MTG)
            if not m.get("artist"):
                m["artist"] = raw_data.get("artist")

            # Strip nulls to keep payload lean
            meta = {k: v for k, v in m.items() if v is not None}

    return jsonify({"copies": out, "meta": meta})


# ═══════════════════════════════════════════════════════════════════════════════
# Hold API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/hold", methods=["POST"])
def create_hold():
    """
    Submit a hold request.

    POST body:
    {
        "customer_name": "Mark",
        "customer_phone": "555-1234",
        "items": [
            {"card_name": "Charizard ex", "set_name": "...", "condition": "NM", "qty": 2},
            ...
        ]
    }

    Resolves which specific barcodes to hold, marks them PULLED state
    (actually PULLED happens when staff scans — here we just reserve them).
    Returns hold_id + summary.
    """
    data     = request.get_json() or {}
    name     = (data.get("customer_name") or "").strip()
    phone    = (data.get("customer_phone") or "").strip()
    items    = data.get("items") or []

    if not name:
        return jsonify({"error": "Customer name required"}), 400
    if not items:
        return jsonify({"error": "No items in hold request"}), 400

    # Validate total quantity
    total_qty = sum(int(i.get("qty", 1)) for i in items)
    if total_qty > MAX_HOLD_ITEMS:
        return jsonify({"error": f"Maximum {MAX_HOLD_ITEMS} cards per hold (requested {total_qty})"}), 400
    if total_qty < 1:
        return jsonify({"error": "Must request at least 1 card"}), 400

    # For each line item, find available STORED cards matching card+set+condition
    # Lock them by setting current_hold_id
    assigned = []
    errors   = []

    # Resolve which cards to hold before opening transaction.
    # Variant is part of the matching key — same (name,set,cond) can hold both
    # 1st Ed and Unlimited copies, and a cart line for one must not steal from
    # the other. Default variants (normal/holofoil/null) fold into '' to match
    # how /api/browse groups.
    lines_resolved = []
    for line in items:
        card_name = line.get("card_name", "")
        set_name  = line.get("set_name", "")
        condition = line.get("condition", "NM")
        variant   = (line.get("variant") or "").strip()
        tcgplayer_id = line.get("tcgplayer_id")
        scrydex_id   = (line.get("scrydex_id") or "").strip()
        qty       = max(1, int(line.get("qty", 1)))

        # Disambiguate same-name-same-set-same-variant cards (e.g. OP14-041
        # and OP14-112 Boa Hancock both fold to altArt) by tcgplayer_id /
        # scrydex_id. Without this, a hold for one would steal copies of
        # the other in created_at order.
        id_filter = ""
        id_params: list = []
        if scrydex_id:
            id_filter = " AND scrydex_id = %s"
            id_params.append(scrydex_id)
        elif tcgplayer_id:
            id_filter = " AND tcgplayer_id = %s"
            id_params.append(int(tcgplayer_id))

        available = db.query(f"""
            SELECT id, barcode FROM raw_cards
            WHERE card_name = %s AND set_name = %s
              AND condition = %s AND state = 'STORED'
              AND current_hold_id IS NULL
              AND CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil') THEN '' ELSE variant END = %s
              {id_filter}
            ORDER BY created_at ASC
            LIMIT %s
        """, (card_name, set_name, condition, variant, *id_params, qty))

        if not available:
            errors.append(f"No {condition} copies available for {card_name}")
            continue
        if len(available) < qty:
            errors.append(f"Only {len(available)} {condition} {card_name} available (requested {qty})")

        for card in available:
            lines_resolved.append({
                "card_name": card_name, "set_name": set_name,
                "condition": condition, "variant": variant,
                "card_id": str(card["id"]), "barcode": card["barcode"],
            })

    if not lines_resolved:
        return jsonify({"error": "No cards available for any requested items", "details": errors}), 409

    # Single transaction: create hold + reserve cards + create hold_items
    with db.get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                from psycopg2.extras import RealDictCursor
                cur = conn.cursor(cursor_factory=RealDictCursor)

                cur.execute("""
                    INSERT INTO holds (customer_name, customer_phone, status, item_count)
                    VALUES (%s, %s, 'PENDING', %s) RETURNING id
                """, (name, phone or None, len(lines_resolved)))
                hold_id = str(cur.fetchone()["id"])

                for r in lines_resolved:
                    cur.execute("""
                        UPDATE raw_cards SET current_hold_id = %s WHERE id = %s
                    """, (hold_id, r["card_id"]))
                    cur.execute("""
                        INSERT INTO hold_items (hold_id, raw_card_id, barcode, status)
                        VALUES (%s, %s, %s, 'REQUESTED')
                    """, (hold_id, r["card_id"], r["barcode"]))
                    assigned.append({"card_name": r["card_name"], "condition": r["condition"], "barcode": r["barcode"]})

                conn.commit()
        except Exception:
            conn.rollback()
            raise

    return jsonify({
        "success":   True,
        "hold_id":   hold_id,
        "assigned":  len(assigned),
        "requested": total_qty,
        "warnings":  errors,
        "items":     assigned,
    })


@app.route("/api/hold/<hold_id>")
def get_hold(hold_id):
    hold = db.query_one("SELECT * FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Hold not found"}), 404
    items = db.query("""
        SELECT hi.*, rc.card_name, rc.set_name, rc.condition,
               rc.current_price, rc.card_number, rc.image_url
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.hold_id = %s
        ORDER BY hi.created_at
    """, (hold_id,))
    return jsonify({
        "hold":  _ser(dict(hold)),
        "items": [_ser(dict(i)) for i in items],
    })


def _ser(d: dict) -> dict:
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif hasattr(v, "__float__"):
            d[k] = float(v)
    return d


# ═══════════════════════════════════════════════════════════════════════════════
# Shopify Admin API helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _shopify_rest(method, path, **kwargs):
    """Shopify Admin REST API call with retry."""
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    for attempt in range(4):
        try:
            r = _requests.request(method, url, headers=headers, timeout=30, **kwargs)
            r.raise_for_status()
            return r.json() if r.content else {}
        except (_requests.Timeout, _requests.ConnectionError, _requests.HTTPError) as e:
            if attempt >= 3:
                raise
            if hasattr(e, 'response') and e.response is not None and e.response.status_code < 500:
                raise
            time.sleep(1.0 * (1.5 ** attempt))


def _shopify_gql(query, variables=None):
    """Shopify Admin GraphQL call (for customer lookup + publication)."""
    from shopify_graphql import shopify_gql
    return shopify_gql(query, variables)


# ═══════════════════════════════════════════════════════════════════════════════
# Champion Identification
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/champion/identify", methods=["POST"])
def champion_identify():
    """
    Look up a Shopify customer by email and verify they have VIP3 tag.
    No auth layer — just identity check. They still must log in at Shopify checkout.
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "Email required"}), 400

    if not SHOPIFY_TOKEN or not SHOPIFY_STORE:
        return jsonify({"error": "Shopify not configured"}), 503

    result = _shopify_gql("""
        query($query: String!) {
          customers(first: 1, query: $query) {
            edges {
              node {
                id
                firstName
                lastName
                email
                tags
              }
            }
          }
        }
    """, {"query": f"email:{email}"})

    edges = result.get("data", {}).get("customers", {}).get("edges", [])
    if not edges:
        return jsonify({"verified": False, "reason": "No account found with that email"}), 200

    customer = edges[0]["node"]
    tags = [t.strip().upper() for t in (customer.get("tags") or [])]
    if "VIP3" not in tags:
        return jsonify({"verified": False, "reason": "Only Champions can check out online"}), 200

    return jsonify({
        "verified": True,
        "first_name": customer.get("firstName") or "",
        "customer_gid": customer["id"],
        "email": customer.get("email") or email,
        "checkout_enabled": KIOSK_CHECKOUT_ENABLED,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Champion Checkout
# ═══════════════════════════════════════════════════════════════════════════════

def _create_kiosk_product(card, hold_id):
    """
    Create a real ACTIVE Shopify product for a raw card.
    Published to Online Store with 'hidde' template (blank page), no collections.
    Cleaned up after 30 min if unpurchased.
    Returns {"product_id": ..., "variant_id": ...}
    """
    condition_labels = {
        "NM": "Near Mint", "LP": "Lightly Played",
        "MP": "Moderately Played", "HP": "Heavily Played", "DMG": "Damaged"
    }
    cond_label = condition_labels.get(card["condition"], card["condition"])
    card_num = f" #{card['card_number']}" if card.get("card_number") else ""
    title = f"{card['card_name']}{card_num} [{cond_label}]"
    body = (f"<p>{card['card_name']}{card_num}</p>"
            f"<p>Set: {card.get('set_name', '')}</p>"
            f"<p>Condition: {cond_label}</p>")
    price = float(card.get("current_price") or 0)

    payload = {
        "product": {
            "title": title,
            "body_html": body,
            "status": "active",
            "product_type": "Raw Card",
            "vendor": "Pack Fresh",
            "tags": f"kiosk-raw,kiosk-hold-{hold_id}",
            "template_suffix": "hidde",
            "images": [{"src": card["image_url"]}] if card.get("image_url") else [],
            "variants": [{
                "price": str(round(price, 2)),
                "sku": card["barcode"],
                "barcode": card["barcode"],
                "inventory_management": "shopify",
                "inventory_quantity": 1,
                "requires_shipping": True,
            }],
        }
    }

    result = _shopify_rest("POST", "/products.json", json=payload)
    product = result["product"]
    product_id = product["id"]
    variant_id = product["variants"][0]["id"]

    return {
        "product_id": product_id,
        "variant_id": variant_id,
    }


@app.route("/api/checkout", methods=["POST"])
def champion_checkout():
    """
    Champion checkout flow:
    1. Verify VIP3 status
    2. Create hold (lock cards)
    3. Create real Shopify products (active, Kiosk channel only)
    4. Create Storefront API cart → get checkout URL
    5. Return checkout URL to frontend
    """
    if not KIOSK_CHECKOUT_ENABLED:
        return jsonify({"error": "Online checkout is coming soon!"}), 403

    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    customer_gid = (data.get("customer_gid") or "").strip()
    items = data.get("items") or []

    if not email or not customer_gid:
        return jsonify({"error": "Champion email and customer_gid required"}), 400
    if not items:
        return jsonify({"error": "No items in checkout"}), 400

    if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
        return jsonify({"error": "Shopify not configured"}), 503

    total_qty = sum(int(i.get("qty", 1)) for i in items)
    if total_qty > MAX_HOLD_ITEMS:
        return jsonify({"error": f"Maximum {MAX_HOLD_ITEMS} cards per checkout"}), 400

    # Estimate cart total for minimum check

    # ── Step 1: Re-verify Champion ──────────────────────────────────────────
    verify_result = _shopify_gql("""
        query($id: ID!) {
          customer(id: $id) { tags }
        }
    """, {"id": customer_gid})
    tags = [t.strip().upper() for t in (
        verify_result.get("data", {}).get("customer", {}).get("tags") or []
    )]
    if "VIP3" not in tags:
        return jsonify({"error": "Only Champions can check out online"}), 403

    # ── Step 2: Resolve available cards (same as create_hold) ───────────────
    lines_resolved = []
    errors = []
    for line in items:
        card_name = line.get("card_name", "")
        set_name = line.get("set_name", "")
        condition = line.get("condition", "NM")
        variant = (line.get("variant") or "").strip()
        tcgplayer_id = line.get("tcgplayer_id")
        scrydex_id   = (line.get("scrydex_id") or "").strip()
        qty = max(1, int(line.get("qty", 1)))

        # Disambiguate same-name+set+variant cards via tcg/scrydex id
        # (mirrors create_hold; e.g. OP14-041 vs OP14-112 Boa Hancock)
        id_filter = ""
        id_params: list = []
        if scrydex_id:
            id_filter = " AND scrydex_id = %s"
            id_params.append(scrydex_id)
        elif tcgplayer_id:
            id_filter = " AND tcgplayer_id = %s"
            id_params.append(int(tcgplayer_id))

        available = db.query(f"""
            SELECT id, barcode, card_name, set_name, card_number,
                   condition, current_price, image_url
            FROM raw_cards
            WHERE card_name = %s AND set_name = %s
              AND condition = %s AND state = 'STORED'
              AND current_hold_id IS NULL
              AND CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil') THEN '' ELSE variant END = %s
              {id_filter}
            ORDER BY created_at ASC
            LIMIT %s
        """, (card_name, set_name, condition, variant, *id_params, qty))

        if not available:
            errors.append(f"No {condition} copies available for {card_name}")
            continue
        if len(available) < qty:
            errors.append(f"Only {len(available)} {condition} {card_name} available (requested {qty})")

        for card in available:
            lines_resolved.append(dict(card))

    if not lines_resolved:
        return jsonify({"error": "No cards available", "details": errors}), 409

    # ── Step 3: Create hold + lock cards ────────────────────────────────────
    with db.get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                from psycopg2.extras import RealDictCursor
                cur = conn.cursor(cursor_factory=RealDictCursor)

                cur.execute("""
                    INSERT INTO holds
                        (customer_name, customer_phone, status, item_count,
                         cohort, customer_email, shopify_customer_gid, checkout_status)
                    VALUES (%s, NULL, 'PENDING', %s, 'champion', %s, %s, 'pending')
                    RETURNING id
                """, (email, len(lines_resolved), email, customer_gid))
                hold_id = str(cur.fetchone()["id"])

                for card in lines_resolved:
                    cur.execute("""
                        UPDATE raw_cards SET current_hold_id = %s WHERE id = %s
                    """, (hold_id, card["id"]))
                    cur.execute("""
                        INSERT INTO hold_items (hold_id, raw_card_id, barcode, status)
                        VALUES (%s, %s, %s, 'REQUESTED')
                    """, (hold_id, card["id"], card["barcode"]))

                conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ── Step 4: Create Shopify products (Online Store, hidde template) ─────
    variant_ids = []
    cart_total = 0
    try:
        for card in lines_resolved:
            listing = _create_kiosk_product(card, hold_id)
            variant_ids.append(str(listing["variant_id"]))
            cart_total += float(card.get("current_price") or 0)

            db.execute("""
                UPDATE hold_items
                SET shopify_product_id = %s, shopify_variant_id = %s
                WHERE hold_id = %s AND raw_card_id = %s
            """, (str(listing["product_id"]), str(listing["variant_id"]),
                  hold_id, card["id"]))
    except Exception as e:
        logger.error(f"Failed to create Shopify products for hold {hold_id}: {e}")
        _cleanup_hold(hold_id)
        return jsonify({"error": "Failed to create checkout products"}), 500

    # ── Step 5: Build cart-merge URL ──────────────────────────────────────
    # Redirects customer to theme page that adds items to their existing Shopify cart
    items_param = ",".join(variant_ids)
    checkout_url = f"{SHOPIFY_STOREFRONT_URL}/pages/kiosk-add?items={items_param}"

    db.execute("UPDATE holds SET checkout_url = %s WHERE id = %s", (checkout_url, hold_id))

    logger.info(f"Champion checkout: hold={hold_id} email={email} items={len(lines_resolved)} total=${cart_total:.2f}")

    return jsonify({
        "success": True,
        "hold_id": hold_id,
        "checkout_url": checkout_url,
        "item_count": len(lines_resolved),
        "cart_total": round(cart_total, 2),
        "warnings": errors,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Order Webhook — close the loop when Champion pays
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_shopify_webhook(data: bytes, hmac_header: str) -> bool:
    """Verify Shopify webhook HMAC-SHA256 signature."""
    if not SHOPIFY_WEBHOOK_SECRET:
        return False
    digest = hmac.new(SHOPIFY_WEBHOOK_SECRET.encode(), data, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed, hmac_header)


@app.route("/api/webhooks/order-paid", methods=["POST"])
def webhook_order_paid():
    """
    Shopify orders/create webhook.
    When a Champion completes checkout, find the kiosk hold and mark it PAID.
    """
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not _verify_shopify_webhook(request.get_data(), hmac_header):
        return jsonify({"error": "Invalid signature"}), 401

    order = request.get_json(silent=True) or {}
    line_items = order.get("line_items", [])

    # Find kiosk-raw items by checking product tags
    hold_ids = set()
    for item in line_items:
        # Shopify includes tags as a comma-separated string on the product
        tags = (item.get("properties") or [])
        # Also check via product_id against our hold_items
        variant_id = item.get("variant_id")
        if variant_id:
            hold_item = db.query_one("""
                SELECT hi.hold_id FROM hold_items hi
                JOIN holds h ON hi.hold_id = h.id
                WHERE hi.shopify_variant_id = %s AND h.cohort = 'champion'
            """, (str(variant_id),))
            if hold_item:
                hold_ids.add(str(hold_item["hold_id"]))

    if not hold_ids:
        return jsonify({"ok": True, "kiosk": False}), 200

    # Extract order info for staff fulfillment
    shopify_order_id = order.get("id")
    order_number = order.get("name") or f"#{order.get('order_number', '')}"
    shipping = order.get("shipping_address") or {}
    shipping_name = f"{shipping.get('first_name', '')} {shipping.get('last_name', '')}".strip()
    if not shipping_name:
        shipping_name = f"{order.get('customer', {}).get('first_name', '')} {order.get('customer', {}).get('last_name', '')}".strip()
    shipping_addr = ", ".join(filter(None, [
        shipping.get("address1"), shipping.get("address2"),
        shipping.get("city"), shipping.get("province_code"),
        shipping.get("zip"), shipping.get("country"),
    ]))

    for hold_id in hold_ids:
        db.execute("""
            UPDATE holds
            SET checkout_status = 'completed', status = 'PENDING',
                customer_name = %s, shopify_order_number = %s,
                shipping_name = %s, shipping_address = %s
            WHERE id = %s AND cohort = 'champion' AND checkout_status = 'pending'
        """, (shipping_name or order_number, order_number,
              shipping_name, shipping_addr, hold_id))
        logger.info(f"Champion order paid: hold={hold_id} order={order_number} ship_to={shipping_name}")

    return jsonify({"ok": True, "kiosk": True, "holds": list(hold_ids)}), 200


# ═══════════════════════════════════════════════════════════════════════════════
# Cleanup — expire abandoned Champion holds
# ═══════════════════════════════════════════════════════════════════════════════

def _cleanup_hold(hold_id):
    """Release cards and delete Shopify products for an abandoned hold."""
    # Get hold items with Shopify product IDs
    items = db.query("""
        SELECT raw_card_id, shopify_product_id FROM hold_items WHERE hold_id = %s
    """, (hold_id,))

    # Delete Shopify products
    for item in items:
        pid = item.get("shopify_product_id")
        if pid:
            try:
                _shopify_rest("DELETE", f"/products/{pid}.json")
            except Exception as e:
                logger.warning(f"Failed to delete Shopify product {pid}: {e}")

    # Release cards back to STORED
    db.execute("""
        UPDATE raw_cards SET current_hold_id = NULL, state = 'STORED'
        WHERE current_hold_id = %s
    """, (hold_id,))

    # Mark hold abandoned
    db.execute("""
        UPDATE holds SET checkout_status = 'abandoned', status = 'ABANDONED'
        WHERE id = %s
    """, (hold_id,))

    # Clean up hold_items
    db.execute("DELETE FROM hold_items WHERE hold_id = %s", (hold_id,))

    logger.info(f"Cleaned up abandoned Champion hold {hold_id}")


@app.route("/api/cleanup/abandoned", methods=["POST"])
def cleanup_abandoned():
    """
    Expire Champion holds that haven't been paid within CHAMPION_HOLD_MINUTES.
    Called by Railway cron every 10 minutes.
    """
    auth = request.headers.get("Authorization", "")
    if CLEANUP_SECRET and auth != f"Bearer {CLEANUP_SECRET}":
        return jsonify({"error": "Unauthorized"}), 401

    cutoff = datetime.utcnow() - timedelta(minutes=CHAMPION_HOLD_MINUTES)
    expired = db.query("""
        SELECT id FROM holds
        WHERE cohort = 'champion'
          AND checkout_status = 'pending'
          AND created_at < %s
    """, (cutoff,))

    cleaned = 0
    for hold in expired:
        _cleanup_hold(str(hold["id"]))
        cleaned += 1

    if cleaned:
        logger.info(f"Cleanup: expired {cleaned} abandoned Champion hold(s)")

    return jsonify({"cleaned": cleaned})


@app.route("/health")
def health():
    return "ok"


# ── Background cleanup: expire abandoned Champion holds every 10 min ─────────
def _cleanup_loop():
    while True:
        time.sleep(600)
        try:
            cutoff = datetime.utcnow() - timedelta(minutes=CHAMPION_HOLD_MINUTES)
            expired = db.query("""
                SELECT id FROM holds
                WHERE cohort = 'champion'
                  AND checkout_status = 'pending'
                  AND created_at < %s
            """, (cutoff,))
            for hold in expired:
                _cleanup_hold(str(hold["id"]))
            if expired:
                logger.info(f"Background cleanup: expired {len(expired)} abandoned Champion hold(s)")
        except Exception as e:
            logger.warning(f"Background cleanup error: {e}")

_cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True)
_cleanup_thread.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5005)), debug=False)
