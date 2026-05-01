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
import re
import hmac
import hashlib
import base64
import logging
import secrets
import time
import threading
import requests as _requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Response, render_template, redirect, g

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()


def _ensure_kiosk_tables():
    """Create kiosk-specific tables and extend hold_items for sealed/slab items.
    Idempotent — runs at startup. ALTER statements use IF NOT EXISTS so reruns
    are no-ops; the raw_card_id NOT NULL drop is wrapped in try/except because
    older Postgres needs ALTER ... DROP NOT NULL even when already nullable.
    """
    try:
        # ── Device identity: one row per activated iPad ───────────────────────
        db.execute("""
            CREATE TABLE IF NOT EXISTS kiosk_devices (
                device_id     VARCHAR(64) PRIMARY KEY,
                label         VARCHAR(120),
                activated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen_at  TIMESTAMP,
                revoked_at    TIMESTAMP,
                user_agent    VARCHAR(500)
            )
        """)
        # ── One-time activation tokens (24h TTL, single-use) ──────────────────
        db.execute("""
            CREATE TABLE IF NOT EXISTS kiosk_activation_tokens (
                token              VARCHAR(64) PRIMARY KEY,
                label              VARCHAR(120),
                created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by         VARCHAR(200),
                expires_at         TIMESTAMP NOT NULL,
                used_at             TIMESTAMP,
                used_by_device_id  VARCHAR(64)
            )
        """)
        # ── hold_items extensions for sealed/slab line items ──────────────────
        # Sealed/slab items have no raw_cards row; raw_card_id must be nullable.
        # SKU is what staff scans (printed on the Shopify product); barcode
        # column is reused for raw cards. unit_price snapshots the price at
        # request time so cart totals don't drift while customer browses.
        for sql in [
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS item_kind VARCHAR(20) DEFAULT 'raw'",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS sku VARCHAR(200)",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS title VARCHAR(500)",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS image_url TEXT",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS unit_price NUMERIC(10,2)",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS returned_at TIMESTAMP",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS returned_by VARCHAR(200)",
            "ALTER TABLE hold_items ADD COLUMN IF NOT EXISTS shopify_order_id BIGINT",
        ]:
            try:
                db.execute(sql)
            except Exception as e:
                logger.debug(f"hold_items migration skipped ({e}): {sql[:60]}")
        # raw_card_id NOT NULL → NULLABLE so sealed/slab items can use the table
        try:
            db.execute("ALTER TABLE hold_items ALTER COLUMN raw_card_id DROP NOT NULL")
        except Exception as e:
            logger.debug(f"raw_card_id DROP NOT NULL skipped ({e})")
        # Index sku for scan-out lookup speed
        try:
            db.execute("CREATE INDEX IF NOT EXISTS idx_hold_items_sku ON hold_items(sku)")
        except Exception:
            pass
        # ── inventory_product_cache.sku migration ────────────────────────────
        # Owned by the inventory service via shared/cache_manager.py, but kiosk
        # reads from it for the sealed/slab catalog. If the inventory service
        # hasn't re-deployed yet (or its cache_manager version predates the
        # column), the kiosk's /api/products query 500s with "column does not
        # exist". Add it ourselves — purely additive, idempotent.
        try:
            db.execute("ALTER TABLE inventory_product_cache ADD COLUMN IF NOT EXISTS sku VARCHAR(200)")
        except Exception as e:
            logger.debug(f"inventory_product_cache.sku migration skipped ({e})")
        try:
            db.execute("ALTER TABLE inventory_product_cache ADD COLUMN IF NOT EXISTS image_url TEXT")
        except Exception as e:
            logger.debug(f"inventory_product_cache.image_url migration skipped ({e})")
        logger.info("kiosk tables ensured")
    except Exception as e:
        logger.warning(f"_ensure_kiosk_tables warning: {e}")


_ensure_kiosk_tables()


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

# Access key — legacy in-store gate (kept as fallback during cookie migration)
KIOSK_ACCESS_KEY = os.environ.get("KIOSK_ACCESS_KEY", "")

# Cookie name + lifetime for in-store device identity. Set on /activate, cleared
# server-side via revoked_at on kiosk_devices. Long expiry because Kiosk Pro on
# the iPad never clears cookies once configured.
KIOSK_DEVICE_COOKIE = "pf_kiosk_device"
KIOSK_DEVICE_COOKIE_MAX_AGE = 10 * 365 * 24 * 3600  # 10 years
ACTIVATION_TOKEN_TTL_HOURS = 24

# Idle / pull-request thresholds
INSTORE_HOLD_REQUEST_EXPIRY_MIN = 15      # REQUESTED hold_items unclaimed → expire
PULLED_UNRESOLVED_AFTER_HOURS   = 4       # PULLED but no return / no order → flag

# Tags that identify sealed and slab products in inventory_product_cache.tags (CSV)
SEALED_TAG = "sealed"
SLAB_TAG   = "slab"

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


# ═══════════════════════════════════════════════════════════════════════════════
# Mode detection — instore (device cookie) vs champion (verified email header)
# ═══════════════════════════════════════════════════════════════════════════════

def _lookup_device(device_id: str) -> dict | None:
    """Return the kiosk_devices row if the device is active (not revoked)."""
    if not device_id:
        return None
    try:
        row = db.query_one(
            "SELECT device_id, label, activated_at, revoked_at "
            "FROM kiosk_devices WHERE device_id = %s",
            (device_id,),
        )
        if not row:
            return None
        if row.get("revoked_at"):
            return None
        return row
    except Exception as e:
        logger.warning(f"device lookup failed: {e}")
        return None


def _resolve_kiosk_mode():
    """
    Determine the mode of the current request:
      - 'instore'  : trusted iPad with valid device cookie (or legacy ?key=)
      - 'champion' : remote VIP3 customer (verified via X-Champion-Email header)
      - None       : unauthenticated; only the gate UI / champion identify is allowed

    Sets g.kiosk_mode + g.kiosk_device_id. Cheap; safe to call from before_request.
    """
    # Device cookie wins — in-store iPads always present a cookie
    device_id = request.cookies.get(KIOSK_DEVICE_COOKIE, "")
    if device_id:
        dev = _lookup_device(device_id)
        if dev:
            g.kiosk_mode = "instore"
            g.kiosk_device_id = device_id
            # Lightweight last_seen update — best effort, ignore failures
            try:
                db.execute(
                    "UPDATE kiosk_devices SET last_seen_at = CURRENT_TIMESTAMP "
                    "WHERE device_id = %s",
                    (device_id,),
                )
            except Exception:
                pass
            return
    # Legacy URL key (transition fallback) — also bumps to instore
    if KIOSK_ACCESS_KEY:
        key = request.headers.get("X-Kiosk-Key", "") or request.args.get("key", "")
        if key and key == KIOSK_ACCESS_KEY:
            g.kiosk_mode = "instore"
            g.kiosk_device_id = None
            return
    # Champion path
    if request.headers.get("X-Champion-Email", ""):
        g.kiosk_mode = "champion"
        g.kiosk_device_id = None
        return
    g.kiosk_mode = None
    g.kiosk_device_id = None


# Paths that never need a mode (handled by their own auth or are public)
_PUBLIC_PATHS = {
    "/", "/health", "/activate",
    "/api/mode", "/api/champion/identify",
}
_PUBLIC_PREFIXES = ("/api/webhooks/", "/api/cleanup/", "/api/admin/", "/admin/", "/staff/")


@app.before_request
def gate_api():
    """Resolve kiosk mode for every request, then gate /api/* endpoints
    that require a recognised cohort. Webhooks/cleanup/admin/staff have
    their own auth (HMAC, bearer, JWT) and bypass this gate."""
    _resolve_kiosk_mode()
    path = request.path
    if path in _PUBLIC_PATHS:
        return
    for prefix in _PUBLIC_PREFIXES:
        if path.startswith(prefix):
            return
    # Read-only browse + cart endpoints require either instore or champion
    if path.startswith("/api/"):
        if not g.kiosk_mode:
            return jsonify({"error": "Access required", "code": "no_mode"}), 403


# ═══════════════════════════════════════════════════════════════════════════════
# Activation — turn a one-time token into a long-lived device cookie
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/activate")
def activate_device():
    """
    Hit once per iPad during setup:
        https://kiosk.pack-fresh.com/activate?token=<one-time>

    Validates the token, mints a new device_id, sets the long-lived
    pf_kiosk_device cookie (HttpOnly, Secure, SameSite=Strict, 10y), and
    redirects to /. The token is single-use.
    """
    token = (request.args.get("token") or "").strip()
    if not token:
        return Response("Missing activation token", status=400)
    row = db.query_one(
        "SELECT token, expires_at, used_at, label "
        "FROM kiosk_activation_tokens WHERE token = %s",
        (token,),
    )
    if not row:
        return Response("Unknown activation token", status=404)
    if row.get("used_at"):
        return Response("Activation token already used", status=409)
    if row["expires_at"] and row["expires_at"] < datetime.utcnow():
        return Response("Activation token expired", status=410)

    device_id = secrets.token_urlsafe(32)
    label = row.get("label") or "Kiosk iPad"
    user_agent = request.headers.get("User-Agent", "")[:500]

    db.execute(
        "INSERT INTO kiosk_devices (device_id, label, user_agent) VALUES (%s, %s, %s)",
        (device_id, label, user_agent),
    )
    db.execute(
        "UPDATE kiosk_activation_tokens SET used_at = CURRENT_TIMESTAMP, "
        "used_by_device_id = %s WHERE token = %s",
        (device_id, token),
    )
    logger.info(f"Activated kiosk device label={label!r} device_id={device_id[:8]}…")

    resp = redirect("/")
    resp.set_cookie(
        KIOSK_DEVICE_COOKIE,
        device_id,
        max_age=KIOSK_DEVICE_COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="Strict",
        path="/",
    )
    return resp


@app.route("/api/mode")
def api_mode():
    """Tell the frontend whether this device is in-store, a champion, or
    needs to identify. Cheap, called once during page boot."""
    return jsonify({
        "mode":          g.kiosk_mode,             # 'instore' | 'champion' | None
        "device_label":  None,                     # filled below if instore
        "show_kinds":    _allowed_kinds(g.kiosk_mode),
    })


def _allowed_kinds(mode: str | None) -> list[str]:
    """Which catalogs the current cohort may browse. Champions never see
    sealed/slabs — they have pack-fresh.com for that. Anonymous sees nothing."""
    if mode == "instore":
        return ["raw", "sealed", "slab"]
    if mode == "champion":
        return ["raw"]
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# Admin: mint activation token, list / revoke devices (JWT-gated)
# ═══════════════════════════════════════════════════════════════════════════════

def _require_staff(roles=("owner", "manager")):
    """Use shared JWT auth for staff endpoints. Returns a Flask response on
    failure (caller should `return` it), or None when the user is allowed."""
    try:
        from auth import require_auth  # shared/auth.py on PYTHONPATH
    except Exception as e:
        logger.error(f"shared/auth not available: {e}")
        return jsonify({"error": "auth backend missing"}), 500
    return require_auth(roles=list(roles))


@app.route("/api/admin/mint-activation", methods=["POST"])
def admin_mint_activation():
    """Generate a one-time activation token. Staff types the resulting URL into
    the iPad's Kiosk Pro home URL during setup; the device hits /activate, gets
    a long-lived cookie, and the token is consumed. Manager+ only."""
    err = _require_staff()
    if err is not None:
        return err
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "Kiosk iPad").strip()[:120]
    token = secrets.token_urlsafe(24)
    expires_at = datetime.utcnow() + timedelta(hours=ACTIVATION_TOKEN_TTL_HOURS)
    user = getattr(g, "user", {}) or {}
    db.execute(
        "INSERT INTO kiosk_activation_tokens (token, label, created_by, expires_at) "
        "VALUES (%s, %s, %s, %s)",
        (token, label, user.get("email") or user.get("name") or "unknown", expires_at),
    )
    base = request.host_url.rstrip("/")
    return jsonify({
        "token":          token,
        "label":          label,
        "expires_at":     expires_at.isoformat() + "Z",
        "activation_url": f"{base}/activate?token={token}",
    })


@app.route("/api/admin/devices")
def admin_list_devices():
    """List activated kiosk devices for the admin UI."""
    err = _require_staff()
    if err is not None:
        return err
    rows = db.query(
        "SELECT device_id, label, activated_at, last_seen_at, revoked_at, user_agent "
        "FROM kiosk_devices ORDER BY activated_at DESC"
    )
    return jsonify({"devices": [_ser(dict(r)) for r in rows]})


@app.route("/admin/devices")
def admin_devices_page():
    """Manager-facing UI: list of activated kiosks + button to mint a new
    activation URL. Manager+ only."""
    err = _require_staff()
    if err is not None:
        return err
    return render_template("admin_devices.html")


@app.route("/api/admin/devices/<device_id>/revoke", methods=["POST"])
def admin_revoke_device(device_id):
    """Mark a device as revoked. Future requests with that cookie fall through
    to anonymous (locked) mode. Useful for lost iPads."""
    err = _require_staff()
    if err is not None:
        return err
    db.execute(
        "UPDATE kiosk_devices SET revoked_at = CURRENT_TIMESTAMP "
        "WHERE device_id = %s AND revoked_at IS NULL",
        (device_id,),
    )
    return jsonify({"revoked": True})


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
        # MTG quirk: a colorless card's color_identity is an EMPTY jsonb array
        # ([]), NOT ["C"]. So `field ?| ['C']` never matches it. Treat "C" as
        # a sentinel for "empty color_identity" and split it out from the
        # other-color matching below.
        is_mtg_colors = (game == "magic")
        has_c = is_mtg_colors and ("C" in colors)
        chromatic = [c for c in colors if c != "C"] if is_mtg_colors else colors
        if color_mode == "exactly":
            # Exactly C alone = colorless card. C plus other colors is a
            # contradiction (a card is either colorless or it has colors), so
            # we drop the C in that case and match on the chromatic set.
            if has_c and not chromatic:
                parts.append(f"jsonb_array_length({field}) = 0")
            elif chromatic:
                arr_lit_ph = ",".join(["%s"] * len(chromatic))
                parts.append(
                    f"(SELECT COALESCE(array_agg(DISTINCT x ORDER BY x), ARRAY[]::text[]) "
                    f" FROM jsonb_array_elements_text({field}) AS x) "
                    f"= (SELECT COALESCE(array_agg(DISTINCT v ORDER BY v), ARRAY[]::text[]) "
                    f" FROM unnest(ARRAY[{arr_lit_ph}]) AS v)"
                )
                p.extend(chromatic)
        else:
            # Any: card's identity contains at least one of the selected
            # chromatic colors, OR (if C was picked) is colorless.
            any_parts: list[str] = []
            if chromatic:
                any_parts.append(f"{field} ?| %s::text[]")
                p.append(chromatic)
            if has_c:
                any_parts.append(f"jsonb_array_length({field}) = 0")
            if any_parts:
                parts.append("(" + " OR ".join(any_parts) + ")")

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

    Query params: q, set, page (25 per page = 5 cols × 5 rows), plus per-game
    advanced filters:
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
    # per_page lets the client size pages to its column count so the last
    # row never ends up half-empty (iPad=5 cols → 25; desktop=6 → 30; etc).
    # Clamp to keep payloads bounded.
    per_page   = max(1, min(60, int(request.args.get("per_page", 25))))
    offset     = (page - 1) * per_page

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
        LIMIT %s OFFSET %s
    """, tuple(params) + (per_page, offset))

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
        "cards":     cards,
        "total":     total,
        "page":      page,
        "per_page":  per_page,
        "pages":     max(1, (total + per_page - 1) // per_page),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Sealed / Slabs catalog — reads from inventory_product_cache
# ═══════════════════════════════════════════════════════════════════════════════

def _kind_tag(kind: str) -> str | None:
    if kind == "sealed":
        return SEALED_TAG
    if kind == "slab":
        return SLAB_TAG
    return None


# Tag-based filter buckets for sealed/slab. Tags are normalized (lowercased,
# comma-separator collapsed) before LIKE matching, so the values here are the
# exact lowercase tag strings as they appear in inventory_product_cache.tags.
GAME_TAG_BUCKETS = {
    # MTG products carry one of two redundant tags depending on when they were
    # ingested — match either.
    "pokemon":   ["pokemon"],
    "mtg":       ["mtg", "magic: the gathering (mtg)"],
    "lorcana":   ["lorcana"],
    "one_piece": ["one piece"],
}

# Sealed-only product format buckets
FORMAT_TAG_BUCKETS = {
    "booster_box":    ["booster box"],
    "booster_pack":   ["booster pack"],
    "etb":            ["etb", "pcetb"],
    "collection_box": ["collection box"],
    "blister":        ["blister"],
    "tin":            ["tin"],
    "sleeved":        ["sleeved"],
}

# Pokemon-era buckets — match the tag strings as ingested
ERA_TAG_BUCKETS = {
    "mega":           ["mega era"],
    "scarlet_violet": ["scarlet violet era"],
    "sword_shield":   ["sword shield era"],
    "sun_moon":       ["sun moon era"],
    "xy":             ["x&y era"],
    "vintage":        ["vintage"],
}

# Slab grade buckets (PSA — agencies are uniformly tagged grade-N)
GRADE_TAG_BUCKETS = {
    "10": ["grade-10"],
    "9":  ["grade-9"],
    "8":  ["grade-8"],
}

# Title fallback for slabs that pre-date the grade-N tag automation.
# Postgres POSIX (case-insensitive ~*); the trailing alternation rules out
# "PSA 100", "PSA 10.5", "PSA 9.5", "PSA 95" from matching whole grades.
GRADE_TITLE_REGEX_PG = {
    "10": r"\mpsa\s*10($|[^0-9.])",
    "9":  r"\mpsa\s*9($|[^0-9.])",
    "8":  r"\mpsa\s*8($|[^0-9.])",
}
GRADE_TITLE_REGEX_PY = {
    k: re.compile(r"\bpsa\s*" + k + r"(?:$|[^0-9.])", re.I)
    for k in GRADE_TAG_BUCKETS
}


def _add_grade_clause(where: list, params: list, grades: list[str]) -> None:
    """Match a slab by either the grade-N tag (set on new listings) or a
    PSA-grade pattern in the title (covers older listings that pre-date
    auto-tagging). OR-joined per requested grade."""
    ors = []
    for g_key in grades:
        for tag in GRADE_TAG_BUCKETS.get(g_key, []):
            frag, pat = _csv_tag_clause(tag)
            ors.append(frag)
            params.append(pat)
        title_re = GRADE_TITLE_REGEX_PG.get(g_key)
        if title_re:
            ors.append("title ~* %s")
            params.append(title_re)
    if ors:
        where.append("(" + " OR ".join(ors) + ")")


def _csv_tag_clause(tag: str) -> tuple[str, str]:
    """Return (where-fragment, like-pattern) matching one CSV tag."""
    return ("LOWER(',' || REPLACE(COALESCE(tags,''), ', ', ',') || ',') LIKE %s",
            f"%,{tag.lower()},%")


def _add_bucket_clause(where: list, params: list, bucket_keys: list[str],
                       bucket_table: dict[str, list[str]]) -> None:
    """OR-join all the tags in the requested buckets and AND-attach to WHERE."""
    tags = []
    for k in bucket_keys:
        tags.extend(bucket_table.get(k, []))
    if not tags:
        return
    ors = []
    for t in tags:
        frag, pat = _csv_tag_clause(t)
        ors.append(frag)
        params.append(pat)
    where.append("(" + " OR ".join(ors) + ")")


@app.route("/api/products")
def list_products():
    """
    Sealed + slab catalog. Reads inventory_product_cache (the live Shopify
    mirror refreshed by shared/cache_manager.py — no extra Shopify API calls).

    Query params:
      kind=sealed|slab     (required)
      q=search             (matches title)
      page=1               (25 per page = 5 cols × 5 rows)
      sort=name_asc|price_asc|price_desc
      game=pokemon|mtg|lorcana|one_piece
      format=booster_box,booster_pack,...     (sealed only, comma-list)
      era=scarlet_violet,sword_shield,...     (comma-list)
      grade=10,9,8                            (slab only, comma-list)
      min_price, max_price                    (numeric)

    Returns rows with available_qty = shopify_qty − active hold_items.
    """
    kind = (request.args.get("kind") or "").strip().lower()
    if kind not in ("sealed", "slab"):
        return jsonify({"error": "kind must be sealed or slab"}), 400
    if kind not in _allowed_kinds(g.kiosk_mode):
        return jsonify({"error": "Not available in this mode", "code": "kind_forbidden"}), 403

    tag = _kind_tag(kind)
    q    = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "name_asc").strip()
    page = max(1, int(request.args.get("page", 1)))
    per_page = max(1, min(60, int(request.args.get("per_page", 25))))
    offset = (page - 1) * per_page

    where = ["status = 'ACTIVE'", "shopify_qty > 0"]
    params: list = []
    # Always require the kind tag (sealed or slab)
    _add_bucket_clause(where, params, [kind], {kind: [tag]})
    if q:
        where.append("title ILIKE %s")
        params.append(f"%{q}%")

    # Game (single-select)
    game = (request.args.get("game") or "").strip().lower()
    if game in GAME_TAG_BUCKETS:
        _add_bucket_clause(where, params, [game], GAME_TAG_BUCKETS)

    # Format (sealed only, multi-select CSV)
    if kind == "sealed":
        fmt_csv = (request.args.get("format") or "").strip().lower()
        formats = [f for f in fmt_csv.split(",") if f in FORMAT_TAG_BUCKETS]
        if formats:
            _add_bucket_clause(where, params, formats, FORMAT_TAG_BUCKETS)

    # Era (multi-select CSV)
    era_csv = (request.args.get("era") or "").strip().lower()
    eras = [e for e in era_csv.split(",") if e in ERA_TAG_BUCKETS]
    if eras:
        _add_bucket_clause(where, params, eras, ERA_TAG_BUCKETS)

    # Grade (slab only, multi-select CSV)
    if kind == "slab":
        grade_csv = (request.args.get("grade") or "").strip().lower()
        grades = [g for g in grade_csv.split(",") if g in GRADE_TAG_BUCKETS]
        if grades:
            _add_grade_clause(where, params, grades)

    # Price range (NUMERIC compare on shopify_price)
    def _opt_price(name: str):
        raw = (request.args.get(name) or "").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    min_p = _opt_price("min_price")
    max_p = _opt_price("max_price")
    if min_p is not None:
        where.append("COALESCE(shopify_price, 0) >= %s")
        params.append(min_p)
    if max_p is not None:
        where.append("COALESCE(shopify_price, 0) <= %s")
        params.append(max_p)

    sort_map = {
        "name_asc":   "title ASC",
        "price_asc":  "shopify_price ASC NULLS LAST",
        "price_desc": "shopify_price DESC NULLS LAST",
    }
    order_by = sort_map.get(sort, "title ASC")

    where_sql = " AND ".join(where)

    count_row = db.query_one(
        f"SELECT COUNT(*) AS total FROM inventory_product_cache WHERE {where_sql}",
        tuple(params),
    )
    total = count_row["total"] if count_row else 0

    # available_qty subtracts in-flight requests (REQUESTED + PULLED on
    # active holds) so the catalog reflects what's actually grabbable.
    rows = db.query(f"""
        SELECT
            ipc.shopify_product_id,
            ipc.shopify_variant_id,
            ipc.title,
            ipc.handle,
            ipc.tags,
            ipc.sku,
            ipc.image_url,
            ipc.shopify_price,
            ipc.shopify_qty,
            COALESCE((
                SELECT COUNT(*)::int FROM hold_items hi
                JOIN holds h ON hi.hold_id = h.id
                WHERE hi.shopify_variant_id = ipc.shopify_variant_id
                  AND hi.item_kind = %s
                  AND hi.status IN ('REQUESTED','PULLED')
                  AND COALESCE(h.status, '') NOT IN ('ABANDONED','COMPLETED')
            ), 0) AS in_flight
        FROM inventory_product_cache ipc
        WHERE {where_sql}
        ORDER BY {order_by}
        LIMIT %s OFFSET %s
    """, (kind, *params, per_page, offset))

    products = []
    for r in rows:
        qty = int(r["shopify_qty"] or 0)
        in_flight = int(r["in_flight"] or 0)
        avail = max(0, qty - in_flight)
        if avail <= 0:
            continue  # shouldn't happen often; skip if fully reserved
        products.append({
            "shopify_product_id": r["shopify_product_id"],
            "shopify_variant_id": r["shopify_variant_id"],
            "title":              r["title"],
            "handle":              r["handle"],
            "sku":                r.get("sku") or "",
            "image_url":          r.get("image_url") or None,
            "price":              float(r["shopify_price"]) if r["shopify_price"] is not None else None,
            "available_qty":      avail,
            "storefront_url":     f"{SHOPIFY_STOREFRONT_URL}/products/{r['handle']}" if r["handle"] else None,
        })

    return jsonify({
        "kind":     kind,
        "products": products,
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, (total + per_page - 1) // per_page),
    })


@app.route("/api/products/filter-meta")
def products_filter_meta():
    """
    Returns the filter buckets available for the sealed/slab catalog with
    **faceted** counts — each facet's counts apply ALL OTHER selected
    filters but NOT the facet's own selection, the same way the raw filter
    sheet works. So selecting Pokémon hides ETB / counts non-Pokémon
    formats, but the game chips themselves keep their full counts so you
    can switch.

    Query params:
      kind=sealed|slab        (required)
      game=…                  (current selection — single)
      format=a,b,…            (current selection — sealed only, multi)
      era=a,b,…               (current selection — multi)
      grade=10,9,…            (current selection — slab only, multi)
      min_price, max_price    (current selection — numeric)

    Response: same shape as before, but each bucket's `count` reflects
    "rows that would match if you toggled this bucket on (and kept the
    other facets' selections)". Empty buckets are dropped.
    """
    kind = (request.args.get("kind") or "").strip().lower()
    if kind not in ("sealed", "slab"):
        return jsonify({"error": "kind must be sealed or slab"}), 400
    if kind not in _allowed_kinds(g.kiosk_mode):
        return jsonify({"error": "Not available in this mode", "code": "kind_forbidden"}), 403

    kind_tag = _kind_tag(kind)
    kind_frag, kind_pat = _csv_tag_clause(kind_tag)

    # Read current selections
    sel_game    = (request.args.get("game") or "").strip().lower()
    if sel_game not in GAME_TAG_BUCKETS:
        sel_game = ""
    sel_formats = [f for f in (request.args.get("format") or "").lower().split(",")
                   if f in FORMAT_TAG_BUCKETS] if kind == "sealed" else []
    sel_eras    = [e for e in (request.args.get("era") or "").lower().split(",")
                   if e in ERA_TAG_BUCKETS]
    sel_grades  = [gr for gr in (request.args.get("grade") or "").lower().split(",")
                   if gr in GRADE_TAG_BUCKETS] if kind == "slab" else []

    def _opt_price(name: str):
        raw = (request.args.get(name) or "").strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    sel_min = _opt_price("min_price")
    sel_max = _opt_price("max_price")

    def _base_where(exclude: str) -> tuple[list[str], list]:
        """WHERE/params for "all current selections EXCEPT the named facet"."""
        where = ["status='ACTIVE'", "shopify_qty > 0", kind_frag]
        params: list = [kind_pat]
        if exclude != "game" and sel_game:
            _add_bucket_clause(where, params, [sel_game], GAME_TAG_BUCKETS)
        if exclude != "format" and sel_formats:
            _add_bucket_clause(where, params, sel_formats, FORMAT_TAG_BUCKETS)
        if exclude != "era" and sel_eras:
            _add_bucket_clause(where, params, sel_eras, ERA_TAG_BUCKETS)
        if exclude != "grade" and sel_grades:
            _add_grade_clause(where, params, sel_grades)
        if sel_min is not None:
            where.append("COALESCE(shopify_price, 0) >= %s")
            params.append(sel_min)
        if sel_max is not None:
            where.append("COALESCE(shopify_price, 0) <= %s")
            params.append(sel_max)
        return where, params

    def _facet_counts(facet_name: str, table: dict[str, list[str]]) -> dict[str, int]:
        """Pull rows that match all OTHER facets, then count per bucket
        in Python. The candidate set is small (<1k) so this is cheap and
        avoids one round-trip per bucket key. Title is also pulled so the
        grade facet can fall back to the PSA-N regex for older slabs."""
        where, params = _base_where(facet_name)
        rows = db.query(
            f"SELECT tags, title FROM inventory_product_cache WHERE {' AND '.join(where)}",
            tuple(params),
        )
        row_sets: list[tuple[str, str]] = []
        for r in rows:
            csv = ("," + (r.get("tags") or "").replace(", ", ",") + ",").lower()
            title = r.get("title") or ""
            row_sets.append((csv, title))
        counts: dict[str, int] = {}
        for key, tags in table.items():
            patterns = [f",{t.lower()}," for t in tags]
            title_re = GRADE_TITLE_REGEX_PY.get(key) if facet_name == "grade" else None
            if title_re is not None:
                n = sum(1 for csv, title in row_sets
                        if any(p in csv for p in patterns) or title_re.search(title))
            else:
                n = sum(1 for csv, _t in row_sets if any(p in csv for p in patterns))
            counts[key] = n
        return counts

    def _bucket_list(facet_name: str, table: dict[str, list[str]],
                     labels: dict[str, str]) -> list[dict]:
        counts = _facet_counts(facet_name, table)
        out = []
        for key in table.keys():
            n = counts.get(key, 0)
            if n > 0:
                out.append({"key": key, "label": labels.get(key, key), "count": n})
        return out

    GAME_LABELS  = {"pokemon": "Pokémon", "mtg": "Magic: the Gathering",
                    "lorcana": "Lorcana", "one_piece": "One Piece"}
    FMT_LABELS   = {"booster_box": "Booster Box", "booster_pack": "Booster Pack",
                    "etb": "ETB", "collection_box": "Collection Box",
                    "blister": "Blister", "tin": "Tin", "sleeved": "Sleeved"}
    ERA_LABELS   = {"mega": "Mega Evolution", "scarlet_violet": "Scarlet & Violet",
                    "sword_shield": "Sword & Shield", "sun_moon": "Sun & Moon",
                    "xy": "XY", "vintage": "Vintage"}
    GRADE_LABELS = {"10": "PSA 10", "9": "PSA 9", "8": "PSA 8"}

    out = {
        "kind":  kind,
        "games": _bucket_list("game", GAME_TAG_BUCKETS, GAME_LABELS),
        "eras":  _bucket_list("era",  ERA_TAG_BUCKETS,  ERA_LABELS),
    }
    if kind == "sealed":
        out["formats"] = _bucket_list("format", FORMAT_TAG_BUCKETS, FMT_LABELS)
    if kind == "slab":
        out["grades"]  = _bucket_list("grade",  GRADE_TAG_BUCKETS, GRADE_LABELS)
    return jsonify(out)


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
    """Per-game advanced-filter schema + faceted qty counts that reflect
    the user's CURRENT filter selection.

    Query:
      ?game=magic|pokemon|onepiece (required)
      &colors=W,U                  (current color selection — affects card_type and rarity counts)
      &color_mode=any|exactly      (only meaningful with colors)
      &card_type=Creature,Sorcery  (current card_type selection — affects color and rarity counts)
      &card_rarity=Rare,Mythic     (current rarity selection — affects color and card_type counts)

    Each facet's counts are computed by applying ALL the OTHER selected
    filters and excluding the facet itself. So toggling within one group
    doesn't collapse that group's own options to 0 — but selecting Black
    correctly narrows "Creature: 26" down to "Creature: 4" in the
    card_type group, because card_type's count includes the Black filter.

    Response:
    {
      "game": "magic",
      "color_modes": ["any", "exactly"],
      "filters": {
        "colors":    {"label": "Color", "options": [{"value":"W","label":"White","qty": 12}, ...]},
        "card_type": {"label": "Card type", "options": [{"value":"Creature","qty":4}, ...]},
        "rarity":    {"label": "Rarity", "options": [{"value":"Rare","qty":38}, ...]}
      }
    }
    """
    game = (request.args.get("game") or "").strip().lower()
    if not game or game not in GAME_FILTER_SCHEMA:
        return jsonify({"game": game, "filters": {}, "color_modes": []})

    schema = GAME_FILTER_SCHEMA[game]
    sx_game = _GAME_TO_SCRYDEX_GAME.get(game, game)

    # Read the current filter state.
    sel_colors     = _multi_param("colors")
    sel_color_mode = (request.args.get("color_mode") or "any").strip().lower()
    if sel_color_mode not in ("any", "exactly"):
        sel_color_mode = "any"
    sel_card_types = _multi_param("card_type")
    sel_rarities   = _multi_param("card_rarity")

    # Build a set of "extra WHERE clauses + params" snippets, then for each
    # facet we'll OR-together the snippets it needs (i.e. all groups except
    # itself). The clauses are written against the same JOIN shape used by
    # _count_jsonb_facet / _count_rarity_facet below: rc + pc + m.
    def colors_predicate(colors_set, mode: str) -> tuple[str, list]:
        """Build the SQL predicate for an ARBITRARY color selection — used
        both for the live filter (current selection) and for the per-chip
        what-if counts shown on the colors facet."""
        if not colors_set or "colors" not in schema:
            return "", []
        spec = schema["colors"]
        field = "m." + spec["field"]
        is_mtg = (game == "magic")
        has_c  = is_mtg and ("C" in colors_set)
        chrom  = [c for c in colors_set if c != "C"] if is_mtg else list(colors_set)
        if mode == "exactly":
            if has_c and not chrom:
                return f"jsonb_array_length({field}) = 0", []
            if not chrom:
                return "", []
            arr_lit_ph = ",".join(["%s"] * len(chrom))
            return (
                f"(SELECT COALESCE(array_agg(DISTINCT x ORDER BY x), ARRAY[]::text[]) "
                f" FROM jsonb_array_elements_text({field}) AS x) "
                f"= (SELECT COALESCE(array_agg(DISTINCT v ORDER BY v), ARRAY[]::text[]) "
                f" FROM unnest(ARRAY[{arr_lit_ph}]) AS v)"
            ), list(chrom)
        # any
        any_parts: list[str] = []
        params: list = []
        if chrom:
            any_parts.append(f"{field} ?| %s::text[]")
            params.append(chrom)
        if has_c:
            any_parts.append(f"jsonb_array_length({field}) = 0")
        if not any_parts:
            return "", []
        return "(" + " OR ".join(any_parts) + ")", params

    def colors_clause() -> tuple[str, list]:
        return colors_predicate(set(sel_colors), sel_color_mode)

    def card_type_clause() -> tuple[str, list]:
        if not sel_card_types or "card_type" not in schema:
            return "", []
        spec = schema["card_type"]
        field = "m." + spec["field"]
        if spec["type"] == "jsonb":
            return f"{field} ?| %s::text[]", [list(sel_card_types)]
        ph = ",".join(["%s"] * len(sel_card_types))
        return f"{field} IN ({ph})", list(sel_card_types)

    def rarity_clause() -> tuple[str, list]:
        if not sel_rarities:
            return "", []
        ph = ",".join(["%s"] * len(sel_rarities))
        return f"rc.rarity IN ({ph})", list(sel_rarities)

    color_w,  color_p  = colors_clause()
    cardt_w,  cardt_p  = card_type_clause()
    rarity_w, rarity_p = rarity_clause()

    def merged_extra(*pairs: tuple[str, list]) -> tuple[str, list]:
        """AND together a subset of (where, params) pairs."""
        parts, params = [], []
        for w, p in pairs:
            if w:
                parts.append(w)
                params.extend(p)
        if not parts:
            return "", []
        return " AND " + " AND ".join(parts), params

    # ── Colors facet (count of cards per color, given other filters) ────────
    out: dict = {
        "game": game,
        "color_modes": list(schema.get("colors", {}).get("modes", [])),
        "filters": {},
    }

    if "colors" in schema:
        spec = schema["colors"]
        # Other filters ⇒ card_type + rarity (skip colors itself).
        extra_w, extra_p = merged_extra((cardt_w, cardt_p), (rarity_w, rarity_p))
        field = "m." + spec["field"]
        labels = spec.get("labels") or {}
        counts: dict[str, int] = {}

        if sel_color_mode == "exactly":
            # Per-chip count semantics in exactly mode:
            #   - ON chip  → count of cards matching the CURRENT exact-set
            #     (matches what the result grid is showing). Tapping it would
            #     remove the chip, but until then this chip *is* the filter.
            #   - OFF chip → count if you ADD this color to the exact-set
            #     (i.e. exactly = current ∪ {v}). Lets you preview multi-color
            #     exact matches before committing.
            # The old "symmetric diff = toggle me" math made the only-selected
            # chip read its own count as the unfiltered total, because
            # toggling the only chip cleared the color predicate entirely.
            current = set(sel_colors)
            for v in spec["options"]:
                pred_set = current if v in current else (current | {v})
                pred_w, pred_p = colors_predicate(pred_set, "exactly")
                if not pred_set or not pred_w:
                    counts[v] = 0
                    continue
                where_extra = f"{extra_w} AND {pred_w}" if extra_w else f" AND {pred_w}"
                where_params = list(extra_p) + list(pred_p)
                row = db.query_one(f"""
                    SELECT COUNT(DISTINCT rc.tcgplayer_id) AS n
                      FROM raw_cards rc
                      JOIN scrydex_price_cache pc
                        ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
                      JOIN scrydex_card_meta m
                        ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
                     WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL
                       AND rc.game = %s
                           {where_extra}
                """, (sx_game, game, *where_params))
                counts[v] = int((row or {}).get("n") or 0)
        else:
            # any-mode: unfold color_identity into rows and group-count.
            rows = db.query(f"""
                SELECT elem AS k, COUNT(DISTINCT rc.tcgplayer_id) AS n
                  FROM raw_cards rc
                  JOIN scrydex_price_cache pc
                    ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
                  JOIN scrydex_card_meta m
                    ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
                  JOIN LATERAL jsonb_array_elements_text({field}) AS elem ON TRUE
                 WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
                       {extra_w}
                 GROUP BY elem
            """, (sx_game, game, *extra_p))
            counts = {r["k"]: int(r["n"]) for r in rows}

            # MTG: a colorless card has color_identity=[] and never shows up
            # in the lateral elements-of-array unfold above. Count it
            # separately so the "C" chip reflects reality.
            if game == "magic":
                row = db.query_one(f"""
                    SELECT COUNT(DISTINCT rc.tcgplayer_id) AS n
                      FROM raw_cards rc
                      JOIN scrydex_price_cache pc
                        ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
                      JOIN scrydex_card_meta m
                        ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
                     WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
                       AND jsonb_array_length({field}) = 0
                           {extra_w}
                """, (sx_game, game, *extra_p))
                counts["C"] = int((row or {}).get("n") or 0)

        out["filters"]["colors"] = {
            "label":   spec["label"],
            "options": [
                {"value": v, "label": labels.get(v, v), "qty": counts.get(v, 0)}
                for v in spec["options"]
            ],
        }

    # ── Card type facet ─────────────────────────────────────────────────────
    if "card_type" in schema:
        spec = schema["card_type"]
        # Other filters ⇒ colors + rarity (skip card_type itself).
        extra_w, extra_p = merged_extra((color_w, color_p), (rarity_w, rarity_p))
        field = "m." + spec["field"]
        if spec["type"] == "jsonb":
            rows = db.query(f"""
                SELECT elem AS k, COUNT(DISTINCT rc.tcgplayer_id) AS n
                  FROM raw_cards rc
                  JOIN scrydex_price_cache pc
                    ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
                  JOIN scrydex_card_meta m
                    ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
                  JOIN LATERAL jsonb_array_elements_text({field}) AS elem ON TRUE
                 WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
                       {extra_w}
                 GROUP BY elem
            """, (sx_game, game, *extra_p))
        else:
            rows = db.query(f"""
                SELECT {field} AS k, COUNT(DISTINCT rc.tcgplayer_id) AS n
                  FROM raw_cards rc
                  JOIN scrydex_price_cache pc
                    ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
                  JOIN scrydex_card_meta m
                    ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
                 WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
                   AND {field} IS NOT NULL
                       {extra_w}
                 GROUP BY {field}
            """, (sx_game, game, *extra_p))
        counts = {r["k"]: int(r["n"]) for r in rows}
        out["filters"]["card_type"] = {
            "label":   spec["label"],
            "options": [
                {"value": v, "label": v, "qty": counts.get(v, 0)}
                for v in spec["options"]
            ],
        }

    # ── Rarity facet (lives on raw_cards directly, no meta join needed for
    # the rarity column itself — but we may still need meta JOIN if colors
    # or card_type filters are active). ─────────────────────────────────────
    extra_w, extra_p = merged_extra((color_w, color_p), (cardt_w, cardt_p))
    if extra_w:
        rarity_rows = db.query(f"""
            SELECT rc.rarity AS k, COUNT(DISTINCT rc.tcgplayer_id) AS n
              FROM raw_cards rc
              JOIN scrydex_price_cache pc
                ON pc.tcgplayer_id = rc.tcgplayer_id AND pc.game = %s
              JOIN scrydex_card_meta m
                ON m.game = pc.game AND m.scrydex_id = pc.scrydex_id
             WHERE rc.state = 'STORED' AND rc.current_hold_id IS NULL AND rc.game = %s
               AND rc.rarity IS NOT NULL AND rc.rarity <> ''
                   {extra_w}
             GROUP BY rc.rarity
             ORDER BY n DESC
        """, (sx_game, game, *extra_p))
    else:
        rarity_rows = db.query("""
            SELECT rarity AS k, COUNT(*) AS n FROM raw_cards
             WHERE state='STORED' AND current_hold_id IS NULL AND game = %s
               AND rarity IS NOT NULL AND rarity <> ''
             GROUP BY rarity
             ORDER BY n DESC
        """, (game,))
    out["filters"]["rarity"] = {
        "label":   "Rarity",
        "options": [
            {"value": r["k"], "label": r["k"], "qty": int(r["n"])}
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
    Submit a hold request. Supports mixed raw / sealed / slab items in one
    cart for in-store guests; sealed/slab items go straight into hold_items
    with item_kind set, no raw_cards row.

    POST body:
    {
        "customer_name": "Mark",
        "customer_phone": "555-1234",
        "items": [
            // raw card (default kind):
            {"kind": "raw", "card_name": "Charizard ex", "set_name": "...",
             "condition": "NM", "qty": 2, "variant": "", "tcgplayer_id": 12345},
            // sealed / slab:
            {"kind": "sealed", "shopify_variant_id": 4242,
             "sku": "PF-ABC123", "title": "Surging Sparks Booster Box",
             "qty": 1, "unit_price": 159.99}
        ]
    }
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

    total_qty = sum(int(i.get("qty", 1)) for i in items)
    if total_qty > MAX_HOLD_ITEMS:
        return jsonify({"error": f"Maximum {MAX_HOLD_ITEMS} items per hold (requested {total_qty})"}), 400
    if total_qty < 1:
        return jsonify({"error": "Must request at least 1 item"}), 400

    # Sealed/slab restricted to in-store mode
    allowed = set(_allowed_kinds(g.kiosk_mode))
    for line in items:
        kind = (line.get("kind") or "raw").strip().lower()
        if kind not in ("raw", "sealed", "slab"):
            return jsonify({"error": f"Unknown item kind: {kind}"}), 400
        if kind not in allowed:
            return jsonify({"error": f"{kind} items are not available in {g.kiosk_mode} mode"}), 403

    # ── Resolve raw lines (lock raw_cards rows by current_hold_id) ───────────
    lines_resolved: list[dict] = []
    errors: list[str] = []
    assigned: list[dict] = []

    for line in items:
        kind = (line.get("kind") or "raw").strip().lower()
        qty  = max(1, int(line.get("qty", 1)))

        if kind == "raw":
            card_name = line.get("card_name", "")
            set_name  = line.get("set_name", "")
            condition = line.get("condition", "NM")
            variant   = (line.get("variant") or "").strip()
            tcgplayer_id = line.get("tcgplayer_id")
            scrydex_id   = (line.get("scrydex_id") or "").strip()

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
                    "kind": "raw",
                    "card_name": card_name, "set_name": set_name,
                    "condition": condition, "variant": variant,
                    "card_id": str(card["id"]), "barcode": card["barcode"],
                })
        else:
            # sealed / slab
            variant_id = line.get("shopify_variant_id")
            sku        = (line.get("sku") or "").strip()
            title      = (line.get("title") or "").strip()
            unit_price = line.get("unit_price")
            if not variant_id or not sku:
                errors.append(f"{kind} item missing variant_id/sku")
                continue
            # Re-verify availability — guard against stale frontend state.
            avail_row = db.query_one(f"""
                SELECT ipc.shopify_qty,
                       COALESCE((SELECT COUNT(*) FROM hold_items hi
                                 JOIN holds h ON hi.hold_id = h.id
                                 WHERE hi.shopify_variant_id = ipc.shopify_variant_id
                                   AND hi.item_kind = %s
                                   AND hi.status IN ('REQUESTED','PULLED')
                                   AND COALESCE(h.status,'') NOT IN ('ABANDONED','COMPLETED')
                                ), 0) AS in_flight,
                       ipc.title AS cache_title,
                       ipc.shopify_price AS cache_price
                FROM inventory_product_cache ipc
                WHERE ipc.shopify_variant_id = %s
            """, (kind, int(variant_id)))
            if not avail_row:
                errors.append(f"{title or sku} not found in catalog")
                continue
            avail = max(0, int(avail_row["shopify_qty"] or 0) - int(avail_row["in_flight"] or 0))
            if avail < qty:
                errors.append(f"Only {avail} {title or sku} available (requested {qty})")
                qty = avail
                if qty <= 0:
                    continue
            # Trust the cache for title + price snapshot if frontend didn't supply them
            title = title or (avail_row.get("cache_title") or sku)
            if unit_price is None and avail_row.get("cache_price") is not None:
                unit_price = float(avail_row["cache_price"])
            for _ in range(qty):
                lines_resolved.append({
                    "kind":      kind,
                    "variant_id": int(variant_id),
                    "sku":       sku,
                    "title":     title,
                    "unit_price": unit_price,
                })

    if not lines_resolved:
        return jsonify({"error": "No items available for any requested lines", "details": errors}), 409

    # ── Persist: one transaction creates the hold + all lines ───────────────
    with db.get_conn() as conn:
        conn.autocommit = False
        try:
            from psycopg2.extras import RealDictCursor
            cur = conn.cursor(cursor_factory=RealDictCursor)

            cur.execute("""
                INSERT INTO holds (customer_name, customer_phone, status, item_count)
                VALUES (%s, %s, 'PENDING', %s) RETURNING id
            """, (name, phone or None, len(lines_resolved)))
            hold_id = str(cur.fetchone()["id"])

            for r in lines_resolved:
                if r["kind"] == "raw":
                    cur.execute("""
                        UPDATE raw_cards SET current_hold_id = %s WHERE id = %s
                    """, (hold_id, r["card_id"]))
                    cur.execute("""
                        INSERT INTO hold_items (hold_id, raw_card_id, barcode, status, item_kind)
                        VALUES (%s, %s, %s, 'REQUESTED', 'raw')
                    """, (hold_id, r["card_id"], r["barcode"]))
                    assigned.append({"kind": "raw", "card_name": r["card_name"],
                                     "condition": r["condition"], "barcode": r["barcode"]})
                else:
                    cur.execute("""
                        INSERT INTO hold_items
                            (hold_id, raw_card_id, barcode, status, item_kind,
                             sku, title, shopify_variant_id, unit_price)
                        VALUES (%s, NULL, %s, 'REQUESTED', %s, %s, %s, %s, %s)
                    """, (hold_id, r["sku"], r["kind"], r["sku"], r["title"],
                          r["variant_id"], r["unit_price"]))
                    assigned.append({"kind": r["kind"], "title": r["title"], "sku": r["sku"]})

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


# ═══════════════════════════════════════════════════════════════════════════════
# Staff scan flow — pulls + returns for in-store hold fulfillment
# ═══════════════════════════════════════════════════════════════════════════════

def _find_hold_item_by_scan(scan: str) -> dict | None:
    """Resolve a scanned barcode/SKU to its hold_item. The barcode column is
    populated for both raw (raw_cards.barcode) and sealed/slab (sku), so a
    single lookup covers both."""
    scan = (scan or "").strip()
    if not scan:
        return None
    return db.query_one("""
        SELECT hi.id, hi.hold_id, hi.barcode, hi.sku, hi.title, hi.item_kind,
               hi.status, hi.raw_card_id, hi.shopify_variant_id, hi.pulled_at,
               hi.returned_at, h.customer_name, h.status AS hold_status
        FROM hold_items hi
        JOIN holds h ON hi.hold_id = h.id
        WHERE hi.barcode = %s
          AND COALESCE(h.status, '') NOT IN ('ABANDONED','COMPLETED')
        ORDER BY hi.created_at DESC
        LIMIT 1
    """, (scan,))


@app.route("/api/staff/scan-out", methods=["POST"])
def staff_scan_out():
    """Mark a REQUESTED hold_item as PULLED. Staff scans the barcode (raw)
    or printed SKU (sealed/slab) when bringing it to the counter."""
    err = _require_staff(roles=("owner", "manager", "associate"))
    if err is not None:
        return err
    data = request.get_json() or {}
    scan = (data.get("scan") or "").strip()
    item = _find_hold_item_by_scan(scan)
    if not item:
        return jsonify({"error": "No active hold matches that scan"}), 404
    if item["status"] not in ("REQUESTED",):
        return jsonify({
            "error": f"Item is already {item['status']}",
            "current_status": item["status"],
        }), 409
    user = (getattr(g, "user", {}) or {}).get("name") or "staff"
    db.execute(
        "UPDATE hold_items SET status = 'PULLED', pulled_at = CURRENT_TIMESTAMP "
        "WHERE id = %s",
        (item["id"],),
    )
    return jsonify({
        "success": True,
        "hold_id": str(item["hold_id"]),
        "item_kind": item["item_kind"],
        "title": item.get("title") or item.get("barcode"),
        "pulled_by": user,
    })


@app.route("/api/staff/scan-return", methods=["POST"])
def staff_scan_return():
    """Mark a PULLED sealed/slab item as RETURNED. Raw cards don't return
    via this path — they go through the existing accept/reject hold flow."""
    err = _require_staff(roles=("owner", "manager", "associate"))
    if err is not None:
        return err
    data = request.get_json() or {}
    scan = (data.get("scan") or "").strip()
    item = _find_hold_item_by_scan(scan)
    if not item:
        return jsonify({"error": "No active hold matches that scan"}), 404
    if item["item_kind"] not in ("sealed", "slab"):
        return jsonify({
            "error": "Scan-return is for sealed/slab items only — raw cards use the existing reject flow",
        }), 400
    if item["status"] != "PULLED":
        return jsonify({
            "error": f"Item is {item['status']}, can only return PULLED",
            "current_status": item["status"],
        }), 409
    user = (getattr(g, "user", {}) or {}).get("name") or "staff"
    db.execute(
        "UPDATE hold_items SET status = 'RETURNED', returned_at = CURRENT_TIMESTAMP, "
        "returned_by = %s WHERE id = %s",
        (user, item["id"]),
    )
    return jsonify({
        "success": True,
        "hold_id": str(item["hold_id"]),
        "title": item.get("title") or item.get("sku"),
        "returned_by": user,
    })


@app.route("/api/staff/pulls")
def staff_pulls_data():
    """Active pulls grouped by hold + the recent UNRESOLVED list. Drives
    the /staff/pulls page."""
    err = _require_staff(roles=("owner", "manager", "associate"))
    if err is not None:
        return err
    rows = db.query("""
        SELECT hi.id, hi.hold_id, hi.barcode, hi.sku, hi.title, hi.item_kind,
               hi.status, hi.created_at, hi.pulled_at, hi.returned_at,
               hi.unit_price, hi.shopify_order_id,
               h.customer_name, h.customer_phone, h.created_at AS hold_created_at,
               h.status AS hold_status
        FROM hold_items hi
        JOIN holds h ON hi.hold_id = h.id
        WHERE hi.status IN ('REQUESTED','PULLED','UNRESOLVED')
          AND COALESCE(h.status,'') NOT IN ('ABANDONED','COMPLETED')
        ORDER BY h.created_at DESC, hi.created_at ASC
    """)
    holds: dict[str, dict] = {}
    for r in rows:
        hid = str(r["hold_id"])
        if hid not in holds:
            holds[hid] = {
                "hold_id":        hid,
                "customer_name":  r["customer_name"],
                "customer_phone": r["customer_phone"],
                "created_at":     r["hold_created_at"].isoformat() if r["hold_created_at"] else None,
                "items":          [],
            }
        holds[hid]["items"].append({
            "id":          str(r["id"]),
            "kind":        r["item_kind"],
            "barcode":     r["barcode"],
            "sku":         r["sku"],
            "title":       r["title"] or r["barcode"],
            "status":      r["status"],
            "pulled_at":   r["pulled_at"].isoformat() if r["pulled_at"] else None,
            "returned_at": r["returned_at"].isoformat() if r["returned_at"] else None,
            "unit_price":  float(r["unit_price"]) if r["unit_price"] is not None else None,
        })
    return jsonify({"holds": list(holds.values())})


@app.route("/staff/pulls")
def staff_pulls_page():
    """Standalone scan UI for staff. Lists active holds and their items, with
    a global scan input that routes to scan-out (REQUESTED) or scan-return
    (PULLED sealed/slab) based on current item status."""
    err = _require_staff(roles=("owner", "manager", "associate"))
    if err is not None:
        return err
    return render_template("staff_pulls.html")


@app.route("/api/hold/<hold_id>")
def get_hold(hold_id):
    hold = db.query_one("SELECT * FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Hold not found"}), 404
    # LEFT JOIN raw_cards because sealed/slab hold_items have raw_card_id IS NULL.
    # Fall back to hold_items.title/sku for non-raw items.
    items = db.query("""
        SELECT hi.*,
               rc.card_name, rc.set_name, rc.condition,
               rc.current_price, rc.card_number, rc.image_url AS rc_image_url
        FROM hold_items hi
        LEFT JOIN raw_cards rc ON hi.raw_card_id = rc.id
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
    shopify_order_id = order.get("id")

    # ── 1. Champion raw checkout: match by variant_id (existing path) ────────
    hold_ids = set()
    for item in line_items:
        variant_id = item.get("variant_id")
        if variant_id:
            hold_item = db.query_one("""
                SELECT hi.hold_id FROM hold_items hi
                JOIN holds h ON hi.hold_id = h.id
                WHERE hi.shopify_variant_id = %s AND h.cohort = 'champion'
            """, (str(variant_id),))
            if hold_item:
                hold_ids.add(str(hold_item["hold_id"]))

    # ── 2. In-store sealed/slab POS sale: match by line-item SKU ─────────────
    # When staff rings up a PULLED sealed/slab item at POS, the Shopify order's
    # line_item.sku is the same SKU recorded on the hold_item. Mark it SOLD so
    # UNRESOLVED reconciliation doesn't flag it later. Best-effort — logs only.
    sealed_slab_skus: list[str] = []
    for item in line_items:
        sku = (item.get("sku") or "").strip()
        if sku:
            sealed_slab_skus.append(sku)
    if sealed_slab_skus:
        try:
            ph = ",".join(["%s"] * len(sealed_slab_skus))
            db.execute(f"""
                UPDATE hold_items
                SET status = 'SOLD', shopify_order_id = %s
                WHERE sku IN ({ph})
                  AND item_kind IN ('sealed','slab')
                  AND status = 'PULLED'
                  AND shopify_order_id IS NULL
            """, (shopify_order_id, *sealed_slab_skus))
        except Exception as e:
            logger.warning(f"sealed/slab POS match failed: {e}")

    # ── 3. Raw card POS sale: match by barcode (SKU on the listing) ──────────
    # Card manager creates active listings with SKU = barcode. When sold at POS,
    # flip raw_cards PENDING_SALE → SOLD, zero out inventory, and archive.
    if sealed_slab_skus:  # same SKUs, raw cards use barcode as SKU too
        try:
            ph = ",".join(["%s"] * len(sealed_slab_skus))
            sold_raw = db.query(f"""
                UPDATE raw_cards
                SET state = 'SOLD', current_hold_id = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE barcode IN ({ph})
                  AND state = 'PENDING_SALE'
                RETURNING id, barcode, shopify_product_id, shopify_variant_id
            """, tuple(sealed_slab_skus))
            for card in (sold_raw or []):
                pid = card.get("shopify_product_id")
                vid = card.get("shopify_variant_id")
                if pid:
                    try:
                        # Zero inventory so Shopify doesn't show stale "in stock"
                        if vid:
                            vdata = _shopify_rest("GET", f"/variants/{vid}.json")
                            iid = vdata.get("variant", {}).get("inventory_item_id")
                            if iid:
                                levels = _shopify_rest("GET", f"/inventory_levels.json?inventory_item_ids={iid}")
                                for lv in levels.get("inventory_levels", []):
                                    _shopify_rest("POST", "/inventory_levels/set.json",
                                                  json={"location_id": lv["location_id"],
                                                        "inventory_item_id": iid,
                                                        "available": 0})
                        _shopify_rest("PUT", f"/products/{pid}.json",
                                      json={"product": {"id": pid, "status": "archived"}})
                    except Exception:
                        logger.warning(f"Failed to clean up raw listing product_id={pid}")
                logger.info(f"Raw card POS sold: barcode={card['barcode']}")
        except Exception as e:
            logger.warning(f"raw card POS match failed: {e}")

    if not hold_ids:
        return jsonify({"ok": True, "kiosk": "pos_match_attempted"}), 200

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


# ── Background cleanup: Champion holds + in-store hold lifecycle ─────────────
def _expire_unclaimed_instore_requests():
    """In-store guest REQUESTED items unclaimed after INSTORE_HOLD_REQUEST_EXPIRY_MIN
    minutes → EXPIRED_UNCLAIMED, release any raw_cards lock so the items return
    to circulation. Sealed/slab REQUESTED rows just get marked expired (no
    inventory lock to release)."""
    cutoff = datetime.utcnow() - timedelta(minutes=INSTORE_HOLD_REQUEST_EXPIRY_MIN)
    # Release raw_card locks for expiring raw items
    try:
        db.execute("""
            UPDATE raw_cards SET current_hold_id = NULL
            WHERE current_hold_id IN (
                SELECT DISTINCT hi.hold_id FROM hold_items hi
                JOIN holds h ON hi.hold_id = h.id
                WHERE hi.status = 'REQUESTED' AND hi.created_at < %s
                  AND COALESCE(h.cohort,'') <> 'champion'
                  AND COALESCE(h.status,'') NOT IN ('ABANDONED','COMPLETED')
            )
            AND id IN (
                SELECT raw_card_id FROM hold_items hi2
                WHERE hi2.status = 'REQUESTED' AND hi2.created_at < %s
                  AND hi2.raw_card_id IS NOT NULL
            )
        """, (cutoff, cutoff))
    except Exception as e:
        logger.warning(f"Failed releasing expired raw locks: {e}")
    # Mark hold_items expired
    db.execute("""
        UPDATE hold_items SET status = 'EXPIRED_UNCLAIMED'
        WHERE status = 'REQUESTED' AND created_at < %s
          AND hold_id IN (
              SELECT id FROM holds
              WHERE COALESCE(cohort,'') <> 'champion'
          )
    """, (cutoff,))


def _auto_close_fully_resolved_holds():
    """If every hold_item on an active hold is in a terminal state
    (EXPIRED_UNCLAIMED / MISSING / REJECTED / SOLD / RETURNED / UNRESOLVED /
    CANCELLED) — i.e. nothing is REQUESTED, PULLED, or ACCEPTED any more —
    flip the hold itself to AUTO_EXPIRED so it falls out of the active queue.

    AUTO_EXPIRED is distinct from manual CANCELLED so an auditor can tell
    "the queue cleaned itself up" from "a staff member explicitly closed
    this." Both are terminal and excluded from the active hold list.

    Holds with zero hold_items (degenerate state) also auto-close — the
    NOT IN subquery returns no rows for them, so they match.
    """
    db.execute("""
        UPDATE holds
           SET status = 'AUTO_EXPIRED'
         WHERE status IN ('PENDING','PULLING','READY')
           AND id NOT IN (
               SELECT DISTINCT hold_id FROM hold_items
                WHERE status IN ('REQUESTED','PULLED','ACCEPTED')
                  AND hold_id IS NOT NULL
           )
    """)


def _flag_unresolved_pulls():
    """PULLED sealed/slab items with no return AND no matching order after
    PULLED_UNRESOLVED_AFTER_HOURS → UNRESOLVED. This is the loss-detection
    signal — surfaced on the staff page in red."""
    cutoff = datetime.utcnow() - timedelta(hours=PULLED_UNRESOLVED_AFTER_HOURS)
    db.execute("""
        UPDATE hold_items SET status = 'UNRESOLVED'
        WHERE status = 'PULLED'
          AND item_kind IN ('sealed','slab')
          AND pulled_at < %s
          AND returned_at IS NULL
          AND shopify_order_id IS NULL
    """, (cutoff,))


def _cleanup_loop():
    while True:
        time.sleep(600)
        try:
            # 1. Champion checkout abandonment (existing)
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

            # 2. In-store unclaimed requests
            _expire_unclaimed_instore_requests()

            # 3. Unresolved pulls (loss signal)
            _flag_unresolved_pulls()

            # 4. Auto-close holds whose items are now ALL terminal — has to
            #    run after (2) and (3) so the just-flipped EXPIRED/UNRESOLVED
            #    items count as terminal in the same loop iteration.
            _auto_close_fully_resolved_holds()
        except Exception as e:
            logger.warning(f"Background cleanup error: {e}")

_cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True)
_cleanup_thread.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5005)), debug=False)
